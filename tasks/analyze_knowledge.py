import os
import time
from datetime import datetime
from celery.utils.log import get_task_logger

from tasks.celery_app import app

# R2 / S3 client
import boto3

# OpenAI SDK (optional)
try:
    from openai import OpenAI
    _openai_import_error = None
except Exception as _openai_e:
    OpenAI = None
    _openai_import_error = repr(_openai_e)

from .utils.knowledge_utils import (
    build_knowledge_prompt,
    parse_model_json_output,
    build_analysis_artifact_key,
)


logger = get_task_logger(__name__)


def _get_r2_client():
    R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
    R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
    R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]):
        return None, None
    client = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    )
    return client, R2_BUCKET_NAME


@app.task(bind=True)
def analyze_knowledge_file(self, *, tenant_id: str, location_id: str, knowledge_type: str,
                           key: str, unique_filename: str, content_type: str,
                           public_url: str | None = None) -> dict:
    """Celery task to analyze a knowledge file stored in R2 using OpenAI, then save JSON analysis back to R2.

    Returns a dict with artifact locations and analysis status. Poll via GET /api/task/<task_id>.
    """
    start_ts = time.time()
    started_at = datetime.utcnow().isoformat() + 'Z'

    client, bucket = _get_r2_client()
    if client is None or bucket is None:
        msg = "Cloudflare R2 not configured"
        logger.error(msg)
        return {
            'success': False,
            'error': msg,
            'file': {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'key': key,
                'filename': unique_filename,
                'url': public_url,
                'content_type': content_type,
            },
            'analysis': {'status': 'skipped', 'reason': 'storage_not_configured'},
            'artifacts': None,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    # 1) Download the original file from R2
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        file_content = obj['Body'].read()
        size = len(file_content)
    except Exception as e:
        logger.exception("Failed to download file from R2")
        return {
            'success': False,
            'error': f'Failed to download file from R2: {e}',
            'file': {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'key': key,
                'filename': unique_filename,
                'url': public_url,
                'size': None,
                'content_type': content_type,
            },
            'analysis': {'status': 'error', 'reason': 'download_failed'},
            'artifacts': None,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    # 2) Run OpenAI analysis
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key or OpenAI is None:
        reason = f"OpenAI not configured. client_available={OpenAI is not None}; key_present={bool(api_key)}"
        logger.warning("[Analysis] %s", reason)
        return {
            'success': False,
            'error': 'OpenAI not configured',
            'file': {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'key': key,
                'filename': unique_filename,
                'url': public_url,
                'size': size,
                'content_type': content_type,
            },
            'analysis': {'status': 'skipped', 'reason': 'openai_not_configured'},
            'artifacts': None,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    try:
        oa_client = OpenAI(api_key=api_key)
        uploaded = oa_client.files.create(file=(unique_filename, file_content), purpose='assistants')

        prompt = build_knowledge_prompt(knowledge_type)
        resp = oa_client.responses.create(
            model=os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini'),
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_file", "file_id": uploaded.id}
                    ]
                }
            ],
            temperature=0.2
        )

        # Extract text output from new Responses API
        analysis_text = getattr(resp, 'output_text', None)
        if analysis_text is None and getattr(resp, 'choices', None):
            # fallback for compatibility
            try:
                analysis_text = resp.choices[0].message.content
            except Exception:
                analysis_text = None

        parsed, raw = parse_model_json_output(analysis_text)
        status = 'success' if parsed is not None else 'raw'
        model_used = os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini')

        # 3) Save analysis artifact to R2
        analysis_key = build_analysis_artifact_key(tenant_id, location_id, unique_filename)
        import json as _json
        payload = parsed if parsed is not None else {"raw": raw}
        client.put_object(
            Bucket=bucket,
            Key=analysis_key,
            Body=_json.dumps(payload).encode('utf-8'),
            ContentType='application/json',
            Metadata={
                'tenant_id': str(tenant_id),
                'location_id': str(location_id),
                'knowledge_type': knowledge_type,
                'source': 'openai',
            }
        )

        public_base = os.getenv('R2_PUBLIC_BASE_URL', 'https://assets.speako.ai')
        analysis_url = f"{public_base}/{analysis_key}"

        return {
            'success': True,
            'file': {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'key': key,
                'filename': unique_filename,
                'url': public_url,
                'size': size,
                'content_type': content_type,
            },
            'analysis': {
                'status': status,
                'model': model_used,
                'file_id': uploaded.id,
            },
            'artifacts': {
                'analysis_key': analysis_key,
                'analysis_url': analysis_url,
            },
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    except Exception as e:
        logger.exception("OpenAI analysis failed")
        # Still attempt to write an error artifact to R2 for debugging
        try:
            analysis_key = build_analysis_artifact_key(tenant_id, location_id, unique_filename)
            import json as _json
            client.put_object(
                Bucket=bucket,
                Key=analysis_key,
                Body=_json.dumps({"error": str(e)}).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'tenant_id': str(tenant_id),
                    'location_id': str(location_id),
                    'knowledge_type': knowledge_type,
                    'source': 'openai',
                }
            )
            public_base = os.getenv('R2_PUBLIC_BASE_URL', 'https://assets.speako.ai')
            analysis_url = f"{public_base}/{analysis_key}"
        except Exception:
            analysis_key = None
            analysis_url = None

        return {
            'success': False,
            'error': str(e),
            'file': {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'key': key,
                'filename': unique_filename,
                'url': public_url,
                'size': size,
                'content_type': content_type,
            },
            'analysis': {
                'status': 'error',
                'message': str(e)
            },
            'artifacts': {
                'analysis_key': analysis_key,
                'analysis_url': analysis_url,
            } if analysis_key else None,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
