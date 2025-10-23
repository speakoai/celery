import os
import time
from datetime import datetime
from celery.utils.log import get_task_logger

from tasks.celery_app import app

# R2 / S3 client
import boto3
# New: HTTP download support when file_url is provided
import requests

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
    preprocess_for_model,
)


logger = get_task_logger(__name__)

# Load .env if present (useful for local dev workers)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def _get_r2_client():
    R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
    R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
    R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
    missing = []
    if not R2_ACCESS_KEY_ID:
        missing.append("R2_ACCESS_KEY_ID")
    if not R2_SECRET_ACCESS_KEY:
        missing.append("R2_SECRET_ACCESS_KEY")
    if not R2_ENDPOINT_URL:
        missing.append("R2_ENDPOINT_URL")
    if not R2_BUCKET_NAME:
        missing.append("R2_BUCKET_NAME")

    if missing:
        logger.error("[R2] Missing environment variables: %s", ", ".join(missing))
        return None, None, missing
    client = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    )
    return client, R2_BUCKET_NAME, []


@app.task(bind=True)
def analyze_knowledge_file(self, *, tenant_id: str, location_id: str, knowledge_type: str,
                           key: str, unique_filename: str, content_type: str,
                           public_url: str | None = None,
                           file_url: str | None = None,
                           speako_task_id: str | None = None) -> dict:
    """Celery task to analyze a knowledge file using OpenAI, then save JSON analysis back to R2.

    Source file selection:
    - If file_url is provided, the file will be downloaded via HTTP(S) from that URL.
    - Otherwise, the file will be downloaded from R2 using the provided key.

    Returns a dict with artifact locations and analysis status. Poll via GET /api/task/<task_id>.
    """
    start_ts = time.time()
    started_at = datetime.utcnow().isoformat() + 'Z'

    # Prefer given file_url for the file source when available
    chosen_url = file_url or public_url

    client, bucket, missing_env = _get_r2_client()
    if client is None or bucket is None:
        msg = "Cloudflare R2 not configured"
        logger.error(msg)
        return {
            'success': False,
            'error': msg,
            'missing_env': missing_env if missing_env else None,
            'file': {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'key': key,
                'filename': unique_filename,
                'url': chosen_url,
                'content_type': content_type,
            },
            'analysis': {'status': 'skipped', 'reason': 'storage_not_configured'},
            'artifacts': None,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    # 1) Get the original file: from URL if provided, else from R2
    try:
        if file_url:
            resp = requests.get(file_url, timeout=30)
            resp.raise_for_status()
            file_content = resp.content
            size = len(file_content)
        else:
            obj = client.get_object(Bucket=bucket, Key=key)
            file_content = obj['Body'].read()
            size = len(file_content)
    except Exception as e:
        logger.exception("Failed to download file from %s", 'URL' if file_url else 'R2')
        return {
            'success': False,
            'error': f"Failed to download file from {'URL' if file_url else 'R2'}: {e}",
            'file': {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'key': key,
                'filename': unique_filename,
                'url': chosen_url,
                'size': None,
                'content_type': content_type,
            },
            'analysis': {'status': 'error', 'reason': 'download_failed'},
            'artifacts': None,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    # 2) Preprocess file for model (decide text vs file mode)
    prep = preprocess_for_model(file_content, unique_filename, content_type)

    # 3) Run OpenAI analysis
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
                'url': chosen_url,
                'size': size,
                'content_type': content_type,
            },
            'analysis': {'status': 'skipped', 'reason': 'openai_not_configured'},
            'artifacts': None,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    try:
        oa_client = OpenAI(api_key=api_key)
        prompt = build_knowledge_prompt(knowledge_type)

        model_name = os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini')
        uploaded = None

        if prep.get('mode') == 'file':
            # Keep PDF path: upload as file and reference it
            uploaded = oa_client.files.create(file=(
                unique_filename,
                prep['file_bytes']
            ), purpose='assistants')
            input_payload = [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_file", "file_id": uploaded.id}
                    ]
                }
            ]
        elif prep.get('mode') == 'text':
            # Provide prompt and the extracted text as two text blocks
            text_content = prep['text']
            # Guardrail: limit very large text to avoid blowing token limits
            max_bytes = int(os.getenv('KNOWLEDGE_MAX_TEXT_BYTES', '200000'))
            if len(text_content.encode('utf-8', errors='ignore')) > max_bytes:
                text_content = text_content.encode('utf-8', errors='ignore')[:max_bytes].decode('utf-8', errors='ignore')
            input_payload = [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_text", "text": text_content}
                    ]
                }
            ]
        else:
            # Unsupported type: record a clear error but continue to write an error artifact later
            raise RuntimeError(f"Unsupported document type: {prep.get('reason', 'unknown')}")

        resp = oa_client.responses.create(
            model=model_name,
            input=input_payload,
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
        model_used = model_name

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
                'url': chosen_url,
                'size': size,
                'content_type': content_type,
            },
            'analysis': {
                'status': status,
                'model': model_used,
                **({ 'file_id': uploaded.id } if uploaded is not None else {})
            },
            'artifacts': {
                'analysis_key': analysis_key,
                'analysis_url': analysis_url,
            },
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
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
                'url': chosen_url,
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
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
