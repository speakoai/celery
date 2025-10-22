import os
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin

import requests
# No HTML parsing needed when using ScraperAPI Markdown output

from celery.utils.log import get_task_logger
from tasks.celery_app import app
import boto3

trafilatura = None
Document = None

try:
    from openai import OpenAI
    _openai_import_error = None
except Exception as _openai_e:
    OpenAI = None
    _openai_import_error = repr(_openai_e)

from .utils.knowledge_utils import (
    build_scrape_artifact_paths,
    build_knowledge_prompt,
    parse_model_json_output,
)


logger = get_task_logger(__name__)

# Note: We are switching to ScraperAPI for fetching rendered content in Markdown.
# Any Playwright-based rendering paths are no longer used.
sync_playwright = None


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


def _host_allowed(url: str) -> bool:
    allowed = os.getenv('SCRAPE_ALLOWED_HOSTS', '').strip()
    if not allowed:
        return True
    host = urlparse(url).hostname or ''
    allowed_hosts = [h.strip() for h in allowed.split(',') if h.strip()]
    return host in allowed_hosts


def _fetch_markdown_via_scraperapi(url: str, timeout_ms: int) -> tuple[str, dict]:
    """Fetch URL via ScraperAPI with JS rendering and Markdown output.

    Requires environment variable SCRAPERAPI_KEY.
    Returns (markdown_text, meta_headers_dict).
    """
    api_key = os.getenv('SCRAPERAPI_KEY')
    if not api_key:
        raise RuntimeError('SCRAPERAPI_KEY not configured')
    timeout = max(1, timeout_ms // 1000)
    params = {
        'api_key': api_key,
        'url': url,
        'render': 'true',
        'output_format': 'markdown',
    }
    resp = requests.get('https://api.scraperapi.com/', params=params, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    # Build minimal synthetic headers/meta for traceability
    meta = {
        'X-ScraperAPI-Render': 'true',
        'X-ScraperAPI-Output-Format': 'markdown',
        'X-ScraperAPI-Status': str(resp.status_code),
    }
    return resp.text or '', meta


def _fetch_html_via_scraperapi(url: str, timeout_ms: int) -> tuple[str, dict]:
    """Optionally fetch raw HTML via ScraperAPI when save_raw_html is enabled."""
    api_key = os.getenv('SCRAPERAPI_KEY')
    if not api_key:
        raise RuntimeError('SCRAPERAPI_KEY not configured')
    timeout = max(1, timeout_ms // 1000)
    params = {
        'api_key': api_key,
        'url': url,
        'render': 'true',
        # No output_format => defaults to HTML
    }
    resp = requests.get('https://api.scraperapi.com/', params=params, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    meta = {
        'X-ScraperAPI-Render': 'true',
        'X-ScraperAPI-Output-Format': 'html',
        'X-ScraperAPI-Status': str(resp.status_code),
    }
    return resp.text or '', meta


def _render_page_with_js(url: str, timeout_ms: int, user_agent: str | None = None) -> str:
    """Deprecated: JS rendering is handled by ScraperAPI (render=true)."""
    raise RuntimeError('playwright_render_deprecated')


def _extract_main_html(html: str, base_url: str) -> tuple[str, str]:
    """Deprecated: We now receive Markdown directly from ScraperAPI."""
    return '', html


def _html_to_markdown(title: str, html: str, base_url: str) -> str:
    """Deprecated: ScraperAPI returns Markdown directly; passthrough."""
    return html


@app.task(bind=True)
def scrape_url_to_markdown(self, *, tenant_id: str, location_id: str, url: str,
                           pipeline: str = 'markdown-only', knowledge_type: str | None = None,
                           save_raw_html: bool = False) -> dict:
    start_ts = time.time()
    started_at = datetime.utcnow().isoformat() + 'Z'

    if not _host_allowed(url):
        return {
            'success': False,
            'error': 'Host not allowed',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    r2, bucket = _get_r2_client()
    if r2 is None:
        return {
            'success': False,
            'error': 'Cloudflare R2 not configured',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    try:
        timeout_ms = int(os.getenv('SCRAPE_TIMEOUT_MS', '15000'))
        # Fetch directly from ScraperAPI as Markdown
        markdown, headers = _fetch_markdown_via_scraperapi(url, timeout_ms)

        keys = build_scrape_artifact_paths(tenant_id, location_id, url)
        public_base = os.getenv('R2_PUBLIC_BASE_URL', 'https://assets.speako.ai')

        # Save markdown
        r2.put_object(
            Bucket=bucket,
            Key=keys['markdown_key'],
            Body=markdown.encode('utf-8'),
            ContentType='text/markdown',
            Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
        )

        # Save metadata
        import json as _json
        meta = {
            'url': url,
            'title': None,
            'fetched_at': datetime.utcnow().isoformat() + 'Z',
            'headers': headers,
            'content_length': len(markdown.encode('utf-8')),
            'extractor': 'scraperapi',
        }
        r2.put_object(
            Bucket=bucket,
            Key=keys['meta_key'],
            Body=_json.dumps(meta).encode('utf-8'),
            ContentType='application/json',
            Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
        )

        if save_raw_html or os.getenv('SCRAPE_SAVE_RAW_HTML', 'false').lower() == 'true':
            try:
                raw_html, _raw_headers = _fetch_html_via_scraperapi(url, timeout_ms)
                r2.put_object(
                    Bucket=bucket,
                    Key=keys['raw_key'],
                    Body=raw_html.encode('utf-8', errors='ignore'),
                    ContentType='text/html',
                    Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
                )
            except Exception as _raw_e:
                logger.warning(f"Saving raw HTML via ScraperAPI failed: {_raw_e}")

        artifacts = {
            'markdown_key': keys['markdown_key'],
            'markdown_url': f"{public_base}/{keys['markdown_key']}",
            'meta_key': keys['meta_key'],
            'meta_url': f"{public_base}/{keys['meta_key']}",
        }

        # Optional analysis chain
        if pipeline == 'analyze' and knowledge_type:
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key or OpenAI is None:
                analysis = {'status': 'skipped', 'reason': 'openai_not_configured'}
            else:
                try:
                    client = OpenAI(api_key=api_key)
                    prompt = build_knowledge_prompt(knowledge_type)
                    # Guardrail: limit size
                    max_bytes = int(os.getenv('KNOWLEDGE_MAX_TEXT_BYTES', '200000'))
                    md_text = markdown
                    if len(md_text.encode('utf-8', errors='ignore')) > max_bytes:
                        md_text = md_text.encode('utf-8', errors='ignore')[:max_bytes].decode('utf-8', errors='ignore')
                    resp = client.responses.create(
                        model=os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini'),
                        input=[{
                            "role": "user",
                            "content": [
                                {"type": "input_text", "text": prompt},
                                {"type": "input_text", "text": md_text}
                            ]
                        }],
                        temperature=0.2
                    )
                    analysis_text = getattr(resp, 'output_text', None)
                    if analysis_text is None and getattr(resp, 'choices', None):
                        try:
                            analysis_text = resp.choices[0].message.content
                        except Exception:
                            analysis_text = None
                    parsed, raw = parse_model_json_output(analysis_text)
                    payload = parsed if parsed is not None else {"raw": raw}
                    r2.put_object(
                        Bucket=bucket,
                        Key=keys['analysis_key'],
                        Body=_json.dumps(payload).encode('utf-8'),
                        ContentType='application/json',
                        Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'openai'}
                    )
                    artifacts['analysis_key'] = keys['analysis_key']
                    artifacts['analysis_url'] = f"{public_base}/{keys['analysis_key']}"
                    analysis = {'status': 'success' if parsed is not None else 'raw'}
                except Exception as ae:
                    logger.exception("Scrape analysis failed")
                    analysis = {'status': 'error', 'message': str(ae)}

            return {
                'success': True,
                'url': url,
                'artifacts': artifacts,
                'analysis': analysis,
                'job': {
                    'task_id': self.request.id,
                    'started_at': started_at,
                    'completed_at': datetime.utcnow().isoformat() + 'Z',
                    'duration_ms': int((time.time() - start_ts) * 1000),
                }
            }

        # Markdown-only
        return {
            'success': True,
            'url': url,
            'artifacts': artifacts,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    except Exception as e:
        logger.exception("Scrape failed")
        return {
            'success': False,
            'error': str(e),
            'url': url,
            'job': {
                'task_id': self.request.id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
