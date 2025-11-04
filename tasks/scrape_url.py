import os
import time
import json
from datetime import datetime
from urllib.parse import urlparse

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
    extract_dual_output,
)
from .utils.task_db import mark_task_running, mark_task_failed, mark_task_succeeded, record_task_artifact, upsert_tenant_integration_param

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


def _get_scraper_strategy() -> tuple[str, bool]:
    """Determine which scraper to use and whether fallback is enabled.
    
    Returns:
        tuple: (primary_scraper, enable_fallback)
            primary_scraper: 'scraperapi' or 'zenrows'
            enable_fallback: True if fallback to alternate scraper is allowed
    
    Environment:
        SCRAPER_PRIORITY controls behavior:
            - "scraperapi" (default): Use ScraperAPI with ZenRows fallback
            - "zenrows" or "zenrows-only": Use only ZenRows
            - "scraperapi-only": Use only ScraperAPI, no fallback
    """
    priority = os.getenv('SCRAPER_PRIORITY', 'scraperapi').lower()
    
    if priority in ('zenrows', 'zenrows-only'):
        return ('zenrows', False)
    elif priority == 'scraperapi-only':
        return ('scraperapi', False)
    else:  # default: 'scraperapi' with fallback
        return ('scraperapi', True)


def _submit_async_job(url: str, *, render: bool = True, output_format: str | None = 'markdown', timeout_ms: int = 15000) -> tuple[str, str]:
    """Submit an async job to ScraperAPI. Returns (job_id, status_url).

    Uses the documented async payload shape with 'urls' and 'apiParams'.
    """
    api_key = os.getenv('SCRAPERAPI_KEY')
    if not api_key:
        raise RuntimeError('SCRAPERAPI_KEY not configured')
    timeout = max(1, timeout_ms // 1000)
    api_params: dict = {}
    if render:
        api_params['render'] = 'true'
    if output_format:
        api_params['output_format'] = output_format
    payload: dict = {
        'apiKey': api_key,
        'urls': [url],
    }
    if api_params:
        payload['apiParams'] = api_params
    resp = requests.post('https://async.scraperapi.com/jobs', json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    job_id = None
    status_url = None
    # Response may be a dict or a list of job objects when using 'urls'
    if isinstance(data, dict):
        job_id = data.get('id') or data.get('jobId')
        status_url = data.get('statusUrl') or data.get('statusURL')
    elif isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            job_id = first.get('id') or first.get('jobId')
            status_url = first.get('statusUrl') or first.get('statusURL')
    if not job_id or not status_url:
        raise RuntimeError(f'Invalid async job response shape: {data}')
    return str(job_id), str(status_url)


def _poll_async_job(status_url: str, *, per_req_timeout_ms: int = 10000, total_timeout_ms: int = 180000) -> dict:
    """Poll ScraperAPI async job status until finished or timeout.

    Returns the final JSON payload which should contain a 'response' object with 'body'.
    """
    started = time.time()
    attempt = 0
    per_req_timeout = max(1, per_req_timeout_ms // 1000)
    while True:
        attempt += 1
        resp = requests.get(status_url, timeout=per_req_timeout)
        resp.raise_for_status()
        data = resp.json()
        status = data.get('status')
        if status == 'finished':
            return data
        if status in ('failed', 'error'):
            raise RuntimeError(f'Async job failed: {data}')
        # backoff sleep
        elapsed = time.time() - started
        if elapsed * 1000 > total_timeout_ms:
            raise TimeoutError(f'Async job timed out after {int(elapsed)}s: {status_url}')
        sleep_s = min(5.0, 0.5 + attempt * 0.5)
        time.sleep(sleep_s)


def _fetch_via_zenrows(url: str, timeout_ms: int, output_format: str | None = 'markdown') -> tuple[str, dict]:
    """Fetch content via ZenRows REST API service.
    
    Args:
        url: Target URL to scrape
        timeout_ms: Request timeout in milliseconds
        output_format: 'markdown' for markdown output, None for HTML
    
    Returns:
        tuple: (content, headers_dict)
            content: Scraped markdown or HTML content
            headers_dict: Metadata headers for tracking
    
    Environment:
        ZENROWS_API_KEY: Required API key for ZenRows service
    """
    api_key = os.getenv('ZENROWS_API_KEY')
    if not api_key:
        raise RuntimeError('ZENROWS_API_KEY not configured')
    
    # JS instructions for comprehensive page interaction
    js_instructions = [
        {"wait_for": "main, article, #__next, #app, [role='main']"},
        {"click": "[aria-label*='accept' i], .cookie-accept, .cc-accept, button[aria-label='Accept all']"},
        {"wait_event": "networkalmostidle"},
        {"scroll_y": 1200},
        {"wait": 400},
        {"scroll_y": 2400},
        {"wait": 400},
        {"evaluate": "window.scrollTo(0, document.body.scrollHeight);"},
        {"wait_event": "networkidle"},
        {"wait_for": ".content, .article, .rich-text, [itemprop='articleBody'], .services, .service, [data-testid='content'], [data-testid='main-content']"}
    ]
    
    # Build API request parameters (requests library will handle URL encoding)
    params = {
        'apikey': api_key,
        'url': url,
        'js_render': 'true',
        'js_instructions': json.dumps(js_instructions),
    }
    
    if output_format == 'markdown':
        params['response_type'] = 'markdown'
    
    timeout_s = max(1, timeout_ms // 1000)
    
    # Make request to ZenRows REST API
    logger.info(f"🔍 [_fetch_via_zenrows] Calling ZenRows API with url={url}, output_format={output_format}")
    response = requests.get('https://api.zenrows.com/v1/', params=params, timeout=timeout_s)
    logger.info(f"🔍 [_fetch_via_zenrows] ZenRows responded with status={response.status_code}, content_length={len(response.text)}")
    response.raise_for_status()
    
    content = response.text
    headers = {
        'X-Scraper-Source': 'zenrows',
        'X-ZenRows-StatusCode': str(response.status_code),
        'X-ZenRows-ContentLength': str(len(content)),
    }
    
    return content, headers


def _fetch_html_via_zenrows(url: str, timeout_ms: int) -> tuple[str, dict]:
    """Fetch raw HTML via ZenRows (for raw HTML saving)."""
    return _fetch_via_zenrows(url, timeout_ms, output_format=None)

def _fetch_html_via_scraperapi_async(url: str, timeout_ms: int, total_timeout_ms: int) -> tuple[str, dict]:
    """Fetch raw HTML via ScraperAPI Async (rendered)."""
    job_id, status_url = _submit_async_job(url, render=True, output_format=None, timeout_ms=timeout_ms)
    final = _poll_async_job(status_url, per_req_timeout_ms=timeout_ms, total_timeout_ms=total_timeout_ms)
    resp_info = _extract_async_response_info(final) or {}
    body = resp_info.get('body', '') or ''
    meta = {
        'X-ScraperAPI-Async': 'true',
        'X-ScraperAPI-StatusUrl': status_url,
        'X-ScraperAPI-JobId': job_id,
        'X-ScraperAPI-Output-Format': 'html',
        'X-ScraperAPI-StatusCode': str(resp_info.get('statusCode')) if resp_info.get('statusCode') is not None else None,
    }
    return body, meta


def _fetch_html_with_fallback(url: str, timeout_ms: int, total_timeout_ms: int) -> tuple[str, dict]:
    """Fetch raw HTML using configured scraper with fallback support.
    
    Respects SCRAPER_PRIORITY environment variable to determine which scraper to use.
    """
    primary, enable_fallback = _get_scraper_strategy()
    
    if primary == 'zenrows':
        logger.info(f"🎯 [_fetch_html_with_fallback] Using ZenRows for HTML (SCRAPER_PRIORITY={os.getenv('SCRAPER_PRIORITY', 'zenrows')})")
        return _fetch_html_via_zenrows(url, timeout_ms)
    else:
        try:
            return _fetch_html_via_scraperapi_async(url, timeout_ms, total_timeout_ms)
        except Exception as e:
            if enable_fallback:
                logger.warning(f"⚠️ ScraperAPI HTML fetch failed: {e}, falling back to ZenRows")
                return _fetch_html_via_zenrows(url, timeout_ms)
            else:
                raise


def _extract_async_response_info(data: object) -> dict:
    """Return a uniform response info dict from ScraperAPI async job result.

    Handles shapes:
    - { response: { body, statusCode, headers, ... } }
    - { response: [ { body, ... }, ... ] }
    - { results: [ { response: { ... } }, ... ] }
    - [ { response: { ... } }, ... ]
    Returns an empty dict if nothing matches.
    """
    try:
        # Dict shapes
        if isinstance(data, dict):
            resp = data.get('response')  # type: ignore[attr-defined]
            if isinstance(resp, dict):
                return resp
            if isinstance(resp, list) and resp:
                first = resp[0]
                if isinstance(first, dict):
                    return first
            results = data.get('results')  # type: ignore[attr-defined]
            if isinstance(results, list) and results:
                for item in results:
                    if isinstance(item, dict) and isinstance(item.get('response'), dict):
                        return item['response']
                # fallback to first item if no response key found
                first_item = results[0]
                if isinstance(first_item, dict):
                    maybe_resp = first_item.get('response')
                    if isinstance(maybe_resp, dict):
                        return maybe_resp
                return {}
        # List shape
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                if isinstance(first.get('response'), dict):
                    return first['response']
                return first
        return {}
    except Exception:
        return {}


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
                           save_raw_html: bool = False,
                           speako_task_id: str | None = None,
                           tenant_integration_param: dict | None = None) -> dict:
    start_ts = time.time()
    started_at = datetime.utcnow().isoformat() + 'Z'

    # Log tenant integration param if provided
    if tenant_integration_param:
        logger.info(f"📋 [scrape_url_to_markdown] tenantIntegrationParam received: {tenant_integration_param}")
    else:
        logger.info(f"📋 [scrape_url_to_markdown] No tenantIntegrationParam provided")

    if not _host_allowed(url):
        # Early exit: mark failed if speako_task_id present
        if speako_task_id:
            try:
                mark_task_failed(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                 error_code='host_not_allowed', error_message='Host not allowed',
                                 details={'url': url}, actor='celery')
            except Exception as db_e:
                logger.warning(f"mark_task_failed (host_not_allowed) failed: {db_e}")
        return {
            'success': False,
            'error': 'Host not allowed',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    r2, bucket = _get_r2_client()
    if r2 is None:
        # Early exit: mark failed if speako_task_id present
        if speako_task_id:
            try:
                mark_task_failed(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                 error_code='r2_not_configured', error_message='Cloudflare R2 not configured',
                                 details={'url': url}, actor='celery')
            except Exception as db_e:
                logger.warning(f"mark_task_failed (r2_not_configured) failed: {db_e}")
        return {
            'success': False,
            'error': 'Cloudflare R2 not configured',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    # Mark task as running in DB (best-effort)
    if speako_task_id:
        try:
            mark_task_running(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                              message='Scrape started', details={'url': url}, actor='celery')
        except Exception as db_e:
            logger.warning(f"mark_task_running failed: {db_e}")

    try:
        timeout_ms = int(os.getenv('SCRAPE_TIMEOUT_MS', '15000'))
        total_timeout_ms = int(os.getenv('SCRAPERAPI_POLL_TOTAL_TIMEOUT_MS', '180000'))
        
        # Determine scraper strategy
        primary, enable_fallback = _get_scraper_strategy()
        scraper_source = None
        markdown = None
        headers = None
        
        if primary == 'zenrows':
            # Use ZenRows directly
            logger.info(f"🎯 [scrape_url_to_markdown] Using ZenRows (SCRAPER_PRIORITY={os.getenv('SCRAPER_PRIORITY', 'zenrows')})")
            markdown, headers = _fetch_via_zenrows(url, timeout_ms, output_format='markdown')
            scraper_source = 'zenrows'
        else:
            # Try ScraperAPI first
            try:
                logger.info(f"🎯 [scrape_url_to_markdown] Using ScraperAPI (SCRAPER_PRIORITY={os.getenv('SCRAPER_PRIORITY', 'scraperapi')})")
                job_id, status_url = _submit_async_job(url, render=True, output_format='markdown', timeout_ms=timeout_ms)
                final = _poll_async_job(status_url, per_req_timeout_ms=timeout_ms, total_timeout_ms=total_timeout_ms)
                resp_info = _extract_async_response_info(final) or {}
                markdown = (resp_info.get('body') or '').strip()
                scraper_source = 'scraperapi'
                headers = {
                    'X-ScraperAPI-Async': 'true',
                    'X-ScraperAPI-StatusUrl': status_url,
                    'X-ScraperAPI-JobId': job_id,
                    'X-ScraperAPI-Output-Format': 'markdown',
                    'X-ScraperAPI-StatusCode': str(resp_info.get('statusCode')) if resp_info.get('statusCode') is not None else None,
                }
            except Exception as scraper_error:
                if enable_fallback:
                    # Fallback to ZenRows
                    logger.warning(f"⚠️ ScraperAPI failed: {scraper_error}, falling back to ZenRows")
                    logger.info(f"🔄 [scrape_url_to_markdown] Attempting ZenRows fallback for URL: {url}")
                    markdown, headers = _fetch_via_zenrows(url, timeout_ms, output_format='markdown')
                    scraper_source = 'zenrows'
                    logger.info(f"✅ [scrape_url_to_markdown] ZenRows fallback succeeded, retrieved {len(markdown)} chars of markdown")
                else:
                    # No fallback enabled, re-raise
                    logger.error(f"❌ ScraperAPI failed and fallback disabled (SCRAPER_PRIORITY=scraperapi-only)")
                    raise

        keys = build_scrape_artifact_paths(tenant_id, location_id, url)
        public_base = os.getenv('R2_PUBLIC_BASE_URL', 'https://assets.speako.ai')

        # Save markdown
        md_bytes = markdown.encode('utf-8')
        put_md = r2.put_object(
            Bucket=bucket,
            Key=keys['markdown_key'],
            Body=md_bytes,
            ContentType='text/markdown',
            Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
        )
        if speako_task_id:
            try:
                record_task_artifact(
                    task_id=str(speako_task_id),
                    kind='markdown',
                    uri=f"{public_base}/{keys['markdown_key']}",
                    bucket=bucket,
                    object_key=keys['markdown_key'],
                    mime_type='text/markdown',
                    size_bytes=len(md_bytes),
                    etag=(put_md or {}).get('ETag') if isinstance(put_md, dict) else None,
                    version_id=(put_md or {}).get('VersionId') if isinstance(put_md, dict) else None,
                    metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
                )
            except Exception as db_e:
                logger.warning(f"record_task_artifact(markdown) failed: {db_e}")

        # Save metadata
        import json as _json
        meta = {
            'url': url,
            'title': None,
            'fetched_at': datetime.utcnow().isoformat() + 'Z',
            'headers': headers,
            'content_length': len(md_bytes),
            'extractor': scraper_source,
            'scraper_priority': os.getenv('SCRAPER_PRIORITY', 'scraperapi'),
            'was_fallback': scraper_source == 'zenrows' and primary == 'scraperapi',
        }
        meta_bytes = _json.dumps(meta).encode('utf-8')
        put_meta = r2.put_object(
            Bucket=bucket,
            Key=keys['meta_key'],
            Body=meta_bytes,
            ContentType='application/json',
            Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
        )
        if speako_task_id:
            try:
                record_task_artifact(
                    task_id=str(speako_task_id),
                    kind='metadata',
                    uri=f"{public_base}/{keys['meta_key']}",
                    bucket=bucket,
                    object_key=keys['meta_key'],
                    mime_type='application/json',
                    size_bytes=len(meta_bytes),
                    etag=(put_meta or {}).get('ETag') if isinstance(put_meta, dict) else None,
                    version_id=(put_meta or {}).get('VersionId') if isinstance(put_meta, dict) else None,
                    metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
                )
            except Exception as db_e:
                logger.warning(f"record_task_artifact(metadata) failed: {db_e}")

        if save_raw_html or os.getenv('SCRAPE_SAVE_RAW_HTML', 'false').lower() == 'true':
            try:
                raw_html, _raw_headers = _fetch_html_with_fallback(url, timeout_ms, total_timeout_ms)
                raw_bytes = raw_html.encode('utf-8', errors='ignore')
                put_raw = r2.put_object(
                    Bucket=bucket,
                    Key=keys['raw_key'],
                    Body=raw_bytes,
                    ContentType='text/html',
                    Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
                )
                if speako_task_id:
                    try:
                        record_task_artifact(
                            task_id=str(speako_task_id),
                            kind='raw_html',
                            uri=f"{public_base}/{keys['raw_key']}",
                            bucket=bucket,
                            object_key=keys['raw_key'],
                            mime_type='text/html',
                            size_bytes=len(raw_bytes),
                            etag=(put_raw or {}).get('ETag') if isinstance(put_raw, dict) else None,
                            version_id=(put_raw or {}).get('VersionId') if isinstance(put_raw, dict) else None,
                            metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
                        )
                    except Exception as db_e:
                        logger.warning(f"record_task_artifact(raw_html) failed: {db_e}")
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
                    
                    # Extract json_data and markdown_data from the parsed response
                    json_payload, markdown_text = extract_dual_output(parsed)
                    
                    payload = json_payload if json_payload is not None else {"raw": raw}
                    payload_bytes = _json.dumps(payload).encode('utf-8')
                    put_analysis = r2.put_object(
                        Bucket=bucket,
                        Key=keys['analysis_key'],
                        Body=payload_bytes,
                        ContentType='application/json',
                        Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'openai'}
                    )
                    if speako_task_id:
                        try:
                            record_task_artifact(
                                task_id=str(speako_task_id),
                                kind='analysis',
                                uri=f"{public_base}/{keys['analysis_key']}",
                                bucket=bucket,
                                object_key=keys['analysis_key'],
                                mime_type='application/json',
                                size_bytes=len(payload_bytes),
                                etag=(put_analysis or {}).get('ETag') if isinstance(put_analysis, dict) else None,
                                version_id=(put_analysis or {}).get('VersionId') if isinstance(put_analysis, dict) else None,
                                metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'openai'}
                            )
                        except Exception as db_e:
                            logger.warning(f"record_task_artifact(analysis) failed: {db_e}")
                    artifacts['analysis_key'] = keys['analysis_key']
                    artifacts['analysis_url'] = f"{public_base}/{keys['analysis_key']}"
                    analysis = {'status': 'success' if parsed is not None else 'raw'}
                except Exception as ae:
                    logger.exception("Scrape analysis failed")
                    analysis = {'status': 'error', 'message': str(ae)}

            # Mark succeeded before returning
            if speako_task_id:
                # Generate AI description if analysis was performed
                ai_description = None
                if payload is not None:
                    try:
                        from .utils.knowledge_utils import generate_ai_description
                        ai_description = generate_ai_description(payload, knowledge_type)
                        if ai_description:
                            logger.info(f"📝 [scrape_url_to_markdown] Generated AI description ({len(ai_description)} chars)")
                    except Exception as desc_e:
                        logger.warning(f"⚠️ [scrape_url_to_markdown] Failed to generate AI description: {desc_e}")
                
                # Update tenant_integration_params table to mark as configured
                try:
                    # Check if analysis variables are available in locals()
                    analysis_to_save = payload if 'payload' in locals() and payload else None
                    markdown_to_save = markdown_text if 'markdown_text' in locals() and markdown_text else None
                    
                    param_id = upsert_tenant_integration_param(
                        tenant_integration_param=tenant_integration_param,
                        analysis_result=analysis_to_save,
                        ai_description=ai_description,
                        value_text=markdown_to_save
                    )
                    if param_id:
                        if analysis_to_save:
                            desc_msg = " and AI description" if ai_description else ""
                            markdown_msg = " and markdown" if markdown_to_save else ""
                            logger.info(f"✅ [scrape_url_to_markdown] Updated tenant_integration_param (param_id={param_id}) status to 'configured' with analysis JSON{desc_msg}{markdown_msg} saved")
                        else:
                            logger.info(f"✅ [scrape_url_to_markdown] Updated tenant_integration_param (param_id={param_id}) status to 'configured'")
                    else:
                        logger.warning(f"⚠️ [scrape_url_to_markdown] Failed to update tenant_integration_param - no param_id returned")
                except Exception as tip_e:
                    logger.warning(f"[tasks] upsert_tenant_integration_param failed: {tip_e}")
                
                try:
                    mark_task_succeeded(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                        details={'url': url, 'artifacts': artifacts, 'pipeline': pipeline, 'knowledge_type': knowledge_type},
                                        actor='celery', progress=100)
                except Exception as db_e:
                    logger.warning(f"mark_task_succeeded failed: {db_e}")

            return {
                'success': True,
                'url': url,
                'artifacts': artifacts,
                'analysis': analysis,
                'job': {
                    'task_id': self.request.id,
                    'speako_task_id': speako_task_id,
                    'started_at': started_at,
                    'completed_at': datetime.utcnow().isoformat() + 'Z',
                    'duration_ms': int((time.time() - start_ts) * 1000),
                }
            }

        # Markdown-only success: mark succeeded before returning
        if speako_task_id:
            # Update tenant_integration_params table to mark as configured
            try:
                param_id = upsert_tenant_integration_param(tenant_integration_param=tenant_integration_param)
                if param_id:
                    logger.info(f"✅ [scrape_url_to_markdown] Updated tenant_integration_param (param_id={param_id}) status to 'configured'")
                else:
                    logger.warning(f"⚠️ [scrape_url_to_markdown] Failed to update tenant_integration_param - no param_id returned")
            except Exception as tip_e:
                logger.warning(f"[tasks] upsert_tenant_integration_param failed: {tip_e}")
            
            try:
                mark_task_succeeded(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                    details={'url': url, 'artifacts': artifacts, 'pipeline': pipeline},
                                    actor='celery', progress=100)
            except Exception as db_e:
                logger.warning(f"mark_task_succeeded failed: {db_e}")

        return {
            'success': True,
            'url': url,
            'artifacts': artifacts,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    except TimeoutError as e:
        error_msg = f"Scraping job timed out for URL: {url}"
        logger.error(f"⏱️  {error_msg} - Exceeded maximum wait time. Consider increasing SCRAPERAPI_POLL_TOTAL_TIMEOUT_MS.")
        if speako_task_id:
            try:
                mark_task_failed(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                 error_code='timeout', error_message=error_msg,
                                 details={'url': url, 'timeout_ms': total_timeout_ms}, actor='celery')
            except Exception as db_e:
                logger.warning(f"mark_task_failed failed: {db_e}")
        return {
            'success': False,
            'error': 'Scraping timed out - the page took too long to process',
            'error_type': 'timeout',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
    except requests.exceptions.ReadTimeout as e:
        error_msg = f"Network timeout while checking scraping status for URL: {url}"
        logger.error(f"🌐 {error_msg} - ScraperAPI status endpoint did not respond in time. Consider increasing SCRAPE_TIMEOUT_MS.")
        if speako_task_id:
            try:
                mark_task_failed(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                 error_code='network_timeout', error_message=error_msg,
                                 details={'url': url, 'timeout_ms': timeout_ms}, actor='celery')
            except Exception as db_e:
                logger.warning(f"mark_task_failed failed: {db_e}")
        return {
            'success': False,
            'error': 'Network timeout - unable to connect to scraping service',
            'error_type': 'network_timeout',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error while scraping URL: {url}"
        logger.error(f"🔌 {error_msg} - {type(e).__name__}: {str(e)}")
        if speako_task_id:
            try:
                mark_task_failed(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                 error_code='network_error', error_message=error_msg,
                                 details={'url': url, 'error_type': type(e).__name__}, actor='celery')
            except Exception as db_e:
                logger.warning(f"mark_task_failed failed: {db_e}")
        return {
            'success': False,
            'error': f'Network error - {type(e).__name__}',
            'error_type': 'network_error',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
    except Exception as e:
        error_msg = f"Unexpected error while scraping URL: {url}"
        logger.error(f"❌ {error_msg} - {type(e).__name__}: {str(e)}")
        logger.exception("Full traceback:")
        if speako_task_id:
            try:
                mark_task_failed(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                 error_code='error', error_message=str(e),
                                 details={'url': url, 'error_type': type(e).__name__}, actor='celery')
            except Exception as db_e:
                logger.warning(f"mark_task_failed failed: {db_e}")
        return {
            'success': False,
            'error': f'Scraping failed - {type(e).__name__}',
            'error_type': 'unexpected_error',
            'url': url,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
