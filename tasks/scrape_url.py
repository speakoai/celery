import os
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

from celery.utils.log import get_task_logger
from tasks.celery_app import app
import boto3

try:
    import trafilatura
except Exception:
    trafilatura = None

try:
    from readability import Document
except Exception:
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

# Optional JS rendering (Playwright)
try:
    from playwright.sync_api import sync_playwright
except Exception:
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


def _fetch_url(url: str, timeout_ms: int) -> tuple[str, dict]:
    headers = {
        'User-Agent': os.getenv('SCRAPE_USER_AGENT', 'SpeakoBot/1.0 (+contact)')
    }
    timeout = max(1, timeout_ms // 1000)
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    return resp.text, dict(resp.headers)


def _render_page_with_js(url: str, timeout_ms: int, user_agent: str | None = None) -> str:
    """Render a JS-heavy page using Playwright if available. Returns HTML string.

    Note: Requires the 'playwright' Python package and browsers installed.
    In Render, add a build step: `python -m playwright install --with-deps chromium`.
    """
    if sync_playwright is None:
        raise RuntimeError('playwright_not_installed')
    wait_timeout = max(1000, timeout_ms)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])  # type: ignore
        context = browser.new_context(
            user_agent=user_agent or os.getenv('SCRAPE_USER_AGENT', 'SpeakoBot/1.0 (+contact)'),
            java_script_enabled=True,
            viewport={"width": 1280, "height": 2000},
        )
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=wait_timeout)
        # Some sites render late; small additional delay if configured
        extra_wait_ms = int(os.getenv('SCRAPE_JS_EXTRA_WAIT_MS', '0'))
        if extra_wait_ms > 0:
            page.wait_for_timeout(extra_wait_ms)
        content = page.content()
        context.close()
        browser.close()
        return content


def _extract_main_html(html: str, base_url: str) -> tuple[str, str]:
    """Return (title, main_html) using trafilatura or readability as fallback."""
    # Try trafilatura first if available
    if trafilatura is not None:
        try:
            extracted = trafilatura.extract(html, include_comments=False, include_tables=True, favor_recall=True)
            if extracted and len(extracted) > 200:
                # Trafilatura returns plain text; wrap minimal HTML for markdownify
                meta = trafilatura.extract_metadata(html)
                title = meta.title if meta else ''
                para_html = extracted.replace("\n", "</p><p>")
                main_html = f"<article><h1>{title or ''}</h1><p>{para_html}</p></article>"
                return title or '', main_html
        except Exception:
            pass

    # Fallback to readability
    if Document is not None:
        try:
            doc = Document(html)
            title = doc.short_title() or ''
            main_html = doc.summary()
            return title, main_html
        except Exception:
            pass

    # Last resort: strip scripts and use body
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()
    title = (soup.title.string if soup.title and soup.title.string else '').strip()
    body = soup.body or soup
    return title, str(body)


def _html_to_markdown(title: str, html: str, base_url: str) -> str:
    # Convert relative links to absolute
    soup = BeautifulSoup(html, 'lxml')
    for a in soup.find_all('a', href=True):
        a['href'] = urljoin(base_url, a['href'])
    for img in soup.find_all('img', src=True):
        img['src'] = urljoin(base_url, img['src'])

    final_html = str(soup)
    content_md = md(final_html, heading_style="ATX")
    header = f"# {title}\n\n" if title else ""
    return header + content_md


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
        html, headers = _fetch_url(url, timeout_ms)
        # Detect JS-only pages or empty content and optionally render with Playwright
        js_required_markers = [
            'not available without javascript',
            'enable javascript',
            'requires javascript',
        ]
        should_try_js = os.getenv('SCRAPE_ENABLE_JS', 'false').lower() == 'true'
        needs_js = (len((html or '').strip()) < 1000) or any(m in (html or '').lower() for m in js_required_markers)
        if should_try_js and needs_js:
            try:
                rendered = _render_page_with_js(url, int(os.getenv('SCRAPE_JS_TIMEOUT_MS', '20000')),
                                                os.getenv('SCRAPE_USER_AGENT', 'SpeakoBot/1.0 (+contact)'))
                if rendered and len(rendered) > len(html):
                    html = rendered
                    headers['X-Scrape-Rendered'] = 'playwright'
            except Exception as _js_e:
                logger.warning(f"Playwright render skipped/failed: {_js_e}")
        title, main_html = _extract_main_html(html, url)
        markdown = _html_to_markdown(title, main_html, url)

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
            'title': title,
            'fetched_at': datetime.utcnow().isoformat() + 'Z',
            'headers': headers,
            'content_length': len(markdown.encode('utf-8')),
            'extractor': ('playwright|' if headers.get('X-Scrape-Rendered') == 'playwright' else '') + 'trafilatura|readability|bs4',
        }
        r2.put_object(
            Bucket=bucket,
            Key=keys['meta_key'],
            Body=_json.dumps(meta).encode('utf-8'),
            ContentType='application/json',
            Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
        )

        if save_raw_html or os.getenv('SCRAPE_SAVE_RAW_HTML', 'false').lower() == 'true':
            r2.put_object(
                Bucket=bucket,
                Key=keys['raw_key'],
                Body=html.encode('utf-8', errors='ignore'),
                ContentType='text/html',
                Metadata={'tenant_id': str(tenant_id), 'location_id': str(location_id), 'source': 'scrape'}
            )

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
