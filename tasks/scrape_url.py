import os
import time
import json
from datetime import datetime
from urllib.parse import urlparse

import requests

from celery.utils.log import get_task_logger
from tasks.celery_app import app
import boto3

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

# ---------------------------------------------------------------------------
# Firecrawl-backed, topic-aware "Add From URL" ingestion.
#
# Instead of scraping only the single pasted URL, we ask Firecrawl `/v1/map`
# for the pages on that site most relevant to the target knowledge_type (a
# per-type search term), scrape the top few via `/v1/scrape`, concatenate them
# into one corpus, and hand that corpus to the EXISTING OpenAI analyze chain.
# The pasted URL is always included. Discovery is best-effort: any map failure
# gracefully falls back to scraping just the pasted URL.
#
# Reuses the Firecrawl config conventions from tasks/scrape_business_profile.py
# (FIRECRAWL_BASE_URL + FIRECRAWL_API_KEY). Replaces the retired ScraperAPI /
# ZenRows fetch path for this task.
# ---------------------------------------------------------------------------

FIRECRAWL_BASE = os.getenv("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev")

# Primary relevance search phrase per knowledge_type, fed to Firecrawl `/v1/map`
# so it ranks the site's URLs by topical relevance. Types NOT in this map
# (notably `custom_message`) skip discovery and scrape only the pasted URL.
TOPIC_SEARCH_TERMS = {
    "food_menu": "menu",
    "service_menu": "services pricing",
    "faq": "faq",
    "service_policy": "policy cancellation",
    "special_promotion": "promotions offers",
    "staff": "team staff",
    "business_info": "about contact",
    "locations": "locations branches",
}


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


# ---------------------------------------------------------------------------
# Firecrawl helpers
# ---------------------------------------------------------------------------

def _firecrawl_headers() -> dict:
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise RuntimeError("FIRECRAWL_API_KEY not set")
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def firecrawl_map(url: str, search: str, limit: int, *, timeout: int = 60) -> list[str]:
    """POST /v1/map with a relevance `search` term. Returns a relevance-ranked
    list of URL strings. Firecrawl returns links as plain strings; older/newer
    shapes may return objects with a `url` field, both are handled."""
    resp = requests.post(
        f"{FIRECRAWL_BASE}/v1/map",
        headers=_firecrawl_headers(),
        json={"url": url, "search": search, "limit": limit},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    links = data.get("links") or []
    out: list[str] = []
    for item in links:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            u = item.get("url") or item.get("href")
            if u:
                out.append(u)
    return out


def firecrawl_scrape(url: str, *, formats: list[str] | None = None,
                     only_main_content: bool = True, timeout: int = 90) -> dict:
    """POST /v1/scrape. Returns the Firecrawl `data` object, e.g.
    {"markdown": "...", "metadata": {"sourceURL": ..., "title": ...}}."""
    body = {
        "url": url,
        "formats": list(formats or ["markdown"]),
        "onlyMainContent": only_main_content,
    }
    resp = requests.post(
        f"{FIRECRAWL_BASE}/v1/scrape",
        headers=_firecrawl_headers(),
        json=body,
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("data") or {}


def _norm_url(u: str) -> str:
    """Normalize a URL for dedup: lowercase + strip trailing slash/whitespace."""
    return (u or "").strip().rstrip("/").lower()


def select_urls(pasted_url: str, ranked_links: list[str], max_pages: int) -> list[str]:
    """Pick up to `max_pages` URLs to scrape. The pasted URL is ALWAYS included
    (first), then the highest-ranked discovered links fill the remaining slots,
    deduped by normalized form."""
    out: list[str] = []
    seen: set[str] = set()

    def _add(u: str) -> None:
        if not u:
            return
        key = _norm_url(u)
        if key in seen:
            return
        seen.add(key)
        out.append(u)

    _add(pasted_url)
    for link in ranked_links:
        if len(out) >= max_pages:
            break
        _add(link)
    return out[:max_pages]


def compose_topic_corpus(pages: list[dict], max_bytes: int) -> str:
    """Concatenate scraped pages into one corpus with clear per-page separators
    (source URL heading), capped at `max_bytes` (utf-8)."""
    parts: list[str] = []
    total = 0
    for p in pages:
        header = f"\n\n===== SOURCE: {p['url']} =====\n"
        if p.get("title"):
            header += f"# {p['title']}\n"
        header += "\n"
        block = header + p["markdown"]
        b = block.encode("utf-8", errors="ignore")
        if total + len(b) >= max_bytes:
            remaining = max(0, max_bytes - total)
            if remaining:
                parts.append(b[:remaining].decode("utf-8", errors="ignore"))
            break
        parts.append(block)
        total += len(b)
    return "".join(parts).strip()


def build_topic_corpus(url: str, knowledge_type: str | None, *,
                       max_pages: int, max_bytes: int) -> tuple[str, list[str], dict]:
    """Firecrawl map -> select topic-relevant URLs -> scrape each -> corpus.

    Returns (corpus_markdown, scraped_urls, discovery_meta).

    Discovery is best-effort and NEVER hard-fails the task: if map errors or
    returns nothing usable we fall back to scraping only the pasted URL. A hard
    failure is raised ONLY if not a single page (including the pasted URL) could
    be scraped.
    """
    search_term = TOPIC_SEARCH_TERMS.get(knowledge_type or "")
    discovery = {
        "search_term": search_term,
        "discovery_used": bool(search_term),
        "discovery_fallback": False,
        "discovered_urls": [],
    }

    selected = [url]
    if search_term:
        ranked: list[str] = []
        try:
            ranked = firecrawl_map(url, search_term, limit=max(max_pages * 2, max_pages))
        except Exception as map_e:
            logger.warning(f"⚠️ [scrape_url] Firecrawl map failed ({map_e}); falling back to pasted URL only")
            discovery["discovery_fallback"] = True
        discovery["discovered_urls"] = ranked[:10]
        if ranked:
            selected = select_urls(url, ranked, max_pages)
        else:
            # No usable discovery results -> pasted URL only (graceful)
            discovery["discovery_fallback"] = True

    pages: list[dict] = []
    scraped_urls: list[str] = []
    for u in selected:
        try:
            data = firecrawl_scrape(u)
            md = (data.get("markdown") or "").strip()
            if not md:
                logger.warning(f"⚠️ [scrape_url] Firecrawl scrape returned empty markdown for {u}")
                continue
            meta = data.get("metadata") or {}
            source_url = meta.get("sourceURL") or meta.get("url") or u
            pages.append({
                "url": source_url,
                "title": meta.get("title") or "",
                "markdown": md,
            })
            scraped_urls.append(source_url)
        except Exception as scrape_e:
            logger.warning(f"⚠️ [scrape_url] Firecrawl scrape failed for {u}: {scrape_e}")
            continue

    if not pages:
        raise RuntimeError("Firecrawl returned no readable content for the selected URLs")

    corpus = compose_topic_corpus(pages, max_bytes)
    return corpus, scraped_urls, discovery


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
        max_pages = int(os.getenv('URL_SCRAPE_MAX_PAGES', '4'))
        max_bytes = int(os.getenv('KNOWLEDGE_MAX_TEXT_BYTES', '200000'))

        # Topic-aware Firecrawl fetch: discover relevant pages for this
        # knowledge_type, scrape the top few (+ the pasted URL), build one corpus.
        logger.info(
            f"🎯 [scrape_url_to_markdown] Firecrawl fetch — url={url} "
            f"knowledge_type={knowledge_type} max_pages={max_pages}"
        )
        markdown, scraped_urls, discovery = build_topic_corpus(
            url, knowledge_type, max_pages=max_pages, max_bytes=max_bytes
        )
        scraper_source = 'firecrawl'
        headers = {
            'X-Scraper-Source': 'firecrawl',
            'X-Firecrawl-Search-Term': discovery.get('search_term'),
            'X-Firecrawl-Pages-Scraped': str(len(scraped_urls)),
            'X-Firecrawl-Discovery-Fallback': str(discovery.get('discovery_fallback')),
        }
        logger.info(
            f"✅ [scrape_url_to_markdown] Firecrawl corpus built — pages={len(scraped_urls)} "
            f"chars={len(markdown)} urls={scraped_urls}"
        )

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
            'source': 'firecrawl',
            'knowledge_type': knowledge_type,
            'search_term': discovery.get('search_term'),
            'discovery_used': discovery.get('discovery_used'),
            'discovery_fallback': discovery.get('discovery_fallback'),
            'discovered_urls': discovery.get('discovered_urls'),
            'scraped_urls': scraped_urls,
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
                raw_data = firecrawl_scrape(url, formats=['html'])
                raw_html = raw_data.get('html') or raw_data.get('rawHtml') or ''
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
                logger.warning(f"Saving raw HTML via Firecrawl failed: {_raw_e}")

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
                payload = None
                markdown_text = None
                try:
                    client = OpenAI(api_key=api_key)
                    prompt = build_knowledge_prompt(knowledge_type)
                    # Guardrail: limit size
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
