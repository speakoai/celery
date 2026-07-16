"""
Prospect scrape pilot (Firecrawl).

Crawls a business website (<= page_limit pages) via Firecrawl, extracts a
structured "business profile" + long-form narrative with OpenAI, then:

  1. fills tenant_info + location_info basics (FILL-ONLY-EMPTY — never
     overwrites existing values, so re-runs and normal-onboarding graduation
     are safe),
  2. upserts the narrative into tenant_integration_params as the
     speako/knowledge/custom_message knowledge,
  3. stores the raw markdown corpus in R2 as a task artifact (audit/debug),
  4. chains a native agent republish so the very first call to the agent
     already knows the business.

Plan: speako-workspace docs/plans/prospect-scrape-pilot.md
Anti-hallucination: extracted phone/email/socials must literally appear in the
scraped corpus or they are dropped before any DB write.
"""

import os
import re
import time
import json
from datetime import datetime

import requests
import psycopg2

from celery.utils.log import get_task_logger
from tasks.celery_app import app

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

from .utils.knowledge_utils import build_scrape_artifact_paths, parse_model_json_output
from .utils.task_db import (
    mark_task_running,
    mark_task_failed,
    mark_task_succeeded,
    record_task_artifact,
    upsert_tenant_integration_param,
)

logger = get_task_logger(__name__)

FIRECRAWL_BASE = os.getenv("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev")
DEFAULT_PAGE_LIMIT = int(os.getenv("BUSINESS_PROFILE_PAGE_LIMIT", "10"))
CORPUS_CHAR_CAP = int(os.getenv("BUSINESS_PROFILE_CORPUS_CHAR_CAP", "150000"))
NARRATIVE_CHAR_CAP = int(os.getenv("BUSINESS_PROFILE_NARRATIVE_CHAR_CAP", "12000"))


def _get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url)


def _get_r2_client():
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    endpoint = os.getenv("R2_ENDPOINT_URL")
    bucket = os.getenv("R2_BUCKET_NAME")
    if not all([access_key, secret_key, endpoint, bucket]):
        return None, None
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",  # R2 rejects AWS region names leaking in from env
    )
    return client, bucket


# ---------------------------------------------------------------------------
# Firecrawl
# ---------------------------------------------------------------------------

def _firecrawl_headers() -> dict:
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise RuntimeError("FIRECRAWL_API_KEY not set")
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def fetch_site_corpus(url: str, page_limit: int, *, poll_interval_s: float = 3.0,
                      total_timeout_s: float = 300.0) -> list[dict]:
    """Crawl the site via Firecrawl and return [{url, title, markdown}, ...]."""
    headers = _firecrawl_headers()
    submit = requests.post(
        f"{FIRECRAWL_BASE}/v1/crawl",
        headers=headers,
        json={
            "url": url,
            "limit": page_limit,
            "scrapeOptions": {"formats": ["markdown"], "onlyMainContent": True},
        },
        timeout=30,
    )
    submit.raise_for_status()
    job = submit.json()
    job_id = job.get("id")
    if not job_id:
        raise RuntimeError(f"Firecrawl crawl submit returned no job id: {json.dumps(job)[:300]}")

    deadline = time.time() + total_timeout_s
    pages: list[dict] = []
    status_url = f"{FIRECRAWL_BASE}/v1/crawl/{job_id}"
    while True:
        if time.time() > deadline:
            raise RuntimeError(f"Firecrawl crawl timed out after {total_timeout_s}s (job {job_id})")
        resp = requests.get(status_url, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        status = payload.get("status")
        if status == "failed":
            raise RuntimeError(f"Firecrawl crawl failed (job {job_id}): {json.dumps(payload)[:300]}")
        if status == "completed":
            data = payload.get("data") or []
            for item in data:
                md = (item.get("markdown") or "").strip()
                meta = item.get("metadata") or {}
                if md:
                    pages.append({
                        "url": meta.get("sourceURL") or meta.get("url") or url,
                        "title": meta.get("title") or "",
                        "markdown": md,
                    })
            # Follow pagination if Firecrawl split the result set
            next_url = payload.get("next")
            if next_url:
                status_url = next_url
                continue
            return pages
        time.sleep(poll_interval_s)


def compose_corpus(pages: list[dict], char_cap: int = CORPUS_CHAR_CAP) -> str:
    parts = []
    total = 0
    for p in pages:
        block = f"\n\n## PAGE: {p['title']}\nURL: {p['url']}\n\n{p['markdown']}"
        if total + len(block) > char_cap:
            block = block[: max(0, char_cap - total)]
        parts.append(block)
        total += len(block)
        if total >= char_cap:
            break
    return "".join(parts).strip()


# ---------------------------------------------------------------------------
# OpenAI extraction
# ---------------------------------------------------------------------------

EXTRACTION_PROMPT = """You are extracting a business profile from the scraped website content below.

Return STRICT JSON only (no code fences, no commentary) with this exact shape:
{
  "basics": {
    "business_phone": string|null,      // main public phone, keep formatting as printed on the site
    "contact_email": string|null,
    "street_address": string|null,      // full street address of the (primary) location
    "opening_hours": string|null,       // concise free text, e.g. "Mon-Fri 9am-5pm; Sat 10am-2pm"
    "tagline": string|null,             // short slogan if the site has one
    "description": string|null,         // 2-3 sentence factual description of the business
    "short_description": string|null,   // 1 sentence version
    "instagram": string|null,           // full URL
    "facebook": string|null             // full URL
  },
  "narrative": string   // see rules below
}

Rules:
- basics: use ONLY values that literally appear in the content. If a value is
  not present, use null. Never invent or guess.
- narrative: a thorough plain-text briefing (up to ~1500 words) for an AI
  phone receptionist who will answer calls for this business. Cover: what the
  business does and offers (services/products/menu highlights with prices if
  shown), who it serves, location/parking/access notes, policies (bookings,
  cancellations, payments), FAQs found on the site, tone/brand personality,
  and anything else a receptionist should know. Only include facts from the
  content. Write in clear prose or short bullet lines, no markdown headers.

WEBSITE CONTENT:
"""


def extract_profile(corpus: str) -> dict:
    if OpenAI is None:
        raise RuntimeError("openai SDK not available")
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    model = os.getenv("BUSINESS_PROFILE_OPENAI_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": EXTRACTION_PROMPT + corpus}],
        temperature=0.2,
    )
    text = (resp.choices[0].message.content or "").strip()
    parsed, _raw = parse_model_json_output(text)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"OpenAI profile extraction returned unparsable output: {text[:200]}")
    basics = parsed.get("basics") or {}
    narrative = (parsed.get("narrative") or "").strip()[:NARRATIVE_CHAR_CAP]
    return {"basics": basics if isinstance(basics, dict) else {}, "narrative": narrative}


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def validate_basics(basics: dict, corpus: str) -> tuple[dict, list[str]]:
    """Drop extracted values that don't literally appear in the corpus.

    Compositional fields (description/tagline/hours) are summaries — kept as-is.
    Identity fields (phone/email/socials/address) must be grounded in the text.
    """
    lower = corpus.lower()
    corpus_digits = _digits(corpus)
    cleaned: dict = {}
    dropped: list[str] = []

    for key, value in (basics or {}).items():
        if value is None or not str(value).strip():
            continue
        value = str(value).strip()
        if key == "business_phone":
            d = _digits(value)
            ok = len(d) >= 7 and d in corpus_digits
        elif key == "contact_email":
            ok = "@" in value and value.lower() in lower
        elif key in ("instagram", "facebook"):
            handle = value.lower().rstrip("/").split("/")[-1]
            ok = ("instagram" in value.lower() or "facebook" in value.lower() or "fb.com" in value.lower()) \
                and bool(handle) and handle in lower
        elif key == "street_address":
            tokens = [t for t in re.split(r"[\s,]+", value.lower()) if len(t) > 2]
            hits = sum(1 for t in tokens if t in lower)
            ok = len(tokens) > 0 and hits >= max(2, len(tokens) // 2)
        else:
            ok = True  # compositional fields
        if ok:
            cleaned[key] = value
        else:
            dropped.append(key)
    return cleaned, dropped


# ---------------------------------------------------------------------------
# DB writes (fill-only-empty)
# ---------------------------------------------------------------------------

def fill_info_tables(tenant_id: int, location_id: int, url: str, basics: dict) -> dict:
    """Upsert tenant_info + location_info, only filling NULL/empty columns.

    Returns {"tenant_info": [cols filled], "location_info": [cols filled]}.
    """
    tenant_values = {
        "website_url": url,
        "contact_phone": basics.get("business_phone"),
        "contact_email": basics.get("contact_email"),
        "description": basics.get("description"),
        "tagline": basics.get("tagline"),
        "instagram": basics.get("instagram"),
        "facebook": basics.get("facebook"),
    }
    location_values = {
        "website_url": url,
        "phone_with_country_code": basics.get("business_phone"),
        "email": basics.get("contact_email"),
        "address": basics.get("street_address"),
        "opening_hours": basics.get("opening_hours"),
        "short_description": basics.get("short_description"),
    }

    filled = {"tenant_info": [], "location_info": []}
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # tenant_info — single-row-per-tenant upsert
                cur.execute("SELECT 1 FROM tenant_info WHERE tenant_id = %s", (tenant_id,))
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO tenant_info (tenant_id) VALUES (%s) ON CONFLICT DO NOTHING", (tenant_id,))
                for col, val in tenant_values.items():
                    if val is None:
                        continue
                    cur.execute(
                        f"""UPDATE tenant_info
                               SET {col} = %s, updated_at = NOW()
                             WHERE tenant_id = %s
                               AND ({col} IS NULL OR {col} = '')""",
                        (val, tenant_id),
                    )
                    if cur.rowcount:
                        filled["tenant_info"].append(col)

                # location_info — row per (tenant, location)
                cur.execute(
                    "SELECT 1 FROM location_info WHERE tenant_id = %s AND location_id = %s",
                    (tenant_id, location_id),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        "INSERT INTO location_info (tenant_id, location_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (tenant_id, location_id),
                    )
                for col, val in location_values.items():
                    if val is None:
                        continue
                    cur.execute(
                        f"""UPDATE location_info
                               SET {col} = %s, updated_at = NOW()
                             WHERE tenant_id = %s AND location_id = %s
                               AND ({col} IS NULL OR {col} = '')""",
                        (val, tenant_id, location_id),
                    )
                    if cur.rowcount:
                        filled["location_info"].append(col)
    finally:
        conn.close()
    return filled


def get_existing_knowledge_param_id(tenant_id: int, location_id: int, param_code: str) -> int | None:
    """upsert_tenant_integration_param only UPDATEs when given a param_id —
    without one it plain-INSERTs and re-runs would hit uq_tip_knowledge_speako."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT param_id FROM tenant_integration_params
                    WHERE tenant_id = %s AND location_id = %s
                      AND provider = 'speako' AND service = 'knowledge' AND param_code = %s
                    LIMIT 1""",
                (tenant_id, location_id, param_code),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def get_location_provider(tenant_id: int, location_id: int) -> str:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT voice_ai_provider FROM locations WHERE tenant_id = %s AND location_id = %s",
                (tenant_id, location_id),
            )
            row = cur.fetchone()
            return (row[0] if row and row[0] else "azure")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# The task
# ---------------------------------------------------------------------------

@app.task(bind=True, name="tasks.scrape_business_profile")
def scrape_business_profile(self, *, tenant_id: str, location_id: str, url: str,
                            speako_task_id: str | None = None,
                            page_limit: int | None = None,
                            trigger_publish: bool = True) -> dict:
    start_ts = time.time()
    started_at = datetime.utcnow().isoformat() + "Z"
    page_limit = int(page_limit or DEFAULT_PAGE_LIMIT)

    def _job(extra: dict | None = None) -> dict:
        out = {
            "task_id": self.request.id,
            "speako_task_id": speako_task_id,
            "started_at": started_at,
            "completed_at": datetime.utcnow().isoformat() + "Z",
            "duration_ms": int((time.time() - start_ts) * 1000),
        }
        if extra:
            out.update(extra)
        return out

    def _fail(code: str, message: str) -> dict:
        logger.error(f"[scrape_business_profile] {code}: {message}")
        if speako_task_id:
            try:
                mark_task_failed(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                 error_code=code, error_message=message,
                                 details={"url": url}, actor="celery")
            except Exception as db_e:
                logger.warning(f"mark_task_failed failed: {db_e}")
        return {"success": False, "error": message, "error_code": code, "url": url, "job": _job()}

    logger.info(
        f"[scrape_business_profile] Started — tenant={tenant_id} location={location_id} "
        f"url={url} page_limit={page_limit} speako_task_id={speako_task_id}"
    )

    if speako_task_id:
        try:
            mark_task_running(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                              message="Business profile scrape started",
                              details={"url": url, "page_limit": page_limit}, actor="celery")
        except Exception as db_e:
            logger.warning(f"mark_task_running failed: {db_e}")

    # 1. Crawl
    try:
        pages = fetch_site_corpus(url, page_limit)
    except Exception as e:
        return _fail("firecrawl_failed", f"Firecrawl crawl failed: {e}")
    if not pages:
        return _fail("empty_crawl", "Firecrawl returned no readable pages")
    corpus = compose_corpus(pages)
    if len(corpus) < 200:
        return _fail("corpus_too_small", f"Scraped content too small ({len(corpus)} chars)")

    # 2. R2 artifact (best-effort)
    artifact_uri = None
    try:
        r2, bucket = _get_r2_client()
        if r2 is not None:
            paths = build_scrape_artifact_paths(str(tenant_id), str(location_id), url)
            key = paths.get("markdown_key") or paths.get("md_key") or (
                f"scrapes/{tenant_id}/{location_id}/business_profile.md"
            )
            key = key.replace(".md", ".business_profile.md")
            r2.put_object(Bucket=bucket, Key=key, Body=corpus.encode("utf-8"),
                          ContentType="text/markdown")
            artifact_uri = f"r2://{bucket}/{key}"
            if speako_task_id:
                record_task_artifact(task_id=str(speako_task_id), kind="scraped_markdown",
                                     uri=artifact_uri, bucket=bucket, object_key=key,
                                     mime_type="text/markdown", size_bytes=len(corpus))
    except Exception as e:
        logger.warning(f"[scrape_business_profile] R2 artifact store failed (non-fatal): {e}")

    # 3. Extract + validate
    try:
        profile = extract_profile(corpus)
    except Exception as e:
        return _fail("extraction_failed", f"OpenAI extraction failed: {e}")
    basics, dropped = validate_basics(profile["basics"], corpus)
    narrative = profile["narrative"]
    if not narrative:
        return _fail("empty_narrative", "Extraction produced no narrative")

    # 4. DB writes
    try:
        filled = fill_info_tables(int(tenant_id), int(location_id), url, basics)
    except Exception as e:
        return _fail("info_write_failed", f"tenant/location info write failed: {e}")

    try:
        existing_param_id = get_existing_knowledge_param_id(int(tenant_id), int(location_id), "custom_message")
        param_dict = {
            "tenant_id": int(tenant_id),
            "location_id": int(location_id),
            "provider": "speako",
            "service": "knowledge",
            "param_code": "custom_message",
            "param_kind": "text",
        }
        if existing_param_id:
            param_dict["param_id"] = existing_param_id
        param_id = upsert_tenant_integration_param(
            tenant_integration_param=param_dict,
            value_text=narrative,
            ai_description="Business profile scraped from the tenant's website (prospect scrape pilot)",
        )
    except Exception as e:
        return _fail("knowledge_write_failed", f"custom_message knowledge write failed: {e}")

    # 5. Chain republish so the agent knows the business on the next call
    publish_dispatched = False
    if trigger_publish:
        try:
            provider = get_location_provider(int(tenant_id), int(location_id))
            from tasks.publish_native_agent import publish_native_agent
            publish_native_agent.delay(
                tenant_id=str(tenant_id),
                location_id=str(location_id),
                publish_job_id="0",
                provider=provider,
            )
            publish_dispatched = True
        except Exception as e:
            logger.warning(f"[scrape_business_profile] republish dispatch failed (non-fatal): {e}")

    summary = {
        "pages_crawled": len(pages),
        "corpus_chars": len(corpus),
        "narrative_chars": len(narrative),
        "basics_extracted": sorted(basics.keys()),
        "basics_dropped_unverified": dropped,
        "filled": filled,
        "knowledge_param_id": param_id,
        "artifact": artifact_uri,
        "publish_dispatched": publish_dispatched,
    }

    if speako_task_id:
        try:
            mark_task_succeeded(task_id=str(speako_task_id), celery_task_id=str(self.request.id),
                                details={"url": url}, output=summary, actor="celery")
        except Exception as db_e:
            logger.warning(f"mark_task_succeeded failed: {db_e}")

    logger.info(f"[scrape_business_profile] Done — {json.dumps(summary)[:500]}")
    return {"success": True, "url": url, **summary, "job": _job()}
