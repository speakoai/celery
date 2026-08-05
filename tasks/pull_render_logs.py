"""
Pull per-call server logs from the Render Logs API and attach them to
conversations.

Phase 2 of docs/plans/call-server-log-capture.md (speako-workspace).

speako-voice-ai stamps every log line with the Twilio CallSid and a per-call
sequence number, and marks the audio-timeline subset with a ``~ct~`` channel
prefix (see speako-voice-ai/modules/call_log_context.py). This task sweeps a
time window of that service's logs, splits the ``~ct~`` lines by call, writes
one gzipped JSONL artifact per call to R2, and merges a pointer into
``location_conversations.raw_metadata``.

Nothing here runs on the call path — Render already captured these lines
regardless of what the voice service did afterwards, which is why crashed calls
and calls rejected before the media stream are covered too.

Verified against the live API on 2026-08-05:

* ``GET /v1/logs`` requires ``ownerId`` + ``resource``; ``text`` is a LITERAL
  SUBSTRING matched mid-line (not regex — ``[TurnMetrics]`` matches that exact
  string, it is not a character class). The ``~ct~<call_sid>`` design rests on
  this.
* Rate limit is **30 requests/minute** for the logs endpoint specifically, with
  ``ratelimit-limit`` / ``-remaining`` / ``-reset`` headers on every response.
* Pagination walks BACKWARD: keep ``startTime`` fixed and set
  ``endTime = nextEndTime`` each iteration until ``hasMore`` is false. No
  duplicates observed across pages; we dedupe by log id anyway.
* ``type=app`` excludes uvicorn request logs server-side, and every app log
  carries an ``instance`` label.

Design constraints, all deliberate:

* **The task never sleeps.** Celery runs a single monolithic worker at low
  concurrency (see tasks/celery_app.py) — blocking a slot on a rate-limit
  backoff would starve availability regeneration. On 429, or on hitting the
  per-run request cap, the sweep stops cleanly and reports what it skipped; the
  next scheduled run picks up from the cursor.
* **Bounded memory.** Hard caps on requests, lines and window width, so a
  backlog after an outage cannot balloon the worker's RSS.
* **Idempotent.** R2 keys are deterministic and the raw_metadata merge is
  additive, so overlapping windows and re-runs are free.
* **No silent truncation.** Every cap that bites is named in the summary line.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from celery.utils.log import get_task_logger

from tasks.celery_app import app
from tasks.utils.publish_r2 import upload_call_log_to_r2
from tasks.utils.render_log_parse import (
    CHANNEL_MARKER,
    _build_artifact,
    _group_by_call,
)

logger = get_task_logger(__name__)

RENDER_LOGS_URL = "https://api.render.com/v1/logs"

# ── Bounds ───────────────────────────────────────────────────────────────────
PAGE_LIMIT = 100                # API maximum
MAX_REQUESTS_PER_RUN = 20       # of the 30/min budget, leaving headroom
MAX_LINES_PER_RUN = 20000       # ~3.5MB of text — bounds worker memory
MAX_WINDOW_MINUTES = 60         # a backlog can't drag in an unbounded window
DEFAULT_WINDOW_MINUTES = 15     # first run, or when the cursor is missing
INGEST_LAG_SECONDS = 60         # don't read the very edge; Render needs a moment
# Re-read margin: calls still in flight at the window edge get picked up again
# on the next sweep. Cheap because artifact writes are idempotent.
SETTLE_SECONDS = 240


class RateLimited(Exception):
    """Render returned 429. Stop the sweep; do not block a worker slot."""


# ── Render API ───────────────────────────────────────────────────────────────

def _fetch_page(session, api_key, owner_id, resource, start_time, end_time):
    params = {
        "ownerId": owner_id,
        "resource": resource,
        "startTime": start_time,
        "endTime": end_time,
        "limit": PAGE_LIMIT,
        "type": "app",
        "text": CHANNEL_MARKER,
    }
    resp = session.get(
        RENDER_LOGS_URL,
        params=params,
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        timeout=30,
    )
    if resp.status_code == 429:
        raise RateLimited(resp.headers.get("ratelimit-reset", "?"))
    resp.raise_for_status()
    return resp.json()


def _sweep(api_key, owner_id, resource, start_time, end_time):
    """Collect every ~ct~ line in [start_time, end_time].

    Returns (entries, stats). Pagination walks backward from end_time with
    start_time held fixed, which is the behaviour the live API exhibits.
    """
    session = requests.Session()
    entries, seen_ids = [], set()
    requests_made = 0
    cursor_end = end_time
    truncated = None

    while True:
        if requests_made >= MAX_REQUESTS_PER_RUN:
            truncated = "request_cap"
            break
        if len(entries) >= MAX_LINES_PER_RUN:
            truncated = "line_cap"
            break

        try:
            page = _fetch_page(session, api_key, owner_id, resource,
                               start_time, cursor_end)
        except RateLimited as exc:
            logger.warning(
                "[RenderLogs] 429 after %d requests (resets in %ss) — stopping "
                "this run; the next sweep resumes from the cursor",
                requests_made, exc,
            )
            truncated = "rate_limited"
            break

        requests_made += 1
        logs = page.get("logs") or []
        for entry in logs:
            if entry["id"] in seen_ids:
                continue
            seen_ids.add(entry["id"])
            entries.append(entry)

        if not page.get("hasMore") or not logs:
            break
        next_end = page.get("nextEndTime")
        if not next_end or next_end == cursor_end:
            break  # cursor not advancing — bail rather than spin
        cursor_end = next_end

    return entries, {"requests": requests_made, "truncated": truncated}


# ── Attach ───────────────────────────────────────────────────────────────────

def _attach_pointer(conn, call, r2_key):
    """Merge the artifact pointer into location_conversations.raw_metadata.

    Matches on raw_metadata->>'call_sid', which speako-voice-ai writes at
    finalize (Phase 3). Scoped by tenant + location so the planner can use the
    existing indexes rather than scanning on a JSONB expression — there is no
    index on raw_metadata->>'call_sid' and adding one would be a schema change.

    Additive: the `||` only sets the keys named here, leaving the rest of
    raw_metadata untouched.
    """
    if not call["tenant_id"]:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE location_conversations
               SET raw_metadata = COALESCE(raw_metadata, '{}'::jsonb) || %s::jsonb,
                   updated_at = CURRENT_TIMESTAMP
             WHERE tenant_id = %s
               AND location_id = %s
               AND raw_metadata->>'call_sid' = %s
            """,
            (
                json.dumps({
                    "log_r2_key": r2_key,
                    "log_lines": len(call["lines"]),
                    "log_incomplete": bool(call["gaps"]),
                    "log_pulled_at": datetime.now(timezone.utc).isoformat(),
                }),
                call["tenant_id"], call["location_id"], call["call_sid"],
            ),
        )
        return cur.rowcount


# ── Cursor ───────────────────────────────────────────────────────────────────

def _cursor_key(resource):
    return f"render_logs:cursor:{resource}"


def _read_cursor(resource):
    """Last swept timestamp, or None. A missing/broken cursor is not an error —
    the sweep just falls back to a fixed window."""
    try:
        import redis
        url = os.getenv("REDIS_URL")
        if not url:
            return None
        value = redis.Redis.from_url(url, decode_responses=True).get(_cursor_key(resource))
        return value or None
    except Exception as exc:
        logger.warning("[RenderLogs] cursor read failed (%s) — using default window", exc)
        return None


def _write_cursor(resource, value):
    try:
        import redis
        url = os.getenv("REDIS_URL")
        if not url:
            return
        # Tiny single key; expires on its own so it can never become litter in
        # the shared availability cache.
        redis.Redis.from_url(url, decode_responses=True).set(
            _cursor_key(resource), value, ex=7 * 24 * 3600,
        )
    except Exception as exc:
        logger.warning("[RenderLogs] cursor write failed (%s) — next run re-reads", exc)


# ── Task ─────────────────────────────────────────────────────────────────────

@app.task(bind=True, name="tasks.pull_render_logs", acks_late=True)
def pull_render_logs(self, service_id=None, is_dev=False, window_minutes=None,
                     start_time=None, end_time=None):
    """
    Sweep one window of a voice-ai service's logs and attach per-call artifacts.

    Args:
        service_id: Render service id. Defaults to RENDER_VOICE_AZURE_SERVICE_ID
            (or ..._DEV when is_dev).
        is_dev: read the dev service and write the dev R2 bucket.
        window_minutes: override the sweep width (bounded by MAX_WINDOW_MINUTES).
        start_time / end_time: explicit ISO window, for backfilling one call.

    Safe to run repeatedly over overlapping windows: R2 keys are deterministic
    and the raw_metadata merge is additive.
    """
    api_key = os.getenv("RENDER_API_KEY")
    owner_id = os.getenv("RENDER_OWNER_ID")
    if not service_id:
        service_id = os.getenv(
            "RENDER_VOICE_AZURE_SERVICE_ID_DEV" if is_dev
            else "RENDER_VOICE_AZURE_SERVICE_ID"
        )
    if not all([api_key, owner_id, service_id]):
        logger.error(
            "[RenderLogs] not configured — need RENDER_API_KEY, RENDER_OWNER_ID "
            "and a service id (have key=%s owner=%s service=%s)",
            bool(api_key), bool(owner_id), bool(service_id),
        )
        return {"success": False, "error": "not_configured"}

    # ── Window ──
    now = datetime.now(timezone.utc)
    if not end_time:
        end_time = (now - timedelta(seconds=INGEST_LAG_SECONDS)).isoformat()
    if not start_time:
        cursor = _read_cursor(service_id)
        if cursor:
            start_time = cursor
        else:
            minutes = window_minutes or DEFAULT_WINDOW_MINUTES
            start_time = (now - timedelta(minutes=minutes)).isoformat()

    # Clamp: after an outage the cursor could be days behind, and one run must
    # never try to drag all of that into memory.
    earliest = now - timedelta(minutes=MAX_WINDOW_MINUTES)
    if datetime.fromisoformat(start_time.replace("Z", "+00:00")) < earliest:
        logger.warning(
            "[RenderLogs] cursor %s older than the %dmin cap — clamping; "
            "earlier logs stay in Render until retention expires",
            start_time, MAX_WINDOW_MINUTES,
        )
        start_time = earliest.isoformat()

    window = {"start": start_time, "end": end_time}
    entries, stats = _sweep(api_key, owner_id, service_id, start_time, end_time)
    calls = _group_by_call(entries)

    # ── Persist ──
    uploaded = attached = skipped_inflight = failed = 0
    gap_calls = 0
    conn = None
    try:
        db_url = os.getenv("DATABASE_URL")
        conn = psycopg2.connect(db_url) if db_url else None

        for call_sid, call in calls.items():
            # A call still in progress at the window edge is left for the next
            # sweep, which re-reads SETTLE_SECONDS of overlap.
            if not call["complete"] and call["lines"]:
                last_ts = call["lines"][-1]["ts"] or ""
                try:
                    last = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    if (now - last).total_seconds() < SETTLE_SECONDS:
                        skipped_inflight += 1
                        continue
                except ValueError:
                    pass

            try:
                call_date = (call["lines"][0]["ts"] or now.isoformat())[:10]
                r2_key = upload_call_log_to_r2(
                    tenant_id=call["tenant_id"],
                    location_id=call["location_id"],
                    call_sid=call_sid,
                    call_date=call_date,
                    gz_bytes=_build_artifact(call, service_id, window),
                    extra_metadata={"lines": len(call["lines"]),
                                    "incomplete": bool(call["gaps"])},
                    use_dev=is_dev,
                )
                uploaded += 1
                if call["gaps"]:
                    gap_calls += 1
                if conn and _attach_pointer(conn, call, r2_key):
                    attached += 1
            except Exception as exc:
                failed += 1
                logger.error("[RenderLogs] %s failed: %s", call_sid, exc)

        if conn:
            conn.commit()
    finally:
        if conn:
            conn.close()

    # Advance the cursor with overlap so a call spanning the boundary is
    # re-read rather than half-captured. Not advanced past a truncated sweep.
    if not stats["truncated"]:
        _write_cursor(
            service_id,
            (datetime.fromisoformat(end_time.replace("Z", "+00:00"))
             - timedelta(seconds=SETTLE_SECONDS)).isoformat(),
        )

    summary = {
        "success": True,
        "service": service_id,
        "env": "dev" if is_dev else "prod",
        "window": window,
        "api_requests": stats["requests"],
        "lines": len(entries),
        "calls_seen": len(calls),
        "uploaded": uploaded,
        "attached": attached,
        "skipped_inflight": skipped_inflight,
        "calls_with_gaps": gap_calls,
        "failed": failed,
        "truncated": stats["truncated"],
    }
    # One line per sweep, deliberately greppable: with Phase 5 monitoring out of
    # scope, this is the only way to notice the pipeline has stopped working.
    logger.info("[RenderLogs] sweep %s", json.dumps(summary))
    return summary
