"""
Dispatch script: drain the speako-web integration outbox (Phase 3, Slice 4).

Durability net for the calendar/online-meeting integration. Booking side effects
(Google Calendar events, Zoom meetings) are enqueued as `integration_outbox` rows
and normally processed by a best-effort post-commit kick in speako-web. If a row
requeues (transient Google/Zoom error, token refresh, provider-ordering, etc.),
nothing else retries it — so this cron periodically POSTs the shared internal
drain endpoint, which claims due rows and dispatches them.

The integration ADAPTERS live in speako-web (Node), not celery
(D-ADAPTER-LOCATION=A) — so this script does NOT touch the DB or run any adapter
logic itself. It is a thin scheduler that pokes the web endpoint. Deliberately
does NOT import tasks.celery_app, to stay lightweight (no heavy task imports).

Endpoint (speako-web):
    POST {SPEAKO_WEB_BASE_URL}/api/integrations/outbox/process
    x-api-key: {API_SECRET_KEY}          # web validates this against its CELERY_API_KEY
    { "limit": <int> }

IDEMPOTENT: the endpoint claims rows with FOR UPDATE SKIP LOCKED and each artifact
is idempotency-keyed, so overlapping runs and repeated calls are safe.

Env:
    SPEAKO_WEB_BASE_URL   e.g. https://dev.speako.ai (prod: https://speako.ai)
    API_SECRET_KEY        shared web<->celery key (same value web stores as CELERY_API_KEY)
    OUTBOX_DRAIN_LIMIT    optional, default 25 (rows per run)

Usage:
    PYTHONPATH=. python dispatch/process_integration_outbox_dispatch.py
"""

import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

WEB_BASE_URL = (os.getenv("SPEAKO_WEB_BASE_URL") or "").rstrip("/")
API_KEY = os.getenv("API_SECRET_KEY")
try:
    LIMIT = max(1, min(100, int(os.getenv("OUTBOX_DRAIN_LIMIT", "25"))))
except ValueError:
    LIMIT = 25


def main() -> int:
    if not WEB_BASE_URL or not API_KEY:
        print(
            "[process_integration_outbox] SPEAKO_WEB_BASE_URL or API_SECRET_KEY not set — skipping",
            flush=True,
        )
        return 0  # not an error: env simply not configured yet

    url = f"{WEB_BASE_URL}/api/integrations/outbox/process"
    try:
        resp = requests.post(
            url,
            json={"limit": LIMIT},
            headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
            timeout=90,
        )
    except requests.RequestException as e:
        # Network/timeout — log and fail so Render marks the run failed (visible),
        # but the next scheduled run will retry. Never print the api key.
        print(f"[process_integration_outbox] request error: {e.__class__.__name__}", flush=True)
        return 1

    body = (resp.text or "")[:200]
    print(f"[process_integration_outbox] {resp.status_code} {body}", flush=True)
    return 0 if resp.ok else 1


if __name__ == "__main__":
    sys.exit(main())
