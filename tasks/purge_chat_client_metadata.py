"""
purge_chat_client_metadata — retention for web-widget visitor network identifiers.

speako-web stores each widget visitor message's network/device context on that row:
chat_messages.metadata.client = {ip, forwarded_for, user_agent, page_url, test_mode?}.
IP, forwarded-for and user agent are personal data (HK PDPO DPP2 / GDPR storage limitation),
so they are stripped once a message is older than CHAT_CLIENT_META_RETENTION_DAYS (default 90,
Owner-confirmed 2026-11-04). page_url and the transcript itself are kept.

Idempotent: rows already stripped no longer match `metadata->'client' ? 'ip'`. Works in batches
so a large backlog never holds one long transaction. Logs counts only — never values.

Dispatched alongside summarize_chat_sessions by the existing 30-minute Render cron
(dispatch/summarize_chat_sessions_dispatch.py). This project does NOT use Celery Beat.
See docs/plans/unified-inbox.md (speako-workspace).
"""
import os

from celery.utils.log import get_task_logger

from tasks.celery_app import app
from .utils.task_db import _get_conn

logger = get_task_logger(__name__)

RETENTION_DAYS = int(os.getenv("CHAT_CLIENT_META_RETENTION_DAYS", "90"))
BATCH_SIZE = 5000
MAX_BATCHES = 200  # 1M rows per run at most; the next run continues


@app.task(bind=True, name="tasks.purge_chat_client_metadata.purge_chat_client_metadata")
def purge_chat_client_metadata(self):
    conn = _get_conn()
    total = 0
    try:
        for _ in range(MAX_BATCHES):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE chat_messages
                       SET metadata = metadata #- '{client,ip}'
                                               #- '{client,forwarded_for}'
                                               #- '{client,user_agent}'
                     WHERE message_id IN (
                           SELECT message_id
                             FROM chat_messages
                            WHERE metadata->'client' ? 'ip'
                              AND created_at < now() - make_interval(days => %s)
                            LIMIT %s)
                    """,
                    (RETENTION_DAYS, BATCH_SIZE),
                )
                n = cur.rowcount
            conn.commit()
            total += n
            if n < BATCH_SIZE:
                break
        logger.info("[purge_chat_client_metadata] stripped ip/ua on %s message(s) older than %s days", total, RETENTION_DAYS)
        return {"purged": total, "retention_days": RETENTION_DAYS}
    except Exception:
        conn.rollback()
        logger.exception("[purge_chat_client_metadata] failed after %s row(s)", total)
        raise
    finally:
        conn.close()
