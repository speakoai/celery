"""
Chat maintenance sweep — runs with summarize_chat_sessions on the existing 30-minute cron.

1. purge_chat_client_metadata — retention for web-widget visitor network identifiers.
   speako-web stores each widget visitor message's network/device context on that row:
   chat_messages.metadata.client = {ip, forwarded_for, user_agent, page_url, test_mode?}.
   IP / forwarded-for / user agent are stripped once a message is older than the tenant's chat
   retention period when chat_settings.retention_enabled, else CHAT_CLIENT_META_RETENTION_DAYS
   (default 90, Owner-confirmed 2026-11-04). page_url and the transcript itself are kept.

2. apply_chat_retention — per-tenant chat history retention (chat_settings.retention_enabled,
   retention_days, default 180; Owner R1/R2 2026-11-04, all text channels). Deletes messages
   older than N days, then sessions left empty and idle > N days (their visitor_metadata holds
   captured name/phone/email), and AI memory summaries (customer_episodes) older than N days.
   Sessions a team member is handling are never deleted. Bookings and consent records are untouched.

4. purge_knowledge_revisions — knowledge version history is kept 18 months (Owner K3,
   docs/plans/knowledge-approval.md). Older revisions are deleted, never the current live one
   or a pending draft, so every item keeps a restorable baseline.

3. release_stale_takeovers — asks speako-web to hand expired human take-overs back to the AI
   (no staff reply for 30 min, or the tenant switched take-over off). speako-web does it so the
   visitor notice reuses its channel adapters. Uses SPEAKO_WEB_BASE_URL + BOOKING_WEBHOOK_SECRET,
   the same pair the reminder emails use.

All idempotent and batched; logs counts only — never values. This project does NOT use Celery
Beat. See docs/plans/unified-inbox.md (speako-workspace).
"""
import os

import requests
from celery.utils.log import get_task_logger

from tasks.celery_app import app
from .utils.task_db import _get_conn

logger = get_task_logger(__name__)

RETENTION_DAYS = int(os.getenv("CHAT_CLIENT_META_RETENTION_DAYS", "90"))
BATCH_SIZE = 5000
MAX_BATCHES = 200  # 1M rows per run at most; the next run continues


def _batched(conn, sql, params) -> int:
    total = 0
    for _ in range(MAX_BATCHES):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            n = cur.rowcount
        conn.commit()
        total += n
        if n < BATCH_SIZE:
            break
    return total


@app.task(bind=True, name="tasks.purge_chat_client_metadata.purge_chat_client_metadata")
def purge_chat_client_metadata(self):
    conn = _get_conn()
    try:
        total = _batched(
            conn,
            """
            UPDATE chat_messages
               SET metadata = metadata #- '{client,ip}'
                                       #- '{client,forwarded_for}'
                                       #- '{client,user_agent}'
             WHERE message_id IN (
                   SELECT m.message_id
                     FROM chat_messages m
                     LEFT JOIN chat_settings cs ON cs.tenant_id = m.tenant_id
                    WHERE m.metadata->'client' ? 'ip'
                      AND m.created_at < now() - make_interval(days =>
                            CASE WHEN cs.retention_enabled THEN cs.retention_days ELSE %s END)
                    LIMIT %s)
            """,
            (RETENTION_DAYS, BATCH_SIZE),
        )
        logger.info("[purge_chat_client_metadata] stripped ip/ua on %s message(s)", total)
        return {"purged": total, "default_retention_days": RETENTION_DAYS}
    except Exception:
        conn.rollback()
        logger.exception("[purge_chat_client_metadata] failed")
        raise
    finally:
        conn.close()


@app.task(bind=True, name="tasks.purge_chat_client_metadata.apply_chat_retention")
def apply_chat_retention(self):
    conn = _get_conn()
    summary = {"tenants": 0, "messages": 0, "sessions": 0, "episodes": 0}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tenant_id, retention_days FROM chat_settings WHERE retention_enabled")
            tenants = cur.fetchall()
        conn.commit()

        for tenant_id, days in tenants:
            msgs = _batched(
                conn,
                """
                DELETE FROM chat_messages
                 WHERE message_id IN (
                       SELECT message_id FROM chat_messages
                        WHERE tenant_id = %s AND created_at < now() - make_interval(days => %s)
                        LIMIT %s)
                """,
                (tenant_id, days, BATCH_SIZE),
            )
            with conn.cursor() as cur:
                if msgs:
                    # Keep the per-session counters honest after deleting history.
                    cur.execute(
                        """
                        UPDATE chat_sessions s
                           SET message_count = COALESCE(
                                 (SELECT COUNT(*) FROM chat_messages m WHERE m.session_id = s.session_id), 0)
                         WHERE s.tenant_id = %s
                        """,
                        (tenant_id,),
                    )
                cur.execute(
                    """
                    DELETE FROM chat_sessions s
                     WHERE s.tenant_id = %s
                       AND s.updated_at < now() - make_interval(days => %s)
                       AND s.handled_since IS NULL
                       AND NOT EXISTS (SELECT 1 FROM chat_messages m WHERE m.session_id = s.session_id)
                    """,
                    (tenant_id, days),
                )
                sessions = cur.rowcount
                cur.execute(
                    "DELETE FROM customer_episodes WHERE tenant_id = %s AND created_at < now() - make_interval(days => %s)",
                    (tenant_id, days),
                )
                episodes = cur.rowcount
            conn.commit()

            summary["tenants"] += 1
            summary["messages"] += msgs
            summary["sessions"] += sessions
            summary["episodes"] += episodes
            if msgs or sessions or episodes:
                logger.info(
                    "[apply_chat_retention] tenant=%s days=%s deleted messages=%s sessions=%s episodes=%s",
                    tenant_id, days, msgs, sessions, episodes,
                )
        return summary
    except Exception:
        conn.rollback()
        logger.exception("[apply_chat_retention] failed (%s)", summary)
        raise
    finally:
        conn.close()


@app.task(bind=True, name="tasks.purge_chat_client_metadata.purge_knowledge_revisions")
def purge_knowledge_revisions(self):
    conn = _get_conn()
    try:
        total = _batched(
            conn,
            """
            DELETE FROM knowledge_revisions
             WHERE revision_id IN (
                   SELECT revision_id FROM knowledge_revisions
                    WHERE created_at < now() - interval '18 months'
                      AND state NOT IN ('live', 'pending')
                    LIMIT %s)
            """,
            (BATCH_SIZE,),
        )
        if total:
            logger.info("[purge_knowledge_revisions] deleted %s revision(s) older than 18 months", total)
        return {"deleted": total}
    except Exception:
        conn.rollback()
        logger.exception("[purge_knowledge_revisions] failed")
        raise
    finally:
        conn.close()


@app.task(bind=True, name="tasks.purge_chat_client_metadata.release_stale_takeovers")
def release_stale_takeovers(self):
    base = (os.getenv("SPEAKO_WEB_BASE_URL") or "").rstrip("/")
    secret = os.getenv("BOOKING_WEBHOOK_SECRET")
    if not base or not secret:
        logger.warning("[release_stale_takeovers] skipped: SPEAKO_WEB_BASE_URL / BOOKING_WEBHOOK_SECRET not set")
        return {"skipped": True}
    try:
        r = requests.post(
            f"{base}/api/webhooks/chat-maintenance",
            headers={"x-webhook-secret": secret},
            json={},
            timeout=60,
        )
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        logger.info("[release_stale_takeovers] HTTP %s released=%s", r.status_code, body.get("released"))
        return {"status": r.status_code, "released": body.get("released")}
    except Exception as e:
        logger.warning("[release_stale_takeovers] call failed: %s", e)
        return {"error": str(e)}
