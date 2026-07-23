"""
embed_knowledge_param — param-scoped (re)embed of a SINGLE saved knowledge row
into the pgvector `knowledge_chunks` table, driven by the dashboard "Save = Live"
flow (speako-web knowledge editor Save / delete).

Unlike `sync_speako_data` (which REGENERATES structured knowledge from the DB and
OVERWRITES `tenant_integration_params.value_text`), this task embeds the *saved
freeform `value_text` as-is* for one `(tenant_id, location_id, param_code)`. It
NEVER writes `tenant_integration_params` — so it cannot clobber the user's edit.

It reads current DB state and reconciles `knowledge_chunks`:
  - row present, in-scope status, non-empty value_text -> chunk + embed
    (delete-then-reinsert, handled by chunk_and_embed_knowledge)
  - row missing / status 'removed' / empty value_text  -> purge stale chunks

Works for ALL param_codes (business_info, custom_message, custom docs, ...)
because chunk_and_embed_knowledge is content-agnostic. Skips NON_CONTENT_PARAM_CODES.

Invoked on demand from speako-web via POST /api/knowledge/embed-param — NOT Celery
Beat (this project does not use it).
"""
from celery.utils.log import get_task_logger
from psycopg2.extras import RealDictCursor

from tasks.celery_app import app
from .utils.task_db import _get_conn
from .utils.knowledge_utils import chunk_and_embed_knowledge, NON_CONTENT_PARAM_CODES

logger = get_task_logger(__name__)

# Statuses whose value_text is considered live knowledge worth embedding.
_LIVE_STATUSES = ("configured", "published")


def _purge_chunks(conn, tenant_id, location_id, param_code) -> int:
    """Delete all chunks for one (tenant, location, param_code). Returns rows removed."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.knowledge_chunks "
            "WHERE tenant_id = %s AND location_id = %s AND param_code = %s",
            (tenant_id, location_id, param_code),
        )
        removed = cur.rowcount
    conn.commit()
    return removed


@app.task(bind=True, name='tasks.embed_knowledge_param.embed_knowledge_param')
def embed_knowledge_param(self, tenant_id=None, location_id=None, param_code=None):
    """(Re)embed or purge `knowledge_chunks` for one saved knowledge param.

    Returns a small status dict. Idempotent — safe to call after any knowledge
    save / remove / delete; the task decides embed-vs-purge from current DB state.
    """
    if not tenant_id or not location_id or not param_code:
        raise ValueError("tenant_id, location_id and param_code are required")

    if param_code in NON_CONTENT_PARAM_CODES:
        logger.info("[embed_knowledge_param] skip non-content param_code=%s", param_code)
        return {"action": "skipped", "reason": "non_content", "param_code": param_code}

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT value_text, status FROM public.tenant_integration_params "
                "WHERE tenant_id = %s AND location_id = %s AND param_code = %s "
                "  AND provider = 'speako' AND service = 'knowledge' "
                "LIMIT 1",
                (tenant_id, location_id, param_code),
            )
            row = cur.fetchone()

        value_text = (row or {}).get("value_text") or ""
        status = (row or {}).get("status")
        is_live = row is not None and status in _LIVE_STATUSES and bool(value_text.strip())

        if not is_live:
            # Row removed / blanked / out of scope: drop any stale chunks so the
            # widget stops retrieving deleted knowledge. (chunk_and_embed_knowledge
            # returns 0 WITHOUT deleting on empty text, so purge explicitly here.)
            removed = _purge_chunks(conn, tenant_id, location_id, param_code)
            logger.info(
                "[embed_knowledge_param] purged t=%s l=%s p=%s (status=%s): %d chunks",
                tenant_id, location_id, param_code, status, removed,
            )
            return {"action": "purged", "removed": removed, "chunks": 0, "param_code": param_code}

        n_chunks = chunk_and_embed_knowledge(
            tenant_id, location_id, param_code, value_text, db_conn=conn,
        )
        logger.info(
            "[embed_knowledge_param] embedded t=%s l=%s p=%s: %d chunks",
            tenant_id, location_id, param_code, n_chunks,
        )
        return {"action": "embedded", "chunks": n_chunks, "param_code": param_code}
    finally:
        conn.close()
