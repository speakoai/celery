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
from psycopg2.extras import execute_values

from .utils.knowledge_utils import (
    chunk_and_embed_knowledge, NON_CONTENT_PARAM_CODES, EMBEDDING_MODEL, _chunk_text, _vector_literal,
)

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


@app.task(bind=True, name='tasks.embed_knowledge_param.embed_knowledge_draft')
def embed_knowledge_draft(self, revision_id=None):
    """Knowledge approval Phase 3: (re)index ONE pending draft into knowledge_draft_chunks so the
    dashboard's Test it live page can chat against it ("Live + pending changes"). Never touches the
    live knowledge_chunks. Not pending any more (approved / rejected / discarded) or empty (a
    removal request) → its draft chunks are deleted. Idempotent; if the draft text changes while
    embedding, the stale result is dropped (the save that changed it queued a fresh run)."""
    if not revision_id:
        raise ValueError("revision_id is required")
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT tenant_id, location_id, param_code, value_text, state "
                "FROM public.knowledge_revisions WHERE revision_id = %s",
                (revision_id,),
            )
            rev = cur.fetchone()
        conn.commit()
        text = (rev or {}).get("value_text") or ""
        if not rev or rev["state"] != "pending" or not text.strip() or rev["location_id"] is None \
                or rev["param_code"] in NON_CONTENT_PARAM_CODES:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM public.knowledge_draft_chunks WHERE revision_id = %s", (revision_id,))
                    removed = cur.rowcount
            return {"action": "purged", "removed": removed, "revision_id": revision_id}

        chunks = _chunk_text(text)
        embeddings = _embed_texts(chunks) if chunks else []
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value_text, state FROM public.knowledge_revisions WHERE revision_id = %s FOR UPDATE",
                    (revision_id,),
                )
                now = cur.fetchone()
                if not now or now[1] != "pending" or now[0] != text:
                    logger.info("[embed_knowledge_draft] revision %s changed while embedding; skipped", revision_id)
                    return {"action": "stale", "revision_id": revision_id}
                cur.execute("DELETE FROM public.knowledge_draft_chunks WHERE revision_id = %s", (revision_id,))
                if chunks:
                    execute_values(
                        cur,
                        "INSERT INTO public.knowledge_draft_chunks "
                        "(revision_id, tenant_id, location_id, param_code, chunk_index, content, embedding) VALUES %s",
                        [(revision_id, rev["tenant_id"], rev["location_id"], rev["param_code"], i, chunks[i],
                          _vector_literal(embeddings[i])) for i in range(len(chunks))],
                        template="(%s,%s,%s,%s,%s,%s,%s::vector)",
                    )
        logger.info("[embed_knowledge_draft] revision %s: %d chunks", revision_id, len(chunks))
        return {"action": "embedded", "chunks": len(chunks), "revision_id": revision_id}
    finally:
        conn.close()


def _embed_texts(chunks):
    import os
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    resp = client.embeddings.create(model=EMBEDDING_MODEL, input=chunks)
    return [d.embedding for d in sorted(resp.data, key=lambda d: d.index)]
