"""
build_chat_vector_store.py — per-tenant aggregate OpenAI vector store for text chat.

The text chat (Messenger pilot) is TENANT-scoped, but knowledge lives per location
(locations.openai_vector_store_id, one store each). OpenAI file_search allows at
most 2 vector stores per request, so a multi-location tenant cannot pass one store
per branch. This builds ONE aggregate vector store per tenant — all branches'
knowledge in a single, branch-labelled markdown — and records its id so the chat
brain (speako-voice-ai/app_text.py) can do RAG over the whole business in one store.

Source of truth is unchanged: knowledge is read from tenant_integration_params
(provider='speako', service='knowledge'), the same rows the per-location stores
are built from. The aggregate store id is recorded as:
    tenant_integration_params(provider='speako', service='chat',
                              param_code='vector_store', location_id IS NULL)

Hook: call build_tenant_chat_vector_store(tenant_id) at the end of a tenant's
publish flow (after per-location native agents are published). See messenger-chat-plan.md.
"""

from dotenv import load_dotenv
load_dotenv()

import logging
import os
import tempfile

import psycopg2
import requests
from psycopg2.extras import RealDictCursor

from tasks.celery_app import app

logger = logging.getLogger(__name__)

_OPENAI_FILES_URL = "https://api.openai.com/v1/files"
_OPENAI_VS_URL = "https://api.openai.com/v1/vector_stores"


def _vs_headers(key: str) -> dict:
    return {"Authorization": f"Bearer {key}", "OpenAI-Beta": "assistants=v2"}


def _collect_tenant_knowledge(tenant_id) -> list[dict]:
    """All published/configured speako knowledge for the tenant, grouped by branch."""
    query = """
        SELECT l.location_id, l.name AS location_name, tip.param_code, tip.value_text
        FROM tenant_integration_params tip
        JOIN locations l
          ON l.tenant_id = tip.tenant_id AND l.location_id = tip.location_id
        WHERE tip.tenant_id = %s
          AND tip.provider = 'speako'
          AND tip.service = 'knowledge'
          AND tip.status IN ('configured', 'published')
          AND tip.value_text IS NOT NULL
          AND length(trim(tip.value_text)) > 0
          AND COALESCE(l.is_active, true) = true
        ORDER BY l.location_id ASC, tip.param_code ASC;
    """
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (tenant_id,))
            return cur.fetchall()
    finally:
        conn.close()


def _build_markdown(rows: list[dict]) -> str:
    """Combine knowledge into one markdown, sectioned by branch."""
    by_loc: dict = {}
    for r in rows:
        by_loc.setdefault((r["location_id"], r["location_name"]), []).append(r)
    parts: list[str] = []
    for (loc_id, name), items in by_loc.items():
        parts.append(f"# Branch: {name} (location_id {loc_id})\n")
        for it in items:
            title = (it["param_code"] or "knowledge").replace("_", " ").title()
            parts.append(f"## {title}\n\n{it['value_text'].strip()}\n")
        parts.append("\n---\n")
    return "\n".join(parts)


def _get_existing_store_id(tenant_id) -> str | None:
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT value_text FROM tenant_integration_params
                    WHERE tenant_id = %s AND provider = 'speako' AND service = 'chat'
                      AND param_code = 'vector_store' AND location_id IS NULL
                    LIMIT 1""",
                (tenant_id,),
            )
            row = cur.fetchone()
            return row["value_text"] if row and row.get("value_text") else None
    finally:
        conn.close()


def _save_store_id(tenant_id, vector_store_id: str) -> None:
    """Manual upsert (location_id IS NULL means ON CONFLICT can't match cleanly)."""
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE tenant_integration_params
                      SET value_text = %s, status = 'published', updated_at = now()
                    WHERE tenant_id = %s AND provider = 'speako' AND service = 'chat'
                      AND param_code = 'vector_store' AND location_id IS NULL""",
                (vector_store_id, tenant_id),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """INSERT INTO tenant_integration_params
                          (tenant_id, location_id, provider, service, param_code,
                           param_kind, value_text, status)
                       VALUES (%s, NULL, 'speako', 'chat', 'vector_store',
                               'text', %s, 'published')""",
                    (tenant_id, vector_store_id),
                )
        conn.commit()
    finally:
        conn.close()


def _clear_store_files(vector_store_id: str, headers: dict) -> None:
    try:
        resp = requests.get(f"{_OPENAI_VS_URL}/{vector_store_id}/files", headers=headers, timeout=15)
        if resp.ok:
            for f in resp.json().get("data", []):
                requests.delete(
                    f"{_OPENAI_VS_URL}/{vector_store_id}/files/{f.get('id')}",
                    headers=headers, timeout=10,
                )
    except Exception as e:
        logger.warning("[chat_vs] clearing files failed for %s: %s", vector_store_id, e)


def build_tenant_chat_vector_store(tenant_id) -> str | None:
    """
    Create or refresh the tenant's aggregate chat vector store. Returns its id.
    Plain function (callable directly or from the publish flow).
    """
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.error("[chat_vs] OPENAI_API_KEY not set")
        return None

    rows = _collect_tenant_knowledge(tenant_id)
    if not rows:
        logger.warning("[chat_vs] tenant %s has no knowledge to aggregate", tenant_id)
        return None
    md = _build_markdown(rows)

    headers = _vs_headers(openai_key)
    vector_store_id = _get_existing_store_id(tenant_id)

    if not vector_store_id:
        resp = requests.post(
            _OPENAI_VS_URL, headers=headers,
            json={"name": f"speako-chat-{tenant_id}",
                  "metadata": {"tenant_id": str(tenant_id), "kind": "chat_aggregate"}},
            timeout=30,
        )
        if not resp.ok:
            logger.error("[chat_vs] create store failed: %s %s", resp.status_code, resp.text[:300])
            return None
        vector_store_id = resp.json().get("id")
        logger.info("[chat_vs] created aggregate store %s for tenant %s", vector_store_id, tenant_id)
    else:
        logger.info("[chat_vs] refreshing aggregate store %s for tenant %s", vector_store_id, tenant_id)
        _clear_store_files(vector_store_id, headers)

    # Upload combined markdown and attach.
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as tmp:
            tmp.write(md)
            tmp_path = tmp.name
        with open(tmp_path, "rb") as fh:
            up = requests.post(
                _OPENAI_FILES_URL,
                headers={"Authorization": f"Bearer {openai_key}"},
                files={"file": (f"chat-knowledge-{tenant_id}.md", fh, "text/markdown")},
                data={"purpose": "assistants"},
                timeout=60,
            )
        if not up.ok:
            logger.error("[chat_vs] file upload failed: %s %s", up.status_code, up.text[:300])
            return vector_store_id
        file_id = up.json().get("id")
        attach = requests.post(
            f"{_OPENAI_VS_URL}/{vector_store_id}/files",
            headers=headers, json={"file_id": file_id}, timeout=30,
        )
        if attach.ok:
            logger.info("[chat_vs] attached file %s (%d bytes) to %s", file_id, len(md), vector_store_id)
        else:
            logger.error("[chat_vs] attach failed: %s %s", attach.status_code, attach.text[:300])
    except Exception as e:
        logger.error("[chat_vs] upload error: %s", e)
        return vector_store_id
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    _save_store_id(tenant_id, vector_store_id)
    return vector_store_id


@app.task(name="tasks.build_chat_vector_store.build")
def build_tenant_chat_vector_store_task(tenant_id):
    """Celery wrapper — call at the end of a tenant's publish flow."""
    vs_id = build_tenant_chat_vector_store(tenant_id)
    return {"tenant_id": tenant_id, "vector_store_id": vs_id, "ok": bool(vs_id)}
