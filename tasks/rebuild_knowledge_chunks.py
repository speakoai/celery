"""
rebuild_knowledge_chunks — one-time / on-demand backfill of the pgvector
knowledge_chunks table from existing tenant_integration_params knowledge rows.

Iterates published/configured knowledge rows (value_text markdown) and
(re)chunks+embeds each into knowledge_chunks. Idempotent — the UNIQUE constraint
plus chunk_and_embed_knowledge's delete-then-reinsert make it safe to re-run.

Invoked on demand (or via a Render cron job if desired) — NOT Celery Beat,
which this project does not use.
"""
import os

from celery.utils.log import get_task_logger

from tasks.celery_app import app
from .utils.task_db import _get_conn
from .utils.knowledge_utils import chunk_and_embed_knowledge, NON_CONTENT_PARAM_CODES

logger = get_task_logger(__name__)


@app.task(bind=True, name='tasks.rebuild_knowledge_chunks.rebuild_knowledge_chunks')
def rebuild_knowledge_chunks(self, tenant_id=None):
    """Backfill knowledge_chunks. If tenant_id is given, limit to that tenant;
    otherwise process all tenants."""
    from openai import OpenAI
    from psycopg2.extras import RealDictCursor

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    client = OpenAI(api_key=api_key)

    where = [
        "service = 'knowledge'",
        "provider = 'speako'",
        "status IN ('configured', 'published')",
        "value_text IS NOT NULL",
        "value_text <> ''",
        "location_id IS NOT NULL",
    ]
    params = []
    if tenant_id is not None:
        where.append("tenant_id = %s")
        params.append(tenant_id)

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT tenant_id, location_id, param_code, value_text "
                "FROM public.tenant_integration_params "
                f"WHERE {' AND '.join(where)}",
                tuple(params),
            )
            rows = cur.fetchall()

        n_rows = n_chunks = skipped = 0
        for r in rows:
            if r['param_code'] in NON_CONTENT_PARAM_CODES:
                skipped += 1
                continue
            try:
                n_chunks += chunk_and_embed_knowledge(
                    r['tenant_id'], r['location_id'], r['param_code'], r['value_text'],
                    openai_client=client, db_conn=conn,
                )
                n_rows += 1
            except Exception as e:
                logger.warning(
                    "[rebuild_knowledge_chunks] row failed t=%s l=%s p=%s: %s",
                    r['tenant_id'], r['location_id'], r['param_code'], e,
                )

        logger.info(
            "[rebuild_knowledge_chunks] done: %d rows, %d chunks, %d skipped",
            n_rows, n_chunks, skipped,
        )
        return {"rows": n_rows, "chunks": n_chunks, "skipped": skipped}
    finally:
        conn.close()
