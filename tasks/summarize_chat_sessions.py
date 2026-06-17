"""
summarize_chat_sessions — Tier-3 episodic memory population.

Sweeps stale active chat sessions (idle > 2h), summarises each conversation with
OpenAI, embeds the summary, writes a customer_episodes row, and marks the session
expired. The text-chat brain (app_text2) later retrieves these summaries on a
fresh session for continuity.

Invoked by a RENDER CRON JOB every 30 minutes (dispatch/summarize_chat_sessions_dispatch.py).
This project does NOT use Celery Beat.
"""
import os

from celery.utils.log import get_task_logger

from tasks.celery_app import app
from .utils.task_db import _get_conn

logger = get_task_logger(__name__)

SUMMARY_MODEL = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")
EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
MIN_USER_MESSAGES = 3

_PROMPT = (
    "Summarise this customer service conversation in 2-3 sentences. Focus on: "
    "what the customer wanted, what was resolved or left unresolved, and any "
    "booking or order outcome. Be factual and concise.\n\nConversation:\n"
)


def _expire(conn, session_id) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE chat_sessions SET status = 'expired' WHERE session_id = %s", (session_id,))
    conn.commit()


@app.task(bind=True, name="tasks.summarize_chat_sessions.summarize_chat_sessions")
def summarize_chat_sessions(self):
    from openai import OpenAI
    from psycopg2.extras import RealDictCursor

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    client = OpenAI(api_key=api_key)

    conn = _get_conn()
    summarized = skipped = 0
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT session_id, tenant_id, channel, channel_session_id
                FROM chat_sessions
                WHERE status = 'active' AND updated_at < now() - interval '2 hours'
                """
            )
            sessions = cur.fetchall()
        conn.commit()

        for s in sessions:
            try:
                # customer_episodes.channel_user_id is NOT NULL — skip sessions without one.
                if not s["channel_session_id"]:
                    _expire(conn, s["session_id"])
                    skipped += 1
                    continue

                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT role, content FROM chat_messages
                        WHERE session_id = %s AND content IS NOT NULL
                        ORDER BY created_at ASC
                        """,
                        (s["session_id"],),
                    )
                    msgs = cur.fetchall()
                conn.commit()

                if sum(1 for m in msgs if m["role"] == "user") < MIN_USER_MESSAGES:
                    _expire(conn, s["session_id"])
                    skipped += 1
                    continue

                transcript = "\n".join(f"{m['role']}: {m['content']}" for m in msgs)
                summary = (
                    client.chat.completions.create(
                        model=SUMMARY_MODEL,
                        messages=[{"role": "user", "content": _PROMPT + transcript}],
                        max_tokens=200,
                        temperature=0.2,
                    )
                    .choices[0]
                    .message.content.strip()
                )
                emb = client.embeddings.create(model=EMBEDDING_MODEL, input=[summary]).data[0].embedding
                vec = "[" + ",".join(repr(float(x)) for x in emb) + "]"

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO customer_episodes
                            (tenant_id, channel, channel_user_id, summary, embedding)
                        VALUES (%s, %s, %s, %s, %s::vector)
                        """,
                        (s["tenant_id"], s["channel"], s["channel_session_id"], summary, vec),
                    )
                    cur.execute(
                        "UPDATE chat_sessions SET status = 'expired' WHERE session_id = %s",
                        (s["session_id"],),
                    )
                conn.commit()
                summarized += 1
            except Exception as e:
                conn.rollback()
                logger.warning("[summarize_chat_sessions] session %s failed: %s", s.get("session_id"), e)

        logger.info(
            "[summarize_chat_sessions] summarized=%d skipped/expired=%d total=%d",
            summarized, skipped, len(sessions),
        )
        return {"summarized": summarized, "skipped": skipped, "total": len(sessions)}
    finally:
        conn.close()
