"""
summarize_chat_sessions — Tier-3 episodic memory population (per conversation segment).

A chat_sessions row is a LONG-LIVED visitor (Tier-2 core memory lives on it).
`conversation_id` delimits a conversation: messages share the session's
`current_conversation_id`. This task summarises a *closed* segment — one that is idle,
has >=3 user messages, and has no episode yet — into `customer_episodes`, then ROTATES
`current_conversation_id` to a fresh value (the session stays `status='active'`; it is
NOT expired here — long-idle cleanup is a separate future task).

Legacy messages with NULL conversation_id are treated as one segment (matched via
IS NOT DISTINCT FROM) and rotated off once summarised.

Invoked by a Render cron every 30 minutes (dispatch/summarize_chat_sessions_dispatch.py).
This project does NOT use Celery Beat.
"""
import os

from celery.utils.log import get_task_logger

from tasks.celery_app import app
from .utils.task_db import _get_conn

logger = get_task_logger(__name__)

SUMMARY_MODEL = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")
EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
# Unified idle timeout (shared concept with the brain's CHAT_IDLE_TIMEOUT_SECONDS /
# legacy CHAT_SESSION_TTL). A segment whose last message is older than this is "closed".
IDLE_TIMEOUT_SECONDS = int(os.getenv("CHAT_IDLE_TIMEOUT_SECONDS", os.getenv("CHAT_SESSION_TTL", "7200")))
MIN_USER_MESSAGES = 3

_PROMPT = (
    "Summarise this customer service conversation in 2-3 sentences. Focus on: "
    "what the customer wanted, what was resolved or left unresolved, and any "
    "booking or order outcome. Be factual and concise.\n\nConversation:\n"
)


@app.task(bind=True, name="tasks.summarize_chat_sessions.summarize_chat_sessions")
def summarize_chat_sessions(self):
    from openai import OpenAI
    from psycopg2.extras import RealDictCursor

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    client = OpenAI(api_key=api_key)

    conn = _get_conn()
    summarized = 0
    try:
        # Closed segments: status='active' sessions whose CURRENT segment is idle, has
        # >=3 user messages, and has no episode yet. (NULL conversation_id = legacy segment.)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cs.session_id, cs.tenant_id, cs.channel, cs.channel_session_id,
                       cs.current_conversation_id
                FROM chat_sessions cs
                WHERE cs.status = 'active'
                  AND EXISTS (
                        SELECT 1 FROM chat_messages cm
                        WHERE cm.session_id = cs.session_id
                          AND cm.conversation_id IS NOT DISTINCT FROM cs.current_conversation_id
                        GROUP BY cm.session_id
                        HAVING max(cm.created_at) < now() - make_interval(secs => %s)
                           AND count(*) FILTER (WHERE cm.role = 'user') >= %s
                      )
                  AND (cs.current_conversation_id IS NULL OR NOT EXISTS (
                        SELECT 1 FROM customer_episodes ce
                        WHERE ce.conversation_id = cs.current_conversation_id
                      ))
                """,
                (IDLE_TIMEOUT_SECONDS, MIN_USER_MESSAGES),
            )
            segments = cur.fetchall()
        conn.commit()

        for s in segments:
            try:
                if not s["channel_session_id"]:
                    continue  # customer_episodes.channel_user_id is NOT NULL
                seg = s["current_conversation_id"]

                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT role, content FROM chat_messages
                        WHERE session_id = %s
                          AND conversation_id IS NOT DISTINCT FROM %s
                          AND content IS NOT NULL
                        ORDER BY created_at ASC
                        """,
                        (s["session_id"], seg),
                    )
                    msgs = cur.fetchall()
                conn.commit()

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

                # Atomic: write the episode for this segment + rotate to a fresh segment.
                # Keep status='active' (no expiry). updated_at is left untouched so an
                # already-summarised (now-empty) segment is not re-selected next run.
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO customer_episodes
                            (tenant_id, channel, channel_user_id, summary, embedding, conversation_id)
                        VALUES (%s, %s, %s, %s, %s::vector, %s)
                        """,
                        (s["tenant_id"], s["channel"], s["channel_session_id"], summary, vec, seg),
                    )
                    cur.execute(
                        "UPDATE chat_sessions SET current_conversation_id = gen_random_uuid() WHERE session_id = %s",
                        (s["session_id"],),
                    )
                conn.commit()
                summarized += 1
            except Exception as e:
                conn.rollback()
                logger.warning("[summarize_chat_sessions] session %s failed: %s", s.get("session_id"), e)

        logger.info("[summarize_chat_sessions] summarised %d segment(s) of %d candidate(s)",
                    summarized, len(segments))
        return {"summarized": summarized, "candidates": len(segments)}
    finally:
        conn.close()
