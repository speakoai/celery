"""
summarize_chat_sessions — Tier-3 episodic memory (per conversation segment).

A chat_sessions row is a LONG-LIVED visitor (Tier-2 core memory lives on it).
`conversation_id` delimits a conversation; messages share the session's
`current_conversation_id` until it's rotated. A segment is summarised into
`customer_episodes` when it is CLOSED — either:
  - superseded: its conversation_id != the session's current_conversation_id
    (the session re-activated, or the user hit "Start new conversation"), or
  - current-but-idle: it is still current but its last message is older than the
    idle timeout.
…and it has >=3 user messages and no episode yet. After summarising the CURRENT
segment we rotate current_conversation_id (so the next messages start fresh);
superseded segments need no rotation. The session stays status='active' (no expiry;
long-idle cleanup is a separate future task). Legacy NULL-conversation messages are
treated as one segment.

Invoked by a Render cron every 30 minutes. This project does NOT use Celery Beat.
"""
import os

from celery.utils.log import get_task_logger

from tasks.celery_app import app
from .utils.task_db import _get_conn

logger = get_task_logger(__name__)

SUMMARY_MODEL = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")
EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
IDLE_TIMEOUT_SECONDS = int(os.getenv("CHAT_IDLE_TIMEOUT_SECONDS", os.getenv("CHAT_SESSION_TTL", "7200")))
MIN_USER_MESSAGES = 3

_PROMPT = (
    "Summarise this customer service conversation in 2-3 sentences. Focus on: "
    "what the customer wanted, what was resolved or left unresolved, and any "
    "booking or order outcome. Be factual and concise.\n\nConversation:\n"
)


def _summarize_segment(conn, client, row) -> bool:
    """Summarise one (session, conversation_id) segment. Rotate current_conversation_id
    only if this segment is the CURRENT one. Returns True if an episode was written."""
    from psycopg2.extras import RealDictCursor

    seg = row["seg"]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT role, content FROM chat_messages
            WHERE session_id = %s AND conversation_id IS NOT DISTINCT FROM %s AND content IS NOT NULL
            ORDER BY created_at ASC
            """,
            (row["session_id"], seg),
        )
        msgs = cur.fetchall()
    conn.commit()

    if sum(1 for m in msgs if m["role"] == "user") < MIN_USER_MESSAGES:
        return False

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
                (tenant_id, channel, channel_user_id, summary, embedding, conversation_id)
            VALUES (%s, %s, %s, %s, %s::vector, %s)
            """,
            (row["tenant_id"], row["channel"], row["channel_session_id"], summary, vec, seg),
        )
        # Rotate only the CURRENT segment (so the visitor's next messages start fresh).
        if seg == row["current_conversation_id"]:
            cur.execute(
                "UPDATE chat_sessions SET current_conversation_id = gen_random_uuid() WHERE session_id = %s",
                (row["session_id"],),
            )
    conn.commit()
    return True


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
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cs.session_id, cs.tenant_id, cs.channel, cs.channel_session_id,
                       cs.current_conversation_id, cm.conversation_id AS seg
                FROM chat_messages cm
                JOIN chat_sessions cs ON cs.session_id = cm.session_id
                WHERE cs.channel_session_id IS NOT NULL AND cm.content IS NOT NULL
                GROUP BY cs.session_id, cs.tenant_id, cs.channel, cs.channel_session_id,
                         cs.current_conversation_id, cm.conversation_id
                HAVING count(*) FILTER (WHERE cm.role = 'user') >= %s
                   AND ( cm.conversation_id IS DISTINCT FROM cs.current_conversation_id
                         OR max(cm.created_at) < now() - make_interval(secs => %s) )
                   AND NOT EXISTS (
                         SELECT 1 FROM customer_episodes ce
                         WHERE ce.tenant_id = cs.tenant_id
                           AND ce.channel_user_id = cs.channel_session_id
                           AND ce.conversation_id IS NOT DISTINCT FROM cm.conversation_id
                       )
                """,
                (MIN_USER_MESSAGES, IDLE_TIMEOUT_SECONDS),
            )
            rows = cur.fetchall()
        conn.commit()

        for row in rows:
            try:
                if _summarize_segment(conn, client, row):
                    summarized += 1
            except Exception as e:
                conn.rollback()
                logger.warning("[summarize_chat_sessions] session %s seg %s failed: %s",
                               row.get("session_id"), row.get("seg"), e)

        logger.info("[summarize_chat_sessions] summarised %d of %d candidate segment(s)",
                    summarized, len(rows))
        return {"summarized": summarized, "candidates": len(rows)}
    finally:
        conn.close()
