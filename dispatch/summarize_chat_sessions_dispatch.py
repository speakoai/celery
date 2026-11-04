"""
Dispatch: enqueue the summarize_chat_sessions sweep (Tier-3 episodic memory), plus the chat
maintenance sweep in tasks/purge_chat_client_metadata.py (visitor IP/UA retention, per-tenant
chat history retention, and handing expired human take-overs back to the AI) — riding on this
existing chat cron so no extra Render service is needed.

Run by a Render cron job every 30 minutes. This project does NOT use Celery Beat.

Usage:
    PYTHONPATH=. python dispatch/summarize_chat_sessions_dispatch.py
"""
from dotenv import load_dotenv

load_dotenv()

from tasks.summarize_chat_sessions import summarize_chat_sessions
from tasks.purge_chat_client_metadata import (
    apply_chat_retention,
    purge_chat_client_metadata,
    purge_knowledge_revisions,
    release_stale_takeovers,
)

if __name__ == "__main__":
    res = summarize_chat_sessions.delay()
    print(f"[dispatch] summarize_chat_sessions queued: {res.id}")
    for task in (purge_chat_client_metadata, apply_chat_retention, release_stale_takeovers,
                 purge_knowledge_revisions):
        queued = task.delay()
        print(f"[dispatch] {task.name} queued: {queued.id}")
