"""
Dispatch: enqueue the summarize_chat_sessions sweep (Tier-3 episodic memory), plus the
purge_chat_client_metadata retention sweep (widget visitor IP/UA older than 90 days) —
riding on this existing chat cron so no extra Render service is needed.

Run by a Render cron job every 30 minutes. This project does NOT use Celery Beat.

Usage:
    PYTHONPATH=. python dispatch/summarize_chat_sessions_dispatch.py
"""
from dotenv import load_dotenv

load_dotenv()

from tasks.summarize_chat_sessions import summarize_chat_sessions
from tasks.purge_chat_client_metadata import purge_chat_client_metadata

if __name__ == "__main__":
    res = summarize_chat_sessions.delay()
    print(f"[dispatch] summarize_chat_sessions queued: {res.id}")
    purge = purge_chat_client_metadata.delay()
    print(f"[dispatch] purge_chat_client_metadata queued: {purge.id}")
