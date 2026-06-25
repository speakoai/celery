"""
Dispatch: enqueue the summarize_chat_sessions sweep (Tier-3 episodic memory).

Run by a Render cron job every 30 minutes. This project does NOT use Celery Beat.

Usage:
    PYTHONPATH=. python dispatch/summarize_chat_sessions_dispatch.py
"""
from dotenv import load_dotenv

load_dotenv()

from tasks.summarize_chat_sessions import summarize_chat_sessions

if __name__ == "__main__":
    res = summarize_chat_sessions.delay()
    print(f"[dispatch] summarize_chat_sessions queued: {res.id}")
