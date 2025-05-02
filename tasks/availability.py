from dotenv import load_dotenv
load_dotenv()

from tasks.celery_app import app
from celery.utils.log import get_task_logger
import os
import psycopg2


logger = get_task_logger(__name__)

@app.task
def fetch_sample_data():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set")
        return

    logger.info(f"[PRODUCTION] Connecting to PostgreSQL at: {db_url}")

    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' LIMIT 3;")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        logger.info(f"[PRODUCTION] Successfully fetched {len(rows)} tables.")
        return rows
    except Exception as e:
        logger.error(f"[PRODUCTION] Database error: {e}")
        return None
