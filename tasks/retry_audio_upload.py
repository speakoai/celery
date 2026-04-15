"""
Retry audio download from Twilio and upload to R2.

This task is enqueued by the OpenAI conversation webhook when the initial
audio download fails (typically because Twilio hasn't finished processing
the recording yet). It re-enqueues itself with a 60-second delay up to
a configurable max number of retries.
"""

import os
import logging

import requests
from celery.utils.log import get_task_logger

from tasks.celery_app import app

logger = get_task_logger(__name__)

MAX_RETRIES = 10
RETRY_DELAY_SECONDS = 60


@app.task(bind=True, name="tasks.retry_audio_upload")
def retry_audio_upload(
    self,
    tenant_id: str,
    location_id: str,
    location_conversation_id: int,
    recording_sid: str,
    conversation_id: str,
    attempt: int = 1,
    is_dev: bool = False,
):
    """
    Download audio from Twilio and upload to R2, retrying on failure.

    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        location_conversation_id: DB row ID in location_conversations
        recording_sid: Twilio recording SID
        conversation_id: Provider conversation ID (used as R2 filename)
        attempt: Current attempt number (1-based)
    """
    logger.info(
        "[RetryAudio] Attempt %d/%d for recording %s (conv %s)",
        attempt, MAX_RETRIES, recording_sid, location_conversation_id,
    )

    twilio_sid = os.getenv("TWILIO_ACCOUNT_SID")
    twilio_token = os.getenv("TWILIO_AUTH_TOKEN")

    if not twilio_sid or not twilio_token:
        logger.error("[RetryAudio] Twilio credentials not configured, giving up")
        return {"success": False, "error": "no_twilio_creds"}

    # Download from Twilio
    twilio_url = f"https://api.twilio.com/2010-04-01/Accounts/{twilio_sid}/Recordings/{recording_sid}.mp3"
    try:
        resp = requests.get(twilio_url, auth=(twilio_sid, twilio_token), timeout=30)
    except Exception as e:
        logger.warning("[RetryAudio] Download error: %s", e)
        return _maybe_retry(self, tenant_id, location_id, location_conversation_id, recording_sid, conversation_id, attempt, str(e), is_dev=is_dev)

    if resp.status_code == 404:
        logger.info("[RetryAudio] Recording still not ready (404)")
        return _maybe_retry(self, tenant_id, location_id, location_conversation_id, recording_sid, conversation_id, attempt, "404", is_dev=is_dev)

    if resp.status_code != 200 or len(resp.content) == 0:
        logger.warning("[RetryAudio] Unexpected response: status=%s size=%d", resp.status_code, len(resp.content))
        return _maybe_retry(self, tenant_id, location_id, location_conversation_id, recording_sid, conversation_id, attempt, f"status_{resp.status_code}", is_dev=is_dev)

    # Upload to R2
    logger.info("[RetryAudio] Downloaded %d bytes, uploading to R2", len(resp.content))
    try:
        from tasks.utils.publish_r2 import upload_audio_to_r2

        # is_dev passed from caller (voice-ai knows its environment)

        r2_key, public_url = upload_audio_to_r2(
            str(tenant_id), str(location_id), conversation_id,
            resp.content, content_type="audio/mpeg", use_dev=is_dev,
        )
        logger.info("[RetryAudio] Uploaded to R2: %s", public_url)
    except Exception as e:
        logger.error("[RetryAudio] R2 upload failed: %s", e)
        return _maybe_retry(self, tenant_id, location_id, location_conversation_id, recording_sid, conversation_id, attempt, str(e), is_dev=is_dev)

    # Update DB
    try:
        import psycopg2
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE location_conversations SET audio_r2_path = %s, updated_at = CURRENT_TIMESTAMP WHERE location_conversation_id = %s",
                (public_url, location_conversation_id),
            )
        conn.commit()
        conn.close()
        logger.info("[RetryAudio] Updated audio_r2_path for conversation %s", location_conversation_id)
    except Exception as e:
        logger.error("[RetryAudio] DB update failed: %s", e)
        return {"success": False, "error": f"db_update_failed: {e}"}

    return {"success": True, "attempt": attempt, "r2_url": public_url}


def _maybe_retry(task, tenant_id, location_id, location_conversation_id, recording_sid, conversation_id, attempt, reason, is_dev=False):
    """Re-enqueue with delay if under max retries, otherwise give up."""
    if attempt >= MAX_RETRIES:
        logger.warning("[RetryAudio] Giving up after %d attempts for recording %s: %s", attempt, recording_sid, reason)
        return {"success": False, "attempts": attempt, "last_error": reason}

    logger.info("[RetryAudio] Re-enqueueing attempt %d/%d in %ds", attempt + 1, MAX_RETRIES, RETRY_DELAY_SECONDS)
    retry_audio_upload.apply_async(
        kwargs={
            "tenant_id": tenant_id,
            "location_id": location_id,
            "location_conversation_id": location_conversation_id,
            "recording_sid": recording_sid,
            "conversation_id": conversation_id,
            "attempt": attempt + 1,
            "is_dev": is_dev,
        },
        countdown=RETRY_DELAY_SECONDS,
    )
    return {"success": False, "retrying": True, "next_attempt": attempt + 1}
