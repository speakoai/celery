"""
Verify STT-dropped-speech findings against the call recording.

Phase 8 of docs/plans/call-quality-audio-verify.md (speako-workspace).

When the log says `EMPTY CALLER TRANSCRIPT` or `Caller transcription FAILED`,
this asks the recording whether the caller was actually speaking at that moment.
A confirmed case turns a symptom into a diagnosis: "STT returned nothing" is not
actionable, but "the caller spoke for 0.5s at 10% their usual level and STT
returned nothing" points at input gain, handset distance or STT sensitivity.

Deliberately narrow. Dead air, overlap and turn timing are NOT verified here —
those measurements flagged 9/9 calls at every threshold and disagreed with the
log by ~2x where ground truth existed, so shipping them would repeat an earlier
mistake at greater cost.

Separate from the log sweep on purpose: recordings arrive AFTER the call (Twilio
finalises them, then voice-ai uploads with retries), so the audio frequently is
not there yet when the log sweep runs.

⚠️ Never overrides a human. The verifier writes `confidence` and `evidence`, and
may set `status` only while it is still `open`.
"""

import gzip
import json
import os

import psycopg2
import requests
from celery.utils.log import get_task_logger

from tasks.celery_app import app
from tasks.utils.call_verify import unverifiable, verify_finding
from tasks.utils.publish_r2 import download_call_log_from_r2

logger = get_task_logger(__name__)

VERIFIABLE_RULES = ("stt_empty_transcript", "stt_transcription_failed")

MAX_CALLS_PER_RUN = 25
AUDIO_TIMEOUT_SECONDS = 30
MAX_AUDIO_BYTES = 25 * 1024 * 1024      # a long call is ~2MB; this is a sanity cap


@app.task(bind=True, name="tasks.verify_call_findings.verify_call_findings")
def verify_call_findings(self, is_dev=False, limit=None):
    """Sweep unverified stt_* findings and check each against its recording."""
    db_url = os.getenv("DATABASE_URL" if is_dev else "DATABASE_URL_PROD")
    if not db_url:
        return {"success": False, "error": "no_database_url"}

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT f.id, f.rule_id, f.evidence, f.location_conversation_id,
               c.audio_r2_path, c.raw_metadata->>'log_r2_key'
          FROM call_quality_findings f
          JOIN location_conversations c
            ON c.location_conversation_id = f.location_conversation_id
         WHERE f.rule_id = ANY(%s)
           AND f.status = 'open'
           AND NOT (f.evidence ? 'audio_verified')
           AND c.raw_metadata ? 'log_r2_key'
         ORDER BY f.created_at DESC
         LIMIT %s
        """,
        (list(VERIFIABLE_RULES), min(limit or MAX_CALLS_PER_RUN, MAX_CALLS_PER_RUN)),
    )
    rows = cur.fetchall()

    counts = {"spoke": 0, "silent": 0, "ambiguous": 0,
              "unaligned": 0, "unverifiable": 0}
    for fid, rule_id, evidence, conv_id, audio_url, log_key in rows:
        try:
            if not audio_url:
                result = unverifiable("no_audio")
            else:
                resp = requests.get(audio_url, timeout=AUDIO_TIMEOUT_SECONDS)
                if resp.status_code != 200 or not resp.content:
                    result = unverifiable("audio_missing")
                elif len(resp.content) > MAX_AUDIO_BYTES:
                    result = unverifiable("audio_too_large")
                else:
                    log_text = download_call_log_from_r2(
                        log_key, use_dev=is_dev)
                    result = verify_finding(
                        {"rule_id": rule_id, "evidence": evidence or {}},
                        gzip.decompress(log_text).decode("utf-8", "replace"),
                        resp.content,
                    )
        except Exception as exc:
            logger.warning("[AudioVerify] finding %s failed: %s", fid, exc)
            result = unverifiable("error")

        verdict = result.get("audio_verified", "unverifiable")
        counts[verdict if verdict in counts else "unverifiable"] += 1

        # Only `spoke` raises confidence. A `silent` verdict does NOT clear the
        # finding: alignment drifts ~1-1.5s, so absence in the window is much
        # weaker evidence than presence. Confirm or abstain, never contradict.
        confidence = "high" if verdict == "spoke" else None
        cur.execute(
            """UPDATE call_quality_findings
                  SET evidence = COALESCE(evidence,'{}'::jsonb) || %s::jsonb,
                      confidence = COALESCE(%s, confidence)
                WHERE id = %s AND status = 'open'""",
            (json.dumps(result), confidence, fid),
        )

    conn.commit()
    conn.close()

    summary = {"success": True, "env": "dev" if is_dev else "prod",
               "considered": len(rows), **counts}
    logger.info("[AudioVerify] %s", json.dumps(summary))
    return summary
