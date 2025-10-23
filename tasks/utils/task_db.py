import os
import psycopg2
from psycopg2.extras import Json
from typing import Optional, Dict, Any
from datetime import datetime


def _get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url)


def mark_task_running(*, task_id: str, celery_task_id: str, message: Optional[str] = None,
                      details: Optional[Dict[str, Any]] = None, actor: str = "celery") -> Optional[Dict[str, Any]]:
    """Set tasks.status='running' and add a 'running' event into task_events.

    Returns a dict with {'task_id': ..., 'attempt': ...} if updated, else None.
    """
    if not task_id:
        return None

    details = details.copy() if details else {}
    details.setdefault('celery_task_id', celery_task_id)

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Update tasks row and get canonical id and attempts after update
                cur.execute(
                    """
                    UPDATE public.tasks t
                    SET
                        status = 'running'::task_status,
                        started_at = COALESCE(started_at, now()),
                        updated_at = now(),
                        celery_task_id = COALESCE(celery_task_id, %s),
                        attempts = attempts + CASE WHEN t.status = 'queued'::task_status THEN 1 ELSE 0 END
                    WHERE t.task_id = %s
                    RETURNING t.task_id, t.attempts
                    """,
                    (celery_task_id, task_id)
                )
                row = cur.fetchone()
                if not row:
                    return None
                canonical_task_id, attempts = row[0], row[1]

                # Insert event (let event_type default to 'info')
                cur.execute(
                    """
                    INSERT INTO public.task_events (task_id, status, progress, message, details, actor, attempt)
                    VALUES (%s, 'running'::task_status, NULL, %s, %s, %s, %s)
                    """,
                    (canonical_task_id, message, Json(details), actor, attempts)
                )
                return {"task_id": str(canonical_task_id), "attempt": attempts}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_task_failed(*, task_id: str, celery_task_id: str, error_code: Optional[str] = None,
                     error_message: Optional[str] = None, details: Optional[Dict[str, Any]] = None,
                     actor: str = "celery", progress: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Set tasks.status='failed' and add a 'failed' event into task_events."""
    if not task_id:
        return None

    details = details.copy() if details else {}
    details.setdefault('celery_task_id', celery_task_id)
    if error_code and 'error_code' not in details:
        details['error_code'] = error_code

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.tasks t
                    SET
                        status = 'failed'::task_status,
                        finished_at = now(),
                        updated_at = now(),
                        celery_task_id = COALESCE(celery_task_id, %s),
                        error_code = COALESCE(%s, error_code),
                        error_message = COALESCE(%s, error_message),
                        progress = COALESCE(%s, progress)
                    WHERE t.task_id = %s
                    RETURNING t.task_id, t.attempts
                    """,
                    (celery_task_id, error_code, error_message, progress, task_id)
                )
                row = cur.fetchone()
                if not row:
                    return None
                canonical_task_id, attempts = row[0], row[1]

                cur.execute(
                    """
                    INSERT INTO public.task_events (task_id, status, progress, message, details, actor, attempt)
                    VALUES (%s, 'failed'::task_status, %s, %s, %s, %s, %s)
                    """,
                    (canonical_task_id, progress, error_message, Json(details), actor, attempts)
                )
                return {"task_id": str(canonical_task_id), "attempt": attempts}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_task_succeeded(*, task_id: str, celery_task_id: str, details: Optional[Dict[str, Any]] = None,
                        actor: str = "celery", progress: int = 100, output: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Set tasks.status='succeeded' and add a 'succeeded' event into task_events."""
    if not task_id:
        return None

    details = details.copy() if details else {}
    details.setdefault('celery_task_id', celery_task_id)

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE public.tasks t
                    SET
                        status = 'succeeded'::task_status,
                        finished_at = now(),
                        updated_at = now(),
                        celery_task_id = COALESCE(celery_task_id, %s),
                        progress = %s,
                        output = COALESCE(%s, output)
                    WHERE t.task_id = %s
                    RETURNING t.task_id, t.attempts
                    """,
                    (celery_task_id, progress, Json(output) if output is not None else None, task_id)
                )
                row = cur.fetchone()
                if not row:
                    return None
                canonical_task_id, attempts = row[0], row[1]

                cur.execute(
                    """
                    INSERT INTO public.task_events (task_id, status, progress, message, details, actor, attempt)
                    VALUES (%s, 'succeeded'::task_status, %s, %s, %s, %s, %s)
                    """,
                    (canonical_task_id, progress, 'Completed', Json(details), actor, attempts)
                )
                return {"task_id": str(canonical_task_id), "attempt": attempts}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def record_task_artifact(*, task_id: str, kind: str, uri: str,
                         provider: str = 'cloudflare_r2',
                         bucket: Optional[str] = None,
                         object_key: Optional[str] = None,
                         version_id: Optional[str] = None,
                         etag: Optional[str] = None,
                         mime_type: Optional[str] = None,
                         size_bytes: Optional[int] = None,
                         checksum: Optional[str] = None,
                         expires_at: Optional[datetime] = None,
                         metadata: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """Insert a task artifact row and return the artifact_id.

    Required: task_id, kind, uri. Provider defaults to 'cloudflare_r2'.
    """
    if not task_id or not kind or not uri:
        return None

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.task_artifacts
                        (task_id, kind, uri, provider, bucket, object_key, version_id, etag, mime_type, bytes, checksum, expires_at, metadata)
                    VALUES
                        (%s, %s, %s, %s::storage_provider, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING artifact_id
                    """,
                    (
                        task_id,
                        kind,
                        uri,
                        provider,
                        bucket,
                        object_key,
                        version_id,
                        etag,
                        mime_type,
                        size_bytes,
                        checksum,
                        expires_at,
                        Json(metadata or {})
                    )
                )
                row = cur.fetchone()
                return int(row[0]) if row else None
    finally:
        try:
            conn.close()
        except Exception:
            pass
