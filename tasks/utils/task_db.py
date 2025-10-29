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


def upsert_tenant_integration_param(*, tenant_integration_param: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """Upsert tenant_integration_params table based on the provided parameter dict.
    
    If param_id is present: UPDATE the existing row and set status='configured'
    If param_id is missing: INSERT a new row with status='configured' and empty JSON value
    
    Returns the param_id if successful, None otherwise.
    """
    if not tenant_integration_param:
        return None
    
    # Extract fields from the dict (handle both camelCase from API and snake_case)
    param_id = tenant_integration_param.get('paramId') or tenant_integration_param.get('param_id')
    tenant_id = tenant_integration_param.get('tenantId') or tenant_integration_param.get('tenant_id')
    location_id = tenant_integration_param.get('locationId') or tenant_integration_param.get('location_id')
    provider = tenant_integration_param.get('provider')
    service = tenant_integration_param.get('service')
    param_code = tenant_integration_param.get('paramCode') or tenant_integration_param.get('param_code')
    param_kind = tenant_integration_param.get('paramKind') or tenant_integration_param.get('param_kind')
    
    # Validate required fields
    if not tenant_id or not provider or not param_code or not param_kind:
        return None
    
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if param_id:
                    # UPDATE existing row
                    cur.execute(
                        """
                        UPDATE public.tenant_integration_params
                        SET
                            status = 'configured'::integration_status,
                            updated_at = now()
                        WHERE param_id = %s
                        RETURNING param_id
                        """,
                        (param_id,)
                    )
                    row = cur.fetchone()
                    return int(row[0]) if row else None
                else:
                    # INSERT new row with ON CONFLICT handling
                    cur.execute(
                        """
                        INSERT INTO public.tenant_integration_params
                            (tenant_id, location_id, provider, service, param_code, param_kind, value_json, status)
                        VALUES
                            (%s, %s, %s, %s, %s, %s, %s, 'configured'::integration_status)
                        ON CONFLICT (tenant_id, location_id, provider, service, param_code, param_kind, value_text, value_numeric, value_boolean, value_json)
                        DO UPDATE SET
                            status = 'configured'::integration_status,
                            updated_at = now()
                        RETURNING param_id
                        """,
                        (tenant_id, location_id, provider, service, param_code, param_kind, Json({}))
                    )
                    row = cur.fetchone()
                    return int(row[0]) if row else None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_ai_knowledge_type_by_key(key: str) -> Optional[Dict[str, Any]]:
    """Get AI knowledge type configuration from ai_knowledge_types table by key.
    
    Returns a dict with knowledge type details if found and active, None otherwise.
    """
    if not key:
        return None
    
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        id, category, key, name, description, is_active,
                        created_at, updated_at, display_order, category_display_name,
                        color_code, icon, schema_template, allowed_source, ai_prompt
                    FROM public.ai_knowledge_types
                    WHERE key = %s AND is_active = true
                    LIMIT 1
                    """,
                    (key,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                
                return {
                    'id': row[0],
                    'category': row[1],
                    'key': row[2],
                    'name': row[3],
                    'description': row[4],
                    'is_active': row[5],
                    'created_at': row[6],
                    'updated_at': row[7],
                    'display_order': row[8],
                    'category_display_name': row[9],
                    'color_code': row[10],
                    'icon': row[11],
                    'schema_template': row[12],
                    'allowed_source': row[13],
                    'ai_prompt': row[14],
                }
    finally:
        try:
            conn.close()
        except Exception:
            pass
