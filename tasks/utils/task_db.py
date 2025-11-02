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


def upsert_tenant_integration_param(*, tenant_integration_param: Optional[Dict[str, Any]] = None,
                                    analysis_result: Optional[Dict[str, Any]] = None,
                                    ai_description: Optional[str] = None) -> Optional[int]:
    """Upsert tenant_integration_params table based on the provided parameter dict.
    
    If param_id is present: UPDATE the existing row and set status='configured', optionally update value_json and ai_description
    If param_id is missing: INSERT a new row with status='configured' and optional analysis result and description
    
    Args:
        tenant_integration_param: Dict containing integration parameter info
        analysis_result: Optional dict containing the OpenAI analysis result to store in value_json
        ai_description: Optional string containing AI-generated description to store in ai_description field
    
    Returns the param_id if successful, None otherwise.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info(f"🔍 [DB] upsert_tenant_integration_param called")
    logger.info(f"🔍 [DB] tenant_integration_param: {tenant_integration_param}")
    
    if not tenant_integration_param:
        logger.warning(f"⚠️ [DB] tenant_integration_param is None/empty - returning None")
        return None
    
    # Extract fields from the dict (handle both camelCase from API and snake_case)
    param_id = tenant_integration_param.get('paramId') or tenant_integration_param.get('param_id')
    tenant_id = tenant_integration_param.get('tenantId') or tenant_integration_param.get('tenant_id')
    location_id = tenant_integration_param.get('locationId') or tenant_integration_param.get('location_id')
    provider = tenant_integration_param.get('provider')
    service = tenant_integration_param.get('service')
    param_code = tenant_integration_param.get('paramCode') or tenant_integration_param.get('param_code')
    param_kind = tenant_integration_param.get('paramKind') or tenant_integration_param.get('param_kind')
    
    logger.info(f"🔍 [DB] Extracted fields: param_id={param_id}, tenant_id={tenant_id}, location_id={location_id}")
    logger.info(f"🔍 [DB] provider={provider}, service={service}, param_code={param_code}, param_kind={param_kind}")
    
    # Validate required fields
    if not tenant_id or not provider or not param_code or not param_kind:
        logger.warning(f"⚠️ [DB] Validation FAILED - Missing required fields:")
        logger.warning(f"⚠️ [DB]   tenant_id: {tenant_id} (present: {bool(tenant_id)})")
        logger.warning(f"⚠️ [DB]   provider: {provider} (present: {bool(provider)})")
        logger.warning(f"⚠️ [DB]   param_code: {param_code} (present: {bool(param_code)})")
        logger.warning(f"⚠️ [DB]   param_kind: {param_kind} (present: {bool(param_kind)})")
        return None
    
    logger.info(f"✅ [DB] Validation PASSED - all required fields present")
    
    # Prepare value_json - use analysis_result if provided, otherwise empty dict
    value_json_data = analysis_result if analysis_result else {}
    logger.info(f"🔍 [DB] value_json_data prepared, type: {type(value_json_data)}")
    
    conn = _get_conn()
    logger.info(f"🔍 [DB] Database connection obtained")
    
    try:
        with conn:
            with conn.cursor() as cur:
                if param_id:
                    logger.info(f"🔄 [DB] UPDATE path - param_id={param_id} exists, updating existing row")
                    # UPDATE existing row
                    if analysis_result or ai_description:
                        # Build SET clause dynamically based on what we have
                        set_clauses = ["status = 'configured'::integration_status", "updated_at = now()"]
                        params = []
                        
                        if analysis_result:
                            set_clauses.append("value_json = %s")
                            params.append(Json(value_json_data))
                            logger.info(f"🔍 [DB] Will update value_json")
                        
                        if ai_description:
                            set_clauses.append("ai_description = %s")
                            params.append(ai_description)
                            logger.info(f"🔍 [DB] Will update ai_description")
                        
                        params.append(param_id)
                        
                        sql = f"""
                            UPDATE public.tenant_integration_params
                            SET {', '.join(set_clauses)}
                            WHERE param_id = %s
                            RETURNING param_id
                            """
                        logger.info(f"🔍 [DB] Executing UPDATE with {len(params)} params")
                        cur.execute(sql, tuple(params))
                    else:
                        # Update without changing value_json or ai_description
                        logger.info(f"🔍 [DB] Updating status only (no analysis/description)")
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
                    result = int(row[0]) if row else None
                    logger.info(f"✅ [DB] UPDATE completed, returned param_id: {result}")
                    return result
                else:
                    logger.info(f"➕ [DB] INSERT path - no param_id provided, creating new row")
                    # INSERT new row (no unique constraint, so no ON CONFLICT needed)
                    # Build INSERT dynamically based on optional fields
                    insert_fields = ["tenant_id", "location_id", "provider", "service", "param_code", "param_kind", "value_json", "status"]
                    insert_values = [tenant_id, location_id, provider, service, param_code, param_kind, Json(value_json_data), 'configured']
                    
                    logger.info(f"🔍 [DB] INSERT will use: tenant_id={tenant_id}, location_id={location_id}")
                    logger.info(f"🔍 [DB] provider={provider}, service={service}, param_code={param_code}, param_kind={param_kind}")
                    logger.info(f"🔍 [DB] status='configured', value_json type={type(value_json_data)}")
                    
                    if ai_description:
                        insert_fields.append("ai_description")
                        insert_values.append(ai_description)
                        logger.info(f"🔍 [DB] Will also insert ai_description ({len(ai_description)} chars)")
                    
                    placeholders = ', '.join(['%s'] * len(insert_values))
                    
                    sql = f"""
                        INSERT INTO public.tenant_integration_params
                            ({', '.join(insert_fields)})
                        VALUES
                            ({placeholders})
                        RETURNING param_id
                        """
                    logger.info(f"🔍 [DB] Executing INSERT with {len(insert_values)} values")
                    logger.info(f"🔍 [DB] SQL: {sql}")
                    
                    cur.execute(sql, tuple(insert_values))
                    row = cur.fetchone()
                    result = int(row[0]) if row else None
                    logger.info(f"✅ [DB] INSERT completed, new param_id: {result}")
                    return result
    except Exception as e:
        logger.error(f"❌ [DB] Database error: {type(e).__name__}: {e}")
        logger.exception(f"🔍 [DB] Full traceback:")
        return None
    finally:
        try:
            conn.close()
            logger.info(f"🔍 [DB] Database connection closed")
        except Exception as close_e:
            logger.warning(f"⚠️ [DB] Error closing connection: {close_e}")


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
