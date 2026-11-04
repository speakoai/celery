import logging
import os
import psycopg2
from psycopg2.extras import Json
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


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


def _record_knowledge_revision(cur, tenant_id, location_id, param_code, new_text, source, old_text):
    """Version history for Speako knowledge (knowledge_revisions). Runs inside the caller's
    transaction. When History doesn't already end with the previous text (first recorded change,
    or a write that skipped history) it is kept as a 'backfill' version so it can be restored. Unchanged text (e.g. a routine auto-sync) records nothing.
    Best-effort: a missing table (pre-DDL env) is ignored."""
    if old_text is not None and old_text == new_text:
        return
    try:
        cur.execute("SAVEPOINT knowledge_rev")
        item = (tenant_id, location_id, param_code)
        # Keep the text being replaced unless History already ends with it (first recorded change,
        # or a write that skipped version history), so nothing that was live is lost.
        cur.execute(
            """SELECT value_text FROM public.knowledge_revisions
                WHERE tenant_id = %s AND location_id IS NOT DISTINCT FROM %s AND param_code = %s
                  AND state = 'live'
                ORDER BY created_at DESC LIMIT 1""",
            item,
        )
        _last = cur.fetchone()
        if old_text and old_text != new_text and (_last is None or _last[0] != old_text):
            cur.execute(
                """INSERT INTO public.knowledge_revisions
                     (tenant_id, location_id, param_code, value_text, state, source)
                   VALUES (%s, %s, %s, %s, 'superseded', 'backfill')""",
                item + (old_text,),
            )
        cur.execute(
            """UPDATE public.knowledge_revisions SET state = 'superseded'
                WHERE tenant_id = %s AND location_id IS NOT DISTINCT FROM %s AND param_code = %s
                  AND state = 'live'""",
            item,
        )
        cur.execute(
            """INSERT INTO public.knowledge_revisions
                 (tenant_id, location_id, param_code, value_text, state, source)
               VALUES (%s, %s, %s, %s, 'live', %s)""",
            item + (new_text, source),
        )
        cur.execute("RELEASE SAVEPOINT knowledge_rev")
    except Exception as e:
        cur.execute("ROLLBACK TO SAVEPOINT knowledge_rev")
        logger.warning(f"[knowledge_revisions] not recorded for {param_code}: {e}")


def knowledge_approval_enabled(tenant_id) -> bool:
    """Knowledge approval (docs/plans/knowledge-approval.md, Phase 2): chat_settings switch.
    Missing table/column (pre-DDL env) or no row = off."""
    if not tenant_id:
        return False
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT knowledge_approval_enabled FROM public.chat_settings WHERE tenant_id = %s",
                    (tenant_id,),
                )
                row = cur.fetchone()
                return bool(row and row[0])
    except Exception as e:
        logger.warning(f"[knowledge approval] setting not read for tenant {tenant_id}: {e}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def save_knowledge_draft(*, tenant_id, location_id, param_code, value_text, source) -> Optional[int]:
    """With knowledge approval on, a file/URL import becomes the item's pending draft
    (knowledge_revisions state='pending', one per item) instead of changing
    tenant_integration_params; the live text, voice agent and chat index are untouched until an
    owner/admin approves in the dashboard. A NEW draft also adds a bell notification
    ('knowledge' type). Returns the revision_id, or None on failure."""
    if not (tenant_id and location_id and param_code and value_text):
        return None
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT revision_id FROM public.knowledge_revisions
                        WHERE tenant_id = %s AND location_id = %s AND param_code = %s AND state = 'pending'
                        FOR UPDATE""",
                    (tenant_id, location_id, param_code),
                )
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """UPDATE public.knowledge_revisions
                              SET value_text = %s, source = %s, created_by = NULL,
                                  created_by_name = NULL, created_at = clock_timestamp(), check_result = NULL
                            WHERE revision_id = %s""",
                        (value_text, source, row[0]),
                    )
                    return int(row[0])
                cur.execute(
                    """INSERT INTO public.knowledge_revisions
                         (tenant_id, location_id, param_code, value_text, state, source)
                       VALUES (%s, %s, %s, %s, 'pending', %s)
                       RETURNING revision_id""",
                    (tenant_id, location_id, param_code, value_text, source),
                )
                revision_id = int(cur.fetchone()[0])
                try:
                    cur.execute("SAVEPOINT knowledge_notify")
                    cur.execute("SELECT name FROM public.ai_knowledge_types WHERE key = %s LIMIT 1", (param_code,))
                    _n = cur.fetchone()
                    name = (_n[0] if _n else None) or "Knowledge"
                    how = "A website import" if source == "url" else "A file import"
                    cur.execute(
                        """INSERT INTO public.notifications
                             (type_key, title, message, link_url, link_label, metadata, is_broadcast)
                           VALUES ('knowledge', %s, %s, %s, 'Review changes', %s, false)
                           RETURNING notification_id""",
                        (
                            f"{name}: changes waiting for approval",
                            f"{how} updated {name}. The AI keeps using the current version until an owner or admin approves.",
                            f"/agent/knowledges/all?review={param_code}",
                            Json({"kind": "knowledge_draft", "param_code": param_code}),
                        ),
                    )
                    nid = cur.fetchone()[0]
                    cur.execute(
                        "INSERT INTO public.tenant_notifications (tenant_id, notification_id) VALUES (%s, %s)",
                        (tenant_id, nid),
                    )
                    cur.execute("RELEASE SAVEPOINT knowledge_notify")
                except Exception as ne:
                    # The draft is the source of truth; a missing bell entry isn't fatal.
                    cur.execute("ROLLBACK TO SAVEPOINT knowledge_notify")
                    logger.warning(f"[knowledge approval] notification not created for {param_code}: {ne}")
                return revision_id
    except Exception as e:
        logger.warning(f"[knowledge approval] draft not saved for {param_code}: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upsert_tenant_integration_param(*, tenant_integration_param: Optional[Dict[str, Any]] = None,
                                    analysis_result: Optional[Dict[str, Any]] = None,
                                    ai_description: Optional[str] = None,
                                    value_text: Optional[str] = None,
                                    revision_source: Optional[str] = None) -> Optional[int]:
    """Upsert tenant_integration_params table based on the provided parameter dict.
    
    If param_id is present: UPDATE the existing row and set status='configured', optionally update value_json, ai_description, and value_text
    If param_id is missing: INSERT a new row with status='configured' and optional analysis result, description, and text
    
    Args:
        tenant_integration_param: Dict containing integration parameter info
        analysis_result: Optional dict containing the OpenAI analysis result to store in value_json
        ai_description: Optional string containing AI-generated description to store in ai_description field
        value_text: Optional string containing markdown or text content to store in value_text field
        revision_source: for Speako knowledge writes, records a knowledge_revisions version
            ('file' | 'url' | 'sync' | 'scrape') in the same transaction (version history,
            docs/plans/knowledge-approval.md). None = no revision (non-knowledge params).
    
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
    
    # Prepare value_json - use analysis_result if provided, otherwise empty dict
    value_json_data = analysis_result if analysis_result else {}
    
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                record_rev = bool(
                    revision_source and value_text
                    and provider == 'speako' and service == 'knowledge'
                )
                old_text = None
                if param_id and record_rev:
                    cur.execute(
                        "SELECT value_text FROM public.tenant_integration_params WHERE param_id = %s FOR UPDATE",
                        (param_id,),
                    )
                    _old = cur.fetchone()
                    old_text = _old[0] if _old else None
                if param_id:
                    # UPDATE existing row
                    if analysis_result or ai_description or value_text:
                        # Build SET clause dynamically based on what we have
                        set_clauses = ["status = 'configured'::integration_status", "updated_at = now()"]
                        params = []
                        
                        if analysis_result:
                            set_clauses.append("value_json = %s")
                            params.append(Json(value_json_data))
                        
                        if ai_description:
                            set_clauses.append("ai_description = %s")
                            params.append(ai_description)
                        
                        if value_text:
                            set_clauses.append("value_text = %s")
                            params.append(value_text)
                        
                        params.append(param_id)
                        
                        cur.execute(
                            f"""
                            UPDATE public.tenant_integration_params
                            SET {', '.join(set_clauses)}
                            WHERE param_id = %s
                            RETURNING param_id
                            """,
                            tuple(params)
                        )
                    else:
                        # Update without changing value_json, ai_description, or value_text
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
                    if row and record_rev:
                        _record_knowledge_revision(cur, tenant_id, location_id, param_code,
                                                   value_text, revision_source, old_text)
                    return int(row[0]) if row else None
                else:
                    # INSERT new row (no unique constraint, so no ON CONFLICT needed)
                    # Build INSERT dynamically based on optional fields
                    insert_fields = ["tenant_id", "location_id", "provider", "service", "param_code", "param_kind", "value_json", "status"]
                    insert_values = [tenant_id, location_id, provider, service, param_code, param_kind, Json(value_json_data), 'configured']
                    
                    if ai_description:
                        insert_fields.append("ai_description")
                        insert_values.append(ai_description)
                    
                    if value_text:
                        insert_fields.append("value_text")
                        insert_values.append(value_text)
                    
                    placeholders = ', '.join(['%s'] * len(insert_values))
                    
                    cur.execute(
                        f"""
                        INSERT INTO public.tenant_integration_params
                            ({', '.join(insert_fields)})
                        VALUES
                            ({placeholders})
                        RETURNING param_id
                        """,
                        tuple(insert_values)
                    )
                    row = cur.fetchone()
                    if row and record_rev:
                        _record_knowledge_revision(cur, tenant_id, location_id, param_code,
                                                   value_text, revision_source, None)
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
