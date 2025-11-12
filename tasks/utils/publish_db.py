"""
Database operations for ElevenLabs agent publishing.

This module provides all database query functions needed for the publishing workflow,
including publish job management, knowledge collection, and status tracking.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from typing import Optional, Dict, Any, List
from datetime import datetime
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def _get_conn():
    """Get PostgreSQL database connection."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url)


def get_publish_job(tenant_id: str, publish_job_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch publish job details from publish_jobs table.
    
    Args:
        tenant_id: Tenant identifier
        publish_job_id: Publish job identifier
    
    Returns:
        Dict with publish job details if found, None otherwise
    """
    logger.info(f"[publish_db] Fetching publish job: tenant_id={tenant_id}, publish_job_id={publish_job_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        tenant_id,
                        publish_job_id,
                        location_id,
                        job_type,
                        payload_json,
                        knowledge_file_url,
                        elevenlabs_agent_id,
                        requested_by_user_id,
                        publish_status,
                        started_at,
                        finished_at,
                        http_status_code,
                        response_json,
                        error_message,
                        external_task_id,
                        correlation_id,
                        created_at,
                        updated_at
                    FROM publish_jobs
                    WHERE tenant_id = %s AND publish_job_id = %s
                    """,
                    (tenant_id, publish_job_id)
                )
                row = cur.fetchone()
                if row:
                    result = dict(row)
                    logger.info(
                        f"[publish_db] Found publish job: job_type={result.get('job_type')}, "
                        f"status={result.get('publish_status')}, "
                        f"knowledge_file_url={result.get('knowledge_file_url')}"
                    )
                    return result
                else:
                    logger.warning(f"[publish_db] Publish job not found: tenant_id={tenant_id}, publish_job_id={publish_job_id}")
                    return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_publish_job_status(
    tenant_id: str,
    publish_job_id: str,
    status: str,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    knowledge_file_url: Optional[str] = None,
    http_status_code: Optional[int] = None,
    response_json: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None
) -> None:
    """
    Update publish job status and related fields.
    
    Args:
        tenant_id: Tenant identifier
        publish_job_id: Publish job identifier
        status: New status ('idle', 'in_progress', 'succeeded', 'failed')
        started_at: Optional start timestamp
        finished_at: Optional finish timestamp
        knowledge_file_url: Optional knowledge file URL
        http_status_code: Optional HTTP status code from external API
        response_json: Optional response data from external API
        error_message: Optional error message
    """
    logger.info(
        f"[publish_db] Updating publish job: tenant_id={tenant_id}, publish_job_id={publish_job_id}, "
        f"status={status}, knowledge_file_url={knowledge_file_url}"
    )
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE publish_jobs 
                    SET 
                        publish_status = %s,
                        started_at = COALESCE(%s, started_at),
                        finished_at = COALESCE(%s, finished_at),
                        knowledge_file_url = COALESCE(%s, knowledge_file_url),
                        http_status_code = COALESCE(%s, http_status_code),
                        response_json = COALESCE(%s::jsonb, response_json),
                        error_message = COALESCE(%s, error_message),
                        updated_at = now()
                    WHERE tenant_id = %s AND publish_job_id = %s
                    """,
                    (
                        status,
                        started_at,
                        finished_at,
                        knowledge_file_url,
                        http_status_code,
                        Json(response_json) if response_json is not None else None,
                        error_message,
                        tenant_id,
                        publish_job_id
                    )
                )
                rows_updated = cur.rowcount
                logger.info(
                    f"[publish_db] Updated {rows_updated} row(s) in publish_jobs table"
                )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_elevenlabs_agent_id(tenant_id: str, location_id: str) -> tuple[str, str, str]:
    """
    Get ElevenLabs agent ID, location name, and timezone from locations table.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Tuple of (agent_id, location_name, timezone)
        Example: ("agent_01jvtvy0x0e978a0xa4dk3cgmm", "Nail Lab Sydney", "Australia/Sydney")
    
    Raises:
        ValueError: If agent ID is not configured or location not found
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT elevenlabs_agent_id, name, timezone
                    FROM locations 
                    WHERE tenant_id = %s AND location_id = %s
                    """,
                    (tenant_id, location_id)
                )
                row = cur.fetchone()
                
                if not row:
                    raise ValueError(
                        f"Location not found: tenant_id={tenant_id}, location_id={location_id}"
                    )
                
                agent_id, location_name, timezone = row[0], row[1], row[2]
                
                if not agent_id:
                    raise ValueError(
                        f"ElevenLabs agent ID not configured for location: "
                        f"tenant_id={tenant_id}, location_id={location_id}"
                    )
                
                # Provide fallbacks for missing data
                if not location_name:
                    location_name = f"Location {location_id}"
                if not timezone:
                    timezone = "UTC"
                
                return agent_id, location_name, timezone
    finally:
        try:
            conn.close()
        except Exception:
            pass


def collect_speako_knowledge(tenant_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Collect all configured Speako knowledge entries for a location.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        List of dicts with keys: param_id, value_text, param_code, created_at
        Ordered by created_at ascending (oldest first)
    """
    logger.info(f"[publish_db] Collecting Speako knowledge: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        param_id,
                        value_text,
                        param_code,
                        created_at
                    FROM tenant_integration_params 
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND provider = 'speako' 
                      AND service = 'knowledge' 
                      AND status IN ('configured', 'published')
                      AND value_text IS NOT NULL
                    ORDER BY created_at ASC
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                logger.info(f"[publish_db] Found {len(result)} Speako knowledge entries")
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def collect_speako_greetings(tenant_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Collect all configured Speako greeting entries for a location.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        List of dicts with keys: param_id, value_text, param_code
        Ordered by created_at ascending (oldest first)
    """
    logger.info(f"[publish_db] Collecting Speako greetings: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        param_id,
                        value_text,
                        param_code
                    FROM tenant_integration_params 
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND provider = 'speako' 
                      AND service = 'greetings' 
                      AND status = 'configured'
                    ORDER BY created_at ASC
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                logger.info(f"[publish_db] Found {len(result)} Speako greeting entries")
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_ai_prompt_type(param_code: str) -> Optional[Dict[str, Any]]:
    """
    Get AI prompt type details from ai_prompt_types table.
    
    Args:
        param_code: The param_code to look up (maps to ai_prompt_types.code)
    
    Returns:
        Dict with keys: code, display_name, description, variables_schema
        Returns None if not found
    """
    logger.info(f"[publish_db] Looking up AI prompt type: param_code={param_code}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        code,
                        display_name,
                        description,
                        variables_schema
                    FROM ai_prompt_types 
                    WHERE code = %s
                    """,
                    (param_code,)
                )
                row = cur.fetchone()
                if row:
                    result = dict(row)
                    logger.info(f"[publish_db] Found AI prompt type: code={result['code']}, display_name={result['display_name']}")
                    return result
                else:
                    logger.warning(f"[publish_db] AI prompt type not found for param_code={param_code}")
                    return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_location_operation_hours(tenant_id: str, location_id: str) -> Dict[str, Any]:
    """
    Get location operation hours from location_availability table.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Dict with day names as keys and schedule as values
        Example: {"Monday": [{"start_time": "09:00", "end_time": "17:00"}], "Sunday": [{"status": "closed"}]}
    """
    logger.info(f"[publish_db] Fetching operation hours: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    
    # Map PostgreSQL day_of_week (0=Sunday) to day names
    day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT day_of_week, start_time, end_time, is_closed 
                    FROM location_availability 
                    WHERE tenant_id = %s 
                      AND location_id = %s 
                      AND type = 'recurring' 
                      AND is_active = true
                    ORDER BY day_of_week, start_time
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                
                # Build schedule JSON
                schedule = {}
                for row in rows:
                    day_num = row['day_of_week']
                    day_name = day_names[day_num]
                    
                    if day_name not in schedule:
                        schedule[day_name] = []
                    
                    if row['is_closed']:
                        schedule[day_name] = [{"status": "closed"}]
                    else:
                        # Format times as HH:MM strings
                        start_time = row['start_time'].strftime('%H:%M') if row['start_time'] else None
                        end_time = row['end_time'].strftime('%H:%M') if row['end_time'] else None
                        
                        if start_time and end_time:
                            schedule[day_name].append({
                                "start_time": start_time,
                                "end_time": end_time
                            })
                
                logger.info(f"[publish_db] Built operation hours schedule: {len(schedule)} days configured")
                return schedule
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upsert_ai_prompt(
    tenant_id: str,
    location_id: str,
    type_code: str,
    title: str,
    body_template: str,
    name: str = None,
    locale: str = 'en-US',
    channel: str = 'web',
    variables_schema: Dict[str, Any] = None,
    metadata: Dict[str, Any] = None
) -> int:
    """
    Insert or update AI prompt in tenant_ai_prompts table.
    
    Deactivates any existing active prompt with the same type_code, locale, channel
    before inserting the new one.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        type_code: Type of prompt (e.g., 'first_message_after')
        title: Prompt title
        body_template: Prompt body text (fully resolved)
        name: Optional name for the prompt
        locale: Locale (default: 'en-US')
        channel: Channel (default: 'web')
        variables_schema: Optional variables schema as dict
        metadata: Optional metadata as dict
    
    Returns:
        prompt_id of the inserted/updated record
    """
    logger.info(f"[publish_db] Upserting AI prompt: type_code={type_code}, title={title}")
    conn = _get_conn()
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Get the current maximum version for this type_code
                cur.execute(
                    """
                    SELECT COALESCE(MAX(version), 0) as max_version
                    FROM tenant_ai_prompts
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND type_code = %s
                      AND locale = %s
                      AND channel = %s
                    """,
                    (tenant_id, location_id, type_code, locale, channel)
                )
                row = cur.fetchone()
                current_max_version = row[0] if row else 0
                new_version = current_max_version + 1
                
                logger.info(f"[publish_db] Current max version: {current_max_version}, new version: {new_version}")
                
                # Deactivate existing active prompts with same type/locale/channel and set archived_at
                cur.execute(
                    """
                    UPDATE tenant_ai_prompts
                    SET is_active = false, archived_at = now(), updated_at = now()
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND type_code = %s
                      AND locale = %s
                      AND channel = %s
                      AND is_active = true
                    """,
                    (tenant_id, location_id, type_code, locale, channel)
                )
                
                deactivated_count = cur.rowcount
                if deactivated_count > 0:
                    logger.info(f"[publish_db] Deactivated {deactivated_count} existing prompt(s) and set archived_at")
                
                # Insert new prompt with incremented version
                cur.execute(
                    """
                    INSERT INTO tenant_ai_prompts
                        (tenant_id, location_id, name, type_code, locale, channel, title, body_template, 
                         variables_schema, metadata, version, is_active, effective_from)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, now())
                    RETURNING prompt_id
                    """,
                    (
                        tenant_id,
                        location_id,
                        name or f"{type_code}_{locale}_{channel}",
                        type_code,
                        locale,
                        channel,
                        title,
                        body_template,
                        Json(variables_schema or {}),
                        Json(metadata or {}),
                        new_version
                    )
                )
                
                row = cur.fetchone()
                prompt_id = int(row[0]) if row else None
                
                logger.info(f"[publish_db] Inserted new AI prompt: prompt_id={prompt_id}, version={new_version}")
                
                # Cleanup old versions if exceeding the limit (keep max 3 versions)
                if new_version > 3:
                    version_threshold = new_version - 3
                    cur.execute(
                        """
                        DELETE FROM tenant_ai_prompts
                        WHERE tenant_id = %s 
                          AND location_id = %s
                          AND type_code = %s
                          AND locale = %s
                          AND channel = %s
                          AND version <= %s
                          AND is_active = false
                        """,
                        (tenant_id, location_id, type_code, locale, channel, version_threshold)
                    )
                    
                    deleted_count = cur.rowcount
                    if deleted_count > 0:
                        logger.info(
                            f"[publish_db] Deleted {deleted_count} old version(s) "
                            f"(version <= {version_threshold}) to maintain 3-version limit"
                        )
                
                return prompt_id
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_greeting_params_published(tenant_id: str, location_id: str, param_ids: List[int]) -> int:
    """
    Mark greeting parameters as 'published' after successful processing.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        param_ids: List of param_ids to mark as published
    
    Returns:
        Number of rows updated
    """
    if not param_ids:
        logger.info("[publish_db] No param_ids provided, skipping mark as published")
        return 0
    
    logger.info(f"[publish_db] Marking {len(param_ids)} greeting params as published: {param_ids}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenant_integration_params 
                    SET status = 'published', updated_at = now()
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND provider = 'speako' 
                      AND service = 'greetings' 
                      AND param_id = ANY(%s)
                      AND status = 'configured'
                    """,
                    (tenant_id, location_id, param_ids)
                )
                rows_updated = cur.rowcount
                logger.info(f"[publish_db] ✓ Marked {rows_updated} greeting params as published")
                return rows_updated
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_existing_elevenlabs_knowledge_ids(tenant_id: str, location_id: str) -> List[str]:
    """
    Get all existing ElevenLabs knowledge IDs for a location.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        List of ElevenLabs knowledge IDs (strings)
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT value_text 
                    FROM tenant_integration_params 
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND provider = 'elevenlabs' 
                      AND service = 'knowledge'
                      AND param_code = 'id'
                      AND param_kind = 'id'
                      AND value_text IS NOT NULL
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                return [row[0] for row in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def save_new_elevenlabs_knowledge_id(
    tenant_id: str,
    location_id: str,
    knowledge_id: str
) -> int:
    """
    Save new ElevenLabs knowledge ID to tenant_integration_params.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        knowledge_id: ElevenLabs knowledge document ID
    
    Returns:
        param_id of the inserted record
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenant_integration_params 
                      (tenant_id, location_id, provider, service, param_code, param_kind, value_text, status)
                    VALUES 
                      (%s, %s, 'elevenlabs', 'knowledge', 'id', 'id', %s, 'configured')
                    RETURNING param_id
                    """,
                    (tenant_id, location_id, knowledge_id)
                )
                row = cur.fetchone()
                return int(row[0]) if row else None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_speako_knowledge_published(
    tenant_id: str, 
    location_id: str,
    param_ids: List[int]
) -> int:
    """
    Mark specific Speako knowledge entries as 'published'.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        param_ids: List of param_ids to mark as published
    
    Returns:
        Number of rows updated
    """
    if not param_ids:
        logger.info("[publish_db] No param_ids provided, skipping mark as published")
        return 0
    
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenant_integration_params 
                    SET status = 'published', updated_at = now()
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND provider = 'speako' 
                      AND service = 'knowledge' 
                      AND param_id = ANY(%s)
                      AND status = 'configured'
                    """,
                    (tenant_id, location_id, param_ids)
                )
                rows_updated = cur.rowcount
                logger.info(f"[publish_db] Marked {rows_updated} knowledge entries as published (param_ids: {param_ids})")
                return rows_updated
    finally:
        try:
            conn.close()
        except Exception:
            pass


def delete_old_elevenlabs_knowledge_ids(
    tenant_id: str,
    location_id: str,
    knowledge_ids: List[str]
) -> int:
    """
    Delete old ElevenLabs knowledge IDs from tenant_integration_params.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        knowledge_ids: List of ElevenLabs knowledge IDs to delete
    
    Returns:
        Number of rows deleted
    """
    if not knowledge_ids:
        return 0
    
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM tenant_integration_params 
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND provider = 'elevenlabs' 
                      AND service = 'knowledge'
                      AND value_text = ANY(%s)
                    """,
                    (tenant_id, location_id, knowledge_ids)
                )
                return cur.rowcount
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_business_name(tenant_id: str) -> str:
    """
    Get business name from tenants table.
    
    Args:
        tenant_id: Tenant identifier
    
    Returns:
        Business name string
    """
    logger.info(f"[publish_db] Fetching business name: tenant_id={tenant_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT name FROM tenants WHERE tenant_id = %s",
                    (tenant_id,)
                )
                row = cur.fetchone()
                if row:
                    business_name = row[0]
                    logger.info(f"[publish_db] Found business name: {business_name}")
                    return business_name
                else:
                    logger.warning(f"[publish_db] Business name not found for tenant_id={tenant_id}")
                    return ""
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_location_name(tenant_id: str, location_id: str) -> str:
    """
    Get location name from locations table.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Location name string
    """
    logger.info(f"[publish_db] Fetching location name: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT name FROM locations WHERE tenant_id = %s AND location_id = %s",
                    (tenant_id, location_id)
                )
                row = cur.fetchone()
                if row:
                    location_name = row[0]
                    logger.info(f"[publish_db] Found location name: {location_name}")
                    return location_name
                else:
                    logger.warning(f"[publish_db] Location name not found for tenant_id={tenant_id}, location_id={location_id}")
                    return ""
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_privacy_url(tenant_id: str) -> str:
    """
    Get privacy policy URL from tenant_info table.
    
    Args:
        tenant_id: Tenant identifier
    
    Returns:
        Privacy policy URL string
    """
    logger.info(f"[publish_db] Fetching privacy URL: tenant_id={tenant_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT privacy_policy_url FROM tenant_info WHERE tenant_id = %s",
                    (tenant_id,)
                )
                row = cur.fetchone()
                if row and row[0]:
                    privacy_url = row[0]
                    logger.info(f"[publish_db] Found privacy URL: {privacy_url}")
                    return privacy_url
                else:
                    logger.warning(f"[publish_db] Privacy URL not found for tenant_id={tenant_id}")
                    return ""
    finally:
        try:
            conn.close()
        except Exception:
            pass


def collect_voice_dict_params(tenant_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Collect all configured voice dictionary parameters for a location.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        List of dicts with keys: param_id, service, param_code, value_text, value_numeric
        Ordered by created_at ascending (oldest first)
    """
    logger.info(f"[publish_db] Collecting voice dict params: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        param_id,
                        service,
                        param_code,
                        value_text,
                        value_numeric
                    FROM tenant_integration_params 
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND status = 'configured'
                      AND service IN ('agent', 'turn', 'conversation', 'tts')
                    ORDER BY created_at ASC
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                logger.info(f"[publish_db] Found {len(result)} voice dict params")
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_voice_dict_params_published(tenant_id: str, location_id: str, param_ids: List[int]) -> int:
    """
    Mark voice dict parameters as 'published' after successful processing.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        param_ids: List of param_ids to mark as published
    
    Returns:
        Number of rows updated
    """
    if not param_ids:
        logger.info("[publish_db] No param_ids provided, skipping mark as published")
        return 0
    
    logger.info(f"[publish_db] Marking {len(param_ids)} voice dict params as published: {param_ids}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenant_integration_params 
                    SET status = 'published', updated_at = now()
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND service IN ('agent', 'turn', 'conversation', 'tts', 'dictionary')
                      AND param_id = ANY(%s)
                      AND status = 'configured'
                    """,
                    (tenant_id, location_id, param_ids)
                )
                rows_updated = cur.rowcount
                logger.info(f"[publish_db] ✓ Marked {rows_updated} voice dict params as published")
                return rows_updated
    finally:
        try:
            conn.close()
        except Exception:
            pass


def collect_dictionary_entry(tenant_id: str, location_id: str) -> Optional[Dict[str, Any]]:
    """
    Collect the dictionary entry for a location (single entry).
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Dict with keys: param_id, tenant_id, location_id, value_text, value_json
        Returns None if no entry found
    """
    logger.info(f"[publish_db] Collecting dictionary entry: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        param_id,
                        tenant_id,
                        location_id,
                        value_text,
                        value_json
                    FROM tenant_integration_params 
                    WHERE tenant_id = %s 
                      AND location_id = %s
                      AND status = 'configured'
                      AND service = 'dictionary'
                    LIMIT 1
                    """,
                    (tenant_id, location_id)
                )
                row = cur.fetchone()
                if row:
                    result = dict(row)
                    logger.info(f"[publish_db] Found dictionary entry: param_id={result['param_id']}")
                    return result
                else:
                    logger.info(f"[publish_db] No dictionary entry found")
                    return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_dictionary_param_text(param_id: int, dictionary_id: str) -> int:
    """
    Update value_text field with dictionary_id after create/update.
    
    Args:
        param_id: Parameter ID to update
        dictionary_id: ElevenLabs dictionary ID to store
    
    Returns:
        Number of rows updated
    """
    logger.info(f"[publish_db] Updating dictionary param_id={param_id} with dictionary_id={dictionary_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenant_integration_params 
                    SET value_text = %s, updated_at = now()
                    WHERE param_id = %s
                    """,
                    (dictionary_id, param_id)
                )
                rows_updated = cur.rowcount
                logger.info(f"[publish_db] ✓ Updated {rows_updated} dictionary param")
                return rows_updated
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_prompt_fragments(fragment_keys: List[str]) -> Dict[str, str]:
    """
    Fetch multiple prompt fragments in a single database query (optimized).
    
    Args:
        fragment_keys: List of fragment_key values to fetch
    
    Returns:
        Dict mapping fragment_key to template_text
        Example: {'personality': '{{traits}}...', 'response_style_concise': 'Keep responses brief...'}
    """
    if not fragment_keys:
        logger.warning("[publish_db] No fragment_keys provided, returning empty dict")
        return {}
    
    logger.info(f"[publish_db] Fetching {len(fragment_keys)} prompt fragments: {fragment_keys}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT fragment_key, template_text
                    FROM ai_prompt_fragment
                    WHERE fragment_key = ANY(%s)
                    """,
                    (fragment_keys,)
                )
                rows = cur.fetchall()
                result = {row['fragment_key']: row['template_text'] for row in rows}
                logger.info(f"[publish_db] Found {len(result)} prompt fragments")
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def collect_personality_params(tenant_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Collect personality parameters from tenant_integration_params.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        List of dicts with keys: param_id, param_code, value_text, value_json, value_numeric
        Ordered by param_code
    """
    logger.info(f"[publish_db] Collecting personality params: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT param_id, param_code, value_text, value_json, value_numeric
                    FROM tenant_integration_params
                    WHERE tenant_id = %s
                      AND location_id = %s
                      AND service = 'agents'
                      AND provider = 'elevenlabs'
                      AND param_code IN ('traits', 'tone_of_voice', 'response_style', 'temperature', 'custom_instruction')
                      AND status = 'configured'
                    ORDER BY param_code
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                logger.info(f"[publish_db] Found {len(result)} personality params")
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_personality_params_published(tenant_id: str, location_id: str, param_ids: List[int]) -> int:
    """
    Mark personality parameters as 'published' after successful processing.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        param_ids: List of param IDs to mark as published
    
    Returns:
        Number of rows updated
    """
    if not param_ids:
        logger.info("[publish_db] No param_ids provided, skipping mark as published")
        return 0
    
    logger.info(f"[publish_db] Marking {len(param_ids)} personality params as published: {param_ids}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenant_integration_params
                    SET status = 'published',
                        updated_at = now()
                    WHERE tenant_id = %s
                      AND location_id = %s
                      AND param_id = ANY(%s)
                    RETURNING param_id, param_code
                    """,
                    (tenant_id, location_id, param_ids)
                )
                rows_updated = cur.rowcount
                logger.info(f"[publish_db] ✓ Marked {rows_updated} personality params as published")
                return rows_updated
    finally:
        try:
            conn.close()
        except Exception:
            pass


def collect_tool_params(tenant_id: str, location_id: str) -> List[Dict[str, Any]]:
    """
    Collect tool parameters from tenant_integration_params with tool_ids from ai_tool_types.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        List of dicts with keys: param_id, param_code, value_json, tool_ids
        Ordered by param_code
    """
    logger.info(f"[publish_db] Collecting tool params: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        tip.param_id,
                        tip.param_code,
                        tip.value_json,
                        att.tool_ids
                    FROM tenant_integration_params tip
                    INNER JOIN ai_tool_types att ON tip.param_code = att.key
                    WHERE tip.tenant_id = %s
                      AND tip.location_id = %s
                      AND tip.service = 'tool'
                      AND tip.provider = 'speako'
                      AND tip.status IN ('configured', 'published')
                      AND att.is_active = true
                    ORDER BY tip.param_code
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                logger.info(f"[publish_db] Found {len(result)} tool params")
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def mark_tool_params_published(tenant_id: str, location_id: str, param_ids: List[int]) -> int:
    """
    Mark tool parameters as 'published' after successful processing.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        param_ids: List of param IDs to mark as published
    
    Returns:
        Number of rows updated
    """
    if not param_ids:
        logger.info("[publish_db] No param_ids provided, skipping mark as published")
        return 0
    
    logger.info(f"[publish_db] Marking {len(param_ids)} tool params as published: {param_ids}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenant_integration_params
                    SET status = 'published',
                        updated_at = now()
                    WHERE tenant_id = %s
                      AND location_id = %s
                      AND service = 'tool'
                      AND provider = 'speako'
                      AND param_id = ANY(%s)
                    RETURNING param_id, param_code
                    """,
                    (tenant_id, location_id, param_ids)
                )
                rows_updated = cur.rowcount
                logger.info(f"[publish_db] ✓ Marked {rows_updated} tool params as published")
                return rows_updated
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_location_type(tenant_id: str, location_id: str) -> str:
    """
    Get location_type from locations table.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Location type string ('rest', 'service', 'pending', etc.)
    
    Raises:
        ValueError: If location not found
    """
    logger.info(f"[publish_db] Getting location_type: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT location_type
                    FROM locations
                    WHERE tenant_id = %s
                      AND location_id = %s
                    """,
                    (tenant_id, location_id)
                )
                row = cur.fetchone()
                
                if not row:
                    raise ValueError(
                        f"Location not found: tenant_id={tenant_id}, location_id={location_id}"
                    )
                
                location_type = row[0]
                logger.info(f"[publish_db] Found location_type: {location_type}")
                return location_type
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_tool_prompt_template() -> str:
    """
    Get tool prompt template from ai_prompt_fragment.
    
    Returns:
        Template text string
    
    Raises:
        ValueError: If template not found
    """
    logger.info("[publish_db] Fetching tool prompt template: fragment_key='use_of_tools'")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT template_text
                    FROM ai_prompt_fragment
                    WHERE fragment_key = 'use_of_tools'
                    """,
                    ()
                )
                row = cur.fetchone()
                
                if not row:
                    raise ValueError("Tool prompt template not found: fragment_key='use_of_tools'")
                
                template_text = row[0]
                logger.info(f"[publish_db] Found template: {len(template_text)} characters")
                return template_text
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_tool_service_prompts(tool_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Get service_prompts for multiple tools from ai_tools table.
    
    Args:
        tool_ids: List of tool IDs
    
    Returns:
        List of dicts with keys: tool_id, service_prompts
        Example: [{'tool_id': 'tool1', 'service_prompts': {...}}, ...]
    """
    if not tool_ids:
        logger.info("[publish_db] No tool_ids provided, returning empty list")
        return []
    
    logger.info(f"[publish_db] Fetching service_prompts for {len(tool_ids)} tools: {tool_ids}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT tool_id, service_prompts
                    FROM ai_tools
                    WHERE tool_id = ANY(%s)
                    ORDER BY tool_id
                    """,
                    (tool_ids,)
                )
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                logger.info(f"[publish_db] Found service_prompts for {len(result)} tools")
                
                # Log missing tools
                found_tool_ids = {row['tool_id'] for row in result}
                missing_tool_ids = set(tool_ids) - found_tool_ids
                if missing_tool_ids:
                    logger.warning(f"[publish_db] Tools not found in ai_tools table: {list(missing_tool_ids)}")
                
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass
