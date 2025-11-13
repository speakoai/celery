"""
Database operations for ElevenLabs agent publishing.

This module provides all database query functions needed for the publishing workflow,
including publish job management, knowledge collection, and status tracking.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from typing import Optional, Dict, Any, List, Tuple
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


def collect_and_partition_knowledge(
    tenant_id: str, 
    location_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Collect all knowledge and partition into special and regular entries.
    
    Special knowledge (business_info, locations) will be processed separately
    and saved to tenant_ai_prompts table instead of being uploaded to ElevenLabs.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Tuple of:
        - business_info_entry: Entry with param_code='business_info' or None
        - locations_entry: Entry with param_code='locations' or None  
        - other_knowledge_entries: All other knowledge entries (list)
    """
    logger.info(f"[publish_db] Collecting and partitioning knowledge: tenant_id={tenant_id}, location_id={location_id}")
    
    # Get all knowledge entries
    all_knowledge = collect_speako_knowledge(tenant_id, location_id)
    
    # Partition into special and regular entries
    business_info_entry = None
    locations_entry = None
    other_knowledge_entries = []
    
    for entry in all_knowledge:
        param_code = entry.get('param_code')
        
        if param_code == 'business_info':
            business_info_entry = entry
            logger.info(f"[publish_db] Found business_info entry: param_id={entry['param_id']}")
        elif param_code == 'locations':
            locations_entry = entry
            logger.info(f"[publish_db] Found locations entry: param_id={entry['param_id']}")
        else:
            other_knowledge_entries.append(entry)
    
    logger.info(
        f"[publish_db] Partitioned knowledge: "
        f"business_info={'found' if business_info_entry else 'not found'}, "
        f"locations={'found' if locations_entry else 'not found'}, "
        f"other_knowledge={len(other_knowledge_entries)} entries"
    )
    
    return (business_info_entry, locations_entry, other_knowledge_entries)


def get_knowledge_fragment_template(fragment_key: str) -> Optional[Dict[str, Any]]:
    """
    Get template from ai_prompt_fragment for knowledge processing.
    
    Args:
        fragment_key: Fragment key ('business_info' or 'other_locations')
    
    Returns:
        Dict with {'template_text': '...', 'sort_order': 100} or None if not found
    """
    logger.info(f"[publish_db] Fetching knowledge fragment template: fragment_key='{fragment_key}'")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT template_text, sort_order
                    FROM ai_prompt_fragment
                    WHERE fragment_key = %s
                    """,
                    (fragment_key,)
                )
                row = cur.fetchone()
                
                if not row:
                    logger.warning(f"[publish_db] Fragment template not found: fragment_key='{fragment_key}'")
                    return None
                
                template_text = row[0]
                sort_order = row[1]
                logger.info(f"[publish_db] Found fragment template: {len(template_text)} characters, sort_order={sort_order}")
                return {
                    'template_text': template_text,
                    'sort_order': sort_order
                }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def process_special_knowledge_entry(
    tenant_id: str,
    location_id: str,
    entry: Dict[str, Any],
    fragment_key: str,
    type_code: str,
    name: str,
    title: str
) -> Optional[int]:
    """
    Process a special knowledge entry using fragment template and save to tenant_ai_prompts.
    
    Variable replacement logic:
    - For business_info: replaces {{business_info}} with entry's value_text
    - For locations: replaces {{locations}} with entry's value_text
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        entry: Knowledge entry dict with keys: param_id, value_text, param_code
        fragment_key: Fragment key to fetch template ('business_info' or 'other_locations')
        type_code: Type code for tenant_ai_prompts ('business_info' or 'other_locations')
        name: Name for the prompt
        title: Title for the prompt
    
    Returns:
        prompt_id if successful, None if fragment not found or processing failed
    """
    logger.info(f"[publish_db] Processing special knowledge: fragment_key='{fragment_key}', type_code='{type_code}'")
    
    # Get fragment template
    fragment = get_knowledge_fragment_template(fragment_key)
    if not fragment:
        logger.warning(f"[publish_db] Cannot process special knowledge: fragment template not found for '{fragment_key}'")
        return None
    
    template_text = fragment['template_text']
    sort_order = fragment['sort_order']
    
    # Get value_text (markdown content) from entry
    value_text = entry.get('value_text', '')
    if not value_text:
        logger.warning(f"[publish_db] Entry has empty value_text, using empty string")
        value_text = ''
    
    # Determine placeholder based on fragment_key
    if fragment_key == 'business_info':
        placeholder = '{{business_info}}'
    elif fragment_key == 'other_locations':
        placeholder = '{{locations}}'
    else:
        logger.error(f"[publish_db] Unknown fragment_key: '{fragment_key}'")
        return None
    
    # Replace placeholder with value_text
    resolved_text = template_text.replace(placeholder, value_text)
    logger.info(f"[publish_db] Replaced {placeholder} in template, result length: {len(resolved_text)} chars")
    
    # Save to tenant_ai_prompts using upsert_ai_prompt
    try:
        prompt_id = upsert_ai_prompt(
            tenant_id=tenant_id,
            location_id=location_id,
            name=name,
            type_code=type_code,
            title=title,
            body_template=resolved_text,
            sort_order=sort_order
        )
        logger.info(f"[publish_db] ✓ Saved special knowledge to tenant_ai_prompts: prompt_id={prompt_id}, type_code='{type_code}'")
        return prompt_id
    except Exception as e:
        logger.error(f"[publish_db] Failed to save special knowledge to tenant_ai_prompts: {str(e)}")
        return None


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
    metadata: Dict[str, Any] = None,
    sort_order: int = 0
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
        sort_order: Sort order for the prompt (default: 0)
    
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
                         variables_schema, metadata, version, is_active, effective_from, sort_order)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, now(), %s)
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
                        new_version,
                        sort_order
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


def get_prompt_fragments(fragment_keys: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch multiple prompt fragments in a single database query (optimized).
    
    Args:
        fragment_keys: List of fragment_key values to fetch
    
    Returns:
        Dict mapping fragment_key to dict with template_text and sort_order
        Example: {
            'personality': {'template_text': '{{traits}}...', 'sort_order': 100},
            'response_style_concise': {'template_text': 'Keep responses brief...', 'sort_order': 200}
        }
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
                    SELECT fragment_key, template_text, sort_order
                    FROM ai_prompt_fragment
                    WHERE fragment_key = ANY(%s)
                    """,
                    (fragment_keys,)
                )
                rows = cur.fetchall()
                result = {
                    row['fragment_key']: {
                        'template_text': row['template_text'],
                        'sort_order': row['sort_order']
                    }
                    for row in rows
                }
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


def collect_context_data_for_prompts(tenant_id: str, location_id: str) -> Dict[str, Optional[str]]:
    """
    Collect all context data needed for prompt variable replacement.
    
    Single optimized query fetching:
    - business_name (from tenants)
    - location_name (from locations)
    - location_desc (from locations_info)
    - agent_name (from tenant_integration_params)
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Dict with keys: business_name, location_name, location_desc, agent_name
        Values may be None if not found
    """
    logger.info(f"[publish_db] Collecting context data for prompts: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 
                        t.name as business_name,
                        l.name as location_name,
                        li.short_description as location_desc,
                        tip.value_text as agent_name
                    FROM tenants t
                    INNER JOIN locations l ON l.tenant_id = t.tenant_id
                    LEFT JOIN location_info li ON li.tenant_id = l.tenant_id AND li.location_id = l.location_id
                    LEFT JOIN tenant_integration_params tip ON 
                        tip.tenant_id = t.tenant_id AND 
                        tip.location_id = l.location_id AND
                        tip.param_code = 'agent_name' AND
                        tip.provider = 'elevenlabs' AND
                        tip.service = 'agents' AND
                        tip.status IN ('configured', 'published')
                    WHERE t.tenant_id = %s AND l.location_id = %s
                    """,
                    (tenant_id, location_id)
                )
                row = cur.fetchone()
                
                if not row:
                    logger.error(f"[publish_db] No context data found for tenant_id={tenant_id}, location_id={location_id}")
                    raise ValueError(f"Context data not found for tenant_id={tenant_id}, location_id={location_id}")
                
                result = dict(row)
                logger.info(
                    f"[publish_db] Collected context data: "
                    f"business_name='{result.get('business_name')}', "
                    f"location_name='{result.get('location_name')}', "
                    f"location_desc={'present' if result.get('location_desc') else 'missing'}, "
                    f"agent_name={'present' if result.get('agent_name') else 'missing'}"
                )
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_context_prompt_fragments() -> Dict[str, Dict[str, Any]]:
    """
    Fetch all context-based prompt fragments in one query.
    
    Returns:
        Dict mapping fragment_key to {'template_text': '...', 'sort_order': 10}
        Keys: 'role', 'knowledge_scope', 'important_behavior', 'out_of_scope'
        Returns empty dict if none found
    """
    logger.info("[publish_db] Fetching context prompt fragments")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT fragment_key, template_text, sort_order
                    FROM ai_prompt_fragment
                    WHERE fragment_key IN ('role', 'knowledge_scope', 'important_behavior', 'out_of_scope')
                    """,
                    ()
                )
                rows = cur.fetchall()
                
                result = {}
                for row in rows:
                    fragment_key = row['fragment_key']
                    result[fragment_key] = {
                        'template_text': row['template_text'],
                        'sort_order': row['sort_order']
                    }
                
                logger.info(f"[publish_db] Found {len(result)} context prompt fragments: {list(result.keys())}")
                return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def replace_context_variables(
    template_text: str, 
    context_data: Dict[str, Optional[str]], 
    required_vars: List[str]
) -> str:
    """
    Replace template variables with actual values from context_data.
    
    Args:
        template_text: Template with {{variable}} placeholders
        context_data: Dict with variable values
        required_vars: List of variable names to replace
    
    Returns:
        Resolved text with all variables replaced
    """
    resolved = template_text
    
    for var in required_vars:
        placeholder = '{{' + var + '}}'
        value = context_data.get(var)
        
        # Handle None or empty values
        if not value:
            logger.warning(f"[publish_db] Variable '{var}' is empty or None, using empty string")
            value = ''
        
        resolved = resolved.replace(placeholder, value)
    
    return resolved


def process_context_prompts(tenant_id: str, location_id: str) -> List[int]:
    """
    Process all context-based prompts (role, knowledge_scope, important_behavior, out_of_scope).
    
    Orchestrates:
    1. Collect context data from database
    2. Fetch fragment templates
    3. Replace variables in templates
    4. Save to tenant_ai_prompts
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        List of created prompt_ids
    """
    logger.info(f"[publish_db] Processing context prompts: tenant_id={tenant_id}, location_id={location_id}")
    
    # Step 1: Collect context data
    try:
        context_data = collect_context_data_for_prompts(tenant_id, location_id)
    except Exception as e:
        logger.error(f"[publish_db] Failed to collect context data: {str(e)}")
        return []
    
    # Step 2: Get all fragment templates
    fragments = get_context_prompt_fragments()
    
    if not fragments:
        logger.warning("[publish_db] No context prompt fragments found in ai_prompt_fragment table")
        return []
    
    # Step 3: Define fragment configurations
    fragment_configs = {
        'role': {
            'vars': ['agent_name', 'business_name', 'location_name', 'location_desc'],
            'type_code': 'role',
            'name': 'Role',
            'title': 'Role'
        },
        'knowledge_scope': {
            'vars': ['location_name'],
            'type_code': 'knowledge_scope',
            'name': 'Knowledge Scope',
            'title': 'Knowledge Scope'
        },
        'important_behavior': {
            'vars': ['business_name', 'location_name'],
            'type_code': 'important_behavior',
            'name': 'Important Behavior',
            'title': 'Important Behavior'
        },
        'out_of_scope': {
            'vars': ['business_name', 'location_name'],
            'type_code': 'out_of_scope',
            'name': 'Out Of Scope',
            'title': 'Out Of Scope'
        }
    }
    
    # Step 4: Process each fragment
    created_prompt_ids = []
    
    for fragment_key, config in fragment_configs.items():
        if fragment_key not in fragments:
            logger.warning(f"[publish_db] Fragment '{fragment_key}' not found, skipping")
            continue
        
        fragment = fragments[fragment_key]
        template_text = fragment['template_text']
        sort_order = fragment['sort_order']
        
        logger.info(f"[publish_db] Processing fragment '{fragment_key}': template length={len(template_text)}, sort_order={sort_order}")
        
        # Replace variables
        resolved_text = replace_context_variables(
            template_text=template_text,
            context_data=context_data,
            required_vars=config['vars']
        )
        
        logger.info(f"[publish_db] Resolved '{fragment_key}': result length={len(resolved_text)} chars")
        
        # Save to tenant_ai_prompts
        try:
            prompt_id = upsert_ai_prompt(
                tenant_id=tenant_id,
                location_id=location_id,
                name=config['name'],
                type_code=config['type_code'],
                title=config['title'],
                body_template=resolved_text,
                sort_order=sort_order
            )
            
            if prompt_id:
                created_prompt_ids.append(prompt_id)
                logger.info(f"[publish_db] ✓ Created prompt for '{fragment_key}': prompt_id={prompt_id}")
            else:
                logger.warning(f"[publish_db] ⚠️ Failed to create prompt for '{fragment_key}'")
                
        except Exception as e:
            logger.error(f"[publish_db] Error creating prompt for '{fragment_key}': {str(e)}")
    
    logger.info(f"[publish_db] Processed {len(created_prompt_ids)} context prompts: {created_prompt_ids}")
    return created_prompt_ids


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


def get_tool_prompt_template() -> Dict[str, Any]:
    """
    Get tool prompt template from ai_prompt_fragment.
    
    Returns:
        Dict with keys: template_text, sort_order
    
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
                    SELECT template_text, sort_order
                    FROM ai_prompt_fragment
                    WHERE fragment_key = 'use_of_tools'
                    """,
                    ()
                )
                row = cur.fetchone()
                
                if not row:
                    raise ValueError("Tool prompt template not found: fragment_key='use_of_tools'")
                
                template_text = row[0]
                sort_order = row[1]
                logger.info(f"[publish_db] Found template: {len(template_text)} characters, sort_order={sort_order}")
                return {
                    'template_text': template_text,
                    'sort_order': sort_order
                }
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


def compose_prompts_by_sort_order(tenant_id: str, location_id: str) -> str:
    """
    Compose all active AI prompts ordered by sort_order.
    
    Fetches all active prompts with sort_order > 0 and concatenates their
    body_template fields with double newlines as separators.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Composed prompt text with all prompts joined by \n\n
        Returns empty string if no prompts found
    """
    logger.info(f"[publish_db] Composing prompts by sort_order: tenant_id={tenant_id}, location_id={location_id}")
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT body_template
                    FROM tenant_ai_prompts
                    WHERE tenant_id = %s
                      AND location_id = %s
                      AND sort_order > 0
                      AND is_active = true
                    ORDER BY sort_order ASC
                    """,
                    (tenant_id, location_id)
                )
                rows = cur.fetchall()
                
                if not rows:
                    logger.warning(f"[publish_db] No active prompts found with sort_order > 0")
                    return ""
                
                # Extract body_template from each row and join with double newline
                prompts = [row['body_template'] for row in rows]
                composed_text = '\n\n'.join(prompts)
                
                logger.info(f"[publish_db] Found {len(prompts)} active prompts with sort_order > 0")
                logger.info(f"[publish_db] Composed prompt text: {len(composed_text)} characters")
                
                return composed_text
    finally:
        try:
            conn.close()
        except Exception:
            pass


def compose_and_publish_system_prompt(tenant_id: str, location_id: str) -> Dict[str, Any]:
    """
    Compose all prompts and publish to ElevenLabs as system prompt.
    
    This function orchestrates three steps:
    1. Compose all active prompts (sort_order > 0) into a single text
    2. Update ElevenLabs agent with the composed prompt
    3. Save the composed prompt to tenant_ai_prompts as 'system_prompt'
    
    All steps must succeed - if ElevenLabs API fails, no database write occurs.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
    
    Returns:
        Dict with keys:
        - success: bool
        - composed_text: str (the full composed prompt)
        - character_count: int (length of composed text)
        - prompt_count: int (number of prompts composed)
        - elevenlabs_agent_id: str
        - elevenlabs_status_code: int (HTTP status from API)
        - system_prompt_id: int (prompt_id of saved record)
    
    Raises:
        ValueError: If agent not found or no prompts to compose
        requests.HTTPError: If ElevenLabs API call fails
        RuntimeError: If ELEVENLABS_API_KEY not set
    """
    import requests
    import json
    from .elevenlabs_client import _get_headers, ELEVENLABS_BASE_URL
    
    logger.info("=" * 80)
    logger.info(f"[SystemPrompt] COMPOSING AND PUBLISHING SYSTEM PROMPT")
    logger.info(f"[SystemPrompt]   Tenant ID: {tenant_id}")
    logger.info(f"[SystemPrompt]   Location ID: {location_id}")
    logger.info("=" * 80)
    
    # Step 1: Get Agent ID
    logger.info(f"[SystemPrompt] Step 1: Fetching ElevenLabs agent ID...")
    agent_id, location_name, location_timezone = get_elevenlabs_agent_id(tenant_id, location_id)
    logger.info(f"[SystemPrompt] ✓ Found agent ID: {agent_id}")
    logger.info(f"[SystemPrompt] ✓ Location: name='{location_name}', timezone='{location_timezone}'")
    
    # Step 2: Compose System Prompt
    logger.info(f"[SystemPrompt] Step 2: Composing prompts from tenant_ai_prompts...")
    composed_text = compose_prompts_by_sort_order(tenant_id, location_id)
    
    if not composed_text:
        error_msg = f"No active prompts found to compose for tenant_id={tenant_id}, location_id={location_id}"
        logger.error(f"[SystemPrompt] ❌ {error_msg}")
        raise ValueError(error_msg)
    
    prompt_count = composed_text.count('\n\n') + 1  # Approximate count
    character_count = len(composed_text)
    
    logger.info(f"[SystemPrompt] ✓ Composed {prompt_count} prompts: {character_count} characters")
    
    # Step 3: Update ElevenLabs API
    logger.info(f"[SystemPrompt] Step 3: Updating ElevenLabs agent via API...")
    
    url = f"{ELEVENLABS_BASE_URL}/agents/{agent_id}"
    headers = _get_headers()
    headers['Content-Type'] = 'application/json'
    
    payload = {
        "conversation_config": {
            "agent": {
                "prompt": {
                    "prompt": composed_text
                }
            }
        }
    }
    
    logger.info("=" * 80)
    logger.info(f"[SystemPrompt] 📤 STARTING API CALL: PATCH {url}")
    logger.info(f"[SystemPrompt] Request Payload:")
    logger.info(json.dumps(payload, indent=2))
    logger.info("=" * 80)
    
    try:
        response = requests.patch(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        response.raise_for_status()
        
        logger.info("=" * 80)
        logger.info(f"[SystemPrompt] 📥 API RESPONSE:")
        logger.info(f"[SystemPrompt]   Status Code: {response.status_code}")
        logger.info(f"[SystemPrompt]   Response Body: {response.text}")
        logger.info("=" * 80)
        logger.info(f"[SystemPrompt] ✓ ElevenLabs API call succeeded: {response.status_code} OK")
        
    except requests.HTTPError as e:
        error_msg = f"ElevenLabs API error (HTTP {response.status_code}): {response.text}"
        logger.error("=" * 80)
        logger.error(f"[SystemPrompt] ❌ API CALL FAILED: {error_msg}")
        logger.error("=" * 80)
        raise requests.HTTPError(error_msg, response=response) from e
    
    # Step 4: Save System Prompt Record (only if API succeeded)
    logger.info(f"[SystemPrompt] Step 4: Saving system_prompt to tenant_ai_prompts...")
    
    system_prompt_id = upsert_ai_prompt(
        tenant_id=tenant_id,
        location_id=location_id,
        type_code='system_prompt',
        name='System Prompt',
        title='System Prompt',
        body_template=composed_text,
        sort_order=0
    )
    
    logger.info(f"[SystemPrompt] ✓ Saved system_prompt to tenant_ai_prompts: prompt_id={system_prompt_id}")
    logger.info("=" * 80)
    logger.info(f"[SystemPrompt] ✅ SYSTEM PROMPT PUBLISHED SUCCESSFULLY")
    logger.info("=" * 80)
    
    return {
        'success': True,
        'composed_text': composed_text,
        'character_count': character_count,
        'prompt_count': prompt_count,
        'elevenlabs_agent_id': agent_id,
        'elevenlabs_status_code': response.status_code,
        'system_prompt_id': system_prompt_id
    }
