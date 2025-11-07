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
