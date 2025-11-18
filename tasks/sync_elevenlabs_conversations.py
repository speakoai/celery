"""
Sync ElevenLabs conversations to database.

This task fetches conversation data from ElevenLabs API and stores it in the database.
Includes conversation details, transcripts, billing info, and audio files.
"""

from dotenv import load_dotenv
load_dotenv()

from tasks.celery_app import app
from celery.utils.log import get_task_logger

import os
import psycopg2
import redis
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

from tasks.utils.elevenlabs_client import (
    list_conversations,
    get_conversation_details,
    get_conversation_audio
)
from tasks.utils.publish_r2 import upload_audio_to_r2

logger = get_task_logger(__name__)

# Database and Redis URLs
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

# Redis key prefix for sync tracking
SYNC_REDIS_KEY_PREFIX = "elevenlabs_sync:last_sync"
SYNC_REDIS_TTL = 15552000  # 180 days in seconds


def get_redis_client():
    """Get Redis client for sync tracking."""
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def get_db_connection():
    """Get PostgreSQL database connection."""
    return psycopg2.connect(DATABASE_URL)


def convert_unix_to_location_tz(unix_timestamp: Optional[int], timezone_str: str) -> Optional[datetime]:
    """
    Convert UNIX timestamp to location timezone (without timezone info).
    
    Args:
        unix_timestamp: UNIX timestamp in seconds (or None)
        timezone_str: IANA timezone string (e.g., 'Australia/Sydney')
    
    Returns:
        datetime in location timezone without timezone info (or None)
    """
    if unix_timestamp is None:
        return None
    
    try:
        # Convert to UTC datetime first
        utc_dt = datetime.fromtimestamp(unix_timestamp, tz=ZoneInfo('UTC'))
        
        # Convert to location timezone
        location_tz = ZoneInfo(timezone_str)
        local_dt = utc_dt.astimezone(location_tz)
        
        # Strip timezone info (store as naive datetime in local time)
        return local_dt.replace(tzinfo=None)
    except Exception as e:
        logger.warning(f"[ConvSync] Failed to convert timestamp {unix_timestamp}: {e}")
        return None


def get_last_sync_time(tenant_id: int, location_id: int) -> Optional[datetime]:
    """
    Get last sync timestamp from Redis cache.
    
    Args:
        tenant_id: Tenant ID
        location_id: Location ID
    
    Returns:
        Last sync datetime in UTC (or None if not cached)
    """
    try:
        redis_client = get_redis_client()
        redis_key = f"{SYNC_REDIS_KEY_PREFIX}:{tenant_id}:{location_id}"
        
        last_sync_str = redis_client.get(redis_key)
        
        if last_sync_str:
            last_sync = datetime.fromisoformat(str(last_sync_str))
            logger.info(f"[ConvSync] Found last sync for location {location_id}: {last_sync.isoformat()}")
            return last_sync
        else:
            logger.info(f"[ConvSync] No previous sync found for location {location_id}")
            return None
    except Exception as e:
        logger.warning(f"[ConvSync] Failed to read last sync time from Redis: {e}")
        return None


def update_last_sync_time(tenant_id: int, location_id: int, sync_time: datetime) -> None:
    """
    Update last sync timestamp in Redis cache.
    
    Args:
        tenant_id: Tenant ID
        location_id: Location ID
        sync_time: Sync datetime in UTC
    """
    try:
        redis_client = get_redis_client()
        redis_key = f"{SYNC_REDIS_KEY_PREFIX}:{tenant_id}:{location_id}"
        
        redis_client.setex(
            redis_key,
            SYNC_REDIS_TTL,
            sync_time.isoformat()
        )
        
        logger.info(f"[ConvSync] Updated last sync time for location {location_id}: {sync_time.isoformat()}")
    except Exception as e:
        logger.warning(f"[ConvSync] Failed to update last sync time in Redis: {e}")


def determine_sync_range(tenant_id: int, location_id: int) -> tuple[datetime, datetime]:
    """
    Determine the date range for conversation sync.
    
    Args:
        tenant_id: Tenant ID
        location_id: Location ID
    
    Returns:
        Tuple of (start_time, end_time) in UTC
    """
    end_time = datetime.now(ZoneInfo('UTC'))
    last_sync = get_last_sync_time(tenant_id, location_id)
    
    if last_sync:
        # Incremental sync: fetch from last_sync - 2 hours (buffer)
        start_time = last_sync - timedelta(hours=2)
        logger.info(
            f"[ConvSync] Incremental sync for location {location_id}: "
            f"{start_time.isoformat()} to {end_time.isoformat()}"
        )
    else:
        # First sync: fetch last 90 days
        start_time = end_time - timedelta(days=90)
        logger.info(
            f"[ConvSync] First sync for location {location_id}: "
            f"{start_time.isoformat()} to {end_time.isoformat()} (90 days)"
        )
    
    return start_time, end_time


def filter_conversations_by_date(
    conversations: List[Dict],
    start_time: datetime,
    end_time: datetime
) -> List[Dict]:
    """
    Filter conversations by date range.
    
    Args:
        conversations: List of conversation dictionaries from API
        start_time: Start datetime in UTC
        end_time: End datetime in UTC
    
    Returns:
        Filtered list of conversations within date range
    """
    filtered = []
    
    for conv in conversations:
        # List endpoint returns start_time_unix_secs at top level
        timestamp = conv.get('start_time_unix_secs')
        
        if timestamp:
            try:
                conv_time = datetime.fromtimestamp(timestamp, tz=ZoneInfo('UTC'))
                
                if start_time <= conv_time <= end_time:
                    filtered.append(conv)
            except Exception as e:
                logger.warning(f"[ConvSync] Failed to parse timestamp for conversation: {e}")
                # Include conversation if we can't parse timestamp (to be safe)
                filtered.append(conv)
        else:
            # No timestamp - include to be safe
            filtered.append(conv)
    
    logger.info(
        f"[ConvSync] Filtered {len(filtered)} conversations from {len(conversations)} total "
        f"in range {start_time.isoformat()} to {end_time.isoformat()}"
    )
    
    return filtered


def get_existing_conversation_ids(conn, conversation_ids: List[str]) -> set:
    """
    Check which conversation IDs already exist in database.
    
    Args:
        conn: Database connection
        conversation_ids: List of ElevenLabs conversation IDs
    
    Returns:
        Set of conversation IDs that already exist
    """
    if not conversation_ids:
        return set()
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT eleven_conversation_id
                FROM location_conversations
                WHERE eleven_conversation_id = ANY(%s)
            """, (conversation_ids,))
            
            existing = {row[0] for row in cur.fetchall()}
            
            logger.info(f"[ConvSync] Found {len(existing)} existing conversations out of {len(conversation_ids)}")
            
            return existing
    except Exception as e:
        logger.error(f"[ConvSync] Failed to check existing conversations: {e}")
        return set()


def insert_conversation(
    conn,
    tenant_id: int,
    location_id: int,
    agent_id: str,
    location_name: str,
    timezone_str: str,
    details: Dict[str, Any]
) -> Optional[int]:
    """
    Insert conversation record into location_conversations table.
    
    Args:
        conn: Database connection
        tenant_id: Tenant ID
        location_id: Location ID
        agent_id: ElevenLabs agent ID
        location_name: Location name (fallback for agent_name)
        timezone_str: Location timezone string
        details: Full conversation details from API
    
    Returns:
        Inserted location_conversation_id (or None on failure)
    """
    try:
        # Extract fields from API response
        conversation_id = details.get('conversation_id')
        
        # Get metadata object (contains timestamps and duration)
        metadata = details.get('metadata', {})
        
        # Agent name: use from API or fallback to location name
        agent_name = details.get('agent_name') or location_name
        
        # Convert timestamps to location timezone (from metadata)
        call_start_time = convert_unix_to_location_tz(
            metadata.get('start_time_unix_secs'),
            timezone_str
        )
        
        call_accepted_time = convert_unix_to_location_tz(
            metadata.get('end_time_unix_secs'),  # May not exist (nullable)
            timezone_str
        )
        
        # Call duration (from metadata)
        call_duration_secs = metadata.get('call_duration_secs')
        
        # Message count
        transcript = details.get('transcript', [])
        message_count = len(transcript) if transcript else 0
        
        # Status (top level)
        status = details.get('status', 'unknown')
        
        # Call successful - convert string to boolean
        call_successful_str = details.get('call_successful')
        if call_successful_str:
            call_successful = (call_successful_str == 'success')
        else:
            # Fallback: consider "done" or "completed" as successful
            call_successful = (status in ['done', 'completed'])
        
        # Language
        main_language = details.get('language') or details.get('detected_language')
        
        # Summary - try top-level first, then nested
        transcript_summary = (
            details.get('transcript_summary') or
            details.get('call_summary_title') or
            (details.get('analysis', {}).get('summary') if isinstance(details.get('analysis'), dict) else None)
        )
        
        # Raw metadata as JSONB
        raw_metadata = json.dumps(details)
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO location_conversations (
                    tenant_id, location_id, eleven_conversation_id, eleven_agent_id,
                    agent_name, call_start_time, call_accepted_time, call_duration_secs,
                    message_count, status, call_successful, main_language,
                    transcript_summary, audio_r2_path, raw_metadata
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING location_conversation_id
            """, (
                tenant_id, location_id, conversation_id, agent_id,
                agent_name, call_start_time, call_accepted_time, call_duration_secs,
                message_count, status, call_successful, main_language,
                transcript_summary, None,  # audio_r2_path (updated later)
                raw_metadata
            ))
            
            location_conversation_id = cur.fetchone()[0]
            
            logger.info(
                f"[ConvSync] Inserted conversation {conversation_id}: "
                f"location_conversation_id={location_conversation_id}, status={status}"
            )
            
            return location_conversation_id
            
    except Exception as e:
        logger.error(f"[ConvSync] Failed to insert conversation: {e}")
        import traceback
        traceback.print_exc()
        return None


def insert_conversation_details(
    conn,
    location_conversation_id: int,
    transcript: List[Dict[str, Any]]
) -> int:
    """
    Insert conversation details (transcript messages) into location_conversation_details table.
    
    Args:
        conn: Database connection
        location_conversation_id: Parent conversation ID
        transcript: List of transcript message dictionaries
    
    Returns:
        Number of detail records inserted
    """
    if not transcript:
        logger.info(f"[ConvSync] No transcript to insert for conversation {location_conversation_id}")
        return 0
    
    try:
        inserted_count = 0
        
        with conn.cursor() as cur:
            for idx, message in enumerate(transcript):
                # Extract fields
                role = message.get('role', 'unknown')
                time_in_call_secs = message.get('time_in_call_secs') or message.get('timestamp')
                message_text = message.get('message') or message.get('text') or message.get('content')
                
                # JSONB fields
                tool_calls = json.dumps(message.get('tool_calls')) if message.get('tool_calls') else None
                tool_results = json.dumps(message.get('tool_results')) if message.get('tool_results') else None
                llm_override = message.get('llm_override')
                
                conversation_turn_metrics = message.get('metrics') or message.get('turn_metrics')
                if conversation_turn_metrics:
                    conversation_turn_metrics = json.dumps(conversation_turn_metrics)
                
                rag_retrieval_info = message.get('rag_info') or message.get('rag_retrieval')
                if rag_retrieval_info:
                    rag_retrieval_info = json.dumps(rag_retrieval_info)
                
                cur.execute("""
                    INSERT INTO location_conversation_details (
                        location_conversation_id, message_index, role, time_in_call_secs,
                        message, tool_calls, tool_results, llm_override,
                        conversation_turn_metrics, rag_retrieval_info
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    location_conversation_id, idx, role, time_in_call_secs,
                    message_text, tool_calls, tool_results, llm_override,
                    conversation_turn_metrics, rag_retrieval_info
                ))
                
                inserted_count += 1
        
        logger.info(
            f"[ConvSync] Inserted {inserted_count} transcript messages for "
            f"conversation {location_conversation_id}"
        )
        
        return inserted_count
        
    except Exception as e:
        logger.error(f"[ConvSync] Failed to insert conversation details: {e}")
        import traceback
        traceback.print_exc()
        return 0


def insert_billing_record(
    conn,
    location_conversation_id: int,
    call_duration_secs: Optional[int]
) -> bool:
    """
    Insert billing record into billing_location_conversations table.
    
    Args:
        conn: Database connection
        location_conversation_id: Parent conversation ID
        call_duration_secs: Call duration in seconds
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Default to 0 if duration is None
        duration = call_duration_secs if call_duration_secs is not None else 0
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO billing_location_conversations (
                    location_conversation_id, call_duration_secs
                )
                VALUES (%s, %s)
            """, (location_conversation_id, duration))
        
        logger.info(
            f"[ConvSync] Inserted billing record for conversation {location_conversation_id}: "
            f"duration={duration}s"
        )
        
        return True
        
    except Exception as e:
        logger.error(f"[ConvSync] Failed to insert billing record: {e}")
        return False


def update_audio_path(conn, location_conversation_id: int, audio_r2_path: str) -> bool:
    """
    Update audio_r2_path in location_conversations table.
    
    Args:
        conn: Database connection
        location_conversation_id: Conversation ID
        audio_r2_path: R2 key/path to audio file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE location_conversations
                SET audio_r2_path = %s, updated_at = CURRENT_TIMESTAMP
                WHERE location_conversation_id = %s
            """, (audio_r2_path, location_conversation_id))
        
        logger.info(f"[ConvSync] Updated audio path for conversation {location_conversation_id}: {audio_r2_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"[ConvSync] Failed to update audio path: {e}")
        return False


def fetch_and_upload_audio(
    conversation_id: str,
    tenant_id: int,
    location_id: int
) -> Optional[str]:
    """
    Fetch audio from ElevenLabs and upload to R2.
    
    Args:
        conversation_id: ElevenLabs conversation ID
        tenant_id: Tenant ID
        location_id: Location ID
    
    Returns:
        R2 public URL (or None on failure)
    """
    try:
        # Fetch audio from ElevenLabs
        audio_bytes, content_type = get_conversation_audio(conversation_id)
        
        # Upload to R2
        r2_key, public_url = upload_audio_to_r2(
            str(tenant_id),
            str(location_id),
            conversation_id,
            audio_bytes,
            content_type
        )
        
        logger.info(f"[ConvSync] Uploaded audio for {conversation_id}: {public_url}")
        
        return public_url
        
    except Exception as e:
        logger.warning(f"[ConvSync] Failed to fetch/upload audio for {conversation_id}: {e}")
        return None


@app.task
def sync_conversations_for_location(
    tenant_id: int,
    location_id: int,
    agent_id: str,
    timezone_str: str,
    location_name: str
):
    """
    Sync conversations for a specific location from ElevenLabs to database.
    
    Args:
        tenant_id: Tenant ID
        location_id: Location ID
        agent_id: ElevenLabs agent ID
        timezone_str: Location timezone (e.g., 'Australia/Sydney')
        location_name: Location name
    
    Returns:
        Summary dictionary with sync statistics
    """
    logger.info("=" * 80)
    logger.info(f"[ConvSync] Starting conversation sync for location {location_id}")
    logger.info(f"[ConvSync]   Tenant ID: {tenant_id}")
    logger.info(f"[ConvSync]   Location ID: {location_id}")
    logger.info(f"[ConvSync]   Agent ID: {agent_id}")
    logger.info(f"[ConvSync]   Timezone: {timezone_str}")
    logger.info("=" * 80)
    
    # Initialize counters
    total_fetched = 0
    already_synced = 0
    newly_synced = 0
    failed = 0
    
    try:
        # 1. Determine sync date range
        start_time, end_time = determine_sync_range(tenant_id, location_id)
        
        # 2. Fetch conversation list from ElevenLabs
        logger.info(f"[ConvSync] Fetching conversations from ElevenLabs...")
        all_conversations = list_conversations(agent_id)
        total_fetched = len(all_conversations)
        
        # 3. Filter by date range
        conversations = filter_conversations_by_date(all_conversations, start_time, end_time)
        
        if not conversations:
            logger.info(f"[ConvSync] No conversations to sync for location {location_id}")
            return {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'total_fetched': total_fetched,
                'already_synced': 0,
                'newly_synced': 0,
                'failed': 0,
                'sync_range': f"{start_time.isoformat()} to {end_time.isoformat()}"
            }
        
        # 4. Check existing conversations in database
        conn = get_db_connection()
        
        try:
            conversation_ids = [
                str(c.get('conversation_id')) for c in conversations 
                if c.get('conversation_id') is not None
            ]
            
            existing_ids = get_existing_conversation_ids(conn, conversation_ids)
            already_synced = len(existing_ids)
            
            # Filter to new conversations only
            new_conversations = [
                c for c in conversations 
                if c.get('conversation_id') not in existing_ids
            ]
            
            logger.info(
                f"[ConvSync] Processing {len(new_conversations)} new conversations "
                f"({already_synced} already synced)"
            )
            
            # 5. Process each new conversation
            for conv in new_conversations:
                conversation_id = conv.get('conversation_id')
                
                if not conversation_id:
                    logger.warning("[ConvSync] Skipping conversation without ID")
                    failed += 1
                    continue
                
                try:
                    # Start transaction for this conversation
                    conn.rollback()  # Clear any previous transaction
                    
                    # 5a. Fetch full conversation details
                    logger.info(f"[ConvSync] Fetching details for conversation {conversation_id}")
                    details = get_conversation_details(conversation_id)
                    
                    # 5b. Insert location_conversations
                    location_conversation_id = insert_conversation(
                        conn, tenant_id, location_id, agent_id,
                        location_name, timezone_str, details
                    )
                    
                    if not location_conversation_id:
                        raise Exception("Failed to insert conversation record")
                    
                    # 5c. Insert location_conversation_details (transcript)
                    transcript = details.get('transcript', [])
                    insert_conversation_details(conn, location_conversation_id, transcript)
                    
                    # 5d. Insert billing_location_conversations
                    call_duration_secs = details.get('call_duration_secs') or details.get('duration_seconds')
                    insert_billing_record(conn, location_conversation_id, call_duration_secs)
                    
                    # Commit transaction
                    conn.commit()
                    
                    # 5e. Fetch and upload audio (best effort - outside transaction)
                    audio_r2_path = fetch_and_upload_audio(conversation_id, tenant_id, location_id)
                    
                    if audio_r2_path:
                        update_audio_path(conn, location_conversation_id, audio_r2_path)
                        conn.commit()
                    
                    newly_synced += 1
                    logger.info(f"[ConvSync] ✅ Successfully synced conversation {conversation_id}")
                    
                except Exception as e:
                    conn.rollback()
                    failed += 1
                    logger.error(f"[ConvSync] ❌ Failed to sync conversation {conversation_id}: {e}")
                    import traceback
                    traceback.print_exc()
            
            # 6. Update Redis sync timestamp
            update_last_sync_time(tenant_id, location_id, end_time)
            
        finally:
            conn.close()
        
        # 7. Return summary
        summary = {
            'tenant_id': tenant_id,
            'location_id': location_id,
            'total_fetched': total_fetched,
            'already_synced': already_synced,
            'newly_synced': newly_synced,
            'failed': failed,
            'sync_range': f"{start_time.isoformat()} to {end_time.isoformat()}"
        }
        
        logger.info("=" * 80)
        logger.info(f"[ConvSync] Completed sync for location {location_id}")
        logger.info(f"[ConvSync]   Total fetched: {total_fetched}")
        logger.info(f"[ConvSync]   Already synced: {already_synced}")
        logger.info(f"[ConvSync]   Newly synced: {newly_synced}")
        logger.info(f"[ConvSync]   Failed: {failed}")
        logger.info("=" * 80)
        
        return summary
        
    except Exception as e:
        logger.error(f"[ConvSync] ❌ Fatal error syncing location {location_id}: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'tenant_id': tenant_id,
            'location_id': location_id,
            'total_fetched': total_fetched,
            'already_synced': already_synced,
            'newly_synced': newly_synced,
            'failed': failed,
            'error': str(e)
        }
