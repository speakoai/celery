"""
Helper functions for ElevenLabs publishing workflows.

This module orchestrates the complete publishing workflow by coordinating
database operations, R2 storage, and ElevenLabs API calls.
"""

import os
import json
from typing import Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
from celery.utils.log import get_task_logger

from .publish_db import (
    get_publish_job,
    update_publish_job_status,
    get_elevenlabs_agent_id,
    collect_speako_knowledge,
    get_existing_elevenlabs_knowledge_ids,
    save_new_elevenlabs_knowledge_id,
    mark_speako_knowledge_published,
    delete_old_elevenlabs_knowledge_ids
)
from .publish_r2 import (
    aggregate_knowledge_markdown,
    upload_knowledge_to_r2
)
from .elevenlabs_client import (
    upload_knowledge_file,
    delete_knowledge,
    get_agent_config,
    update_agent_knowledge
)

logger = get_task_logger(__name__)

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = "https://api.openai.com/v1/chat/completions"


def get_human_friendly_operation_hours(schedule_json: Dict[str, Any]) -> str:
    """
    Convert business hours JSON to natural, human-friendly text using OpenAI.
    
    Args:
        schedule_json: Dict with day names as keys and schedule as values
                      Example: {"Monday": [{"start_time": "09:00", "end_time": "17:00"}]}
    
    Returns:
        Human-friendly operation hours string
        Example: "Monday to Friday 9 AM to 5 PM, weekends 10 AM to 4 PM"
    """
    if not OPENAI_API_KEY:
        logger.error("[OpenAI] API key not set - cannot generate friendly hours")
        return "Please check our website for current operating hours."
    
    try:
        logger.info(f"[OpenAI] Converting operation hours to natural language")
        logger.info(f"[OpenAI] Schedule JSON: {json.dumps(schedule_json)}")
        
        response = requests.post(
            OPENAI_BASE_URL,
            headers={
                'Authorization': f'Bearer {OPENAI_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'model': 'gpt-4o-mini',
                'messages': [
                    {
                        'role': 'system',
                        'content': 'You are converting business hours to natural speech. Return ONLY the conversational hours text - no greetings, no "here are", no formatting, no explanations. Just the hours in natural language as if you are speaking directly to a customer. Keep it concise and friendly. Example: "Monday to Friday 9 AM to 5 PM, weekends 10 AM to 4 PM".'
                    },
                    {
                        'role': 'user',
                        'content': f'Convert to natural speech: {json.dumps(schedule_json)}'
                    }
                ],
                'max_tokens': 100,
                'temperature': 0.1
            },
            timeout=30
        )
        
        if not response.ok:
            logger.error(f"[OpenAI] API error: {response.status_code} - {response.text}")
            return "Please check our website for current operating hours."
        
        data = response.json()
        friendly_hours = data.get('choices', [{}])[0].get('message', {}).get('content', '').strip()
        
        if not friendly_hours:
            logger.warning("[OpenAI] Empty response from API")
            return "Please check our website for current operating hours."
        
        logger.info(f"[OpenAI] ✅ Generated friendly hours: {friendly_hours}")
        return friendly_hours
        
    except Exception as e:
        logger.error(f"[OpenAI] Error calling API: {str(e)}")
        return "Please check our website for current operating hours."


def format_timestamp_for_location(timezone_str: str) -> str:
    """
    Generate human-readable timestamp in location's timezone.
    
    Args:
        timezone_str: IANA timezone string (e.g., "Australia/Sydney")
    
    Returns:
        Formatted timestamp (e.g., "Nov 7, 2025 10:30 AM AEDT")
    """
    try:
        # Get current time in UTC
        utc_now = datetime.now(ZoneInfo("UTC"))
        
        # Convert to location timezone
        local_time = utc_now.astimezone(ZoneInfo(timezone_str))
        
        # Format: "Nov 7, 2025 10:30 AM AEDT"
        formatted = local_time.strftime("%b %d, %Y %I:%M %p %Z")
        
        return formatted
    except Exception as e:
        # Fallback to UTC if timezone is invalid
        logger.warning(f"[PublishKnowledge] Invalid timezone '{timezone_str}', using UTC: {e}")
        return datetime.utcnow().strftime("%b %d, %Y %I:%M %p UTC")


def publish_knowledge(
    tenant_id: int,
    location_id: int,
    publish_job_id: int
) -> Dict[str, Any]:
    """
    Complete workflow for publishing knowledge to ElevenLabs agent.
    
    This function orchestrates the entire knowledge publishing process:
    1. Validates the publish job and fetches agent ID
    2. Collects knowledge from Speako database
    3. Aggregates knowledge into markdown and uploads to R2
    4. Uploads knowledge file to ElevenLabs
    5. Merges new knowledge ID with existing agent knowledge
    6. Updates agent configuration with merged knowledge base
    7. Optionally cleans up old knowledge documents
    8. Marks knowledge as published in database
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        publish_job_id: Publish job identifier
        
    Returns:
        Dictionary containing:
            - elevenlabs_agent_id: ElevenLabs agent ID
            - new_knowledge_id: Newly created knowledge document ID
            - old_knowledge_ids: List of previous knowledge IDs (may be empty)
            - merged_knowledge_ids: Final list of knowledge IDs on agent
            - deleted_old_knowledge: List of successfully deleted old knowledge IDs
            - r2_url: Public URL of the uploaded knowledge file
            
    Raises:
        ValueError: If publish job is invalid or agent ID not found
        RuntimeError: If critical steps fail (R2 upload, ElevenLabs upload, agent update)
    """
    logger.info(
        f"[PublishKnowledge] Starting workflow: tenant_id={tenant_id}, "
        f"location_id={location_id}, publish_job_id={publish_job_id}"
    )
    
    # Step 1: Validate publish job and get location data
    publish_job = get_publish_job(tenant_id, publish_job_id)
    if not publish_job:
        raise ValueError(
            f"Publish job not found: tenant_id={tenant_id}, publish_job_id={publish_job_id}"
        )
    
    # Mark as processing and set started_at timestamp
    from datetime import datetime
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='in_progress',
        started_at=datetime.utcnow()
    )
    
    # Get agent ID, location name, and timezone
    elevenlabs_agent_id, location_name, location_timezone = get_elevenlabs_agent_id(tenant_id, location_id)
    if not elevenlabs_agent_id:
        raise ValueError(
            f"ElevenLabs agent ID not found: tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishKnowledge] Found agent ID: {elevenlabs_agent_id}")
    logger.info(f"[PublishKnowledge] Location: name='{location_name}', timezone='{location_timezone}'")

    
    # Step 2: Collect knowledge from Speako
    knowledge_docs = collect_speako_knowledge(tenant_id, location_id)
    
    if not knowledge_docs:
        logger.warning("[PublishKnowledge] No knowledge documents found")
        raise ValueError(
            f"No knowledge found for tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishKnowledge] Collected {len(knowledge_docs)} knowledge documents")
    
    # Step 3: Aggregate knowledge and upload to R2
    aggregated_content, suggested_filename = aggregate_knowledge_markdown(knowledge_docs)
    
    filename = f"knowledge_{tenant_id}_{location_id}.md"
    r2_key, r2_url = upload_knowledge_to_r2(
        content=aggregated_content,
        tenant_id=tenant_id,
        location_id=location_id,
        filename=filename
    )
    
    logger.info(f"[PublishKnowledge] Uploaded to R2: {r2_url}")
    
    # Update publish job with knowledge_file_url
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='in_progress',
        knowledge_file_url=r2_url
    )
    
    # Step 4: Upload to ElevenLabs with location name and timestamp
    # Generate timestamp in location's timezone
    timestamp = format_timestamp_for_location(location_timezone)
    
    # Build knowledge name with location name and timestamp
    knowledge_name = f"{location_name} - {timestamp}"
    
    logger.info(f"[PublishKnowledge] Knowledge name: '{knowledge_name}'")
    
    try:
        new_knowledge_id, new_knowledge_name = upload_knowledge_file(
            file_url=r2_url,
            name=knowledge_name
        )
        logger.info(f"[PublishKnowledge] ✅ Created ElevenLabs knowledge: id={new_knowledge_id}, name={new_knowledge_name}")
    except Exception as e:
        from datetime import datetime
        update_publish_job_status(
            tenant_id=tenant_id,
            publish_job_id=publish_job_id,
            status='failed',
            finished_at=datetime.utcnow(),
            error_message=str(e)
        )
        raise RuntimeError(f"Failed to upload knowledge to ElevenLabs: {str(e)}") from e
    
    # Step 5: Build knowledge item with proper structure (ONLY new knowledge)
    new_knowledge_item = {
        "id": new_knowledge_id,
        "name": new_knowledge_name,
        "type": "file",
        "usage_mode": "auto"
    }
    
    logger.info(
        f"[PublishKnowledge] Updating agent with new knowledge only: {new_knowledge_id}"
    )
    
    # Step 6: Update agent configuration (ONLY new knowledge, no merging with old)
    try:
        updated_config = update_agent_knowledge(
            agent_id=elevenlabs_agent_id,
            knowledge_items=[new_knowledge_item]
        )
    except requests.HTTPError as e:
        # Check if it's a knowledge not found error
        if e.response.status_code == 404 and "knowledge_base_documentation_not_found" in e.response.text:
            logger.warning(
                f"[PublishKnowledge] ⚠️ Knowledge not found error during agent update: {e.response.text}"
            )
            logger.warning(
                f"[PublishKnowledge] This may indicate the knowledge ID {new_knowledge_id} is invalid or was deleted"
            )
        
        # Re-raise to fail the workflow (this shouldn't happen with newly created knowledge)
        from datetime import datetime
        update_publish_job_status(
            tenant_id=tenant_id,
            publish_job_id=publish_job_id,
            status='failed',
            finished_at=datetime.utcnow(),
            error_message=str(e)
        )
        raise RuntimeError(f"Failed to update agent configuration: {str(e)}") from e
    except Exception as e:
        from datetime import datetime
        update_publish_job_status(
            tenant_id=tenant_id,
            publish_job_id=publish_job_id,
            status='failed',
            finished_at=datetime.utcnow(),
            error_message=str(e)
        )
        raise RuntimeError(f"Failed to update agent configuration: {str(e)}") from e
    
    # Step 7: Fetch ALL old knowledge IDs BEFORE saving new one
    old_knowledge_ids = get_existing_elevenlabs_knowledge_ids(tenant_id, location_id)
    logger.info(f"[PublishKnowledge] Found {len(old_knowledge_ids)} old knowledge IDs to delete: {old_knowledge_ids}")
    
    # Step 8: Delete old knowledge from ElevenLabs API
    deleted_old_knowledge = []
    if old_knowledge_ids:
        logger.info(f"[PublishKnowledge] Attempting to delete {len(old_knowledge_ids)} old knowledge documents from ElevenLabs")
        for old_id in old_knowledge_ids:
            try:
                if delete_knowledge(old_id):
                    deleted_old_knowledge.append(old_id)
                    logger.info(f"[PublishKnowledge] ✓ Deleted old knowledge from ElevenLabs: {old_id}")
                else:
                    logger.warning(f"[PublishKnowledge] ⚠️ Could not delete old knowledge from ElevenLabs: {old_id}")
            except Exception as e:
                logger.warning(f"[PublishKnowledge] Error deleting old knowledge {old_id} from ElevenLabs: {str(e)}")
    
    # Step 9: Delete old knowledge IDs from database
    if deleted_old_knowledge:
        try:
            deleted_count = delete_old_elevenlabs_knowledge_ids(
                tenant_id=tenant_id,
                location_id=location_id,
                knowledge_ids=deleted_old_knowledge
            )
            logger.info(f"[PublishKnowledge] ✓ Deleted {deleted_count} old knowledge IDs from database")
        except Exception as e:
            logger.warning(f"[PublishKnowledge] ⚠️ Error deleting old knowledge IDs from database: {str(e)}")
    
    # Step 10: Save NEW knowledge ID to database (AFTER deletion)
    save_new_elevenlabs_knowledge_id(
        tenant_id=tenant_id,
        location_id=location_id,
        knowledge_id=new_knowledge_id
    )
    logger.info(f"[PublishKnowledge] ✓ Saved new knowledge ID to database: {new_knowledge_id}")
    
    # Step 11: Mark ONLY the collected Speako knowledge documents as published
    # Extract param_ids from the knowledge docs collected in Step 2
    collected_param_ids = [doc['param_id'] for doc in knowledge_docs]
    logger.info(f"[PublishKnowledge] Marking {len(collected_param_ids)} knowledge entries as published: {collected_param_ids}")
    
    mark_speako_knowledge_published(
        tenant_id=tenant_id, 
        location_id=location_id,
        param_ids=collected_param_ids
    )
    
    # Step 12: Mark publish job as completed
    from datetime import datetime
    
    # Prepare response JSON for database
    response_data = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'new_knowledge_id': new_knowledge_id,
        'deleted_old_knowledge': deleted_old_knowledge,
        'knowledge_count': len(knowledge_docs)
    }
    
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='succeeded',
        finished_at=datetime.utcnow(),
        http_status_code=200,
        response_json=response_data
    )
    
    # Prepare result
    result = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'new_knowledge_id': new_knowledge_id,
        'old_knowledge_ids': old_knowledge_ids,
        'deleted_old_knowledge': deleted_old_knowledge,
        'r2_url': r2_url,
        'knowledge_count': len(knowledge_docs)
    }
    
    logger.info(
        f"[PublishKnowledge] ✅ Workflow completed: "
        f"agent_id={elevenlabs_agent_id}, new_knowledge_id={new_knowledge_id}"
    )
    
    return result


def publish_greetings(
    tenant_id: int,
    location_id: int,
    publish_job_id: int
) -> Dict[str, Any]:
    """
    Complete workflow for publishing greetings/prompts.
    
    This function orchestrates the greetings publishing process:
    1. Validates the publish job
    2. Collects greeting templates from tenant_integration_params
    3. Resolves dynamic variables ({{operation_hours}}, {{business_name}}, etc.)
    4. Stores resolved prompts in tenant_ai_prompts table
    5. Marks greeting entries as published
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        publish_job_id: Publish job identifier
        
    Returns:
        Dictionary containing:
            - prompts_created: Number of prompts created
            - processed_param_ids: List of param_ids marked as published
            
    Raises:
        ValueError: If publish job is invalid
        RuntimeError: If critical steps fail
    """
    logger.info(
        f"[PublishGreetings] Starting workflow: tenant_id={tenant_id}, "
        f"location_id={location_id}, publish_job_id={publish_job_id}"
    )
    
    # Import here to avoid circular imports
    from .publish_db import (
        collect_speako_greetings,
        get_location_operation_hours,
        get_ai_prompt_type,
        upsert_ai_prompt,
        mark_greeting_params_published
    )
    
    # Step 1: Validate publish job
    publish_job = get_publish_job(tenant_id, publish_job_id)
    if not publish_job:
        raise ValueError(
            f"Publish job not found: tenant_id={tenant_id}, publish_job_id={publish_job_id}"
        )
    
    # Mark as processing
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='in_progress',
        started_at=datetime.utcnow()
    )
    
    # Step 2: Collect greeting templates
    greeting_entries = collect_speako_greetings(tenant_id, location_id)
    
    if not greeting_entries:
        logger.warning("[PublishGreetings] No greeting entries found")
        raise ValueError(
            f"No greetings found for tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishGreetings] Collected {len(greeting_entries)} greeting entries")
    
    # Step 3: Resolve {{operation_hours}} variable
    logger.info("[PublishGreetings] Resolving {{operation_hours}} variable")
    
    schedule_json = get_location_operation_hours(tenant_id, location_id)
    operation_hours_text = get_human_friendly_operation_hours(schedule_json)
    
    logger.info(f"[PublishGreetings] ✓ Operation hours: {operation_hours_text}")
    
    # Step 4: TODO - Resolve other variables
    # {{business_name}}, {{location_name}}, {{ai_agent_name}}, etc.
    # For now, we'll just replace what we have
    
    variables = {
        'operation_hours': operation_hours_text,
        # TODO: Add other variables here
    }
    
    # Step 5: Process each greeting entry and store in tenant_ai_prompts
    prompts_created = 0
    processed_param_ids = []
    
    for entry in greeting_entries:
        param_id = entry['param_id']
        template_text = entry['value_text']
        param_code = entry.get('param_code')
        
        if not template_text:
            logger.warning(f"[PublishGreetings] Skipping param_id={param_id} - empty value_text")
            continue
        
        # Look up AI prompt type from ai_prompt_types table
        prompt_type = None
        if param_code:
            prompt_type = get_ai_prompt_type(param_code)
        
        if not prompt_type:
            logger.warning(f"[PublishGreetings] No prompt type found for param_code={param_code}, using defaults")
            type_code = param_code or 'unknown'
            display_name = f'Greeting {param_id}'
        else:
            type_code = prompt_type['code']
            display_name = prompt_type['display_name']
            logger.info(f"[PublishGreetings] Using prompt type: code={type_code}, display_name={display_name}")
        
        # Replace variables in template
        resolved_text = template_text
        for var_name, var_value in variables.items():
            placeholder = f"{{{{{var_name}}}}}"
            if placeholder in resolved_text:
                resolved_text = resolved_text.replace(placeholder, var_value)
                logger.info(f"[PublishGreetings] Replaced {placeholder} in param_id={param_id}")
        
        # Store in tenant_ai_prompts
        try:
            prompt_id = upsert_ai_prompt(
                tenant_id=tenant_id,
                location_id=location_id,
                type_code=type_code,
                title=display_name,
                name=display_name,
                body_template=resolved_text,
                metadata={'source_param_id': param_id, 'param_code': param_code}
            )
            
            if prompt_id:
                prompts_created += 1
                processed_param_ids.append(param_id)
                logger.info(f"[PublishGreetings] ✓ Created prompt_id={prompt_id} (type_code={type_code}) from param_id={param_id}")
        except Exception as e:
            logger.error(f"[PublishGreetings] ✗ Failed to create prompt from param_id={param_id}: {str(e)}")
            # Continue processing other entries
    
    # Step 6: Mark greeting entries as published
    if processed_param_ids:
        updated_count = mark_greeting_params_published(
            tenant_id=tenant_id,
            location_id=location_id,
            param_ids=processed_param_ids
        )
        logger.info(f"[PublishGreetings] ✓ Marked {updated_count} entries as published")
    
    # Step 7: Mark publish job as completed
    response_data = {
        'prompts_created': prompts_created,
        'processed_param_ids': processed_param_ids,
        'total_entries': len(greeting_entries)
    }
    
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='succeeded',
        finished_at=datetime.utcnow(),
        http_status_code=200,
        response_json=response_data
    )
    
    # Prepare result
    result = {
        'prompts_created': prompts_created,
        'processed_param_ids': processed_param_ids,
        'total_entries': len(greeting_entries)
    }
    
    logger.info(
        f"[PublishGreetings] ✅ Workflow completed: "
        f"created {prompts_created} prompts from {len(greeting_entries)} entries"
    )
    
    return result
