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
    3. Marks greeting entries as published
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        publish_job_id: Publish job identifier
        
    Returns:
        Dictionary containing:
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
        get_business_name,
        get_location_name,
        get_privacy_url,
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
    
    # ===== PASS 1: Build Variable Dictionary =====
    logger.info("[PublishGreetings] === PASS 1: Building variable dictionary ===")
    variables = {}
    
    # Database-derived variables (Level 0 - no dependencies)
    logger.info("[PublishGreetings] Resolving database-derived variables...")
    
    # {{operation_hours}}
    schedule_json = get_location_operation_hours(tenant_id, location_id)
    variables['operation_hours'] = get_human_friendly_operation_hours(schedule_json)
    logger.info(f"[PublishGreetings] ✓ operation_hours: {variables['operation_hours']}")
    
    # {{business_name}}
    variables['business_name'] = get_business_name(tenant_id)
    logger.info(f"[PublishGreetings] ✓ business_name: {variables['business_name']}")
    
    # {{location_name}}
    variables['location_name'] = get_location_name(tenant_id, location_id)
    logger.info(f"[PublishGreetings] ✓ location_name: {variables['location_name']}")
    
    # {{privacy_url}}
    variables['privacy_url'] = get_privacy_url(tenant_id)
    logger.info(f"[PublishGreetings] ✓ privacy_url: {variables['privacy_url']}")
    
    # Greeting-derived variables (Level 1 - may depend on Level 0)
    logger.info("[PublishGreetings] Extracting greeting-derived variables...")
    
    # Build a lookup map for param_code → value_text
    greeting_map = {entry['param_code']: entry['value_text'] for entry in greeting_entries if entry.get('param_code')}
    
    # Helper function to resolve variables in a text
    def resolve_variables(text: str, available_vars: dict) -> str:
        """Replace all {{variable}} placeholders except {{customer_first_name}}"""
        result = text
        for var_name, var_value in available_vars.items():
            placeholder = f"{{{{{var_name}}}}}"
            if placeholder in result:
                result = result.replace(placeholder, var_value or '')
        return result
    
    # {{ai_agent_name}} - from param_code='agent_name'
    if 'agent_name' in greeting_map:
        # Resolve any variables within agent_name first
        variables['ai_agent_name'] = resolve_variables(greeting_map['agent_name'], variables)
        logger.info(f"[PublishGreetings] ✓ ai_agent_name: {variables['ai_agent_name']}")
    else:
        variables['ai_agent_name'] = ''
        logger.warning("[PublishGreetings] ⚠️ ai_agent_name not found (param_code='agent_name' missing)")
    
    # {{after_hours_message}} - from param_code='after_hours'
    if 'after_hours' in greeting_map:
        # Resolve any variables within after_hours (e.g., {{operation_hours}})
        variables['after_hours_message'] = resolve_variables(greeting_map['after_hours'], variables)
        logger.info(f"[PublishGreetings] ✓ after_hours_message: {variables['after_hours_message']}")
    else:
        variables['after_hours_message'] = ''
        logger.warning("[PublishGreetings] ⚠️ after_hours_message not found (param_code='after_hours' missing)")
    
    # {{recording_disclosure}} - from param_code='recording_disclosure'
    if 'recording_disclosure' in greeting_map:
        # Resolve any variables within recording_disclosure
        variables['recording_disclosure'] = resolve_variables(greeting_map['recording_disclosure'], variables)
        logger.info(f"[PublishGreetings] ✓ recording_disclosure: {variables['recording_disclosure']}")
    else:
        variables['recording_disclosure'] = ''
        logger.warning("[PublishGreetings] ⚠️ recording_disclosure not found (param_code='recording_disclosure' missing)")
    
    logger.info(f"[PublishGreetings] Variable dictionary built with {len(variables)} variables")
    
    # ===== PASS 2: Replace Variables in All Greeting Entries =====
    logger.info("[PublishGreetings] === PASS 2: Replacing variables in all greeting entries ===")
    
    for entry in greeting_entries:
        original_text = entry.get('value_text', '')
        param_id = entry['param_id']
        param_code = entry.get('param_code', 'unknown')
        
        # Skip if empty
        if not original_text:
            continue
        
        # Replace all variables except {{customer_first_name}}
        resolved_text = resolve_variables(original_text, variables)
        
        # Check if anything changed
        if resolved_text != original_text:
            entry['value_text'] = resolved_text
            logger.info(f"[PublishGreetings] ✓ Resolved variables in param_id={param_id} (param_code={param_code})")
        else:
            logger.info(f"[PublishGreetings] - No variables to replace in param_id={param_id} (param_code={param_code})")
    
    # ===== STEP 3: Insert Prompts into tenant_ai_prompts Table =====
    logger.info("[PublishGreetings] === STEP 3: Inserting prompts into tenant_ai_prompts ===")
    
    from .publish_db import upsert_ai_prompt
    
    prompts_created = 0
    
    for entry in greeting_entries:
        param_id = entry['param_id']
        param_code = entry.get('param_code', '')
        resolved_text = entry.get('value_text', '')
        
        # Only process initial_greeting and return_customer
        if param_code not in ['initial_greeting', 'return_customer']:
            logger.info(f"[PublishGreetings] Skipping insertion for param_code={param_code} (not a prompt type)")
            continue
        
        if not resolved_text:
            logger.warning(f"[PublishGreetings] Skipping param_id={param_id} - empty resolved text")
            continue
        
        logger.info(f"[PublishGreetings] Processing param_code={param_code}, param_id={param_id}")
        
        # Determine type codes and names based on param_code
        if param_code == 'initial_greeting':
            base_type_code = 'first_message'
            base_name = 'Initial Greeting'
            after_type_code = 'first_message_after'
            after_name = 'Initial Greeting With After Hour Message'
        elif param_code == 'return_customer':
            base_type_code = 'first_message_customer'
            base_name = 'Return Customer Greeting'
            after_type_code = 'first_message_customer_after'
            after_name = 'Return Customer Greeting With After Hour Message'
        
        # Create base version (remove {{after_hours_message}} placeholder)
        base_text = resolved_text.replace('{{after_hours_message}}', '').strip()
        
        try:
            base_prompt_id = upsert_ai_prompt(
                tenant_id=tenant_id,
                location_id=location_id,
                type_code=base_type_code,
                title=base_name,
                name=base_name,
                body_template=base_text,
                metadata={'source_param_id': param_id, 'param_code': param_code, 'version': 'base'}
            )
            
            if base_prompt_id:
                prompts_created += 1
                logger.info(f"[PublishGreetings] ✓ Created base prompt: prompt_id={base_prompt_id}, type_code={base_type_code}")
        except Exception as e:
            logger.error(f"[PublishGreetings] ✗ Failed to create base prompt for param_id={param_id}: {str(e)}")
        
        # Create after-hours version (use fully resolved text)
        try:
            after_prompt_id = upsert_ai_prompt(
                tenant_id=tenant_id,
                location_id=location_id,
                type_code=after_type_code,
                title=after_name,
                name=after_name,
                body_template=resolved_text,
                metadata={'source_param_id': param_id, 'param_code': param_code, 'version': 'after_hours'}
            )
            
            if after_prompt_id:
                prompts_created += 1
                logger.info(f"[PublishGreetings] ✓ Created after-hours prompt: prompt_id={after_prompt_id}, type_code={after_type_code}")
        except Exception as e:
            logger.error(f"[PublishGreetings] ✗ Failed to create after-hours prompt for param_id={param_id}: {str(e)}")
    
    logger.info(f"[PublishGreetings] Total prompts created: {prompts_created}")
    
    # Step 4: Mark greeting entries as published
    processed_param_ids = [entry['param_id'] for entry in greeting_entries]
    
    if processed_param_ids:
        updated_count = mark_greeting_params_published(
            tenant_id=tenant_id,
            location_id=location_id,
            param_ids=processed_param_ids
        )
        logger.info(f"[PublishGreetings] ✓ Marked {updated_count} entries as published")
    
    # Step 5: Mark publish job as completed
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
        f"created {prompts_created} prompts, marked {len(processed_param_ids)} entries as published"
    )
    
    return result


def publish_voice_dict(
    tenant_id: int,
    location_id: int,
    publish_job_id: int
) -> Dict[str, Any]:
    """
    Complete workflow for publishing voice dictionary (conversation config) to ElevenLabs agent.
    
    This function orchestrates the voice dict publishing process:
    1. Validates the publish job
    2. Collects voice dict parameters from tenant_integration_params
    3. Builds conversation_config JSON payload
    4. PATCH to ElevenLabs agent API
    5. Marks parameters as published (only if PATCH succeeds)
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        publish_job_id: Publish job identifier
        
    Returns:
        Dictionary containing:
            - elevenlabs_agent_id: ElevenLabs agent ID
            - http_status_code: HTTP status from ElevenLabs API
            - params_count: Number of parameters processed
            - params_updated: Number of parameters marked as published
            - conversation_config: The payload sent to ElevenLabs
            - processed_param_ids: List of param_ids marked as published
            
    Raises:
        ValueError: If publish job is invalid or no params found
        RuntimeError: If PATCH to ElevenLabs fails
    """
    logger.info(
        f"[PublishVoiceDict] Starting workflow: tenant_id={tenant_id}, "
        f"location_id={location_id}, publish_job_id={publish_job_id}"
    )
    
    # Import here to avoid circular imports
    from .publish_db import (
        collect_voice_dict_params,
        mark_voice_dict_params_published
    )
    from .elevenlabs_client import patch_elevenlabs_agent
    
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
    
    # Step 2: Get ElevenLabs Agent ID
    elevenlabs_agent_id, location_name, timezone = get_elevenlabs_agent_id(tenant_id, location_id)
    if not elevenlabs_agent_id:
        raise ValueError(
            f"ElevenLabs agent ID not found: tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishVoiceDict] Found agent ID: {elevenlabs_agent_id}")
    
    # Step 3: Collect voice dict parameters
    params = collect_voice_dict_params(tenant_id, location_id)
    
    if not params:
        logger.warning("[PublishVoiceDict] No voice dict params found")
        raise ValueError(
            f"No voice dict params found for tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishVoiceDict] Collected {len(params)} voice dict params")
    
    # Store param_ids for later marking as published
    param_ids = [p['param_id'] for p in params]
    
    # Step 4: Build conversation_config JSON payload
    logger.info("[PublishVoiceDict] Building conversation_config payload...")
    
    conversation_config = {
        "tts": {},
        "turn": {},
        "conversation": {}
    }
    
    # Map parameters to conversation_config structure
    for param in params:
        service = param.get('service')
        param_code = param.get('param_code')
        value_text = param.get('value_text')
        value_numeric = param.get('value_numeric')
        
        logger.info(f"[PublishVoiceDict] Processing: service={service}, param_code={param_code}")
        
        try:
            # service='agent' + param_code='voice_id' → conversation_config.tts.voice_id (string)
            if service == 'agent' and param_code == 'voice_id':
                conversation_config['tts']['voice_id'] = value_text
                logger.info(f"[PublishVoiceDict]   → tts.voice_id = {value_text}")
            
            # service='tts' + param_code='speed' → conversation_config.tts.speed (float)
            elif service == 'tts' and param_code == 'speed':
                conversation_config['tts']['speed'] = float(value_numeric) if value_numeric is not None else None
                logger.info(f"[PublishVoiceDict]   → tts.speed = {conversation_config['tts']['speed']}")
            
            # service='turn' + param_code='turn_timeout' → conversation_config.turn.turn_timeout (int)
            elif service == 'turn' and param_code == 'turn_timeout':
                conversation_config['turn']['turn_timeout'] = int(value_numeric) if value_numeric is not None else None
                logger.info(f"[PublishVoiceDict]   → turn.turn_timeout = {conversation_config['turn']['turn_timeout']}")
            
            # service='conversation' + param_code='silence_end_call_timeout' → conversation_config.turn.silence_end_call_timeout (int)
            elif service == 'conversation' and param_code == 'silence_end_call_timeout':
                conversation_config['turn']['silence_end_call_timeout'] = int(value_numeric) if value_numeric is not None else None
                logger.info(f"[PublishVoiceDict]   → turn.silence_end_call_timeout = {conversation_config['turn']['silence_end_call_timeout']}")
            
            # service='conversation' + param_code='max_duration_seconds' → conversation_config.conversation.max_duration_seconds (int)
            elif service == 'conversation' and param_code == 'max_duration_seconds':
                conversation_config['conversation']['max_duration_seconds'] = int(value_numeric) if value_numeric is not None else None
                logger.info(f"[PublishVoiceDict]   → conversation.max_duration_seconds = {conversation_config['conversation']['max_duration_seconds']}")
            
            else:
                logger.warning(f"[PublishVoiceDict]   → Unrecognized service/param_code combination, skipping")
        
        except (ValueError, TypeError) as e:
            logger.error(f"[PublishVoiceDict]   → Error converting value: {str(e)}")
            raise ValueError(f"Invalid value for {service}.{param_code}: {str(e)}")
    
    logger.info(f"[PublishVoiceDict] Built conversation_config: {json.dumps(conversation_config, indent=2)}")
    
    # Step 5: PATCH to ElevenLabs API
    logger.info(f"[PublishVoiceDict] Sending PATCH request to ElevenLabs agent {elevenlabs_agent_id}...")
    
    try:
        http_status_code, response_json = patch_elevenlabs_agent(
            agent_id=elevenlabs_agent_id,
            conversation_config=conversation_config
        )
        
        logger.info(f"[PublishVoiceDict] ✅ PATCH successful: status={http_status_code}")
        
    except Exception as e:
        logger.error(f"[PublishVoiceDict] ✗ PATCH failed: {str(e)}")
        
        # Update publish job as failed
        update_publish_job_status(
            tenant_id=tenant_id,
            publish_job_id=publish_job_id,
            status='failed',
            finished_at=datetime.utcnow(),
            error_message=str(e)
        )
        
        # Re-raise exception - params will NOT be marked as published
        raise RuntimeError(f"Failed to update ElevenLabs agent: {str(e)}") from e
    
    # Step 6: Mark parameters as published (only reached if PATCH succeeded)
    logger.info("[PublishVoiceDict] Marking params as published...")
    params_updated = mark_voice_dict_params_published(
        tenant_id=tenant_id,
        location_id=location_id,
        param_ids=param_ids
    )
    
    logger.info(f"[PublishVoiceDict] ✓ Marked {params_updated} params as published")
    
    # Step 7: Update publish job status (only reached if PATCH succeeded)
    response_data = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'http_status_code': http_status_code,
        'params_count': len(params),
        'params_updated': params_updated,
        'conversation_config': conversation_config
    }
    
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='succeeded',
        finished_at=datetime.utcnow(),
        http_status_code=http_status_code,
        response_json=response_data
    )
    
    # Prepare result
    result = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'http_status_code': http_status_code,
        'params_count': len(params),
        'params_updated': params_updated,
        'conversation_config': conversation_config,
        'processed_param_ids': param_ids
    }
    
    logger.info(
        f"[PublishVoiceDict] ✅ Workflow completed: "
        f"agent_id={elevenlabs_agent_id}, params_count={len(params)}, params_updated={params_updated}"
    )
    
    return result
