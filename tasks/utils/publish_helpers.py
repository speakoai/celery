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
    update_agent_knowledge,
    patch_elevenlabs_agent
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
    
    # Step 5: Collect and process dictionary entry
    logger.info("[PublishVoiceDict] === STEP 5: Processing pronunciation dictionary ===")
    
    from .publish_db import collect_dictionary_entry, update_dictionary_param_text
    from .elevenlabs_client import create_pronunciation_dictionary, update_pronunciation_dictionary
    
    dictionary_entry = collect_dictionary_entry(tenant_id, location_id)
    dictionary_locator = None
    dictionary_param_id = None
    dict_created = False
    dict_updated = False
    dictionary_id = None
    
    if dictionary_entry:
        param_id = dictionary_entry['param_id']
        value_text = dictionary_entry.get('value_text')
        value_json = dictionary_entry.get('value_json')
        
        logger.info(f"[PublishVoiceDict] Found dictionary entry: param_id={param_id}")
        
        # Validate rules
        try:
            rules = value_json if isinstance(value_json, list) else []
            if not rules:
                logger.warning("[PublishVoiceDict] Empty rules array, skipping dictionary")
                dictionary_entry = None
        except Exception as e:
            logger.error(f"[PublishVoiceDict] Invalid value_json: {str(e)}, skipping dictionary")
            dictionary_entry = None
    
    # Process dictionary if valid
    if dictionary_entry and rules:
        dictionary_param_id = param_id
        
        if not value_text or value_text.strip() == '':
            # CREATE new dictionary
            logger.info("[PublishVoiceDict] Creating new pronunciation dictionary...")
            
            # Generate name with timestamp (like knowledge)
            timestamp = format_timestamp_for_location(timezone)
            dict_name = f"{location_name} - {timestamp}"
            
            try:
                dictionary_id, version_id = create_pronunciation_dictionary(rules, dict_name)
                logger.info(f"[PublishVoiceDict] ✅ Created dictionary: id={dictionary_id}, version={version_id}")
                
                # Update param value_text with dictionary_id
                update_dictionary_param_text(param_id, dictionary_id)
                
                dict_created = True
                dictionary_locator = {
                    "pronunciation_dictionary_id": dictionary_id,
                    "version_id": version_id
                }
            except Exception as e:
                logger.error(f"[PublishVoiceDict] Failed to create dictionary: {str(e)}")
                # Re-raise to stop workflow
                update_publish_job_status(
                    tenant_id=tenant_id,
                    publish_job_id=publish_job_id,
                    status='failed',
                    finished_at=datetime.utcnow(),
                    error_message=str(e)
                )
                raise RuntimeError(f"Failed to create pronunciation dictionary: {str(e)}") from e
        
        else:
            # UPDATE existing dictionary
            dictionary_id = value_text.strip()
            logger.info(f"[PublishVoiceDict] Updating existing dictionary: {dictionary_id}...")
            
            try:
                returned_dict_id, latest_version_id = update_pronunciation_dictionary(dictionary_id, rules)
                logger.info(f"[PublishVoiceDict] ✅ Updated dictionary: id={returned_dict_id}, version={latest_version_id}")
                
                # Check if dictionary_id changed (shouldn't happen, but handle it)
                if returned_dict_id != dictionary_id:
                    logger.warning(f"[PublishVoiceDict] Dictionary ID changed: {dictionary_id} → {returned_dict_id}")
                    update_dictionary_param_text(param_id, returned_dict_id)
                    dictionary_id = returned_dict_id
                
                dict_updated = True
                dictionary_locator = {
                    "pronunciation_dictionary_id": dictionary_id,
                    "version_id": latest_version_id
                }
            except Exception as e:
                logger.error(f"[PublishVoiceDict] Failed to update dictionary: {str(e)}")
                # Re-raise to stop workflow
                update_publish_job_status(
                    tenant_id=tenant_id,
                    publish_job_id=publish_job_id,
                    status='failed',
                    finished_at=datetime.utcnow(),
                    error_message=str(e)
                )
                raise RuntimeError(f"Failed to update pronunciation dictionary: {str(e)}") from e
    else:
        logger.info("[PublishVoiceDict] No dictionary to process or invalid rules")
    
    # Step 6: Add dictionary locator to conversation_config if exists
    if dictionary_locator:
        if 'tts' not in conversation_config:
            conversation_config['tts'] = {}
        conversation_config['tts']['pronunciation_dictionary_locators'] = [dictionary_locator]
        logger.info(f"[PublishVoiceDict] ✓ Added dictionary locator to conversation_config")
    
    # Step 7: PATCH to ElevenLabs API
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
    
    # Step 8: Mark parameters as published (only reached if PATCH succeeded)
    logger.info("[PublishVoiceDict] Marking params as published...")
    
    # Collect all param_ids to mark as published
    all_param_ids = param_ids.copy()  # Voice dict params
    if dictionary_param_id:
        all_param_ids.append(dictionary_param_id)
        logger.info(f"[PublishVoiceDict] Including dictionary param_id={dictionary_param_id}")
    
    params_updated = mark_voice_dict_params_published(
        tenant_id=tenant_id,
        location_id=location_id,
        param_ids=all_param_ids
    )
    
    logger.info(f"[PublishVoiceDict] ✓ Marked {params_updated} params as published")
    
    # Step 9: Update publish job status (only reached if PATCH succeeded)
    response_data = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'http_status_code': http_status_code,
        'params_count': len(params),
        'params_updated': params_updated,
        'conversation_config': conversation_config,
        'dictionary_processed': dictionary_entry is not None,
        'dictionary_created': dict_created,
        'dictionary_updated': dict_updated,
        'dictionary_id': dictionary_id
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
        'processed_param_ids': all_param_ids,
        'dictionary_processed': dictionary_entry is not None,
        'dictionary_created': dict_created,
        'dictionary_updated': dict_updated,
        'dictionary_id': dictionary_id
    }
    
    logger.info(
        f"[PublishVoiceDict] ✅ Workflow completed: "
        f"agent_id={elevenlabs_agent_id}, params_count={len(params)}, params_updated={params_updated}, "
        f"dictionary_processed={dictionary_entry is not None}, dict_created={dict_created}, dict_updated={dict_updated}"
    )
    
    return result


def publish_personality(tenant_id: str, location_id: str, publish_job_id: str) -> Dict[str, Any]:
    """
    Publish personality configuration to tenant_ai_prompts.
    
    Phase 1: Handle traits, tone_of_voice, response_style
    Phase 2: Handle temperature and custom_instruction (future implementation)
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        publish_job_id: Publish job identifier
    
    Returns:
        Dict with keys: prompt_created, prompt_id, params_updated, processed_param_ids
    """
    logger.info(
        f"[PublishPersonality] Starting workflow - "
        f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
    )
    
    # Import database functions
    from .publish_db import (
        collect_personality_params,
        get_prompt_fragments,
        upsert_ai_prompt,
        mark_personality_params_published
    )
    
    # Step 1: Collect personality params
    logger.info("[PublishPersonality] Collecting personality parameters...")
    params = collect_personality_params(tenant_id, location_id)
    
    if not params:
        logger.warning("[PublishPersonality] No personality parameters found")
        return {
            'prompt_created': False,
            'prompt_id': None,
            'params_updated': 0,
            'processed_param_ids': []
        }
    
    logger.info(f"[PublishPersonality] Found {len(params)} personality params")
    
    # Step 2: Build param_map for easy lookup - extract from value_json or value_text
    param_map = {}
    for p in params:
        value_json = p.get('value_json')
        value_text = p.get('value_text')
        param_code = p['param_code']
        
        # Handle custom_instruction from value_text (not value_json)
        if param_code == 'custom_instruction':
            param_map[param_code] = value_text or ''
            if value_text:
                logger.info(f"[PublishPersonality] custom_instruction: {value_text[:50]}...")
            else:
                logger.info(f"[PublishPersonality] custom_instruction: empty/null, using empty string")
            continue  # Skip to next param
        
        # Handle other params from value_json (existing logic)
        if value_json and isinstance(value_json, list) and len(value_json) > 0:
            # Join array elements with ", " for traits and tone_of_voice
            # For response_style, just extract the first element
            if param_code in ['traits', 'tone_of_voice']:
                param_map[param_code] = ', '.join(str(v) for v in value_json)
                logger.info(f"[PublishPersonality] {param_code}: {param_map[param_code]}")
            elif param_code == 'response_style':
                param_map[param_code] = str(value_json[0])
                logger.info(f"[PublishPersonality] {param_code}: {param_map[param_code]}")
            else:
                # For other params (temperature), just store as-is
                param_map[param_code] = value_json
                logger.info(f"[PublishPersonality] {param_code}: stored as-is")
        else:
            # Fallback to empty string if value_json is None or empty
            param_map[param_code] = ''
            # Only warn for required params
            if param_code in ['traits', 'tone_of_voice', 'response_style']:
                logger.warning(f"[PublishPersonality] {param_code}: empty/null value_json, using empty string")
    
    logger.info(f"[PublishPersonality] Param map keys: {list(param_map.keys())}")
    
    # Step 3: Fetch ALL prompt fragments in ONE query (optimized)
    logger.info("[PublishPersonality] Fetching prompt fragments (optimized single query)...")
    fragments = get_prompt_fragments([
        'personality',
        'response_style_concise',
        'response_style_balanced',
        'response_style_detailed',
        'custom_instruction'
    ])
    
    logger.info(f"[PublishPersonality] Fetched {len(fragments)} fragments: {list(fragments.keys())}")
    
    # Step 4: Get the personality template
    personality_template = fragments.get('personality', '')
    
    if not personality_template:
        logger.error("[PublishPersonality] Personality template not found in ai_prompt_fragment")
        raise ValueError("Personality template not found in ai_prompt_fragment table")
    
    logger.info(f"[PublishPersonality] Personality template: {personality_template[:100]}...")
    
    # Step 5: Replace {{traits}}
    traits_value = param_map.get('traits', '')
    personality_template = personality_template.replace('{{traits}}', traits_value)
    logger.info(f"[PublishPersonality] ✓ Replaced {{{{traits}}}} with: {traits_value[:50]}...")
    
    # Step 6: Replace {{tone_of_voice}}
    tone_value = param_map.get('tone_of_voice', '')
    personality_template = personality_template.replace('{{tone_of_voice}}', tone_value)
    logger.info(f"[PublishPersonality] ✓ Replaced {{{{tone_of_voice}}}} with: {tone_value[:50]}...")
    
    # Step 7: Replace {{response_style}} with conditional mapping
    response_style_value = param_map.get('response_style', '').strip()
    logger.info(f"[PublishPersonality] Response style value: '{response_style_value}'")
    
    if response_style_value == 'Concise':
        response_style_text = fragments.get('response_style_concise', '')
        logger.info(f"[PublishPersonality] → Using 'Concise' template")
    elif response_style_value == 'Balanced':
        response_style_text = fragments.get('response_style_balanced', '')
        logger.info(f"[PublishPersonality] → Using 'Balanced' template")
    elif response_style_value == 'Detailed':
        response_style_text = fragments.get('response_style_detailed', '')
        logger.info(f"[PublishPersonality] → Using 'Detailed' template")
    else:
        response_style_text = ''
        logger.warning(f"[PublishPersonality] ⚠ Unknown response_style value: '{response_style_value}'")
    
    personality_template = personality_template.replace('{{response_style}}', response_style_text)
    logger.info(f"[PublishPersonality] ✓ Replaced {{{{response_style}}}} with: {response_style_text[:50]}...")
    
    # Step 8: Insert completed personality to tenant_ai_prompts
    logger.info("[PublishPersonality] Inserting personality prompt to tenant_ai_prompts...")
    prompt_id = upsert_ai_prompt(
        tenant_id=tenant_id,
        location_id=location_id,
        name='Personality',
        type_code='personality',
        title='Agent Personality Configuration',
        body_template=personality_template
    )
    
    logger.info(f"[PublishPersonality] ✓ Inserted personality prompt_id={prompt_id}")
    
    # Step 8.5: Process custom_instruction (if exists)
    custom_instruction_value = param_map.get('custom_instruction', '').strip()
    custom_instruction_prompt_id = None
    
    if custom_instruction_value:
        logger.info("[PublishPersonality] Processing custom_instruction...")
        
        # Get custom_instruction template
        custom_instruction_template = fragments.get('custom_instruction', '')
        
        if not custom_instruction_template:
            logger.error("[PublishPersonality] custom_instruction template not found in ai_prompt_fragment")
            raise ValueError("custom_instruction template not found in ai_prompt_fragment table")
        
        logger.info(f"[PublishPersonality] custom_instruction template: {custom_instruction_template[:100]}...")
        
        # Replace {{custom_instruction}} variable
        custom_instruction_resolved = custom_instruction_template.replace(
            '{{custom_instruction}}', 
            custom_instruction_value
        )
        
        logger.info(f"[PublishPersonality] ✓ Replaced {{{{custom_instruction}}}} with: {custom_instruction_value[:50]}...")
        
        # Insert custom_instruction to tenant_ai_prompts
        custom_instruction_prompt_id = upsert_ai_prompt(
            tenant_id=tenant_id,
            location_id=location_id,
            name='Custom Instruction',
            type_code='custom_instruction',
            title='Agent Custom Instruction',
            body_template=custom_instruction_resolved
        )
        
        logger.info(f"[PublishPersonality] ✓ Inserted custom_instruction prompt_id={custom_instruction_prompt_id}")
    else:
        logger.info("[PublishPersonality] No custom_instruction value provided, skipping")
    
    # Step 8.6: Process temperature (if exists)
    temperature_param = next((p for p in params if p['param_code'] == 'temperature'), None)
    temperature_updated = False
    temperature_value = None
    
    if temperature_param and temperature_param.get('value_numeric') is not None:
        logger.info("[PublishPersonality] Processing temperature...")
        
        # Get ElevenLabs agent ID (returns tuple: agent_id, location_name, timezone)
        agent_id, _, _ = get_elevenlabs_agent_id(tenant_id, location_id)
        if not agent_id:
            logger.error("[PublishPersonality] Cannot process temperature: ElevenLabs agent_id not found")
            raise ValueError(f"ElevenLabs agent_id not found for tenant_id={tenant_id}, location_id={location_id}")
        
        # Convert Decimal to float
        temperature_value = float(temperature_param['value_numeric'])
        logger.info(f"[PublishPersonality] Temperature value: {temperature_value}")
        
        # Build conversation_config payload
        conversation_config = {
            "agent": {
                "prompt": {
                    "temperature": temperature_value
                }
            }
        }
        
        try:
            # PATCH to ElevenLabs (returns tuple: status_code, response_dict)
            status_code, response_dict = patch_elevenlabs_agent(agent_id, conversation_config)
            logger.info(f"[PublishPersonality] ✓ Temperature PATCH successful: HTTP {status_code}")
            temperature_updated = True
            
        except Exception as e:
            logger.error(f"[PublishPersonality] Failed to PATCH temperature: {str(e)}")
            from .publish_db import update_publish_job_status
            update_publish_job_status(
                tenant_id=tenant_id,
                publish_job_id=publish_job_id,
                status='failed',
                finished_at=datetime.utcnow(),
                error_message=f"Failed to PATCH temperature: {str(e)}"
            )
            raise RuntimeError(f"Failed to PATCH temperature to ElevenLabs: {str(e)}") from e
    else:
        logger.info("[PublishPersonality] No temperature value provided, skipping")
    
    # Step 9: Mark traits/tone/style as published (always) + custom_instruction (if processed) + temperature (if updated)
    param_ids_to_mark = [
        p['param_id'] for p in params 
        if p['param_code'] in ['traits', 'tone_of_voice', 'response_style']
    ]
    
    # Also mark custom_instruction as published if it was processed
    if custom_instruction_value:
        custom_instruction_param = next(
            (p for p in params if p['param_code'] == 'custom_instruction'), 
            None
        )
        if custom_instruction_param:
            param_ids_to_mark.append(custom_instruction_param['param_id'])
            logger.info("[PublishPersonality] Including custom_instruction in published params")
    
    # Also mark temperature as published if it was updated
    if temperature_updated and temperature_param:
        param_ids_to_mark.append(temperature_param['param_id'])
        logger.info("[PublishPersonality] Including temperature in published params")
    
    logger.info(f"[PublishPersonality] Marking {len(param_ids_to_mark)} params as published")
    
    params_updated = mark_personality_params_published(
        tenant_id=tenant_id,
        location_id=location_id,
        param_ids=param_ids_to_mark
    )
    
    logger.info(f"[PublishPersonality] ✓ Marked {params_updated} params as published")
    
    # Step 10: Update publish job status
    from .publish_db import update_publish_job_status
    
    response_data = {
        'prompt_created': True,
        'prompt_id': prompt_id,
        'custom_instruction_created': custom_instruction_prompt_id is not None,
        'custom_instruction_prompt_id': custom_instruction_prompt_id,
        'temperature_updated': temperature_updated,
        'temperature_value': temperature_value,
        'params_updated': params_updated,
        'processed_param_ids': param_ids_to_mark
    }
    
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='succeeded',
        finished_at=datetime.utcnow(),
        response_json=response_data
    )
    
    # Step 11: Return results
    result = {
        'prompt_created': True,
        'prompt_id': prompt_id,
        'custom_instruction_created': custom_instruction_prompt_id is not None,
        'custom_instruction_prompt_id': custom_instruction_prompt_id,
        'temperature_updated': temperature_updated,
        'temperature_value': temperature_value,
        'params_updated': params_updated,
        'processed_param_ids': param_ids_to_mark
    }
    
    logger.info(
        f"[PublishPersonality] ✅ Workflow completed: "
        f"prompt_id={prompt_id}, params_updated={params_updated}"
    )
    
    return result


def publish_tools(tenant_id: str, location_id: str, publish_job_id: str) -> Dict[str, Any]:
    """
    Publish tool configurations to ElevenLabs agent.
    
    Workflow:
    1. Collect tool params from tenant_integration_params (joined with ai_tool_types)
    2. Filter params with enabled=true (skip greetings, skip disabled)
    3. Extract and deduplicate tool_ids
    4. PATCH tool_ids to ElevenLabs agent
    5. Mark enabled params as published
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        publish_job_id: Publish job identifier
    
    Returns:
        Dict with keys: elevenlabs_agent_id, http_status_code, params_count, 
                       enabled_params_count, unique_tool_ids_count, tool_ids,
                       params_updated, processed_param_ids
    """
    logger.info(
        f"[PublishTools] Starting workflow - "
        f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
    )
    
    # Import database functions
    from .publish_db import (
        collect_tool_params,
        mark_tool_params_published,
        update_publish_job_status
    )
    
    # Step 1: Update publish job status to in_progress
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='in_progress',
        started_at=datetime.utcnow()
    )
    
    # Step 2: Get ElevenLabs agent ID
    elevenlabs_agent_id, location_name, timezone = get_elevenlabs_agent_id(tenant_id, location_id)
    if not elevenlabs_agent_id:
        raise ValueError(
            f"ElevenLabs agent ID not found: tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishTools] Found agent ID: {elevenlabs_agent_id}")
    
    # Step 3: Collect tool params with JOIN
    logger.info("[PublishTools] Collecting tool parameters...")
    params = collect_tool_params(tenant_id, location_id)
    
    if not params:
        logger.warning("[PublishTools] No tool params found")
        # Send empty array to ElevenLabs as per requirements
        logger.info("[PublishTools] Sending empty tool_ids array to ElevenLabs")
        params_count = 0
        enabled_params = []
        unique_tool_ids = []
    else:
        logger.info(f"[PublishTools] Found {len(params)} tool params")
        params_count = len(params)
        
        # Step 4: Filter and extract tool_ids
        logger.info("[PublishTools] Filtering enabled tools and extracting tool_ids...")
        all_tool_ids = []
        enabled_params = []
        
        for param in params:
            param_code = param['param_code']
            value_json = param.get('value_json', {})
            tool_ids = param.get('tool_ids', [])
            
            # Skip greetings
            if param_code == 'greetings':
                logger.info(f"[PublishTools] Skipping greetings tool: param_code={param_code}")
                continue
            
            # Check if enabled
            enabled = value_json.get('enabled', False) if value_json else False
            
            if not enabled:
                logger.info(f"[PublishTools] Skipping disabled tool: param_code={param_code}")
                continue
            
            # Validate tool_ids from ai_tool_types
            if not tool_ids or len(tool_ids) == 0:
                error_msg = f"tool_ids is NULL or empty for param_code={param_code} in ai_tool_types table"
                logger.error(f"[PublishTools] {error_msg}")
                update_publish_job_status(
                    tenant_id=tenant_id,
                    publish_job_id=publish_job_id,
                    status='failed',
                    finished_at=datetime.utcnow(),
                    error_message=error_msg
                )
                raise ValueError(error_msg)
            
            logger.info(f"[PublishTools] Enabled tool: param_code={param_code}, tool_ids={tool_ids}")
            enabled_params.append(param)
            all_tool_ids.extend(tool_ids)
        
        # Step 5: Deduplicate tool_ids
        unique_tool_ids = list(set(all_tool_ids))
        logger.info(f"[PublishTools] Extracted {len(all_tool_ids)} total tool_ids, {len(unique_tool_ids)} unique")
        logger.info(f"[PublishTools] Unique tool_ids: {unique_tool_ids}")
    
    # Step 6: Build conversation_config payload
    logger.info("[PublishTools] Building conversation_config payload...")
    conversation_config = {
        "agent": {
            "prompt": {
                "tool_ids": unique_tool_ids
            }
        }
    }
    
    logger.info(f"[PublishTools] Payload: {json.dumps(conversation_config)}")
    
    # Step 7: PATCH to ElevenLabs
    logger.info(f"[PublishTools] Sending PATCH request to ElevenLabs agent {elevenlabs_agent_id}...")
    
    try:
        status_code, response_dict = patch_elevenlabs_agent(
            agent_id=elevenlabs_agent_id,
            conversation_config=conversation_config
        )
        logger.info(f"[PublishTools] ✓ PATCH successful: HTTP {status_code}")
        
    except Exception as e:
        logger.error(f"[PublishTools] Failed to PATCH ElevenLabs: {str(e)}")
        update_publish_job_status(
            tenant_id=tenant_id,
            publish_job_id=publish_job_id,
            status='failed',
            finished_at=datetime.utcnow(),
            error_message=f"Failed to PATCH ElevenLabs: {str(e)}"
        )
        raise RuntimeError(f"Failed to PATCH tool_ids to ElevenLabs: {str(e)}") from e
    
    # Step 7.5: Compose and insert tools prompt
    prompt_created = False
    prompt_id = None
    
    if len(unique_tool_ids) > 0:
        logger.info("[PublishTools] Starting tools prompt composition...")
        
        try:
            # Step 7.5.1: Get location_type
            from .publish_db import get_location_type, get_tool_prompt_template, get_tool_service_prompts, upsert_ai_prompt
            
            location_type = get_location_type(tenant_id, location_id)
            logger.info(f"[PublishTools] Location type: {location_type}")
            
            # Step 7.5.2: Get tool prompt template
            template = get_tool_prompt_template()
            logger.info(f"[PublishTools] Loaded template: {len(template)} characters")
            
            # Step 7.5.3: Get service prompts for all eligible tools
            tool_prompts_data = get_tool_service_prompts(unique_tool_ids)
            
            if not tool_prompts_data:
                logger.warning("[PublishTools] No service_prompts found for any tools, skipping prompt composition")
            else:
                # Step 7.5.4: Extract and filter prompts by location_type
                extracted_prompts = []
                
                for tool_data in tool_prompts_data:
                    tool_id = tool_data['tool_id']
                    service_prompts = tool_data.get('service_prompts')
                    
                    if not service_prompts:
                        logger.warning(f"[PublishTools] No service_prompts for tool_id={tool_id}, skipping")
                        continue
                    
                    # Navigate JSON: service_prompts.by_service_type.{location_type}
                    by_service_type = service_prompts.get('by_service_type', {})
                    
                    # Determine which service type to use
                    if location_type == 'rest':
                        service_key = 'rest'
                    elif location_type == 'service':
                        service_key = 'service'
                    else:
                        service_key = 'service'  # Default to service
                        logger.info(f"[PublishTools] Using 'service' as default for location_type={location_type}")
                    
                    service_prompts_array = by_service_type.get(service_key, [])
                    
                    if not service_prompts_array:
                        logger.warning(f"[PublishTools] No prompts for tool_id={tool_id}, service={service_key}")
                        continue
                    
                    # Extract markdown from all prompts in array
                    for prompt_obj in service_prompts_array:
                        markdown = prompt_obj.get('markdown', '')
                        if markdown:
                            extracted_prompts.append(markdown)
                            logger.info(f"[PublishTools] ✓ Extracted prompt for tool_id={tool_id}, service={service_key}")
                
                if not extracted_prompts:
                    logger.warning("[PublishTools] No prompts extracted, skipping prompt composition")
                else:
                    # Step 7.5.5: Compose final prompt
                    # Join all extracted prompts with double newlines
                    all_tool_prompts = '\n\n'.join(extracted_prompts)
                    
                    # Replace literal \n with actual newlines
                    all_tool_prompts = all_tool_prompts.replace('\\n', '\n')
                    
                    # Combine template + tool prompts
                    final_prompt = f"{template}\n\n{all_tool_prompts}"
                    
                    logger.info(f"[PublishTools] Composed final prompt: {len(final_prompt)} characters, {len(extracted_prompts)} tool prompts")
                    
                    # Step 7.5.6: Insert into tenant_ai_prompts
                    prompt_id = upsert_ai_prompt(
                        tenant_id=tenant_id,
                        location_id=location_id,
                        name='Use of Tools',
                        type_code='use_of_tools',
                        title='Use of Tools',
                        body_template=final_prompt
                    )
                    
                    prompt_created = True
                    logger.info(f"[PublishTools] ✓ Inserted tools prompt: prompt_id={prompt_id}")
        
        except ValueError as e:
            # Template not found or location not found - log warning but don't fail
            logger.warning(f"[PublishTools] Could not compose tools prompt: {str(e)}")
            logger.warning("[PublishTools] Continuing workflow without prompt composition")
        
        except Exception as e:
            # Other errors - log error but don't fail workflow
            logger.error(f"[PublishTools] Error during prompt composition: {str(e)}")
            logger.error("[PublishTools] Continuing workflow without prompt composition")
    else:
        logger.info("[PublishTools] No tools to process, skipping prompt composition")
    
    # Step 8: Mark enabled params as published
    enabled_param_ids = [p['param_id'] for p in enabled_params]
    
    if enabled_param_ids:
        logger.info(f"[PublishTools] Marking {len(enabled_param_ids)} enabled params as published")
        params_updated = mark_tool_params_published(
            tenant_id=tenant_id,
            location_id=location_id,
            param_ids=enabled_param_ids
        )
        logger.info(f"[PublishTools] ✓ Marked {params_updated} params as published")
    else:
        logger.info("[PublishTools] No enabled params to mark as published")
        params_updated = 0
    
    # Step 9: Update publish job status
    response_data = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'http_status_code': status_code,
        'params_count': params_count,
        'enabled_params_count': len(enabled_params),
        'unique_tool_ids_count': len(unique_tool_ids),
        'tool_ids': unique_tool_ids,
        'prompt_created': prompt_created,
        'prompt_id': prompt_id,
        'params_updated': params_updated,
        'processed_param_ids': enabled_param_ids
    }
    
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='succeeded',
        finished_at=datetime.utcnow(),
        response_json=response_data
    )
    
    # Step 10: Return results
    result = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'http_status_code': status_code,
        'params_count': params_count,
        'enabled_params_count': len(enabled_params),
        'unique_tool_ids_count': len(unique_tool_ids),
        'tool_ids': unique_tool_ids,
        'prompt_created': prompt_created,
        'prompt_id': prompt_id,
        'params_updated': params_updated,
        'processed_param_ids': enabled_param_ids
    }
    
    logger.info(
        f"[PublishTools] ✅ Workflow completed: "
        f"agent_id={elevenlabs_agent_id}, unique_tool_ids={len(unique_tool_ids)}, params_updated={params_updated}"
    )
    
    return result
