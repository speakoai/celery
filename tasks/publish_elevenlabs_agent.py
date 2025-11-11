"""
Publish ElevenLabs AI Agent Task

This task handles the publishing of ElevenLabs AI agents.
Currently a placeholder implementation with full task tracking for workflow testing.
"""

import os
import time
from celery.utils.log import get_task_logger
from tasks.celery_app import app
from .utils.task_db import mark_task_running, mark_task_succeeded, mark_task_failed, upsert_tenant_integration_param
from .utils.publish_helpers import publish_knowledge, publish_greetings, publish_voice_dict, publish_personality
from .utils.publish_db import get_publish_job

logger = get_task_logger(__name__)


@app.task(bind=True, name='tasks.publish_elevenlabs_agent')
def publish_elevenlabs_agent(
    self,
    tenant_id: str,
    location_id: str,
    publish_job_id: str,
    speako_task_id: str = None,
    tenant_integration_param: dict = None
):
    """
    Publish ElevenLabs AI agent.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        publish_job_id: Publish job identifier
        speako_task_id: Optional Speako task ID for correlation
        tenant_integration_param: Optional integration metadata
    
    Returns:
        dict: Success response with received parameters
    """
    celery_task_id = self.request.id
    
    logger.info(
        f"[publish_elevenlabs_agent] Started - "
        f"tenant_id={tenant_id}, location_id={location_id}, "
        f"publish_job_id={publish_job_id}, speako_task_id={speako_task_id}, "
        f"celery_task_id={celery_task_id}"
    )
    
    try:
        # Mark task as running in database
        if speako_task_id:
            mark_task_running(task_id=speako_task_id, celery_task_id=celery_task_id)
            logger.info(f"[publish_elevenlabs_agent] Marked task as running: {speako_task_id}")
        
        # Store tenant integration parameters if provided
        if tenant_integration_param and speako_task_id:
            upsert_tenant_integration_param(tenant_integration_param=tenant_integration_param)
            logger.info(f"[publish_elevenlabs_agent] Stored tenant integration params for task: {speako_task_id}")
        
        # Get publish job to determine job type
        publish_job = get_publish_job(tenant_id, publish_job_id)
        
        if not publish_job:
            raise ValueError(f"Publish job not found: tenant_id={tenant_id}, publish_job_id={publish_job_id}")
        
        job_type = publish_job.get('job_type', 'knowledges')  # Default to 'knowledges'
        logger.info(f"[publish_elevenlabs_agent] Detected job_type: '{job_type}'")
        
        # Branch based on job_type
        if job_type == 'knowledges':
            # EXISTING WORKFLOW - Knowledge publishing
            logger.info(
                f"[publish_elevenlabs_agent] Starting knowledge publishing workflow - "
                f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
            )
            
            publish_result = publish_knowledge(
                tenant_id=tenant_id,
                location_id=location_id,
                publish_job_id=publish_job_id
            )
            
            # Log the ElevenLabs agent ID prominently for cross-checking
            elevenlabs_agent_id = publish_result.get('elevenlabs_agent_id')
            logger.info("=" * 80)
            logger.info(f"ELEVENLABS AGENT ID: {elevenlabs_agent_id}")
            logger.info(f"NEW KNOWLEDGE ID: {publish_result.get('new_knowledge_id')}")
            logger.info(f"KNOWLEDGE COUNT: {publish_result.get('knowledge_count')}")
            logger.info("=" * 80)
            
        elif job_type == 'greetings':
            # REAL WORKFLOW - Greetings publishing
            logger.info(
                f"[publish_elevenlabs_agent] Starting greetings publishing workflow - "
                f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
            )
            
            publish_result = publish_greetings(
                tenant_id=tenant_id,
                location_id=location_id,
                publish_job_id=publish_job_id
            )
            
            # Log the results prominently
            logger.info("=" * 80)
            logger.info(f"PROMPTS CREATED: {publish_result.get('prompts_created')}")
            logger.info(f"TOTAL ENTRIES: {publish_result.get('total_entries')}")
            logger.info(f"PROCESSED PARAM IDS: {publish_result.get('processed_param_ids')}")
            logger.info("=" * 80)
            
        elif job_type == 'voice-dict':
            # REAL WORKFLOW - Voice dictionary publishing
            logger.info(
                f"[publish_elevenlabs_agent] Starting voice-dict publishing workflow - "
                f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
            )
            
            publish_result = publish_voice_dict(
                tenant_id=tenant_id,
                location_id=location_id,
                publish_job_id=publish_job_id
            )
            
            # Log the results prominently
            logger.info("=" * 80)
            logger.info(f"ELEVENLABS AGENT ID: {publish_result.get('elevenlabs_agent_id')}")
            logger.info(f"HTTP STATUS: {publish_result.get('http_status_code')}")
            logger.info(f"PARAMS COUNT: {publish_result.get('params_count')}")
            logger.info(f"PARAMS UPDATED: {publish_result.get('params_updated')}")
            logger.info(f"DICTIONARY PROCESSED: {publish_result.get('dictionary_processed')}")
            logger.info(f"DICTIONARY CREATED: {publish_result.get('dictionary_created')}")
            logger.info(f"DICTIONARY UPDATED: {publish_result.get('dictionary_updated')}")
            logger.info(f"DICTIONARY ID: {publish_result.get('dictionary_id')}")
            logger.info("=" * 80)
            
        elif job_type == 'personality':
            # REAL WORKFLOW - Personality publishing
            logger.info(
                f"[publish_elevenlabs_agent] Starting personality publishing workflow - "
                f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
            )
            
            publish_result = publish_personality(
                tenant_id=tenant_id,
                location_id=location_id,
                publish_job_id=publish_job_id
            )
            
            # Log the results prominently
            logger.info("=" * 80)
            logger.info(f"PROMPT CREATED: {publish_result.get('prompt_created')}")
            logger.info(f"PROMPT ID: {publish_result.get('prompt_id')}")
            logger.info(f"CUSTOM INSTRUCTION CREATED: {publish_result.get('custom_instruction_created')}")
            logger.info(f"CUSTOM INSTRUCTION PROMPT ID: {publish_result.get('custom_instruction_prompt_id')}")
            logger.info(f"TEMPERATURE UPDATED: {publish_result.get('temperature_updated')}")
            logger.info(f"TEMPERATURE VALUE: {publish_result.get('temperature_value')}")
            logger.info(f"PARAMS UPDATED: {publish_result.get('params_updated')}")
            logger.info(f"PROCESSED PARAM IDS: {publish_result.get('processed_param_ids')}")
            logger.info("=" * 80)
            
        elif job_type in ['tools', 'full-agent']:
            # PLACEHOLDER for other job types
            logger.info(f"[publish_elevenlabs_agent] Job type '{job_type}' - executing PLACEHOLDER workflow")
            logger.info(f"[publish_elevenlabs_agent] ⏳ Simulating work for 10 seconds...")
            
            time.sleep(10)
            
            logger.info(f"[publish_elevenlabs_agent] ✅ Placeholder workflow completed for job_type: '{job_type}'")
            
            # Build a generic success result
            publish_result = {
                'job_type': job_type,
                'status': 'completed',
                'message': f'Placeholder workflow completed for {job_type}',
                'simulated': True,
                'duration_seconds': 10
            }
            
        else:
            raise ValueError(f"Unsupported job_type: '{job_type}'. Valid types: knowledges, greetings, voice-dict, personality, tools, full-agent")
        
        # Prepare success response based on job_type
        result = {
            'success': True,
            'message': f'ElevenLabs agent {job_type} completed successfully',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'publish_job_id': publish_job_id,
            'job_type': job_type,
            'celery_task_id': celery_task_id,
        }
        
        # Add job-specific fields
        if job_type == 'knowledges':
            result.update({
                'elevenlabs_agent_id': publish_result.get('elevenlabs_agent_id'),
                'new_knowledge_id': publish_result.get('new_knowledge_id'),
                'knowledge_count': publish_result.get('knowledge_count'),
                'r2_url': publish_result.get('r2_url'),
                'deleted_old_knowledge': publish_result.get('deleted_old_knowledge', [])
            })
        elif job_type == 'greetings':
            result.update({
                'prompts_created': publish_result.get('prompts_created'),
                'total_entries': publish_result.get('total_entries'),
                'processed_param_ids': publish_result.get('processed_param_ids')
            })
        elif job_type == 'voice-dict':
            result.update({
                'elevenlabs_agent_id': publish_result.get('elevenlabs_agent_id'),
                'http_status_code': publish_result.get('http_status_code'),
                'params_count': publish_result.get('params_count'),
                'params_updated': publish_result.get('params_updated'),
                'processed_param_ids': publish_result.get('processed_param_ids'),
                'dictionary_processed': publish_result.get('dictionary_processed'),
                'dictionary_created': publish_result.get('dictionary_created'),
                'dictionary_updated': publish_result.get('dictionary_updated'),
                'dictionary_id': publish_result.get('dictionary_id')
            })
        elif job_type == 'personality':
            result.update({
                'prompt_created': publish_result.get('prompt_created'),
                'prompt_id': publish_result.get('prompt_id'),
                'custom_instruction_created': publish_result.get('custom_instruction_created'),
                'custom_instruction_prompt_id': publish_result.get('custom_instruction_prompt_id'),
                'temperature_updated': publish_result.get('temperature_updated'),
                'temperature_value': publish_result.get('temperature_value'),
                'params_updated': publish_result.get('params_updated'),
                'processed_param_ids': publish_result.get('processed_param_ids')
            })
        else:
            # Placeholder job types
            result.update({
                'simulated': True,
                'placeholder_result': publish_result
            })
        
        # Include speako_task_id in response if provided
        if speako_task_id:
            result['speako_task_id'] = speako_task_id
        
        # Mark task as succeeded in database
        if speako_task_id:
            mark_task_succeeded(
                task_id=speako_task_id,
                celery_task_id=celery_task_id,
                details=result
            )
            logger.info(f"[publish_elevenlabs_agent] Marked task as succeeded: {speako_task_id}")
        
        logger.info(
            f"[publish_elevenlabs_agent] Completed successfully - "
            f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
        )
        
        return result
        
    except Exception as e:
        error_msg = f"Failed to publish ElevenLabs agent: {str(e)}"
        logger.error(
            f"[publish_elevenlabs_agent] Error - "
            f"tenant_id={tenant_id}, location_id={location_id}, "
            f"publish_job_id={publish_job_id}, error={error_msg}"
        )
        
        # Mark task as failed in database
        if speako_task_id:
            mark_task_failed(
                task_id=speako_task_id,
                celery_task_id=celery_task_id,
                error_message=error_msg
            )
            logger.info(f"[publish_elevenlabs_agent] Marked task as failed: {speako_task_id}")
        
        # Re-raise the exception so Celery marks it as failed
        raise
