"""
Publish ElevenLabs AI Agent Task

This task handles the publishing of ElevenLabs AI agents.
Currently a placeholder implementation with full task tracking for workflow testing.
"""

import os
from celery.utils.log import get_task_logger
from tasks.celery_app import app
from .utils.task_db import mark_task_running, mark_task_failed, mark_task_succeeded, upsert_tenant_integration_param

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
            mark_task_running(speako_task_id, celery_task_id)
            logger.info(f"[publish_elevenlabs_agent] Marked task as running: {speako_task_id}")
        
        # Store tenant integration parameters if provided
        if tenant_integration_param and speako_task_id:
            upsert_tenant_integration_param(speako_task_id, tenant_integration_param)
            logger.info(f"[publish_elevenlabs_agent] Stored tenant integration params for task: {speako_task_id}")
        
        # TODO: Implement actual ElevenLabs agent publishing logic here
        # For now, just log the parameters and simulate success
        logger.info(
            f"[publish_elevenlabs_agent] Processing publish job - "
            f"tenant_id={tenant_id}, location_id={location_id}, publish_job_id={publish_job_id}"
        )
        
        # Prepare success response
        result = {
            'success': True,
            'message': 'ElevenLabs agent publish completed successfully',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'publish_job_id': publish_job_id,
            'celery_task_id': celery_task_id,
        }
        
        # Include speako_task_id in response if provided
        if speako_task_id:
            result['speako_task_id'] = speako_task_id
        
        # Mark task as succeeded in database
        if speako_task_id:
            mark_task_succeeded(
                speako_task_id=speako_task_id,
                result_data=result
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
                speako_task_id=speako_task_id,
                error_message=error_msg
            )
            logger.info(f"[publish_elevenlabs_agent] Marked task as failed: {speako_task_id}")
        
        # Re-raise the exception so Celery marks it as failed
        raise
