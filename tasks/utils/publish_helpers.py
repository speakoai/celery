"""
Helper functions for ElevenLabs publishing workflows.

This module orchestrates the complete publishing workflow by coordinating
database operations, R2 storage, and ElevenLabs API calls.
"""

from typing import Dict, Any
from celery.utils.log import get_task_logger

from .publish_db import (
    get_publish_job,
    update_publish_job_status,
    get_elevenlabs_agent_id,
    collect_speako_knowledge,
    get_existing_elevenlabs_knowledge_ids,
    save_new_elevenlabs_knowledge_id,
    mark_speako_knowledge_published
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
    
    # Step 1: Validate publish job and get agent ID
    logger.info("[PublishKnowledge] Step 1: Validating publish job")
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
    
    elevenlabs_agent_id = get_elevenlabs_agent_id(tenant_id, location_id)
    if not elevenlabs_agent_id:
        raise ValueError(
            f"ElevenLabs agent ID not found: tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishKnowledge] Found agent ID: {elevenlabs_agent_id}")
    
    # Step 2: Collect knowledge from Speako
    logger.info("[PublishKnowledge] Step 2: Collecting Speako knowledge")
    knowledge_docs = collect_speako_knowledge(tenant_id, location_id)
    
    if not knowledge_docs:
        logger.warning("[PublishKnowledge] No knowledge documents found")
        raise ValueError(
            f"No knowledge found for tenant_id={tenant_id}, location_id={location_id}"
        )
    
    logger.info(f"[PublishKnowledge] Collected {len(knowledge_docs)} knowledge documents")
    
    # Step 3: Aggregate knowledge and upload to R2
    logger.info("[PublishKnowledge] Step 3: Aggregating and uploading to R2")
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
    logger.info(f"[PublishKnowledge] Updated publish_jobs table with knowledge_file_url")
    
    # Step 4: Upload to ElevenLabs
    logger.info("[PublishKnowledge] Step 4: Uploading to ElevenLabs")
    knowledge_name = f"Speako Knowledge - Tenant {tenant_id} Location {location_id}"
    
    try:
        new_knowledge_id = upload_knowledge_file(
            file_url=r2_url,
            name=knowledge_name
        )
        logger.info(f"[PublishKnowledge] Created ElevenLabs knowledge: {new_knowledge_id}")
    except Exception as e:
        logger.error(f"[PublishKnowledge] Failed to upload to ElevenLabs: {str(e)}")
        from datetime import datetime
        update_publish_job_status(
            tenant_id=tenant_id,
            publish_job_id=publish_job_id,
            status='failed',
            finished_at=datetime.utcnow(),
            error_message=str(e)
        )
        raise RuntimeError(f"Failed to upload knowledge to ElevenLabs: {str(e)}") from e
    
    # Step 5: Get existing knowledge IDs from database
    logger.info("[PublishKnowledge] Step 5: Fetching existing knowledge IDs")
    old_knowledge_ids = get_existing_elevenlabs_knowledge_ids(tenant_id, location_id)
    logger.info(f"[PublishKnowledge] Found {len(old_knowledge_ids)} existing knowledge IDs")
    
    # Step 6: Merge knowledge IDs (new + existing)
    logger.info("[PublishKnowledge] Step 6: Merging knowledge base")
    merged_knowledge_ids = [new_knowledge_id] + old_knowledge_ids
    
    logger.info(
        f"[PublishKnowledge] Merged knowledge: new={new_knowledge_id}, "
        f"old={old_knowledge_ids}, total={len(merged_knowledge_ids)}"
    )
    
    # Step 7: Update agent configuration
    logger.info("[PublishKnowledge] Step 7: Updating agent configuration")
    try:
        updated_config = update_agent_knowledge(
            agent_id=elevenlabs_agent_id,
            knowledge_ids=merged_knowledge_ids
        )
        logger.info(
            f"[PublishKnowledge] Agent updated successfully: agent_id={elevenlabs_agent_id}"
        )
    except Exception as e:
        logger.error(f"[PublishKnowledge] Failed to update agent: {str(e)}")
        from datetime import datetime
        update_publish_job_status(
            tenant_id=tenant_id,
            publish_job_id=publish_job_id,
            status='failed',
            finished_at=datetime.utcnow(),
            error_message=str(e)
        )
        raise RuntimeError(f"Failed to update agent configuration: {str(e)}") from e
    
    # Step 8: Save new knowledge ID to database
    logger.info("[PublishKnowledge] Step 8: Saving knowledge ID to database")
    save_new_elevenlabs_knowledge_id(
        tenant_id=tenant_id,
        location_id=location_id,
        elevenlabs_knowledge_id=new_knowledge_id
    )
    
    # Step 9: Mark knowledge documents as published
    logger.info("[PublishKnowledge] Step 9: Marking knowledge as published")
    knowledge_ids = [doc['id'] for doc in knowledge_docs]
    mark_speako_knowledge_published(knowledge_ids)
    
    # Step 10: Clean up old knowledge (best effort - don't fail if this errors)
    deleted_old_knowledge = []
    if old_knowledge_ids:
        logger.info(
            f"[PublishKnowledge] Step 10: Cleaning up {len(old_knowledge_ids)} old knowledge documents"
        )
        for old_id in old_knowledge_ids:
            try:
                if delete_knowledge(old_id):
                    deleted_old_knowledge.append(old_id)
                    logger.info(f"[PublishKnowledge] Deleted old knowledge: {old_id}")
                else:
                    logger.warning(f"[PublishKnowledge] Failed to delete old knowledge: {old_id}")
            except Exception as e:
                logger.warning(
                    f"[PublishKnowledge] Error deleting old knowledge {old_id}: {str(e)}"
                )
    else:
        logger.info("[PublishKnowledge] Step 10: No old knowledge to clean up")
    
    # Step 11: Mark publish job as completed
    logger.info("[PublishKnowledge] Step 11: Marking publish job as completed")
    from datetime import datetime
    
    # Prepare response JSON for database
    response_data = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'new_knowledge_id': new_knowledge_id,
        'merged_knowledge_ids': merged_knowledge_ids,
        'deleted_old_knowledge': deleted_old_knowledge,
        'knowledge_count': len(knowledge_docs)
    }
    
    update_publish_job_status(
        tenant_id=tenant_id,
        publish_job_id=publish_job_id,
        status='completed',
        finished_at=datetime.utcnow(),
        http_status_code=200,
        response_json=response_data
    )
    
    # Prepare result
    result = {
        'elevenlabs_agent_id': elevenlabs_agent_id,
        'new_knowledge_id': new_knowledge_id,
        'old_knowledge_ids': old_knowledge_ids,
        'merged_knowledge_ids': merged_knowledge_ids,
        'deleted_old_knowledge': deleted_old_knowledge,
        'r2_url': r2_url,
        'knowledge_count': len(knowledge_docs)
    }
    
    logger.info(
        f"[PublishKnowledge] Workflow completed successfully: "
        f"agent_id={elevenlabs_agent_id}, new_knowledge_id={new_knowledge_id}, "
        f"total_knowledge={len(merged_knowledge_ids)}"
    )
    
    return result
