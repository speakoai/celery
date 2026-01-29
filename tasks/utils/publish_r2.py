"""
R2 storage operations for knowledge file management.

This module handles knowledge text aggregation and upload to Cloudflare R2 storage.
"""

import os
import boto3
from datetime import datetime
from typing import List, Dict, Any, Tuple
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


# R2 Configuration from environment variables
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "https://assets.speako.ai")

# Dev R2 Configuration (for webhook fallback when agent is in dev database)
R2_BUCKET_NAME_DEV = os.getenv("R2_BUCKET_NAME_DEV")
R2_PUBLIC_BASE_URL_DEV = os.getenv("R2_PUBLIC_BASE_URL_DEV", "https://assets-dev.speako.ai")


def _get_r2_client():
    """Get configured boto3 S3 client for Cloudflare R2."""
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]):
        raise RuntimeError(
            "R2 storage not configured. Missing one or more environment variables: "
            "R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME"
        )
    
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    )


def aggregate_knowledge_markdown(knowledge_entries: List[Dict[str, Any]]) -> Tuple[str, str]:
    """
    Aggregate knowledge text entries into a single markdown file.
    
    Args:
        knowledge_entries: List of dicts with keys: param_id, value_text, param_code, created_at
    
    Returns:
        Tuple of (combined_text, filename)
        - combined_text: All value_text entries joined with markdown separators
        - filename: Generated filename in format knowledge_{location_id}_{timestamp}.md
    
    Note:
        Entries are separated with '\n\n---\n\n' (markdown horizontal rule)
        for clear visual separation in the knowledge base.
    """
    if not knowledge_entries:
        raise ValueError("Cannot aggregate empty knowledge entries list")
    
    logger.info(f"[publish_r2] Aggregating {len(knowledge_entries)} knowledge entries")
    
    # Extract value_text from each entry and join with markdown separator
    text_entries = [entry['value_text'] for entry in knowledge_entries]
    combined_text = '\n\n---\n\n'.join(text_entries)
    
    # Generate filename with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    
    # Extract location_id from first entry (all should have same location_id)
    # If param_code contains location info, we could use it, but we'll keep it simple
    # The location_id will be passed separately in the upload function
    filename = f"knowledge_combined_{timestamp}.md"
    
    logger.info(
        f"[publish_r2] Aggregated knowledge: filename={filename}, "
        f"total_length={len(combined_text)} chars"
    )
    
    return (combined_text, filename)


def upload_knowledge_to_r2(
    tenant_id: str,
    location_id: str,
    filename: str,
    content: str
) -> Tuple[str, str]:
    """
    Upload knowledge markdown file to Cloudflare R2 storage.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        filename: Name of the file to create
        content: Markdown content to upload
    
    Returns:
        Tuple of (r2_key, public_url)
        - r2_key: Full path/key in the R2 bucket
        - public_url: Public URL to access the file
    
    Upload path structure: {tenant_id}/{location_id}/knowledges/publish/{filename}
    """
    logger.info(
        f"[publish_r2] Uploading to R2: tenant_id={tenant_id}, location_id={location_id}, "
        f"filename={filename}, content_size={len(content)} bytes"
    )
    
    r2_client = _get_r2_client()
    
    # Construct R2 key (path in bucket)
    r2_key = f"{tenant_id}/{location_id}/knowledges/publish/{filename}"
    
    # Prepare metadata
    metadata = {
        'tenant_id': str(tenant_id),
        'location_id': str(location_id),
        'upload_timestamp': datetime.utcnow().isoformat() + 'Z',
        'content_type': 'text/markdown',
        'group': 'knowledge_publish'
    }
    
    # Upload to R2
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=r2_key,
        Body=content.encode('utf-8'),
        ContentType='text/markdown',
        Metadata=metadata
    )
    
    # Construct public URL
    public_url = f"{R2_PUBLIC_BASE_URL}/{r2_key}"
    
    logger.info(
        f"[publish_r2] Successfully uploaded to R2: key={r2_key}, url={public_url}"
    )
    
    return (r2_key, public_url)


def upload_audio_to_r2(
    tenant_id: str,
    location_id: str,
    conversation_id: str,
    audio_bytes: bytes,
    content_type: str = 'audio/mpeg',
    use_dev: bool = False
) -> Tuple[str, str]:
    """
    Upload conversation audio file to Cloudflare R2 storage.

    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        conversation_id: ElevenLabs conversation ID
        audio_bytes: Raw audio file bytes
        content_type: MIME type of audio file (default: 'audio/mpeg')
        use_dev: If True, upload to dev R2 bucket instead of production

    Returns:
        Tuple of (r2_key, public_url)
        - r2_key: Full path/key in the R2 bucket
        - public_url: Public URL to access the audio file

    Upload path structure: {tenant_id}/{location_id}/conversations/{conversation_id}.{ext}
    """
    # Select bucket and base URL based on environment
    if use_dev:
        bucket_name = R2_BUCKET_NAME_DEV
        public_base_url = R2_PUBLIC_BASE_URL_DEV
        env_label = "DEV"
    else:
        bucket_name = R2_BUCKET_NAME
        public_base_url = R2_PUBLIC_BASE_URL
        env_label = "PROD"

    if not bucket_name:
        raise RuntimeError(f"R2 bucket name not configured for {env_label} environment")

    logger.info(
        f"[publish_r2] Uploading audio to R2 ({env_label}): tenant_id={tenant_id}, location_id={location_id}, "
        f"conversation_id={conversation_id}, audio_size={len(audio_bytes)} bytes, type={content_type}"
    )

    r2_client = _get_r2_client()

    # Determine file extension from content type
    extension_map = {
        'audio/mpeg': 'mp3',
        'audio/mp3': 'mp3',
        'audio/wav': 'wav',
        'audio/x-wav': 'wav',
        'audio/wave': 'wav',
        'audio/webm': 'webm',
        'audio/ogg': 'ogg',
    }
    extension = extension_map.get(content_type, 'mp3')  # Default to mp3

    # Construct R2 key (path in bucket)
    r2_key = f"{tenant_id}/{location_id}/conversations/{conversation_id}.{extension}"

    # Prepare metadata
    metadata = {
        'tenant_id': str(tenant_id),
        'location_id': str(location_id),
        'conversation_id': conversation_id,
        'upload_timestamp': datetime.utcnow().isoformat() + 'Z',
        'content_type': content_type,
        'group': 'conversation_audio',
        'environment': env_label.lower()
    }

    # Upload to R2
    r2_client.put_object(
        Bucket=bucket_name,
        Key=r2_key,
        Body=audio_bytes,
        ContentType=content_type,
        Metadata=metadata
    )

    # Construct public URL
    public_url = f"{public_base_url}/{r2_key}"

    logger.info(
        f"[publish_r2] Successfully uploaded audio to R2 ({env_label}): key={r2_key}, url={public_url}"
    )

    return (r2_key, public_url)
