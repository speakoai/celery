"""
ElevenLabs API client for agent and knowledge base management.

This module provides functions to interact with ElevenLabs API endpoints
for uploading, deleting, and managing knowledge base documents.
"""

import os
import requests
from typing import Dict, Any
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# ElevenLabs API Configuration
ELEVENLABS_BASE_URL = "https://api.elevenlabs.io/v1/convai"
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")


def _get_headers() -> Dict[str, str]:
    """Get headers for ElevenLabs API requests."""
    if not ELEVENLABS_API_KEY:
        raise RuntimeError("ELEVENLABS_API_KEY environment variable not set")
    
    return {
        "xi-api-key": ELEVENLABS_API_KEY
    }


def upload_knowledge_file(file_url: str, name: str) -> str:
    """
    Upload knowledge file to ElevenLabs from a public URL.
    
    This function downloads the file from the provided URL and uploads it
    to ElevenLabs knowledge base using multipart/form-data.
    
    Args:
        file_url: Public URL of the knowledge file (typically from R2)
        name: Human-readable name for the knowledge document
        
    Returns:
        knowledge_id: ElevenLabs knowledge document ID (e.g., "kb_abc123")
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response doesn't contain 'id'
        RuntimeError: If file download fails
    """
    logger.info(f"[ElevenLabs] Uploading knowledge from URL: {file_url}")
    
    # Step 1: Download file from URL
    try:
        download_response = requests.get(file_url, timeout=30)
        download_response.raise_for_status()
        file_content = download_response.content
        logger.info(f"[ElevenLabs] Downloaded file: {len(file_content)} bytes")
    except requests.RequestException as e:
        error_msg = f"Failed to download file from {file_url}: {str(e)}"
        logger.error(f"[ElevenLabs] {error_msg}")
        raise RuntimeError(error_msg) from e
    
    # Step 2: Prepare multipart form data
    # Extract filename from URL (use basename)
    filename = os.path.basename(file_url.split('?')[0])  # Remove query params if any
    if not filename or filename == '':
        filename = 'knowledge.md'
    
    files = {
        'file': (
            filename,
            file_content,
            'text/markdown'
        )
    }
    
    data = {
        'name': name
    }
    
    # Step 3: Upload to ElevenLabs
    url = f"{ELEVENLABS_BASE_URL}/knowledge-base/file"
    headers = _get_headers()
    
    logger.info(f"[ElevenLabs] Uploading to ElevenLabs: name='{name}', filename='{filename}'")
    
    try:
        response = requests.post(
            url,
            headers=headers,
            files=files,
            data=data,
            timeout=60
        )
        response.raise_for_status()
    except requests.HTTPError as e:
        error_msg = f"ElevenLabs API error (HTTP {response.status_code}): {response.text}"
        logger.error(f"[ElevenLabs] {error_msg}")
        raise requests.HTTPError(error_msg, response=response) from e
    except requests.RequestException as e:
        error_msg = f"Failed to upload to ElevenLabs: {str(e)}"
        logger.error(f"[ElevenLabs] {error_msg}")
        raise
    
    # Step 4: Extract knowledge_id from response
    try:
        result = response.json()
    except Exception as e:
        error_msg = f"Failed to parse ElevenLabs response: {response.text}"
        logger.error(f"[ElevenLabs] {error_msg}")
        raise ValueError(error_msg) from e
    
    knowledge_id = result.get('id')
    if not knowledge_id:
        error_msg = f"No 'id' in ElevenLabs response: {result}"
        logger.error(f"[ElevenLabs] {error_msg}")
        raise ValueError(error_msg)
    
    logger.info(
        f"[ElevenLabs] Successfully uploaded knowledge: "
        f"id={knowledge_id}, name='{result.get('name')}'"
    )
    
    return knowledge_id


def delete_knowledge(knowledge_id: str) -> bool:
    """
    Delete knowledge document from ElevenLabs.
    
    Uses force=true to delete even if the knowledge is currently used by agents.
    This is a best-effort operation - does not raise exceptions on failure.
    
    Args:
        knowledge_id: ElevenLabs knowledge document ID
        
    Returns:
        True if deletion was successful, False otherwise
        
    Note:
        - Does not raise exceptions - logs errors and returns False
        - Returns True if knowledge is already deleted (404 response)
        - Returns False for other errors
    """
    logger.info(f"[ElevenLabs] Deleting knowledge: id={knowledge_id}")
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/knowledge-base/{knowledge_id}"
        headers = _get_headers()
        params = {'force': 'true'}  # Force delete even if used by agents
        
        response = requests.delete(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        
        logger.info(f"[ElevenLabs] Successfully deleted knowledge: {knowledge_id}")
        return True
        
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            # Knowledge not found - consider it success (already deleted)
            logger.warning(
                f"[ElevenLabs] Knowledge not found (already deleted?): {knowledge_id}"
            )
            return True
        else:
            logger.error(
                f"[ElevenLabs] Failed to delete knowledge {knowledge_id}: "
                f"HTTP {e.response.status_code} - {e.response.text}"
            )
            return False
            
    except requests.RequestException as e:
        logger.error(
            f"[ElevenLabs] Error deleting knowledge {knowledge_id}: {str(e)}"
        )
        return False
    
    except Exception as e:
        logger.error(
            f"[ElevenLabs] Unexpected error deleting knowledge {knowledge_id}: {str(e)}"
        )
        return False
