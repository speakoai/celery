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
        error_msg = "ELEVENLABS_API_KEY environment variable is NOT SET - Cannot make API calls"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise RuntimeError(error_msg)
    
    logger.info(f"[ElevenLabs] ✓ API Key found (ending with: ...{ELEVENLABS_API_KEY[-8:]})")
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
    logger.info(f"[ElevenLabs] 📤 STARTING API CALL: POST {ELEVENLABS_BASE_URL}/knowledge-base/file")
    
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
    logger.info(f"[ElevenLabs] 🌐 Making POST request to: {url}")
    
    try:
        response = requests.post(
            url,
            headers=headers,
            files=files,
            data=data,
            timeout=60
        )
        
        logger.info(f"[ElevenLabs] ✓ Received response: HTTP {response.status_code}")
        
        response.raise_for_status()
    except requests.HTTPError as e:
        error_msg = f"ElevenLabs API error (HTTP {response.status_code}): {response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=response) from e
    except requests.RequestException as e:
        error_msg = f"Failed to upload to ElevenLabs: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
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
    logger.info(f"[ElevenLabs] ✅ API CALL SUCCESSFUL: Created knowledge ID: {knowledge_id}")
    
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
    logger.info(f"[ElevenLabs] 📤 STARTING API CALL: DELETE {ELEVENLABS_BASE_URL}/knowledge-base/{knowledge_id}")
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/knowledge-base/{knowledge_id}"
        headers = _get_headers()
        params = {'force': 'true'}  # Force delete even if used by agents
        
        logger.info(f"[ElevenLabs] 🌐 Making DELETE request to: {url}")
        
        response = requests.delete(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        logger.info(f"[ElevenLabs] ✓ Received response: HTTP {response.status_code}")
        
        response.raise_for_status()
        
        logger.info(f"[ElevenLabs] Successfully deleted knowledge: {knowledge_id}")
        logger.info(f"[ElevenLabs] ✅ API CALL SUCCESSFUL: Deleted knowledge ID: {knowledge_id}")
        return True
        
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            # Knowledge not found - consider it success (already deleted)
            logger.warning(
                f"[ElevenLabs] Knowledge not found (already deleted?): {knowledge_id}"
            )
            logger.info(f"[ElevenLabs] ✓ Knowledge already deleted (404): {knowledge_id}")
            return True
        else:
            logger.error(
                f"[ElevenLabs] ❌ API CALL FAILED - Failed to delete knowledge {knowledge_id}: "
                f"HTTP {e.response.status_code} - {e.response.text}"
            )
            return False
            
    except requests.RequestException as e:
        logger.error(
            f"[ElevenLabs] ❌ REQUEST FAILED - Error deleting knowledge {knowledge_id}: {str(e)}"
        )
        return False
    
    except Exception as e:
        logger.error(
            f"[ElevenLabs] ❌ UNEXPECTED ERROR - Error deleting knowledge {knowledge_id}: {str(e)}"
        )
        return False


def get_agent_config(agent_id: str) -> Dict[str, Any]:
    """
    Fetch the current configuration of an ElevenLabs agent.
    
    Args:
        agent_id: ElevenLabs agent ID
        
    Returns:
        Full agent configuration as a dictionary
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    logger.info(f"[ElevenLabs] Fetching agent configuration: agent_id={agent_id}")
    logger.info(f"[ElevenLabs] 📤 STARTING API CALL: GET {ELEVENLABS_BASE_URL}/agents/{agent_id}")
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/agents/{agent_id}"
        headers = _get_headers()
        
        logger.info(f"[ElevenLabs] 🌐 Making GET request to: {url}")
        
        response = requests.get(
            url,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"[ElevenLabs] ✓ Received response: HTTP {response.status_code}")
        
        response.raise_for_status()
        
        config = response.json()
        
        # Log current knowledge base for visibility
        current_knowledge_ids = []
        if 'conversation_config' in config:
            conv_config = config['conversation_config']
            if 'knowledge_base' in conv_config:
                current_knowledge_ids = [
                    kb.get('knowledge_id') 
                    for kb in conv_config.get('knowledge_base', [])
                    if kb.get('knowledge_id')
                ]
        
        logger.info(
            f"[ElevenLabs] Retrieved agent config: agent_id={agent_id}, "
            f"current_knowledge_count={len(current_knowledge_ids)}, "
            f"knowledge_ids={current_knowledge_ids}"
        )
        logger.info(f"[ElevenLabs] ✅ API CALL SUCCESSFUL: Retrieved agent config")
        
        return config
        
    except requests.HTTPError as e:
        error_msg = f"Failed to fetch agent {agent_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to fetch agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse agent config for {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e


def update_agent_knowledge(agent_id: str, knowledge_ids: list) -> Dict[str, Any]:
    """
    Update the knowledge base of an ElevenLabs agent.
    
    This function REPLACES the entire knowledge_base array with the provided IDs.
    Each knowledge entry uses usage_mode="auto" by default.
    
    Args:
        agent_id: ElevenLabs agent ID
        knowledge_ids: List of knowledge document IDs to set (replaces existing)
        
    Returns:
        Updated agent configuration
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    logger.info(
        f"[ElevenLabs] Updating agent knowledge: agent_id={agent_id}, "
        f"knowledge_count={len(knowledge_ids)}, knowledge_ids={knowledge_ids}"
    )
    logger.info(f"[ElevenLabs] 📤 STARTING API CALL: PATCH {ELEVENLABS_BASE_URL}/agents/{agent_id}")
    
    # Build knowledge_base array with usage_mode="auto"
    knowledge_base = [
        {
            "knowledge_id": kid,
            "usage_mode": "auto"
        }
        for kid in knowledge_ids
    ]
    
    # Prepare PATCH payload (only update conversation_config.knowledge_base)
    payload = {
        "conversation_config": {
            "knowledge_base": knowledge_base
        }
    }
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/agents/{agent_id}"
        headers = _get_headers()
        headers['Content-Type'] = 'application/json'
        
        logger.info(f"[ElevenLabs] 🌐 Making PATCH request to: {url}")
        logger.info(f"[ElevenLabs] Payload: {payload}")
        
        response = requests.patch(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        logger.info(f"[ElevenLabs] ✓ Received response: HTTP {response.status_code}")
        
        response.raise_for_status()
        
        updated_config = response.json()
        
        logger.info(
            f"[ElevenLabs] Successfully updated agent knowledge: agent_id={agent_id}, "
            f"new_knowledge_count={len(knowledge_ids)}"
        )
        logger.info(f"[ElevenLabs] ✅ API CALL SUCCESSFUL: Updated agent configuration")
        
        return updated_config
        
    except requests.HTTPError as e:
        error_msg = f"Failed to update agent {agent_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to update agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse update response for agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e
