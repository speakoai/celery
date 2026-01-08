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
        error_msg = "AI Voice Service API key not configured - Cannot make API calls"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise RuntimeError(error_msg)
    
    logger.info(f"[ElevenLabs] ✓ API Key found (ending with: ...{ELEVENLABS_API_KEY[-8:]})")
    return {
        "xi-api-key": ELEVENLABS_API_KEY
    }


def upload_knowledge_file(file_url: str, name: str) -> tuple[str, str]:
    """
    Upload knowledge file to ElevenLabs from a public URL.
    
    This function downloads the file from the provided URL and uploads it
    to ElevenLabs knowledge base using multipart/form-data.
    
    Args:
        file_url: Public URL of the knowledge file (typically from R2)
        name: Human-readable name for the knowledge document
        
    Returns:
        Tuple of (knowledge_id, knowledge_name) from ElevenLabs response
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response doesn't contain 'id'
        RuntimeError: If file download fails
    """
    logger.info(f"[ElevenLabs] 📤 Uploading knowledge file from URL: {file_url}")
    
    # Step 1: Download file from URL
    try:
        download_response = requests.get(file_url, timeout=30)
        download_response.raise_for_status()
        file_content = download_response.content
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
            'text/plain'
        )
    }
    
    data = {
        'name': name
    }
    
    # Step 3: Upload to ElevenLabs
    url = f"{ELEVENLABS_BASE_URL}/knowledge-base/file"
    headers = _get_headers()
    
    logger.info(f"[ElevenLabs] 🌐 Uploading to ElevenLabs: name='{name}'")
    
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
        error_msg = f"AI Voice Service API error (HTTP {response.status_code}): {response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=response) from e
    except requests.RequestException as e:
        error_msg = f"Failed to upload to AI Voice Service: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
    
    # Step 4: Extract knowledge_id and name from response
    try:
        result = response.json()
    except Exception as e:
        error_msg = f"Failed to parse AI Voice Service response: {response.text}"
        logger.error(f"[ElevenLabs] {error_msg}")
        raise ValueError(error_msg) from e
    
    knowledge_id = result.get('id')
    knowledge_name = result.get('name')
    
    if not knowledge_id:
        error_msg = f"No 'id' in AI Voice Service response: {result}"
        logger.error(f"[ElevenLabs] {error_msg}")
        raise ValueError(error_msg)
    
    logger.info(f"[ElevenLabs] ✅ Created knowledge: id={knowledge_id}, name={knowledge_name}")
    
    return knowledge_id, knowledge_name


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
        
        logger.info(f"[ElevenLabs] ✓ Deleted knowledge: {knowledge_id}")
        return True
        
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            # Knowledge not found - consider it success (already deleted)
            logger.info(f"[ElevenLabs] ✓ Knowledge already deleted (404): {knowledge_id}")
            return True
        else:
            logger.warning(
                f"[ElevenLabs] Failed to delete knowledge {knowledge_id}: "
                f"HTTP {e.response.status_code}"
            )
            return False
            
    except Exception as e:
        logger.warning(f"[ElevenLabs] Error deleting knowledge {knowledge_id}: {str(e)}")
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
    try:
        url = f"{ELEVENLABS_BASE_URL}/agents/{agent_id}"
        headers = _get_headers()
        
        response = requests.get(
            url,
            headers=headers,
            timeout=30
        )
        
        response.raise_for_status()
        
        config = response.json()
        
        # Log current knowledge base count for visibility
        current_knowledge_ids = []
        if 'conversation_config' in config:
            conv_config = config['conversation_config']
            if 'knowledge_base' in conv_config:
                current_knowledge_ids = [
                    kb.get('knowledge_id') 
                    for kb in conv_config.get('knowledge_base', [])
                    if kb.get('knowledge_id')
                ]
        
        logger.info(f"[ElevenLabs] Current agent knowledge: {len(current_knowledge_ids)} documents")
        
        return config
        
    except requests.HTTPError as e:
        error_msg = f"Failed to fetch AI Voice Service agent {agent_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to fetch AI Voice Service agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse AI Voice Service agent config for {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise ValueError(error_msg) from e


def update_agent_knowledge(agent_id: str, knowledge_items: list) -> Dict[str, Any]:
    """
    Update the knowledge base of an ElevenLabs agent.
    
    This function REPLACES the entire knowledge_base array with the provided items.
    Each knowledge entry must include id, name, type, and usage_mode.
    
    Args:
        agent_id: ElevenLabs agent ID
        knowledge_items: List of dicts with keys: 'id', 'name', 'type', 'usage_mode'
                        Example: [{"id": "kb_123", "name": "FAQ", "type": "file", "usage_mode": "auto"}]
        
    Returns:
        Updated agent configuration
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] UPDATING AGENT KNOWLEDGE:")
    logger.info(f"[ElevenLabs]   Agent ID: {agent_id}")
    logger.info(f"[ElevenLabs]   New Knowledge Count: {len(knowledge_items)}")
    logger.info(f"[ElevenLabs]   New Knowledge Items: {knowledge_items}")
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] 📤 STARTING API CALL: PATCH {ELEVENLABS_BASE_URL}/agents/{agent_id}")
    
    # Build knowledge_base array with proper structure
    knowledge_base = [
        {
            "type": item.get("type", "file"),  # Default to "file" if not specified
            "name": item["name"],
            "id": item["id"],
            "usage_mode": item.get("usage_mode", "auto")  # Default to "auto"
        }
        for item in knowledge_items
    ]
    
    logger.info(f"[ElevenLabs] Built knowledge_base payload: {knowledge_base}")
    
    # Prepare PATCH payload with correct nesting: conversation_config.agent.prompt.knowledge_base
    payload = {
        "conversation_config": {
            "agent": {
                "prompt": {
                    "knowledge_base": knowledge_base
                }
            }
        }
    }
    
    logger.info(f"[ElevenLabs] Full PATCH payload: {payload}")
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/agents/{agent_id}"
        headers = _get_headers()
        headers['Content-Type'] = 'application/json'
        
        # Log detailed raw request
        import json
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📤 RAW REQUEST:")
        logger.info(f"[ElevenLabs]   Method: PATCH")
        logger.info(f"[ElevenLabs]   URL: {url}")
        logger.info(f"[ElevenLabs]   Headers: {dict(headers)}")
        logger.info(f"[ElevenLabs]   Request Payload (JSON):")
        logger.info(json.dumps(payload, indent=2))
        logger.info("=" * 80)
        
        response = requests.patch(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # Log detailed raw response
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📥 RAW RESPONSE:")
        logger.info(f"[ElevenLabs]   Status Code: {response.status_code}")
        logger.info(f"[ElevenLabs]   Response Headers: {dict(response.headers)}")
        logger.info(f"[ElevenLabs]   Response Body (Full):")
        logger.info(response.text)
        logger.info("=" * 80)
        
        response.raise_for_status()
        
        updated_config = response.json()
        
        # Extract knowledge base from correct location: conversation_config.agent.prompt.knowledge_base
        updated_knowledge_ids = []
        if 'conversation_config' in updated_config:
            conv_config = updated_config['conversation_config']
            if 'agent' in conv_config:
                agent_config = conv_config['agent']
                if 'prompt' in agent_config:
                    prompt_config = agent_config['prompt']
                    if 'knowledge_base' in prompt_config:
                        knowledge_base = prompt_config['knowledge_base']
                        # Extract 'id' field (not 'knowledge_id')
                        updated_knowledge_ids = [
                            kb.get('id') 
                            for kb in knowledge_base
                            if kb.get('id')
                        ]
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] AGENT UPDATE SUCCESSFUL!")
        logger.info(f"[ElevenLabs]   Agent ID: {agent_id}")
        logger.info(f"[ElevenLabs]   Updated Knowledge Count: {len(updated_knowledge_ids)}")
        logger.info(f"[ElevenLabs]   Updated Knowledge IDs: {updated_knowledge_ids}")
        
        # Log full knowledge_base for debugging
        try:
            kb_full = updated_config['conversation_config']['agent']['prompt']['knowledge_base']
            logger.info(f"[ElevenLabs]   Full Updated Knowledge Base: {kb_full}")
        except KeyError:
            logger.warning(f"[ElevenLabs]   Could not extract full knowledge_base from response")
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] ✅ API CALL SUCCESSFUL: Updated agent configuration")
        
        return updated_config
        
    except requests.HTTPError as e:
        error_msg = f"Failed to update AI Voice Service agent {agent_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to update AI Voice Service agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse AI Voice Service update response for agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e


def patch_elevenlabs_agent(agent_id: str, conversation_config: Dict[str, Any]) -> tuple[int, Dict[str, Any]]:
    """
    PATCH conversation_config to an ElevenLabs agent.
    
    This function sends a PATCH request to update the agent's conversation configuration.
    Used for updating voice settings, turn settings, and conversation parameters.
    
    Args:
        agent_id: ElevenLabs agent ID
        conversation_config: Dictionary with conversation_config structure
                            Example: {"tts": {"voice_id": "...", "speed": 1.05}, "turn": {...}, "conversation": {...}}
        
    Returns:
        Tuple of (http_status_code, response_json)
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    import json
    
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] PATCHING AGENT CONVERSATION CONFIG:")
    logger.info(f"[ElevenLabs]   Agent ID: {agent_id}")
    logger.info(f"[ElevenLabs]   Conversation Config:")
    logger.info(json.dumps(conversation_config, indent=2))
    logger.info("=" * 80)
    
    # Prepare PATCH payload
    payload = {
        "conversation_config": conversation_config
    }
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/agents/{agent_id}"
        headers = _get_headers()
        headers['Content-Type'] = 'application/json'
        
        # Log detailed raw request
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📤 RAW REQUEST:")
        logger.info(f"[ElevenLabs]   Method: PATCH")
        logger.info(f"[ElevenLabs]   URL: {url}")
        logger.info(f"[ElevenLabs]   Headers: {dict(headers)}")
        logger.info(f"[ElevenLabs]   Request Payload (JSON):")
        logger.info(json.dumps(payload, indent=2))
        logger.info("=" * 80)
        
        response = requests.patch(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # Log detailed raw response
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📥 RAW RESPONSE:")
        logger.info(f"[ElevenLabs]   Status Code: {response.status_code}")
        logger.info(f"[ElevenLabs]   Response Headers: {dict(response.headers)}")
        logger.info(f"[ElevenLabs]   Response Body:")
        logger.info(response.text)
        logger.info("=" * 80)
        
        response.raise_for_status()
        
        response_json = response.json()
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] ✅ PATCH SUCCESSFUL!")
        logger.info(f"[ElevenLabs]   Agent ID: {agent_id}")
        logger.info(f"[ElevenLabs]   HTTP Status: {response.status_code}")
        logger.info("=" * 80)
        
        return response.status_code, response_json
        
    except requests.HTTPError as e:
        error_msg = f"Failed to PATCH AI Voice Service agent {agent_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to PATCH AI Voice Service agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse AI Voice Service PATCH response for agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e


def create_pronunciation_dictionary(rules: list, name: str) -> tuple[str, str]:
    """
    Create a pronunciation dictionary in ElevenLabs from rules.
    
    Args:
        rules: List of rule dicts with keys: 'string_to_replace', 'type', 'alias'
               Example: [{"string_to_replace": "tomato", "type": "alias", "alias": "tuh-MAH-to"}]
        name: Human-readable name for the dictionary (e.g., "{location_name} - {timestamp}")
        
    Returns:
        Tuple of (dictionary_id, version_id) from ElevenLabs response
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    import json
    
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] CREATING PRONUNCIATION DICTIONARY:")
    logger.info(f"[ElevenLabs]   Name: {name}")
    logger.info(f"[ElevenLabs]   Rules Count: {len(rules)}")
    logger.info("=" * 80)
    
    # Prepare payload
    payload = {
        "rules": rules,
        "name": name
    }
    
    try:
        url = "https://api.elevenlabs.io/v1/pronunciation-dictionaries/add-from-rules"
        headers = _get_headers()
        headers['Content-Type'] = 'application/json'
        
        # Log detailed raw request
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📤 RAW REQUEST:")
        logger.info(f"[ElevenLabs]   Method: POST")
        logger.info(f"[ElevenLabs]   URL: {url}")
        logger.info(f"[ElevenLabs]   Headers: {dict(headers)}")
        logger.info(f"[ElevenLabs]   Request Payload (JSON):")
        logger.info(json.dumps(payload, indent=2))
        logger.info("=" * 80)
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # Log detailed raw response
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📥 RAW RESPONSE:")
        logger.info(f"[ElevenLabs]   Status Code: {response.status_code}")
        logger.info(f"[ElevenLabs]   Response Headers: {dict(response.headers)}")
        logger.info(f"[ElevenLabs]   Response Body:")
        logger.info(response.text)
        logger.info("=" * 80)
        
        response.raise_for_status()
        
        response_json = response.json()
        
        dictionary_id = response_json.get('id')
        version_id = response_json.get('version_id')
        
        if not dictionary_id or not version_id:
            error_msg = f"Missing 'id' or 'version_id' in response: {response_json}"
            logger.error(f"[ElevenLabs] ❌ {error_msg}")
            raise ValueError(error_msg)
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] ✅ DICTIONARY CREATED SUCCESSFULLY!")
        logger.info(f"[ElevenLabs]   Dictionary ID: {dictionary_id}")
        logger.info(f"[ElevenLabs]   Version ID: {version_id}")
        logger.info("=" * 80)
        
        return dictionary_id, version_id
        
    except requests.HTTPError as e:
        error_msg = f"Failed to create pronunciation dictionary: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to create pronunciation dictionary: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse pronunciation dictionary creation response: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e


def update_pronunciation_dictionary(dictionary_id: str, rules: list) -> tuple[str, str]:
    """
    Update an existing pronunciation dictionary in ElevenLabs.
    
    DEPRECATED: This function uses PATCH which only updates metadata (name, archived).
    Use sync_pronunciation_dictionary_rules() instead for updating rules.
    
    Args:
        dictionary_id: ElevenLabs dictionary ID to update
        rules: List of rule dicts with keys: 'string_to_replace', 'type', 'alias'
               Example: [{"string_to_replace": "tomato", "type": "alias", "alias": "tuh-MAH-to"}]
        
    Returns:
        Tuple of (dictionary_id, latest_version_id) from ElevenLabs response
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    import json
    
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] UPDATING PRONUNCIATION DICTIONARY (DEPRECATED - metadata only):")
    logger.info(f"[ElevenLabs]   Dictionary ID: {dictionary_id}")
    logger.info(f"[ElevenLabs]   Rules Count: {len(rules)}")
    logger.info("=" * 80)
    
    # Prepare payload (NO NAME for update)
    payload = {
        "rules": rules
    }
    
    try:
        url = f"https://api.elevenlabs.io/v1/pronunciation-dictionaries/{dictionary_id}"
        headers = _get_headers()
        headers['Content-Type'] = 'application/json'
        
        # Log detailed raw request
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📤 RAW REQUEST:")
        logger.info(f"[ElevenLabs]   Method: PATCH")
        logger.info(f"[ElevenLabs]   URL: {url}")
        logger.info(f"[ElevenLabs]   Headers: {dict(headers)}")
        logger.info(f"[ElevenLabs]   Request Payload (JSON):")
        logger.info(json.dumps(payload, indent=2))
        logger.info("=" * 80)
        
        response = requests.patch(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        # Log detailed raw response
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📥 RAW RESPONSE:")
        logger.info(f"[ElevenLabs]   Status Code: {response.status_code}")
        logger.info(f"[ElevenLabs]   Response Headers: {dict(response.headers)}")
        logger.info(f"[ElevenLabs]   Response Body:")
        logger.info(response.text)
        logger.info("=" * 80)
        
        response.raise_for_status()
        
        response_json = response.json()
        
        returned_dict_id = response_json.get('id')
        latest_version_id = response_json.get('latest_version_id')
        
        if not returned_dict_id or not latest_version_id:
            error_msg = f"Missing 'id' or 'latest_version_id' in response: {response_json}"
            logger.error(f"[ElevenLabs] ❌ {error_msg}")
            raise ValueError(error_msg)
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] ✅ DICTIONARY UPDATED SUCCESSFULLY!")
        logger.info(f"[ElevenLabs]   Dictionary ID: {returned_dict_id}")
        logger.info(f"[ElevenLabs]   Latest Version ID: {latest_version_id}")
        logger.info("=" * 80)
        
        return returned_dict_id, latest_version_id
        
    except requests.HTTPError as e:
        error_msg = f"Failed to update pronunciation dictionary {dictionary_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to update pronunciation dictionary {dictionary_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse pronunciation dictionary update response: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e


def add_pronunciation_rules(dictionary_id: str, rules: list) -> tuple[str, str, int]:
    """
    Add rules to an existing pronunciation dictionary in ElevenLabs.
    
    Args:
        dictionary_id: ElevenLabs dictionary ID
        rules: List of rule dicts with keys: 'string_to_replace', 'type', 'alias'
               Example: [{"string_to_replace": "tomato", "type": "alias", "alias": "tuh-MAH-to"}]
        
    Returns:
        Tuple of (dictionary_id, version_id, version_rules_num) from ElevenLabs response
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    import json
    
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] ADDING RULES TO PRONUNCIATION DICTIONARY:")
    logger.info(f"[ElevenLabs]   Dictionary ID: {dictionary_id}")
    logger.info(f"[ElevenLabs]   Rules to Add: {len(rules)}")
    logger.info("=" * 80)
    
    payload = {"rules": rules}
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/pronunciation-dictionaries/{dictionary_id}/add-rules"
        headers = _get_headers()
        headers['Content-Type'] = 'application/json'
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📤 RAW REQUEST:")
        logger.info(f"[ElevenLabs]   Method: POST")
        logger.info(f"[ElevenLabs]   URL: {url}")
        logger.info(f"[ElevenLabs]   Request Payload (JSON):")
        logger.info(json.dumps(payload, indent=2))
        logger.info("=" * 80)
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📥 RAW RESPONSE:")
        logger.info(f"[ElevenLabs]   Status Code: {response.status_code}")
        logger.info(f"[ElevenLabs]   Response Body:")
        logger.info(response.text)
        logger.info("=" * 80)
        
        response.raise_for_status()
        
        response_json = response.json()
        
        returned_dict_id = response_json.get('id')
        version_id = response_json.get('version_id')
        version_rules_num = response_json.get('version_rules_num', 0)
        
        if not returned_dict_id or not version_id:
            error_msg = f"Missing 'id' or 'version_id' in response: {response_json}"
            logger.error(f"[ElevenLabs] ❌ {error_msg}")
            raise ValueError(error_msg)
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] ✅ RULES ADDED SUCCESSFULLY!")
        logger.info(f"[ElevenLabs]   Dictionary ID: {returned_dict_id}")
        logger.info(f"[ElevenLabs]   Version ID: {version_id}")
        logger.info(f"[ElevenLabs]   Total Rules: {version_rules_num}")
        logger.info("=" * 80)
        
        return returned_dict_id, version_id, version_rules_num
        
    except requests.HTTPError as e:
        error_msg = f"Failed to add rules to dictionary {dictionary_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to add rules to dictionary {dictionary_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse add-rules response: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e


def remove_pronunciation_rules(dictionary_id: str, rule_strings: list) -> tuple[str, str, int]:
    """
    Remove rules from an existing pronunciation dictionary in ElevenLabs.
    
    Args:
        dictionary_id: ElevenLabs dictionary ID
        rule_strings: List of 'string_to_replace' values to remove
                      Example: ["tomato", "schedule"]
        
    Returns:
        Tuple of (dictionary_id, version_id, version_rules_num) from ElevenLabs response
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    import json
    
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] REMOVING RULES FROM PRONUNCIATION DICTIONARY:")
    logger.info(f"[ElevenLabs]   Dictionary ID: {dictionary_id}")
    logger.info(f"[ElevenLabs]   Rule Strings to Remove: {rule_strings}")
    logger.info("=" * 80)
    
    payload = {"rule_strings": rule_strings}
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/pronunciation-dictionaries/{dictionary_id}/remove-rules"
        headers = _get_headers()
        headers['Content-Type'] = 'application/json'
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📤 RAW REQUEST:")
        logger.info(f"[ElevenLabs]   Method: POST")
        logger.info(f"[ElevenLabs]   URL: {url}")
        logger.info(f"[ElevenLabs]   Request Payload (JSON):")
        logger.info(json.dumps(payload, indent=2))
        logger.info("=" * 80)
        
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] 📥 RAW RESPONSE:")
        logger.info(f"[ElevenLabs]   Status Code: {response.status_code}")
        logger.info(f"[ElevenLabs]   Response Body:")
        logger.info(response.text)
        logger.info("=" * 80)
        
        response.raise_for_status()
        
        response_json = response.json()
        
        returned_dict_id = response_json.get('id')
        version_id = response_json.get('version_id')
        version_rules_num = response_json.get('version_rules_num', 0)
        
        if not returned_dict_id or not version_id:
            error_msg = f"Missing 'id' or 'version_id' in response: {response_json}"
            logger.error(f"[ElevenLabs] ❌ {error_msg}")
            raise ValueError(error_msg)
        
        logger.info("=" * 80)
        logger.info(f"[ElevenLabs] ✅ RULES REMOVED SUCCESSFULLY!")
        logger.info(f"[ElevenLabs]   Dictionary ID: {returned_dict_id}")
        logger.info(f"[ElevenLabs]   Version ID: {version_id}")
        logger.info(f"[ElevenLabs]   Remaining Rules: {version_rules_num}")
        logger.info("=" * 80)
        
        return returned_dict_id, version_id, version_rules_num
        
    except requests.HTTPError as e:
        error_msg = f"Failed to remove rules from dictionary {dictionary_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ API CALL FAILED: {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to remove rules from dictionary {dictionary_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ REQUEST FAILED: {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse remove-rules response: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ PARSE ERROR: {error_msg}")
        raise ValueError(error_msg) from e


def sync_pronunciation_dictionary_rules(
    dictionary_id: str, 
    rules: list, 
    old_rules: list
) -> tuple[str, str, int, dict]:
    """
    Sync pronunciation dictionary rules by comparing new rules vs old rules
    and calling add-rules/remove-rules as needed.
    
    Args:
        dictionary_id: ElevenLabs dictionary ID
        rules: List of new rule dicts (current desired state)
        old_rules: List of old rule dicts (previous state)
        
    Returns:
        Tuple of (dictionary_id, version_id, version_rules_num, sync_info)
        sync_info contains: {'added': [...], 'removed': [...], 'unchanged': [...]}
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] SYNCING PRONUNCIATION DICTIONARY RULES:")
    logger.info(f"[ElevenLabs]   Dictionary ID: {dictionary_id}")
    logger.info(f"[ElevenLabs]   New Rules Count: {len(rules)}")
    logger.info(f"[ElevenLabs]   Old Rules Count: {len(old_rules)}")
    logger.info("=" * 80)
    
    # Build sets of string_to_replace for comparison
    new_strings = {r.get('string_to_replace') for r in rules if r.get('string_to_replace')}
    old_strings = {r.get('string_to_replace') for r in old_rules if r.get('string_to_replace')}
    
    # Determine what to add/remove
    to_remove = old_strings - new_strings
    to_add = new_strings - old_strings
    unchanged = new_strings & old_strings
    
    logger.info(f"[ElevenLabs]   To Remove: {to_remove}")
    logger.info(f"[ElevenLabs]   To Add: {to_add}")
    logger.info(f"[ElevenLabs]   Unchanged: {unchanged}")
    
    version_id = None
    version_rules_num = 0
    returned_dict_id = dictionary_id
    
    # Step 1: Remove rules first (if any)
    if to_remove:
        logger.info(f"[ElevenLabs] Removing {len(to_remove)} rules...")
        returned_dict_id, version_id, version_rules_num = remove_pronunciation_rules(
            dictionary_id, 
            list(to_remove)
        )
    
    # Step 2: Add rules (if any)
    if to_add:
        rules_to_add = [r for r in rules if r.get('string_to_replace') in to_add]
        logger.info(f"[ElevenLabs] Adding {len(rules_to_add)} rules...")
        returned_dict_id, version_id, version_rules_num = add_pronunciation_rules(
            dictionary_id, 
            rules_to_add
        )
    
    # If nothing changed, we need to get the current version_id
    # For now, we'll just return None and let the caller handle it
    if not to_remove and not to_add:
        logger.info("[ElevenLabs] No changes needed - rules are identical")
        # Return None for version_id to indicate no change
        version_id = None
    
    sync_info = {
        'added': list(to_add),
        'removed': list(to_remove),
        'unchanged': list(unchanged)
    }
    
    logger.info("=" * 80)
    logger.info(f"[ElevenLabs] ✅ DICTIONARY SYNC COMPLETE!")
    logger.info(f"[ElevenLabs]   Dictionary ID: {returned_dict_id}")
    logger.info(f"[ElevenLabs]   Version ID: {version_id}")
    logger.info(f"[ElevenLabs]   Total Rules: {version_rules_num}")
    logger.info(f"[ElevenLabs]   Sync Summary: +{len(to_add)} added, -{len(to_remove)} removed, ={len(unchanged)} unchanged")
    logger.info("=" * 80)
    
    return returned_dict_id, version_id, version_rules_num, sync_info


def list_conversations(agent_id: str) -> list:
    """
    List all conversations for a specific ElevenLabs agent.
    
    Args:
        agent_id: ElevenLabs agent ID
        
    Returns:
        List of conversation dictionaries with summary info
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    logger.info(f"[ElevenLabs] 📋 Listing conversations for agent: {agent_id}")
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/conversations"
        headers = _get_headers()
        params = {'agent_id': agent_id}
        
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        # API may return {'conversations': [...]} or just [...]
        if isinstance(result, dict) and 'conversations' in result:
            conversations = result['conversations']
        elif isinstance(result, list):
            conversations = result
        else:
            logger.warning(f"[ElevenLabs] Unexpected response format: {result}")
            conversations = []
        
        logger.info(f"[ElevenLabs] ✅ Found {len(conversations)} conversations for agent {agent_id}")
        
        return conversations
        
    except requests.HTTPError as e:
        error_msg = f"Failed to list conversations for agent {agent_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to list conversations for agent {agent_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse conversations list response: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise ValueError(error_msg) from e


def get_conversation_details(conversation_id: str) -> Dict[str, Any]:
    """
    Get full details of a specific conversation including transcript.
    
    Args:
        conversation_id: ElevenLabs conversation ID
        
    Returns:
        Full conversation details as a dictionary
        
    Raises:
        requests.HTTPError: If API call fails
        ValueError: If response cannot be parsed
    """
    logger.info(f"[ElevenLabs] 📄 Fetching conversation details: {conversation_id}")
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/conversations/{conversation_id}"
        headers = _get_headers()
        
        response = requests.get(
            url,
            headers=headers,
            timeout=30
        )
        
        response.raise_for_status()
        
        details = response.json()
        
        logger.info(f"[ElevenLabs] ✅ Retrieved conversation details for {conversation_id}")
        
        return details
        
    except requests.HTTPError as e:
        error_msg = f"Failed to get conversation {conversation_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to get conversation {conversation_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to parse conversation details response: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise ValueError(error_msg) from e


def get_conversation_audio(conversation_id: str) -> tuple[bytes, str]:
    """
    Download audio file for a conversation.
    
    Args:
        conversation_id: ElevenLabs conversation ID
        
    Returns:
        Tuple of (audio_bytes, content_type)
        - audio_bytes: Raw audio file bytes
        - content_type: MIME type from Content-Type header (e.g., 'audio/mpeg')
        
    Raises:
        requests.HTTPError: If API call fails
        RuntimeError: If download fails or returns no content
    """
    logger.info(f"[ElevenLabs] 🎵 Downloading audio for conversation: {conversation_id}")
    
    try:
        url = f"{ELEVENLABS_BASE_URL}/conversations/{conversation_id}/audio"
        headers = _get_headers()
        
        response = requests.get(
            url,
            headers=headers,
            timeout=60,  # Audio files may be large
            stream=True
        )
        
        response.raise_for_status()
        
        # Get content type for file extension detection
        content_type = response.headers.get('Content-Type', 'audio/mpeg')
        
        # Read audio bytes
        audio_bytes = response.content
        
        if not audio_bytes:
            error_msg = f"No audio content returned for conversation {conversation_id}"
            logger.error(f"[ElevenLabs] ❌ {error_msg}")
            raise RuntimeError(error_msg)
        
        logger.info(
            f"[ElevenLabs] ✅ Downloaded audio for {conversation_id}: "
            f"{len(audio_bytes)} bytes, type={content_type}"
        )
        
        return audio_bytes, content_type
        
    except requests.HTTPError as e:
        error_msg = f"Failed to download audio for conversation {conversation_id}: HTTP {e.response.status_code} - {e.response.text}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise requests.HTTPError(error_msg, response=e.response) from e
        
    except requests.RequestException as e:
        error_msg = f"Failed to download audio for conversation {conversation_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise
        
    except Exception as e:
        error_msg = f"Failed to process audio download for conversation {conversation_id}: {str(e)}"
        logger.error(f"[ElevenLabs] ❌ {error_msg}")
        raise RuntimeError(error_msg) from e
