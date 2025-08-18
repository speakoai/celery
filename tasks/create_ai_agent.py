from dotenv import load_dotenv
load_dotenv()

from tasks.celery_app import app
from celery.utils.log import get_task_logger
import os
import requests
import json
from datetime import datetime

"""
ElevenLabs Conversational AI Agent Management Tasks

This module provides Celery tasks for managing ElevenLabs conversational AI agents.
Core functionality: create AI agents from JSON configuration files.

Required Environment Variables:
    ELEVENLABS_API_KEY: Your ElevenLabs API key

Usage Examples:

    # Create a new AI agent from JSON file
    result = create_conversation_ai_agent.delay("agent.json")
    
    # Or use default agent.json file
    result = create_conversation_ai_agent.delay()

JSON File Format:
    The JSON file should contain the complete agent configuration that matches
    ElevenLabs API requirements. See agent.json for example structure.

API Documentation:
    https://elevenlabs.io/docs/api-reference/agents/create
"""

# Use Celery's task logger for consistent logging
logger = get_task_logger(__name__)

@app.task
def create_conversation_ai_agent(json_filename="agent.json"):
    """
    Create a conversational AI agent using configuration from a JSON file
    
    This function loads an agent configuration from a JSON file and creates
    a new conversational AI agent using the ElevenLabs API. The JSON file
    should contain the complete agent configuration.
    
    Args:
        json_filename (str): Name of the JSON configuration file (default: "agent.json")
                           The file should be located in the same directory as this script.
    
    Returns:
        dict: Response containing:
            - status (str): "success" or "error"
            - agent_id (str): ID of the created agent (if successful)
            - name (str): Name of the agent (extracted from JSON)
            - created_at (str): ISO timestamp of creation
            - full_response (dict): Complete API response
            - message (str): Error message (if failed)
            - details (str): Error details (if failed)
    
    Example:
        # Using default agent.json file
        result = create_conversation_ai_agent.delay()
        
        # Using custom JSON file
        result = create_conversation_ai_agent.delay("custom_agent.json")
    """
    logger.info(f"Creating AI agent from JSON file: {json_filename}")
    
    # ElevenLabs API configuration
    ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
    ELEVENLABS_BASE_URL = "https://api.elevenlabs.io/v1"
    
    if not ELEVENLABS_API_KEY:
        logger.error("ELEVENLABS_API_KEY not set in environment variables")
        return {"status": "error", "message": "ElevenLabs API key not configured"}
    
    # Get the directory of the current file and construct the JSON file path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(current_dir, json_filename)
    
    # Load and validate JSON file
    try:
        if not os.path.exists(json_file_path):
            logger.error(f"JSON file not found: {json_file_path}")
            return {
                "status": "error", 
                "message": f"JSON file not found: {json_filename}",
                "details": f"Expected path: {json_file_path}"
            }
        
        with open(json_file_path, 'r', encoding='utf-8') as file:
            agent_data = json.load(file)
        
        logger.info(f"Successfully loaded JSON file: {json_filename}")
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file {json_filename}: {str(e)}")
        return {
            "status": "error",
            "message": f"Invalid JSON format in {json_filename}",
            "details": str(e)
        }
    except Exception as e:
        logger.error(f"Error reading JSON file {json_filename}: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to read JSON file: {json_filename}",
            "details": str(e)
        }
    
    # Validate JSON structure
    if "conversation_config" not in agent_data:
        logger.error("JSON file missing required 'conversation_config' key")
        return {
            "status": "error",
            "message": "Invalid JSON structure",
            "details": "Missing required 'conversation_config' key in JSON file"
        }
    
    if not isinstance(agent_data["conversation_config"], dict):
        logger.error("'conversation_config' must be an object/dictionary")
        return {
            "status": "error",
            "message": "Invalid JSON structure",
            "details": "'conversation_config' must be an object/dictionary"
        }
    
    # Extract agent name for logging (optional)
    agent_name = agent_data.get("name", "Unknown Agent")
    logger.info(f"Creating agent: {agent_name}")
    
    # Prepare headers
    headers = {
        "Content-Type": "application/json",
        "xi-api-key": ELEVENLABS_API_KEY
    }
    
    try:
        # Create the conversational AI agent using the entire JSON structure
        url = f"{ELEVENLABS_BASE_URL}/convai/agents/create"
        response = requests.post(url, headers=headers, json=agent_data)
        
        if response.status_code in [200, 201]:
            result = response.json()
            agent_id = result.get("agent_id")
            
            logger.info(f"Successfully created AI agent: {agent_name} with ID: {agent_id}")
            
            return {
                "status": "success",
                "agent_id": agent_id,
                "name": agent_name,
                "created_at": datetime.now().isoformat(),
                "full_response": result,
                "source_file": json_filename
            }
        else:
            logger.error(f"Failed to create AI agent. Status: {response.status_code}, Response: {response.text}")
            return {
                "status": "error",
                "message": f"ElevenLabs API error: {response.status_code}",
                "details": response.text
            }
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error while creating AI agent: {str(e)}")
        return {
            "status": "error",
            "message": "Network error occurred",
            "details": str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error while creating AI agent: {str(e)}")
        return {
            "status": "error",
            "message": "Unexpected error occurred",
            "details": str(e)
        }


