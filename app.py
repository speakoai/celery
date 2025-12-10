import os
import secrets
import hmac
import base64
import psycopg2
from functools import wraps
from flask import Flask, flash, render_template, redirect, request, jsonify
from tasks.demo_task import add
from tasks.availability_gen_regen import gen_availability, gen_availability_venue
from tasks.sms import (
    send_sms_confirmation_new, send_sms_confirmation_mod, send_sms_confirmation_can,
    send_email_confirmation_new_rest, send_email_confirmation_new, 
    send_email_confirmation_mod_rest, send_email_confirmation_mod,
    send_email_confirmation_can_rest, send_email_confirmation_can,
    send_email_confirmation_customer_new, send_email_confirmation_customer_mod, send_email_confirmation_customer_can
)
from tasks.celery_app import app as celery_app
from tasks.analyze_knowledge import analyze_knowledge_file
from tasks.scrape_url import scrape_url_to_markdown
from tasks.sync_speako_data import sync_speako_data
from tasks.publish_elevenlabs_agent import publish_elevenlabs_agent
from tasks.create_ai_agent import create_conversation_ai_agent
# Additional imports for R2 uploads
import boto3
from werkzeug.utils import secure_filename
from datetime import datetime
import hashlib
import time
# Imports for webhook processing
from tasks.utils.elevenlabs_client import get_conversation_details
from tasks.utils.publish_r2 import upload_audio_to_r2
from zoneinfo import ZoneInfo
# OpenAI SDK (optional)
try:
    from openai import OpenAI
    _openai_import_error = None
except Exception as _openai_e:
    OpenAI = None
    _openai_import_error = repr(_openai_e)
# New: helpers for file_url mode
import mimetypes
from urllib.parse import urlparse

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

# --- Startup debug for OpenAI config (does NOT print secrets) ---
try:
    _openai_client_available = OpenAI is not None
    _openai_key_present = bool(os.getenv('OPENAI_API_KEY'))
    _openai_model = os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini')
    # Try to detect SDK version even if OpenAI class import fails
    try:
        import openai as _openai_mod
        _openai_version = getattr(_openai_mod, '__version__', 'unknown')
    except Exception:
        _openai_version = None
    # Use warning level so it always shows in Render logs
    app.logger.warning(
        f"[Startup] OpenAI client available: {_openai_client_available}; OPENAI_API_KEY set: {_openai_key_present}; model: {_openai_model}; sdk_version: {_openai_version}; import_error: {_openai_import_error}"
    )
except Exception as _e:
    print(f"[Startup] OpenAI debug logging failed: {_e}")

# ----------------------------
# Cloudflare R2 (S3-compatible) configuration for uploads
# ----------------------------
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
# Optional: custom CDN domain mapped to the bucket (defaults to assets.speako.ai)
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "https://assets.speako.ai")

r2_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
) if all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]) else None

# Database configuration for webhooks
DATABASE_URL = os.getenv("DATABASE_URL")

# Webhook configuration
WEBHOOK_MAX_AUDIO_SIZE = 100 * 1024 * 1024  # 100MB in bytes


def get_db_connection():
    """Get PostgreSQL database connection for webhook processing."""
    return psycopg2.connect(DATABASE_URL)


def allowed_knowledge_file(filename: str) -> bool:
    """Return True if filename has an allowed knowledge extension (doc/x, xls/x, pdf, csv, txt)."""
    if not filename:
        return False
    allowed_ext = {'.doc', '.docx', '.xls', '.xlsx', '.pdf', '.csv', '.txt'}
    ext = os.path.splitext(filename.lower())[1]
    return ext in allowed_ext


def generate_knowledge_filename(tenant_id: str, location_id: str, knowledge_type: str, file_extension: str) -> str:
    """Generate deterministic unique filename for knowledge uploads."""
    base = f"{tenant_id}_{location_id}_{knowledge_type}_{int(time.time())}"
    digest = hashlib.md5(base.encode()).hexdigest()[:8]
    return f"{tenant_id}_{location_id}_{knowledge_type}_{digest}{file_extension}"

# Import avatar API functionality
import sys
import json
from pathlib import Path

# Avatar API class for serving avatar catalog data
class AvatarAPI:
    """API class for serving avatar catalog data."""
    
    def __init__(self, catalog_file: str = "speako-dashboard-avatar/avatar_catalog_simple.json"):
        """Initialize with catalog file path."""
        self.catalog_file = catalog_file
        self.catalog = self.load_catalog()
    
    def load_catalog(self):
        """Load avatar catalog from JSON file."""
        try:
            if os.path.exists(self.catalog_file):
                with open(self.catalog_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {"avatars": [], "metadata": {"total": 0}}
        except Exception as e:
            print(f"Error loading avatar catalog: {str(e)}")
            return {"avatars": [], "metadata": {"total": 0}}
    
    def refresh_catalog(self):
        """Refresh catalog from file."""
        try:
            self.catalog = self.load_catalog()
            return True
        except Exception:
            return False
    
    def get_all_avatars(self):
        """Get all avatars with metadata."""
        return {
            "success": True,
            "data": self.catalog.get("avatars", []),
            "metadata": self.catalog.get("metadata", {}),
            "total": len(self.catalog.get("avatars", []))
        }
    
    def search_avatars(self, filters):
        """Search avatars based on filters."""
        avatars = self.catalog.get("avatars", [])
        filtered_avatars = []
        
        for avatar in avatars:
            tags = avatar.get("tags", [])
            tags_lower = [tag.lower() for tag in tags]
            match = True
            
            # Apply filters based on tags
            if "gender" in filters and filters["gender"]:
                if filters["gender"].lower() not in tags_lower:
                    match = False
            
            if "race" in filters and filters["race"]:
                if filters["race"].lower() not in tags_lower:
                    match = False
            
            if "occupation" in filters and filters["occupation"]:
                if filters["occupation"].lower() not in tags_lower:
                    match = False
            
            if "style" in filters and filters["style"]:
                if filters["style"].lower() not in tags_lower:
                    match = False
            
            if "age_group" in filters and filters["age_group"]:
                if filters["age_group"].lower() not in tags_lower:
                    match = False
            
            # Tags search (any of the provided tags must match)
            if "tags" in filters and filters["tags"]:
                search_tags = [tag.lower() for tag in filters["tags"]]
                if not any(tag in tags_lower for tag in search_tags):
                    match = False
            
            # Text search across all tags
            if "search" in filters and filters["search"]:
                search_term = filters["search"].lower()
                searchable_text = " ".join(tags).lower()
                
                if search_term not in searchable_text:
                    match = False
            
            if match:
                filtered_avatars.append(avatar)
        
        return {
            "success": True,
            "data": filtered_avatars,
            "total": len(filtered_avatars),
            "filters_applied": filters
        }
    
    def get_avatar_by_id(self, avatar_id):
        """Get specific avatar by ID."""
        avatars = self.catalog.get("avatars", [])
        
        for avatar in avatars:
            if avatar.get("id") == avatar_id:
                return {
                    "success": True,
                    "data": avatar
                }
        
        return {
            "success": False,
            "error": "Avatar not found",
            "data": None
        }
    
    def get_avatar_stats(self):
        """Get statistics about avatars."""
        avatars = self.catalog.get("avatars", [])
        
        # Count occurrences of each tag
        tag_counts = {}
        total_avatars = len(avatars)
        
        for avatar in avatars:
            tags = avatar.get("tags", [])
            for tag in tags:
                tag_lower = tag.lower()
                tag_counts[tag_lower] = tag_counts.get(tag_lower, 0) + 1
        
        # Categorize tags
        gender_tags = {}
        occupation_tags = {}
        race_tags = {}
        style_tags = {}
        age_tags = {}
        other_tags = {}
        
        # Common categorization patterns
        genders = ["male", "female", "non-binary"]
        occupations = ["businessman", "businessperson", "teacher", "farmer", "adventurer", "fashion enthusiast"]
        races = ["caucasian", "black", "hispanic", "asian", "indian"]
        styles = ["cartoon", "professional", "casual", "formal"]
        ages = ["young", "middle-aged", "senior"]
        
        for tag, count in tag_counts.items():
            if tag in genders:
                gender_tags[tag] = count
            elif tag in occupations:
                occupation_tags[tag] = count
            elif tag in races:
                race_tags[tag] = count
            elif tag in styles:
                style_tags[tag] = count
            elif tag in ages:
                age_tags[tag] = count
            else:
                other_tags[tag] = count
        
        return {
            "success": True,
            "data": {
                "total_avatars": total_avatars,
                "genders": gender_tags,
                "occupations": occupation_tags,
                "races": race_tags,
                "styles": style_tags,
                "age_groups": age_tags,
                "other_tags": other_tags,
                "all_tags": tag_counts
            }
        }

# Initialize avatar API
avatar_api = AvatarAPI()

# API Authentication decorator
def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check API key
        api_key = request.headers.get('X-API-Key')
        expected_key = os.getenv('API_SECRET_KEY')
        
        if not api_key or not expected_key:
            return jsonify({'error': 'API key required', 'code': 'MISSING_API_KEY'}), 401
            
        if not secrets.compare_digest(api_key, expected_key):
            return jsonify({'error': 'Unauthorized', 'code': 'INVALID_API_KEY'}), 401
        
        # Optional: Check allowed origins (if needed)
        origin = request.headers.get('Origin')
        allowed_origins = os.getenv('ALLOWED_ORIGINS', '').split(',')
        if allowed_origins and allowed_origins != [''] and origin not in allowed_origins:
            return jsonify({'error': 'Forbidden origin'}), 403
            
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def main():
    return render_template('main.html')


@app.route('/add', methods=['POST'])
def add_inputs():
    x = int(request.form['x'] or 0)
    y = int(request.form['y'] or 0)
    add.delay(x, y)
    flash("Your addition job has been submitted.")
    return redirect('/')


# =============================================================================
# API ENDPOINTS FOR EXTERNAL ACCESS
# =============================================================================

@app.route('/api/availability/generate', methods=['POST'])
@require_api_key
def api_generate_availability():
    """
    Generate availability for a location based on business type
    Expected JSON payload:
    {
        "tenant_id": "123",
        "location_id": "456", 
        "location_tz": "America/New_York",
        "business_type": "rest" | "service",  // mandatory - "rest" for restaurant/venue, "service" for staff
        "affected_date": "2025-08-15"  // optional, for regeneration
    }
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400
        
        # Validate required fields
        required_fields = ['tenant_id', 'location_id', 'location_tz', 'business_type']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({
                'error': 'Missing required fields', 
                'missing_fields': missing_fields
            }), 400
        
        # Extract parameters
        tenant_id = data['tenant_id']
        location_id = data['location_id']
        location_tz = data['location_tz']
        business_type = data['business_type']
        affected_date = data.get('affected_date')  # Optional for regeneration
        speako_task_id = data.get('speako_task_id')  # Optional for task tracking
        
        # Validate business_type
        if business_type not in ['rest', 'service']:
            return jsonify({
                'error': 'Invalid business_type',
                'message': 'business_type must be either "rest" or "service"',
                'provided': business_type
            }), 400
        
        # Route to appropriate task based on business_type
        if business_type == 'rest':
            task = gen_availability_venue.delay(tenant_id, location_id, location_tz, affected_date, task_id=speako_task_id)
            task_type = 'venue'
        else:
            task = gen_availability.delay(tenant_id, location_id, location_tz, affected_date, task_id=speako_task_id)
            task_type = 'staff'
        
        response_data = {
            'task_id': task.id,
            'status': 'pending',
            'message': f'{task_type.title()} availability generation task started',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'business_type': business_type,
            'task_type': task_type,
            'is_regeneration': affected_date is not None
        }
        
        # Include speako_task_id in response if provided
        if speako_task_id:
            response_data['speako_task_id'] = speako_task_id
        
        return jsonify(response_data), 202
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/availability/generate-venue', methods=['POST'])
@require_api_key
def api_generate_venue_availability():
    """
    Generate venue availability for a location
    Expected JSON payload:
    {
        "tenant_id": "123",
        "location_id": "456", 
        "location_tz": "America/New_York",
        "affected_date": "2025-08-15"  // optional, for regeneration
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON payload required'}), 400
        
        # Validate required fields
        required_fields = ['tenant_id', 'location_id', 'location_tz']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({
                'error': 'Missing required fields', 
                'missing_fields': missing_fields
            }), 400
        
        # Extract parameters
        tenant_id = data['tenant_id']
        location_id = data['location_id']
        location_tz = data['location_tz']
        affected_date = data.get('affected_date')  # Optional for regeneration
        speako_task_id = data.get('speako_task_id')  # Optional for task tracking
        
        # Trigger the celery task
        task = gen_availability_venue.delay(tenant_id, location_id, location_tz, affected_date, task_id=speako_task_id)
        
        response_data = {
            'task_id': task.id,
            'status': 'pending',
            'message': 'Venue availability generation task started',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'is_regeneration': affected_date is not None
        }
        
        # Include speako_task_id in response if provided
        if speako_task_id:
            response_data['speako_task_id'] = speako_task_id
        
        return jsonify(response_data), 202
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/task/<task_id>', methods=['GET'])
@require_api_key
def api_get_task_status(task_id):
    """
    Get the status of a celery task
    Returns task status, result, and any error information
    """
    try:
        # Get task result from celery
        task_result = celery_app.AsyncResult(task_id)
        
        response = {
            'task_id': task_id,
            'status': task_result.status,
            'ready': task_result.ready()
        }
        
        if task_result.ready():
            if task_result.successful():
                response['result'] = task_result.result
                response['success'] = True
            else:
                response['error'] = str(task_result.info)
                response['success'] = False
        else:
            # Task is still pending/running
            response['message'] = 'Task is still processing'
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e),
            'task_id': task_id
        }), 500


@app.route('/api/booking/notifications/send', methods=['POST'])
@require_api_key
def api_send_sms():
    """
    Send SMS notification and email notification for booking actions
    Expected JSON payload:
    {
        "booking_id": 123,
        "action": "new" | "modify" | "cancel",
        "business_type": "service" | "rest",
        "notify_customer": true | false,  // Optional, defaults to true
        "original_booking_id": 456  // Required only for "modify" action
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON payload required'}), 400
        
        # Validate required fields
        required_fields = ['booking_id', 'action', 'business_type']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({
                'error': 'Missing required fields', 
                'missing_fields': missing_fields
            }), 400
        
        # Extract parameters
        booking_id = data['booking_id']
        action = data['action']
        business_type = data['business_type']
        
        # Handle notify_customer - default to True if not provided or empty string
        notify_customer_raw = data.get('notify_customer', True)
        if notify_customer_raw == "":
            notify_customer = True  # Treat empty string as True
        else:
            notify_customer = notify_customer_raw
        
        original_booking_id = data.get('original_booking_id')
        
        # Validate booking_id is an integer
        try:
            booking_id = int(booking_id)
        except (ValueError, TypeError):
            return jsonify({
                'error': 'Invalid booking_id',
                'message': 'booking_id must be a valid integer',
                'provided': booking_id
            }), 400
        
        # Validate notify_customer is a boolean (after handling empty string)
        if not isinstance(notify_customer, bool):
            return jsonify({
                'error': 'Invalid notify_customer',
                'message': 'notify_customer must be a boolean value (true or false)',
                'provided': notify_customer
            }), 400
        
        # Validate action
        valid_actions = ['new', 'modify', 'cancel']
        if action not in valid_actions:
            return jsonify({
                'error': 'Invalid action',
                'message': f'action must be one of: {", ".join(valid_actions)}',
                'provided': action
            }), 400
        
        # Validate business_type
        valid_business_types = ['service', 'rest']
        if business_type not in valid_business_types:
            return jsonify({
                'error': 'Invalid business_type',
                'message': f'business_type must be one of: {", ".join(valid_business_types)}',
                'provided': business_type
            }), 400
        
        # Validate original_booking_id for modify action
        if action == 'modify':
            if not original_booking_id:
                return jsonify({
                    'error': 'Missing required field for modify action',
                    'message': 'original_booking_id is required when action is "modify"'
                }), 400
            try:
                original_booking_id = int(original_booking_id)
            except (ValueError, TypeError):
                return jsonify({
                    'error': 'Invalid original_booking_id',
                    'message': 'original_booking_id must be a valid integer',
                    'provided': original_booking_id
                }), 400
        
        # Start SMS and Email tasks based on action and business_type
        tasks = []
        task_descriptions = []
        
        if action == 'new':
            # SMS task (only if notify_customer is True)
            if notify_customer:
                sms_task = send_sms_confirmation_new.delay(booking_id)
                tasks.append({
                    'task_id': sms_task.id,
                    'type': 'sms',
                    'description': 'new booking SMS confirmation'
                })
                
                # Customer email task (only if notify_customer is True)
                customer_email_task = send_email_confirmation_customer_new.delay(booking_id)
                tasks.append({
                    'task_id': customer_email_task.id,
                    'type': 'customer_email',
                    'description': 'new booking customer email confirmation'
                })
            
            # Merchant email task (different based on business type) - always runs
            if business_type == 'rest':
                email_task = send_email_confirmation_new_rest.delay(booking_id)
                email_description = 'restaurant new booking merchant email confirmation'
            else:  # business_type == 'service'
                email_task = send_email_confirmation_new.delay(booking_id)
                email_description = 'service new booking merchant email confirmation'
            
            tasks.append({
                'task_id': email_task.id,
                'type': 'merchant_email',
                'description': email_description
            })
            
            action_description = 'new booking confirmation'
            
        elif action == 'modify':
            # SMS task (only if notify_customer is True)
            if notify_customer:
                sms_task = send_sms_confirmation_mod.delay(booking_id)
                tasks.append({
                    'task_id': sms_task.id,
                    'type': 'sms',
                    'description': 'booking modification SMS confirmation'
                })
                
                # Customer email task (only if notify_customer is True)
                customer_email_task = send_email_confirmation_customer_mod.delay(booking_id, original_booking_id)
                tasks.append({
                    'task_id': customer_email_task.id,
                    'type': 'customer_email',
                    'description': 'booking modification customer email confirmation'
                })
            
            # Merchant email task (different based on business type) - always runs
            if business_type == 'rest':
                email_task = send_email_confirmation_mod_rest.delay(booking_id, original_booking_id)
                email_description = 'restaurant booking modification merchant email confirmation'
            else:  # business_type == 'service'
                email_task = send_email_confirmation_mod.delay(booking_id, original_booking_id)
                email_description = 'service booking modification merchant email confirmation'
            
            tasks.append({
                'task_id': email_task.id,
                'type': 'merchant_email',
                'description': email_description
            })
            
            action_description = 'booking modification confirmation'
            
        elif action == 'cancel':
            # SMS task (only if notify_customer is True)
            if notify_customer:
                sms_task = send_sms_confirmation_can.delay(booking_id)
                tasks.append({
                    'task_id': sms_task.id,
                    'type': 'sms',
                    'description': 'booking cancellation SMS confirmation'
                })
                
                # Customer email task (only if notify_customer is True)
                customer_email_task = send_email_confirmation_customer_can.delay(booking_id)
                tasks.append({
                    'task_id': customer_email_task.id,
                    'type': 'customer_email',
                    'description': 'booking cancellation customer email confirmation'
                })
            
            # Merchant email task (different based on business type) - always runs
            if business_type == 'rest':
                email_task = send_email_confirmation_can_rest.delay(booking_id)
                email_description = 'restaurant booking cancellation merchant email confirmation'
            else:  # business_type == 'service'
                email_task = send_email_confirmation_can.delay(booking_id)
                email_description = 'service booking cancellation merchant email confirmation'
            
            tasks.append({
                'task_id': email_task.id,
                'type': 'merchant_email',
                'description': email_description
            })
            
            action_description = 'booking cancellation confirmation'
            
        else:
            # This should never happen due to validation above, but added for safety
            return jsonify({
                'error': 'Undefined action',
                'message': f'No action is defined for "{action}". No SMS or email notifications have been sent.',
                'booking_id': booking_id,
                'action': action,
                'valid_actions': ['new', 'modify', 'cancel']
            }), 400
        
        response = {
            'message': f'{action_description.title()} tasks started',
            'booking_id': booking_id,
            'action': action,
            'business_type': business_type,
            'notify_customer': notify_customer,
            'action_description': action_description,
            'tasks': tasks,
            'total_tasks': len(tasks)
        }
        
        # Add original_booking_id to response if applicable
        if action == 'modify' and original_booking_id:
            response['original_booking_id'] = original_booking_id
        
        return jsonify(response), 202
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/health', methods=['GET'])
def api_health_check():
    """
    Simple health check endpoint (no authentication required)
    """
    return jsonify({
        'status': 'healthy',
        'service': 'speako-celery-api',
        'timestamp': os.popen('date').read().strip()
    }), 200


@app.route('/api/knowledge/analyze-knowledge-file', methods=['POST'])
@require_api_key
def api_upload_knowledge_file():
    """
    [aiknowledges] Upload a knowledge file to Cloudflare R2 OR provide a remote file_url for analysis.

    Accepts either:
    - multipart/form-data with fields: tenant_id, location_id, knowledge_type, file (binary)
    - JSON or form-data with fields: tenant_id, location_id, knowledge_type, file_url (string)
    - Optional: speako_task_id (string) for client correlation
    - Optional: tenantIntegrationParam (object) for integration metadata

    Constraints for file uploads:
    - Allowed types: .doc/.docx, .xls/.xlsx, .pdf, .csv, .txt
    - Max size: 5MB

    Upload path in bucket: knowledges/{tenant_id}/{location_id}/
    """
    try:
        # Gather inputs from form or JSON
        data_json = request.get_json(silent=True) or {}
        file_url = request.form.get('file_url') or data_json.get('file_url')
        speako_task_id = request.form.get('speako_task_id') or data_json.get('speako_task_id')
        
        # Extract tenantIntegrationParam (only available in JSON mode)
        tenant_integration_param = data_json.get('tenantIntegrationParam')

        tenant_id = request.form.get('tenant_id') or data_json.get('tenant_id')
        location_id = request.form.get('location_id') or data_json.get('location_id')
        knowledge_type = request.form.get('knowledge_type') or data_json.get('knowledge_type')

        # Validate required fields
        missing = [k for k, v in {
            'tenant_id': tenant_id,
            'location_id': location_id,
            'knowledge_type': knowledge_type,
        }.items() if not v]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Validate knowledge_type
        valid_types = ['business_info', 'service_menu', 'food_menu', 'locations', 'staff', 'faq', 'service_policy', 'special_promotion', 'custom_message']
        if knowledge_type not in valid_types:
            return jsonify({
                'error': 'Invalid knowledge_type',
                'message': f'knowledge_type must be one of: {", ".join(valid_types)}',
                'provided': knowledge_type
            }), 400

        # Branch: file_url mode (skip R2 upload)
        if file_url:
            parsed = urlparse(file_url)
            ext = os.path.splitext(parsed.path.lower())[1]

            # Optional: enforce allowed extensions when present in URL
            if ext and not allowed_knowledge_file(f"dummy{ext}"):
                return jsonify({'error': 'Unsupported file type', 'message': 'Allowed: .doc, .docx, .xls, .xlsx, .pdf, .csv, .txt', 'provided_ext': ext}), 400

            # Guess content type and extension when missing
            guessed_ct, _ = mimetypes.guess_type(parsed.path)
            content_type = data_json.get('content_type') or guessed_ct or 'application/octet-stream'
            if not ext:
                guessed_ext = mimetypes.guess_extension(content_type) or '.bin'
                ext = guessed_ext

            unique_filename = generate_knowledge_filename(tenant_id, location_id, knowledge_type, ext)
            key = f"knowledges/{tenant_id}/{location_id}/{unique_filename}"

            response_data = {
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'filename': unique_filename,
                'key': key,
                'url': file_url,
                'size': None,
                'content_type': content_type,
                'source': 'file_url',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }

            # Enqueue background analysis task using the remote URL
            try:
                task = analyze_knowledge_file.delay(
                    tenant_id=tenant_id,
                    location_id=location_id,
                    knowledge_type=knowledge_type,
                    key=key,
                    unique_filename=unique_filename,
                    content_type=content_type,
                    public_url=file_url,
                    file_url=file_url,
                    speako_task_id=speako_task_id,
                    tenant_integration_param=tenant_integration_param,
                )
                response_data['analysis'] = {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                }
            except Exception as ae:
                app.logger.error(f"Failed to enqueue analysis task (file_url mode): {ae}")
                response_data['analysis'] = {
                    'status': 'error',
                    'message': 'Failed to enqueue analysis task',
                    'detail': str(ae)
                }

            return jsonify({
                'success': True,
                'message': 'Knowledge analysis started from remote URL',
                'data': response_data
            }), 202

        # Otherwise: file upload mode (requires R2)
        if not r2_client:
            return jsonify({'error': 'Storage not configured', 'message': 'Cloudflare R2 credentials are missing'}), 500

        # Validate file presence
        if 'file' not in request.files:
            return jsonify({'error': 'File is required', 'message': 'No file part in the request'}), 400
        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'Invalid file', 'message': 'No selected file'}), 400

        # Validate extension
        if not allowed_knowledge_file(file.filename):
            return jsonify({'error': 'Unsupported file type', 'message': 'Allowed: .doc, .docx, .xls, .xlsx, .pdf, .csv, .txt'}), 400

        # Enforce 5MB limit
        try:
            file.stream.seek(0, os.SEEK_END)
            size = file.stream.tell()
            file.stream.seek(0)
        except Exception:
            # Fallback: read into memory
            data_peek = file.read()
            size = len(data_peek)
            file.stream.seek(0)
        if size > 5 * 1024 * 1024:
            return jsonify({'error': 'File too large', 'message': 'File size must be less than or equal to 5MB'}), 400

        # Prepare upload
        file_extension = os.path.splitext(secure_filename(file.filename))[1].lower()
        unique_filename = generate_knowledge_filename(tenant_id, location_id, knowledge_type, file_extension)
        key = f"knowledges/{tenant_id}/{location_id}/{unique_filename}"

        file_content = file.read()
        content_type = file.mimetype or 'application/octet-stream'

        # Upload to R2
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=file_content,
            ContentType=content_type,
            Metadata={
                'original_filename': file.filename,
                'upload_timestamp': datetime.utcnow().isoformat() + 'Z',
                'tenant_id': str(tenant_id),
                'location_id': str(location_id),
                'knowledge_type': knowledge_type,
                'group': 'aiknowledges'
            }
        )

        public_url = f"{R2_PUBLIC_BASE_URL}/{key}"

        response_data = {
            'tenant_id': tenant_id,
            'location_id': location_id,
            'knowledge_type': knowledge_type,
            'filename': unique_filename,
            'key': key,
            'url': public_url,
            'size': len(file_content),
            'content_type': content_type,
            'source': 'upload',
            **({'speako_task_id': speako_task_id} if speako_task_id else {})
        }

        # Enqueue background analysis task
        try:
            task = analyze_knowledge_file.delay(
                tenant_id=tenant_id,
                location_id=location_id,
                knowledge_type=knowledge_type,
                key=key,
                unique_filename=unique_filename,
                content_type=content_type,
                public_url=public_url,
                speako_task_id=speako_task_id,
                tenant_integration_param=tenant_integration_param,
            )
            response_data['analysis'] = {
                'status': 'queued',
                'mode': 'background',
                'celery_task_id': task.id
            }
        except Exception as ae:
            # If enqueue fails, return upload success but analysis enqueue error
            app.logger.error(f"Failed to enqueue analysis task: {ae}")
            response_data['analysis'] = {
                'status': 'error',
                'message': 'Failed to enqueue analysis task',
                'detail': str(ae)
            }

        return jsonify({
            'success': True,
            'message': 'Knowledge file uploaded successfully',
            'data': response_data
        }), 201

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


# =============================================================================
# AVATAR API ENDPOINTS
# =============================================================================

@app.route('/api/avatars', methods=['GET'])
@require_api_key
def api_get_avatars():
    """
    Get all avatars or search with filters
    
    Query parameters:
    - gender: Filter by gender (male, female, non-binary)
    - race: Filter by race (caucasian, black, asian, hispanic, etc.)
    - occupation: Filter by occupation (doctor, engineer, teacher, etc.)
    - style: Filter by style (professional, casual, cartoon, etc.)
    - age_group: Filter by age group (young, middle-aged, senior)
    - search: Text search across all fields
    - tags: Comma-separated list of tags
    - page: Page number (default: 1, use 0 or 'all' to get all results)
    - per_page: Items per page (default: 20, max: 100, use 'all' to get all results)
    - all: Set to 'true' to bypass pagination and get all results
    """
    try:
        # Get query parameters
        filters = {}
        
        # Single value filters
        for param in ['gender', 'race', 'occupation', 'style', 'age_group', 'search']:
            value = request.args.get(param)
            if value:
                filters[param] = value
        
        # Multi-value filters (comma-separated)
        tags = request.args.get('tags')
        if tags:
            filters['tags'] = [tag.strip() for tag in tags.split(',')]
        
        # Check if user wants all results (bypass pagination)
        all_results = request.args.get('all', '').lower() == 'true'
        page_param = request.args.get('page', '1')
        per_page_param = request.args.get('per_page', '20')
        
        # Handle special cases for getting all results
        if all_results or page_param == '0' or page_param.lower() == 'all' or per_page_param.lower() == 'all':
            # Get results without pagination
            if filters:
                result = avatar_api.search_avatars(filters)
            else:
                result = avatar_api.get_all_avatars()
            
            # Add pagination info indicating no pagination was applied
            result["pagination"] = {
                "page": "all",
                "per_page": "all",
                "total": result["total"],
                "pages": 1,
                "paginated": False,
                "message": "All results returned (pagination bypassed)"
            }
            
            return jsonify(result), 200
        
        # Normal pagination
        try:
            page = int(page_param)
            per_page = int(per_page_param)
        except ValueError:
            return jsonify({
                'error': 'Invalid pagination parameters',
                'message': 'page and per_page must be integers, or use "all" to get all results'
            }), 400
        
        # Validate pagination parameters
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 20
        if per_page > 100:
            per_page = 100
        
        # Get results
        if filters:
            result = avatar_api.search_avatars(filters)
        else:
            result = avatar_api.get_all_avatars()
        
        # Apply pagination
        total = result["total"]
        total_pages = (total + per_page - 1) // per_page
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        # Check if page number is valid
        if page > total_pages and total > 0:
            return jsonify({
                'error': 'Page not found',
                'message': f'Page {page} does not exist. Total pages: {total_pages}',
                'total_pages': total_pages,
                'total_results': total
            }), 404
        
        result["data"] = result["data"][start_idx:end_idx]
        result["pagination"] = {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": total_pages,
            "paginated": True,
            "has_next": page < total_pages,
            "has_prev": page > 1,
            "next_page": page + 1 if page < total_pages else None,
            "prev_page": page - 1 if page > 1 else None,
            "start_index": start_idx + 1 if total > 0 else 0,
            "end_index": min(end_idx, total)
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/avatars/<avatar_id>', methods=['GET'])
@require_api_key
def api_get_avatar(avatar_id):
    """
    Get specific avatar by ID
    
    Parameters:
    - avatar_id: The avatar ID (e.g., 'avatar_001')
    """
    try:
        result = avatar_api.get_avatar_by_id(avatar_id)
        
        if result["success"]:
            return jsonify(result), 200
        else:
            return jsonify(result), 404
            
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e),
            'avatar_id': avatar_id
        }), 500


@app.route('/api/avatars/stats', methods=['GET'])
@require_api_key
def api_get_avatar_statistics():
    """
    Get avatar statistics and breakdowns
    
    Returns counts by occupation, race, gender, style, age group
    """
    try:
        result = avatar_api.get_avatar_stats()
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/avatars/refresh', methods=['POST'])
@require_api_key
def api_refresh_avatar_catalog():
    """
    Refresh avatar catalog from file
    
    Use this endpoint to reload the catalog after updates
    """
    try:
        success = avatar_api.refresh_catalog()
        
        if success:
            return jsonify({
                "success": True,
                "message": "Avatar catalog refreshed successfully",
                "total_avatars": len(avatar_api.catalog.get("avatars", []))
            }), 200
        else:
            return jsonify({
                "success": False,
                "message": "Failed to refresh avatar catalog"
            }), 500
            
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.route('/api/avatars/health', methods=['GET'])
def api_avatar_health_check():
    """
    Avatar API health check (no authentication required)
    """
    try:
        catalog_loaded = len(avatar_api.catalog.get("avatars", [])) > 0
        
        # Get sample avatar structure for debugging
        sample_avatar = None
        avatar_structure = None
        if catalog_loaded:
            avatars = avatar_api.catalog.get("avatars", [])
            if avatars:
                sample_avatar = avatars[0]
                avatar_structure = list(sample_avatar.keys())
        
        return jsonify({
            "success": True,
            "status": "healthy",
            "service": "avatar-api",
            "catalog_loaded": catalog_loaded,
            "total_avatars": len(avatar_api.catalog.get("avatars", [])),
            "catalog_file": avatar_api.catalog_file,
            "catalog_file_exists": os.path.exists(avatar_api.catalog_file),
            "sample_avatar_structure": avatar_structure,
            "is_simplified_format": avatar_structure == ['id', 'url', 'tags'] if avatar_structure else None,
            "working_directory": os.getcwd(),
            "metadata": avatar_api.catalog.get("metadata", {})
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "status": "unhealthy",
            "error": str(e)
        }), 500


@app.route('/api/avatars/debug', methods=['GET'])
def api_avatar_debug():
    """
    Debug endpoint to check avatar catalog loading (no authentication required)
    """
    try:
        debug_info = {
            "working_directory": os.getcwd(),
            "catalog_file_path": avatar_api.catalog_file,
            "absolute_catalog_path": os.path.abspath(avatar_api.catalog_file),
            "catalog_file_exists": os.path.exists(avatar_api.catalog_file),
            "total_avatars_loaded": len(avatar_api.catalog.get("avatars", [])),
            "metadata": avatar_api.catalog.get("metadata", {}),
            "files_in_speako_dashboard_avatar": []
        }
        
        # Check what files exist in the speako-dashboard-avatar directory
        avatar_dir = "speako-dashboard-avatar"
        if os.path.exists(avatar_dir):
            files = os.listdir(avatar_dir)
            catalog_files = [f for f in files if f.startswith('avatar_catalog') and f.endswith('.json')]
            debug_info["files_in_speako_dashboard_avatar"] = catalog_files
            
            # Get file sizes for catalog files
            debug_info["catalog_file_sizes"] = {}
            for file in catalog_files:
                file_path = os.path.join(avatar_dir, file)
                if os.path.exists(file_path):
                    debug_info["catalog_file_sizes"][file] = os.path.getsize(file_path)
        
        # Check avatar structure
        avatars = avatar_api.catalog.get("avatars", [])
        if avatars:
            first_avatar = avatars[0]
            debug_info["first_avatar_keys"] = list(first_avatar.keys())
            debug_info["is_simplified_format"] = set(first_avatar.keys()) == {'id', 'url', 'tags'}
            debug_info["first_avatar_sample"] = {
                "id": first_avatar.get("id"),
                "url": len(first_avatar.get("url", "")),
                "tags_count": len(first_avatar.get("tags", []))
            }
        
        return jsonify(debug_info), 200
        
    except Exception as e:
        return jsonify({
            "error": "Debug endpoint failed",
            "message": str(e)
        }), 500


def build_knowledge_prompt(knowledge_type: str) -> str:
    """Return a strict JSON-only extraction prompt per knowledge type."""
    if knowledge_type == 'menu':
        return (
            "You are given a document that may contain a service or food menu. "
            "Extract menu items into the following strict JSON schema. Output ONLY JSON, no prose.\n"
            "{\n"
            "  \"type\": \"menu\",\n"
            "  \"items\": [\n"
            "    {\n"
            "      \"name\": string,\n"
            "      \"description\": string|null,\n"
            "      \"prices\": [ { \"label\": string|null, \"amount\": number, \"currency\": string|null } ],\n"
            "      \"options\": [ { \"name\": string, \"price_delta\": number|null } ],\n"
            "      \"allergens\": [ string ]\n"
            "    }\n"
            "  ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    if knowledge_type == 'faq':
        return (
            "You are given a document that may contain FAQ content. "
            "Extract FAQs into the following strict JSON schema. Output ONLY JSON.\n"
            "{\n"
            "  \"type\": \"faq\",\n"
            "  \"faqs\": [ { \"question\": string, \"answer\": string } ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    if knowledge_type == 'events' or knowledge_type == 'event' or knowledge_type == 'promotions':
        return (
            "You are given a document that may contain information about events and/or promotions. "
            "Extract items into the following strict JSON schema. Output ONLY JSON. Do NOT include prose.\n"
            "{\n"
            "  \"type\": \"events_promotions\",\n"
            "  \"items\": [\n"
            "    {\n"
            "      \"title\": string,\n"
            "      \"category\": \"event\" | \"promotion\",\n"
            "      \"description\": string|null,\n"
            "      \"start_datetime\": string|null,  // ISO8601 if available, else null\n"
            "      \"end_datetime\": string|null,    // ISO8601 if available, else null\n"
            "      \"dates\": [ string ],            // optional list of ISO dates for multiple occurrences\n"
            "      \"recurrence\": { \"rule\": string|null, \"notes\": string|null } | null,\n"
            "      \"location\": { \"venue\": string|null, \"address\": string|null, \"city\": string|null },\n"
            "      \"price\": { \"amount\": number|null, \"currency\": string|null } | null,\n"
            "      \"promotion\": {\n"
            "         \"discount_type\": \"percentage\" | \"amount\" | null,\n"
            "         \"value\": number|null,\n"
            "         \"promo_code\": string|null,\n"
            "         \"conditions\": string|null,\n"
            "         \"valid_from\": string|null,   // ISO8601 date or datetime\n"
            "         \"valid_until\": string|null    // ISO8601 date or datetime\n"
            "      } | null,\n"
            "      \"audience\": string|null,\n"
            "      \"url\": string|null,\n"
            "      \"tags\": [ string ]\n"
            "    }\n"
            "  ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    # policy
    return (
        "You are given a document that may contain policies and terms & conditions. "
        "Extract into the following strict JSON schema. Output ONLY JSON.\n"
        "{\n"
        "  \"type\": \"policy\",\n"
        "  \"policies\": [ { \"title\": string, \"body\": string } ],\n"
        "  \"terms\": [ { \"title\": string, \"body\": string } ],\n"
        "  \"source_confidence\": number\n"
        "}"
    )


@app.route('/api/knowledge/scrape-url', methods=['POST'])
@require_api_key
def api_scrape_url():
    """
    [aiknowledges] Scrape a public URL into LLM-ready Markdown and store artifacts in R2.

    Expected JSON payload:
    {
      "tenant_id": "123",           // required
      "location_id": "456",         // required
      "url": "https://...",         // required
      "pipeline": "markdown-only" | "analyze", // optional, default: markdown-only
      "knowledge_type": "menu|faq|policy|events", // required if pipeline=analyze
      "save_raw_html": false,          // optional
      "speako_task_id": "abc-123",    // optional correlation ID
      "tenantIntegrationParam": {...}  // optional integration metadata
    }

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        tenant_id = data.get('tenant_id')
        location_id = data.get('location_id')
        url = data.get('url')
        pipeline = data.get('pipeline', 'markdown-only')
        knowledge_type = data.get('knowledge_type')
        save_raw_html = bool(data.get('save_raw_html', False))
        speako_task_id = data.get('speako_task_id')
        tenant_integration_param = data.get('tenantIntegrationParam')

        missing = [k for k in ['tenant_id', 'location_id', 'url'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        if pipeline not in ['markdown-only', 'analyze']:
            return jsonify({'error': 'Invalid pipeline', 'message': 'pipeline must be markdown-only or analyze'}), 400

        if pipeline == 'analyze':
            valid_types = ['business_info', 'service_menu', 'food_menu', 'locations', 'staff', 'faq', 'service_policy', 'special_promotion', 'custom_message']
            if knowledge_type not in valid_types:
                return jsonify({
                    'error': 'Invalid or missing knowledge_type',
                    'message': f'knowledge_type required for analyze; one of: {", ".join(valid_types)}'
                }), 400

        # Enqueue scrape task
        task = scrape_url_to_markdown.delay(
            tenant_id=tenant_id,
            location_id=location_id,
            url=url,
            pipeline=pipeline,
            knowledge_type=knowledge_type,
            save_raw_html=save_raw_html,
            speako_task_id=speako_task_id,
            tenant_integration_param=tenant_integration_param,
        )

        # Align response shape with analyze_knowledge endpoint
        return jsonify({
            'success': True,
            'message': 'URL scrape task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'url': url,
                'source': 'scrape_url',
                'pipeline': pipeline,
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/api/knowledge/sync-with-speako', methods=['POST'])
@require_api_key
def api_sync_with_speako():
    """
    [aiknowledges] Sync knowledge data directly from Speako's internal database.

    Expected JSON payload:
    {
      "tenant_id": "123",           // required
      "location_id": "456",         // required
      "knowledge_type": "business_info|service_menu|locations|staff", // required
      "speako_task_id": "abc-123",    // optional correlation ID
      "tenantIntegrationParam": {...}  // optional integration metadata
    }

    Note: This endpoint does NOT accept file uploads or URLs. It syncs data directly
    from Speako's internal database based on tenant_id and location_id.

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        tenant_id = data.get('tenant_id')
        location_id = data.get('location_id')
        knowledge_type = data.get('knowledge_type')
        speako_task_id = data.get('speako_task_id')
        tenant_integration_param = data.get('tenantIntegrationParam')

        # Validate required fields
        missing = [k for k in ['tenant_id', 'location_id', 'knowledge_type'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Validate knowledge_type - only allow sync-compatible types
        valid_types = ['business_info', 'service_menu', 'locations', 'staff']
        if knowledge_type not in valid_types:
            return jsonify({
                'error': 'Invalid knowledge_type',
                'message': f'knowledge_type must be one of: {", ".join(valid_types)}',
                'provided': knowledge_type
            }), 400

        # Enqueue sync task
        task = sync_speako_data.delay(
            tenant_id=tenant_id,
            location_id=location_id,
            knowledge_type=knowledge_type,
            speako_task_id=speako_task_id,
            tenant_integration_param=tenant_integration_param,
        )

        # Return response similar to other knowledge endpoints
        return jsonify({
            'success': True,
            'message': 'Speako data sync task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'tenant_id': tenant_id,
                'location_id': location_id,
                'knowledge_type': knowledge_type,
                'source': 'sync_speako_data',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/api/agent/publish/add', methods=['POST'])
@require_api_key
def api_create_ai_agent():
    """
    [aiagent] Create ElevenLabs AI agent.

    Expected JSON payload:
    {
      "location_id": "123",                                // required
      "location_name": "Happy Sushi",                      // required
      "location_timezone": "Australia/Sydney",             // required
      "speako_task_id": "550e8400-e29b-41d4-a716-446655440000",  // optional correlation ID
    }

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        location_id = data.get('location_id')
        location_name = data.get('location_name')
        location_timezone = data.get('location_timezone')
        speako_task_id = data.get('speako_task_id')

        # Validate required fields
        missing = [k for k in ['location_id', 'location_name', 'location_timezone'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Enqueue create agent task
        task = create_conversation_ai_agent.delay(
            location_id=location_id,
            location_name=location_name,
            location_timezone=location_timezone,
        )

        # Return response similar to other agent endpoints
        return jsonify({
            'success': True,
            'message': 'ElevenLabs agent creation task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'location_id': location_id,
                'location_name': location_name,
                'location_timezone': location_timezone,
                'source': 'create_conversation_ai_agent',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/api/agent/publish/update', methods=['POST'])
@require_api_key
def api_publish_elevenlabs_agent():
    """
    [aiagent] Publish ElevenLabs AI agent.

    Expected JSON payload:
    {
      "tenant_id": "1",                                    // required
      "location_id": "123",                                // required
      "publish_job_id": "42",                              // required
      "speako_task_id": "550e8400-e29b-41d4-a716-446655440000",  // optional correlation ID
      "tenantIntegrationParam": {...}                      // optional integration metadata
    }

    Returns 202 with celery_task_id for polling at /api/task/<task_id>.
    """
    try:
        # Be tolerant of clients missing Content-Type or sending invalid JSON
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                'error': 'JSON payload required',
                'message': 'Send a valid JSON body with Content-Type: application/json',
                'content_type': request.headers.get('Content-Type', None)
            }), 400

        tenant_id = data.get('tenant_id')
        location_id = data.get('location_id')
        publish_job_id = data.get('publish_job_id')
        speako_task_id = data.get('speako_task_id')
        tenant_integration_param = data.get('tenantIntegrationParam')

        # Validate required fields
        missing = [k for k in ['tenant_id', 'location_id', 'publish_job_id'] if not data.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Enqueue publish task
        task = publish_elevenlabs_agent.delay(
            tenant_id=tenant_id,
            location_id=location_id,
            publish_job_id=publish_job_id,
            speako_task_id=speako_task_id,
            tenant_integration_param=tenant_integration_param,
        )

        # Return response similar to other knowledge endpoints
        return jsonify({
            'success': True,
            'message': 'ElevenLabs agent publish task started',
            'data': {
                'analysis': {
                    'status': 'queued',
                    'mode': 'background',
                    'celery_task_id': task.id
                },
                'tenant_id': tenant_id,
                'location_id': location_id,
                'publish_job_id': publish_job_id,
                'source': 'publish_elevenlabs_agent',
                **({'speako_task_id': speako_task_id} if speako_task_id else {})
            }
        }), 202

    except Exception as e:
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500


@app.route('/webhook/post_conversation', methods=['POST'])
def elevenlabs_post_conversation_webhook():
    """
    ElevenLabs Post-Conversation Webhook Endpoint
    
    This endpoint receives webhook notifications from ElevenLabs after a conversation ends.
    Processes audio upload and inserts conversation data into database.
    
    HMAC Authentication:
    - ElevenLabs signs the webhook payload with a secret key using HMAC-SHA256
    - The signature is sent in the 'elevenlabs-signature' header
    
    Setup:
    - Set ELEVENLABS_WEBHOOK_SECRET environment variable with the secret from ElevenLabs dashboard
    """
    
    # Get the raw request body for HMAC verification
    payload_bytes = request.get_data()
    
    # Get the signature from headers
    received_signature = request.headers.get('elevenlabs-signature', '')
    
    # Get the webhook secret from environment variable
    webhook_secret = os.getenv('ELEVENLABS_WEBHOOK_SECRET', '')
    
    # Log webhook received
    print("=" * 80)
    print("ELEVENLABS POST-CONVERSATION WEBHOOK RECEIVED")
    print("=" * 80)
    print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    print(f"Payload size: {len(payload_bytes)} bytes")
    
    # HMAC Verification
    if webhook_secret and received_signature:
        computed_signature = hmac.new(
            key=webhook_secret.encode('utf-8'),
            msg=payload_bytes,
            digestmod=hashlib.sha256
        ).hexdigest()
        
        signature_valid = hmac.compare_digest(computed_signature, received_signature)
        
        if not signature_valid:
            print(f"⚠️  HMAC SIGNATURE MISMATCH - Request rejected")
            print("=" * 80)
            return jsonify({
                'error': 'Invalid signature',
                'message': 'HMAC verification failed'
            }), 401
        
        print("✅ HMAC signature verified")
    else:
        if not webhook_secret:
            print(f"⚠️  WARNING: ELEVENLABS_WEBHOOK_SECRET not set")
        if not received_signature:
            print(f"⚠️  WARNING: No signature received in headers")
    
    # Parse JSON payload
    try:
        payload_json = request.get_json(force=True)
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")
        print("=" * 80)
        return jsonify({
            'error': 'Invalid JSON',
            'message': str(e)
        }), 400
    
    # Extract webhook data
    webhook_type = payload_json.get('type')
    event_timestamp = payload_json.get('event_timestamp')
    data = payload_json.get('data', {})
    
    agent_id = data.get('agent_id')
    conversation_id = data.get('conversation_id')
    full_audio_base64 = data.get('full_audio')
    
    print(f"Webhook type: {webhook_type}")
    print(f"Agent ID: {agent_id}")
    print(f"Conversation ID: {conversation_id}")
    print(f"Has audio: {bool(full_audio_base64)}")
    
    # Validate required fields
    if not agent_id or not conversation_id:
        print(f"❌ Missing required fields: agent_id={agent_id}, conversation_id={conversation_id}")
        print("=" * 80)
        return jsonify({
            'error': 'Missing required fields',
            'message': 'agent_id and conversation_id are required'
        }), 400
    
    # Process webhook
    try:
        conn = get_db_connection()
        
        try:
            # Step 1: Lookup location information
            print(f"\n[Step 1] Looking up location for agent_id: {agent_id}")
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tenant_id, location_id, name, timezone
                    FROM locations
                    WHERE elevenlabs_agent_id = %s
                    AND is_active = true
                    LIMIT 1
                """, (agent_id,))
                
                location_row = cur.fetchone()
            
            if not location_row:
                print(f"⚠️  No location found for agent_id: {agent_id}")
                print(f"⚠️  ORPHANED CONVERSATION: {conversation_id}")
                print(f"⚠️  This conversation cannot be inserted without location mapping")
                print("=" * 80)
                
                # Return 200 to prevent retries, but log critical error
                return jsonify({
                    'success': True,
                    'message': 'Webhook received but no location mapping found',
                    'warning': 'Orphaned conversation - needs manual intervention',
                    'conversation_id': conversation_id,
                    'agent_id': agent_id
                }), 200
            
            tenant_id, location_id, location_name, timezone_str = location_row
            print(f"✅ Found location: tenant_id={tenant_id}, location_id={location_id}, name={location_name}")
            
            # Step 2: Check for duplicate conversation (idempotency)
            print(f"\n[Step 2] Checking for duplicate conversation")
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT location_conversation_id, audio_r2_path
                    FROM location_conversations
                    WHERE eleven_conversation_id = %s
                """, (conversation_id,))
                
                existing_row = cur.fetchone()
            
            if existing_row:
                existing_id, existing_audio = existing_row
                print(f"✅ Conversation already exists: location_conversation_id={existing_id}")
                
                if existing_audio:
                    print(f"✅ Audio already uploaded: {existing_audio}")
                    print("=" * 80)
                    return jsonify({
                        'success': True,
                        'message': 'Conversation already processed',
                        'conversation_id': conversation_id,
                        'location_conversation_id': existing_id
                    }), 200
                else:
                    print(f"⚠️  Audio missing, will attempt to upload")
            
            # Step 3: Fetch full conversation details from ElevenLabs API
            print(f"\n[Step 3] Fetching full conversation details from ElevenLabs API")
            
            details = None
            try:
                details = get_conversation_details(conversation_id)
                print(f"✅ Retrieved full conversation details from API")
            except Exception as e:
                print(f"⚠️  Failed to fetch conversation details from API: {e}")
                print(f"⚠️  Will use minimal webhook data only")
            
            # Step 4: Decode and validate audio
            audio_bytes = None
            audio_r2_path = None
            
            if full_audio_base64:
                print(f"\n[Step 4] Decoding base64 audio")
                
                try:
                    audio_bytes = base64.b64decode(full_audio_base64)
                    audio_size = len(audio_bytes)
                    print(f"✅ Decoded audio: {audio_size} bytes ({audio_size / 1024 / 1024:.2f} MB)")
                    
                    # Validate size
                    if audio_size > WEBHOOK_MAX_AUDIO_SIZE:
                        print(f"⚠️  Audio size exceeds limit: {audio_size} > {WEBHOOK_MAX_AUDIO_SIZE}")
                        audio_bytes = None
                    
                except Exception as e:
                    print(f"⚠️  Failed to decode audio: {e}")
                    audio_bytes = None
            else:
                print(f"\n[Step 4] No audio data in webhook")
            
            # Step 5: Upload audio to R2
            if audio_bytes:
                print(f"\n[Step 5] Uploading audio to R2")
                
                try:
                    r2_key, public_url = upload_audio_to_r2(
                        str(tenant_id),
                        str(location_id),
                        conversation_id,
                        audio_bytes,
                        content_type='audio/mpeg'
                    )
                    
                    audio_r2_path = r2_key
                    print(f"✅ Audio uploaded to R2: {public_url}")
                    
                except Exception as e:
                    print(f"⚠️  Failed to upload audio to R2: {e}")
                    audio_r2_path = None
            else:
                print(f"\n[Step 5] Skipping audio upload (no valid audio data)")
            
            # Step 6: If conversation exists, just update audio path
            if existing_row:
                if audio_r2_path:
                    print(f"\n[Step 6] Updating audio path for existing conversation")
                    
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE location_conversations
                            SET audio_r2_path = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE location_conversation_id = %s
                        """, (audio_r2_path, existing_id))
                    
                    conn.commit()
                    print(f"✅ Updated audio path")
                
                print("=" * 80)
                return jsonify({
                    'success': True,
                    'message': 'Conversation updated with audio',
                    'conversation_id': conversation_id,
                    'location_conversation_id': existing_id
                }), 200
            
            # Step 7: Insert new conversation record
            print(f"\n[Step 6] Inserting conversation into database")
            
            # Helper function to convert timestamp to location timezone
            def convert_timestamp(unix_ts):
                if unix_ts is None:
                    return None
                try:
                    utc_dt = datetime.fromtimestamp(unix_ts, tz=ZoneInfo('UTC'))
                    local_dt = utc_dt.astimezone(ZoneInfo(timezone_str))
                    return local_dt.replace(tzinfo=None)
                except Exception:
                    return None
            
            # Extract fields from API details or use webhook fallbacks
            if details:
                metadata = details.get('metadata', {})
                transcript = details.get('transcript', [])
                
                agent_name = details.get('agent_name') or location_name
                call_start_time = convert_timestamp(metadata.get('start_time_unix_secs'))
                call_accepted_time = convert_timestamp(metadata.get('end_time_unix_secs'))
                call_duration_secs = metadata.get('call_duration_secs')
                message_count = len(transcript) if transcript else 0
                status = details.get('status', 'completed')
                
                call_successful_str = details.get('call_successful')
                if call_successful_str:
                    call_successful = (call_successful_str == 'success')
                else:
                    call_successful = (status in ['done', 'completed'])
                
                main_language = details.get('language') or details.get('detected_language')
                transcript_summary = (
                    details.get('transcript_summary') or
                    details.get('call_summary_title') or
                    (details.get('analysis', {}).get('summary') if isinstance(details.get('analysis'), dict) else None)
                )
                
                raw_metadata = json.dumps(details)
            else:
                # Fallback to minimal webhook data
                agent_name = location_name
                call_start_time = convert_timestamp(event_timestamp)
                call_accepted_time = None
                call_duration_secs = None
                message_count = 0
                status = 'webhook_only'
                call_successful = True
                main_language = None
                transcript_summary = None
                raw_metadata = json.dumps(payload_json)
                transcript = []
            
            # Insert conversation record
            location_conversation_id = None
            
            conn.rollback()  # Start fresh transaction
            
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO location_conversations (
                            tenant_id, location_id, eleven_conversation_id, eleven_agent_id,
                            agent_name, call_start_time, call_accepted_time, call_duration_secs,
                            message_count, status, call_successful, main_language,
                            transcript_summary, audio_r2_path, raw_metadata
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING location_conversation_id
                    """, (
                        tenant_id, location_id, conversation_id, agent_id,
                        agent_name, call_start_time, call_accepted_time, call_duration_secs,
                        message_count, status, call_successful, main_language,
                        transcript_summary, audio_r2_path, raw_metadata
                    ))
                    
                    location_conversation_id = cur.fetchone()[0]
                
                print(f"✅ Inserted conversation: location_conversation_id={location_conversation_id}")
                
                # Insert transcript details if available
                if transcript and location_conversation_id:
                    print(f"[Step 7] Inserting {len(transcript)} transcript messages")
                    
                    with conn.cursor() as cur:
                        for idx, message in enumerate(transcript):
                            role = message.get('role', 'unknown')
                            time_in_call_secs = message.get('time_in_call_secs') or message.get('timestamp')
                            message_text = message.get('message') or message.get('text') or message.get('content')
                            
                            tool_calls = json.dumps(message.get('tool_calls')) if message.get('tool_calls') else None
                            tool_results = json.dumps(message.get('tool_results')) if message.get('tool_results') else None
                            llm_override = message.get('llm_override')
                            
                            conversation_turn_metrics = message.get('metrics') or message.get('turn_metrics')
                            if conversation_turn_metrics:
                                conversation_turn_metrics = json.dumps(conversation_turn_metrics)
                            
                            rag_retrieval_info = message.get('rag_info') or message.get('rag_retrieval')
                            if rag_retrieval_info:
                                rag_retrieval_info = json.dumps(rag_retrieval_info)
                            
                            cur.execute("""
                                INSERT INTO location_conversation_details (
                                    location_conversation_id, message_index, role, time_in_call_secs,
                                    message, tool_calls, tool_results, llm_override,
                                    conversation_turn_metrics, rag_retrieval_info
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                location_conversation_id, idx, role, time_in_call_secs,
                                message_text, tool_calls, tool_results, llm_override,
                                conversation_turn_metrics, rag_retrieval_info
                            ))
                    
                    print(f"✅ Inserted {len(transcript)} transcript messages")
                
                # Step 8: Process billing (post-call usage recording)
                print(f"\n[Step 8] Processing billing")
                
                # Normalize call duration to an integer number of seconds
                call_seconds = None
                if call_duration_secs is not None:
                    try:
                        # Handle int, float, or string values consistently
                        call_seconds = int(float(call_duration_secs))
                    except (TypeError, ValueError):
                        call_seconds = None
                
                if not call_seconds or call_seconds <= 0:
                    print(f"[Billing] Skipping billing: no valid call_duration_secs for conversation {conversation_id}")
                else:
                    call_secs = call_seconds
                    
                    # Check if this conversation was already billed (idempotency)
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT 1 
                            FROM billing_minute_ledger
                            WHERE tenant_id = %s 
                              AND location_conversation_id = %s 
                              AND source = 'call_usage'
                            LIMIT 1
                        """, (tenant_id, location_conversation_id))
                        
                        already_billed = cur.fetchone()
                    
                    if already_billed:
                        print(f"[Billing] Skipping billing: call_usage already recorded for location_conversation_id={location_conversation_id}")
                    else:
                        # Query current balances
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT plan_seconds_balance, package_seconds_balance
                                FROM tenant_total_seconds_balance
                                WHERE tenant_id = %s
                            """, (tenant_id,))
                            
                            balance_row = cur.fetchone()
                        
                        if balance_row:
                            plan_balance, package_balance = balance_row
                        else:
                            plan_balance, package_balance = 0, 0
                            print(f"[Billing] No balance row for tenant {tenant_id}, using plan=0, package=0")
                        
                        # Ensure non-negative
                        plan_balance = max(plan_balance or 0, 0)
                        package_balance = max(package_balance or 0, 0)
                        
                        print(f"[Billing] Tenant {tenant_id} balances: plan={plan_balance}s, package={package_balance}s")
                        
                        # Split usage: consume plan pool first, then package pool
                        call_secs = call_seconds
                        
                        # Consume plan pool first
                        plan_use = min(call_secs, plan_balance)
                        remaining = call_secs - plan_use
                        
                        # Consume package pool second
                        package_use = min(remaining, package_balance)
                        
                        # Calculate unbilled leftover
                        leftover = call_secs - plan_use - package_use
                        
                        print(f"[Billing] Tenant {tenant_id}, conv {location_conversation_id}: call={call_secs}s, plan_use={plan_use}s, package_use={package_use}s, leftover={leftover}s")
                        
                        # Insert call_usage rows into billing_minute_ledger
                        with conn.cursor() as cur:
                            # Insert plan usage (if any)
                            if plan_use > 0:
                                cur.execute("""
                                    INSERT INTO billing_minute_ledger (
                                        tenant_id,
                                        location_conversation_id,
                                        source,
                                        usage_bucket,
                                        seconds_delta
                                    ) VALUES (%s, %s, 'call_usage', 'plan', %s)
                                """, (tenant_id, location_conversation_id, -plan_use))
                                
                                print(f"[Billing] Inserted plan usage: -{plan_use}s")
                            
                            # Insert package usage (if any)
                            if package_use > 0:
                                cur.execute("""
                                    INSERT INTO billing_minute_ledger (
                                        tenant_id,
                                        location_conversation_id,
                                        source,
                                        usage_bucket,
                                        seconds_delta
                                    ) VALUES (%s, %s, 'call_usage', 'package', %s)
                                """, (tenant_id, location_conversation_id, -package_use))
                                
                                print(f"[Billing] Inserted package usage: -{package_use}s")
                        
                        # Warn about unbilled time (but don't fail)
                        if leftover > 0:
                            print(f"[Billing] WARNING: {leftover}s of call time not covered by balance (no overage handler in this webhook)")
                        
                        print(f"✅ Billing processed successfully")
                
                # Commit transaction
                conn.commit()
                print(f"✅ Transaction committed successfully")
                
            except Exception as e:
                conn.rollback()
                print(f"❌ Database insert failed: {e}")
                import traceback
                traceback.print_exc()
                raise
            
            print("=" * 80)
            print(f"✅ Webhook processed successfully")
            print(f"   Conversation ID: {conversation_id}")
            print(f"   Location Conversation ID: {location_conversation_id}")
            print(f"   Audio uploaded: {bool(audio_r2_path)}")
            print(f"   Transcript messages: {len(transcript) if transcript else 0}")
            print("=" * 80)
            
            return jsonify({
                'success': True,
                'message': 'Conversation processed successfully',
                'conversation_id': conversation_id,
                'location_conversation_id': location_conversation_id,
                'audio_uploaded': bool(audio_r2_path),
                'transcript_messages': len(transcript) if transcript else 0
            }), 200
            
        finally:
            conn.close()
            
    except Exception as e:
        print(f"❌ Fatal error processing webhook: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 80)
        
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500
