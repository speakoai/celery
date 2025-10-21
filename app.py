import os
import secrets
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
# Additional imports for R2 uploads
import boto3
from werkzeug.utils import secure_filename
from datetime import datetime
import hashlib
import time
# OpenAI SDK (optional)
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

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
    app.logger.info(
        f"[Startup] OpenAI client available: {_openai_client_available}; OPENAI_API_KEY set: {_openai_key_present}; model: {_openai_model}; sdk_version: {_openai_version}"
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
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON payload required'}), 400
        
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
        
        # Validate business_type
        if business_type not in ['rest', 'service']:
            return jsonify({
                'error': 'Invalid business_type',
                'message': 'business_type must be either "rest" or "service"',
                'provided': business_type
            }), 400
        
        # Route to appropriate task based on business_type
        if business_type == 'rest':
            task = gen_availability_venue.delay(tenant_id, location_id, location_tz, affected_date)
            task_type = 'venue'
        else:
            task = gen_availability.delay(tenant_id, location_id, location_tz, affected_date)
            task_type = 'staff'
        
        return jsonify({
            'task_id': task.id,
            'status': 'pending',
            'message': f'{task_type.title()} availability generation task started',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'business_type': business_type,
            'task_type': task_type,
            'is_regeneration': affected_date is not None
        }), 202
        
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
        
        # Trigger the celery task
        task = gen_availability_venue.delay(tenant_id, location_id, location_tz, affected_date)
        
        return jsonify({
            'task_id': task.id,
            'status': 'pending',
            'message': 'Venue availability generation task started',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'is_regeneration': affected_date is not None
        }), 202
        
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


@app.route('/api/knowledge/upload-knowledge-file', methods=['POST'])
@require_api_key
def api_upload_knowledge_file():
    """
    [aiknowledges] Upload a knowledge file to Cloudflare R2.

    Expected multipart/form-data:
    - tenant_id: string (required)
    - location_id: string (required)
    - knowledge_type: one of [menu, faq, policy, events] (required)
    - file: The file to upload (required). Allowed types: Word (.doc, .docx), Excel (.xls, .xlsx), PDF (.pdf), CSV (.csv), Text (.txt)

    Constraints:
    - Max file size: 5MB

    Upload path in bucket: knowledges/{tenant_id}/{location_id}/
    """
    try:
        if not r2_client:
            return jsonify({'error': 'Storage not configured', 'message': 'Cloudflare R2 credentials are missing'}), 500

        # Validate required form fields
        tenant_id = request.form.get('tenant_id')
        location_id = request.form.get('location_id')
        knowledge_type = request.form.get('knowledge_type')

        missing = [k for k in ['tenant_id', 'location_id', 'knowledge_type'] if not request.form.get(k)]
        if missing:
            return jsonify({'error': 'Missing required fields', 'missing_fields': missing}), 400

        # Validate knowledge_type
        valid_types = ['menu', 'faq', 'policy', 'events']
        if knowledge_type not in valid_types:
            return jsonify({
                'error': 'Invalid knowledge_type',
                'message': f'knowledge_type must be one of: {", ".join(valid_types)}',
                'provided': knowledge_type
            }), 400

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
        }

        # ----------------------------
        # Optional: Analyze with OpenAI
        # ----------------------------
        analysis_requested = True  # set to True by default; adjust if you want to gate it later
        if analysis_requested:
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key or OpenAI is None:
                # Log why it was skipped without exposing secrets
                try:
                    app.logger.warning(
                        f"[Analysis] Skipped OpenAI analysis. client_available={OpenAI is not None}; key_present={bool(api_key)}"
                    )
                except Exception:
                    pass
                response_data['analysis'] = {
                    'status': 'skipped',
                    'reason': 'OpenAI not configured'
                }
            else:
                try:
                    client = OpenAI(api_key=api_key)
                    # Upload file to OpenAI Files API
                    uploaded = client.files.create(file=(unique_filename, file_content))

                    prompt = build_knowledge_prompt(knowledge_type)
                    # Use Responses API to attach the file
                    resp = client.responses.create(
                        model=os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini'),
                        input=[
                            {
                                "role": "user",
                                "content": [
                                    {"type": "input_text", "text": prompt},
                                    {"type": "input_file", "file_id": uploaded.id}
                                ]
                            }
                        ],
                        temperature=0.2
                    )
                    # Extract text output
                    analysis_text = resp.output_text if hasattr(resp, 'output_text') else (resp.choices[0].message.content if getattr(resp, 'choices', None) else None)

                    # Try to parse JSON
                    import json as _json
                    parsed = None
                    if analysis_text:
                        # In case the model wraps JSON in code fences
                        txt = analysis_text.strip()
                        if txt.startswith('```'):
                            txt = txt.strip('`')
                            # remove potential language hint like json\n
                            first_nl = txt.find('\n')
                            if first_nl != -1:
                                txt = txt[first_nl+1:]
                        try:
                            parsed = _json.loads(txt)
                        except Exception:
                            parsed = None

                    response_data['analysis'] = {
                        'status': 'success' if parsed else 'raw',
                        'model': os.getenv('OPENAI_KNOWLEDGE_MODEL', 'gpt-4o-mini'),
                        'file_id': uploaded.id,
                        'result': parsed if parsed is not None else analysis_text
                    }
                except Exception as ae:
                    response_data['analysis'] = {
                        'status': 'error',
                        'message': str(ae)
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
