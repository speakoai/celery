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

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

# Import avatar API functionality
import sys
import json
from pathlib import Path

# Avatar API class for serving avatar catalog data
class AvatarAPI:
    """API class for serving avatar catalog data."""
    
    def __init__(self, catalog_file: str = "speako-dashboard-avatar/avatar_catalog.json"):
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
                return {"avatars": [], "metadata": {}}
        except Exception as e:
            print(f"Error loading avatar catalog: {str(e)}")
            return {"avatars": [], "metadata": {}}
    
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
            analysis = avatar.get("analysis", {})
            match = True
            
            # Apply filters
            if "gender" in filters and filters["gender"]:
                if analysis.get("gender", "").lower() != filters["gender"].lower():
                    match = False
            
            if "race" in filters and filters["race"]:
                if analysis.get("race", "").lower() != filters["race"].lower():
                    match = False
            
            if "occupation" in filters and filters["occupation"]:
                if analysis.get("occupation", "").lower() != filters["occupation"].lower():
                    match = False
            
            if "style" in filters and filters["style"]:
                if analysis.get("style", "").lower() != filters["style"].lower():
                    match = False
            
            if "age_group" in filters and filters["age_group"]:
                if analysis.get("age_group", "").lower() != filters["age_group"].lower():
                    match = False
            
            # Tags search
            if "tags" in filters and filters["tags"]:
                search_tags = [tag.lower() for tag in filters["tags"]]
                avatar_tags = [tag.lower() for tag in analysis.get("tags", [])]
                if not any(tag in avatar_tags for tag in search_tags):
                    match = False
            
            # Text search
            if "search" in filters and filters["search"]:
                search_term = filters["search"].lower()
                searchable_text = " ".join([
                    analysis.get("occupation", ""),
                    analysis.get("race", ""),
                    analysis.get("gender", ""),
                    analysis.get("style", ""),
                    " ".join(analysis.get("tags", [])),
                    " ".join(analysis.get("outfit", []))
                ]).lower()
                
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
        
        stats = {
            "total_avatars": len(avatars),
            "occupations": {},
            "races": {},
            "genders": {},
            "styles": {},
            "age_groups": {}
        }
        
        for avatar in avatars:
            analysis = avatar.get("analysis", {})
            
            # Count categories
            for category, key in [
                ("occupations", "occupation"),
                ("races", "race"), 
                ("genders", "gender"),
                ("styles", "style"),
                ("age_groups", "age_group")
            ]:
                value = analysis.get(key, "unknown")
                if not value or value.strip() == "":
                    value = "unknown"
                stats[category][value] = stats[category].get(value, 0) + 1
        
        return {
            "success": True,
            "data": stats
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
        return jsonify({
            "success": True,
            "status": "healthy",
            "service": "avatar-api",
            "catalog_loaded": catalog_loaded,
            "total_avatars": len(avatar_api.catalog.get("avatars", [])),
            "catalog_file": avatar_api.catalog_file,
            "custom_domain": avatar_api.catalog.get("metadata", {}).get("custom_domain", "N/A")
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "status": "unhealthy",
            "error": str(e)
        }), 500
