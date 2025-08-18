import os
import secrets
from functools import wraps
from flask import Flask, flash, render_template, redirect, request, jsonify
from tasks.demo_task import add
from tasks.availability_gen_regen import gen_availability, gen_availability_venue
from tasks.sms import (
    send_sms_confirmation_new, send_sms_confirmation_mod, send_sms_confirmation_can,
    send_email_confirmation_new, send_email_confirmation_mod_rest, send_email_confirmation_mod,
    send_email_confirmation_can_rest, send_email_confirmation_can
)
from tasks.celery_app import app as celery_app

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

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


@app.route('/api/sms/send', methods=['POST'])
@require_api_key
def api_send_sms():
    """
    Send SMS notification and email notification for booking actions
    Expected JSON payload:
    {
        "booking_id": 123,
        "action": "new" | "modify" | "cancel",
        "business_type": "service" | "rest",
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
            # SMS task
            sms_task = send_sms_confirmation_new.delay(booking_id)
            tasks.append({
                'task_id': sms_task.id,
                'type': 'sms',
                'description': 'new booking SMS confirmation'
            })
            
            # Email task (same for both business types)
            email_task = send_email_confirmation_new.delay(booking_id)
            tasks.append({
                'task_id': email_task.id,
                'type': 'email',
                'description': 'new booking email confirmation'
            })
            
            action_description = 'new booking confirmation'
            
        elif action == 'modify':
            # SMS task
            sms_task = send_sms_confirmation_mod.delay(booking_id)
            tasks.append({
                'task_id': sms_task.id,
                'type': 'sms',
                'description': 'booking modification SMS confirmation'
            })
            
            # Email task (different based on business type)
            if business_type == 'rest':
                email_task = send_email_confirmation_mod_rest.delay(booking_id, original_booking_id)
                email_description = 'restaurant booking modification email confirmation'
            else:  # business_type == 'service'
                email_task = send_email_confirmation_mod.delay(booking_id, original_booking_id)
                email_description = 'service booking modification email confirmation'
            
            tasks.append({
                'task_id': email_task.id,
                'type': 'email',
                'description': email_description
            })
            
            action_description = 'booking modification confirmation'
            
        else:  # action == 'cancel'
            # SMS task
            sms_task = send_sms_confirmation_can.delay(booking_id)
            tasks.append({
                'task_id': sms_task.id,
                'type': 'sms',
                'description': 'booking cancellation SMS confirmation'
            })
            
            # Email task (different based on business type)
            if business_type == 'rest':
                email_task = send_email_confirmation_can_rest.delay(booking_id)
                email_description = 'restaurant booking cancellation email confirmation'
            else:  # business_type == 'service'
                email_task = send_email_confirmation_can.delay(booking_id)
                email_description = 'service booking cancellation email confirmation'
            
            tasks.append({
                'task_id': email_task.id,
                'type': 'email',
                'description': email_description
            })
            
            action_description = 'booking cancellation confirmation'
        
        response = {
            'message': f'{action_description.title()} tasks started',
            'booking_id': booking_id,
            'action': action,
            'business_type': business_type,
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
