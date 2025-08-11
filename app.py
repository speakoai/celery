import os
import secrets
from functools import wraps
from flask import Flask, flash, render_template, redirect, request, jsonify
from tasks.demo_task import add
from tasks.availability_gen_regen import gen_availability, gen_availability_venue
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
    Generate staff availability for a location
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
        task = gen_availability.delay(tenant_id, location_id, location_tz, affected_date)
        
        return jsonify({
            'task_id': task.id,
            'status': 'pending',
            'message': 'Availability generation task started',
            'tenant_id': tenant_id,
            'location_id': location_id,
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
