from flask import Flask, request, jsonify, render_template, redirect, url_for
from werkzeug.utils import secure_filename
import redis
import os
from dotenv import load_dotenv
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from html import unescape
from tasks.availability_gen_regen import gen_availability, gen_availability_venue
from functools import wraps
import boto3
import uuid
import hashlib
import re
import time
from datetime import datetime

load_dotenv()

app = Flask(__name__)

# Redis setup
REDIS_URL = os.getenv("REDIS_URL")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# R2 (Cloudflare) setup
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")

# Initialize R2 client
r2_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name='auto'
) if all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]) else None

# Allowed IPs configuration
ALLOWED_IPS = os.getenv("ALLOWED_IPS", "").split(",") if os.getenv("ALLOWED_IPS") else []

# Decorator to restrict access by IP
def restrict_ip(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Get client IP, accounting for proxies
        client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        # X-Forwarded-For may contain multiple IPs (client, proxies); take the first (client)
        if client_ip:
            client_ip = client_ip.split(',')[0].strip()
        
        if not ALLOWED_IPS:  # Allow all if ALLOWED_IPS is empty
            return f(*args, **kwargs)
        if client_ip in ALLOWED_IPS:
            return f(*args, **kwargs)
        else:
            return jsonify({"error": f"Access denied for IP: {client_ip}"}), 403
    return decorated_function

# ----------------------------
# Home Page with 3 Buttons
# ----------------------------
@app.route("/")
@restrict_ip
def home():
    return render_template("home.html")

# ----------------------------
# Cache Viewer Page
# ----------------------------
@app.route("/cache")
@restrict_ip
def cache_viewer():
    tenant_id = request.args.get("tenant_id", "")
    location_id = request.args.get("location_id", "")
    start_date = request.args.get("start_date", "")
    value = None
    key = None

    if tenant_id and location_id:
        key = construct_key(tenant_id, location_id, start_date if start_date else None)
        raw_value = redis_client.get(key)
        if raw_value:
            try:
                parsed = json.loads(raw_value)
                value = json.dumps(parsed, indent=2, ensure_ascii=False)
            except json.JSONDecodeError:
                value = raw_value

    return render_template("cache_viewer.html", tenant_id=tenant_id, location_id=location_id,
                           start_date=start_date, key=key, value=value)

# ----------------------------
# Cache API Endpoint (optional)
# ----------------------------
@app.route("/api")
@restrict_ip
def api():
    tenant_id = request.args.get("tenant_id")
    location_id = request.args.get("location_id")
    start_date = request.args.get("start_date")

    if not tenant_id or not location_id:
        return jsonify({"error": "Missing ?tenant_id= or ?location_id= parameter"}), 400

    key = construct_key(tenant_id, location_id, start_date)
    value = redis_client.get(key)

    if value:
        return jsonify({"key": key, "value": value})
    return jsonify({"error": "Key not found"}), 404

# ----------------------------
# Get Template Availability
# ----------------------------
@app.route("/get_template_availability")
@restrict_ip
def get_template_availability():
    tenant_id = request.args.get("tenant_id")
    template_id = request.args.get("template_id")

    if not tenant_id or not template_id:
        return jsonify({"error": "Missing tenant_id or template_id parameter"}), 400

    try:
        with psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT type, day_of_week, specific_date, start_time, end_time, is_active, service_duration
                    FROM availability_template_details
                    WHERE tenant_id = %s AND template_id = %s AND is_active = true
                    ORDER BY type, day_of_week, specific_date, start_time
                """, (tenant_id, template_id))
                availabilities = cur.fetchall()
                # Convert time and date objects to strings for JSON serialization
                serialized_availabilities = [
                    {
                        "type": avail["type"],
                        "day_of_week": avail["day_of_week"],
                        "specific_date": avail["specific_date"].strftime("%Y-%m-%d") if avail["specific_date"] else None,
                        "start_time": avail["start_time"].strftime("%H:%M:%S"),
                        "end_time": avail["end_time"].strftime("%H:%M:%S"),
                        "is_active": avail["is_active"],
                        "service_duration": avail["service_duration"]
                    }
                    for avail in availabilities
                ]
                return jsonify({"availabilities": serialized_availabilities})
    except Exception as e:
        return jsonify({"error": f"Failed to fetch template availability: {str(e)}"}), 500

# ----------------------------
# Availability Regeneration Route
# ----------------------------
@app.route("/availability", methods=["GET", "POST"])
@restrict_ip
def availability():
    db_url = os.getenv("DATABASE_URL")
    locations = []
    result = None

    with psycopg2.connect(db_url, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            # Fetch active locations
            cur.execute("""
                SELECT tenant_id, location_id, name, location_type, timezone
                FROM locations
                WHERE is_active = true
                ORDER BY name ASC
            """)
            locations = cur.fetchall()

    if request.method == "POST" and request.form.get("action") == "regenerate":
        tenant_id = request.form.get("tenant_id")
        location_id = request.form.get("location_id")
        location_type = request.form.get("location_type")
        timezone = request.form.get("timezone")

        if not tenant_id or not location_id or not location_type or not timezone:
            result = None  # Trigger error display
        else:
            try:
                # Convert tenant_id and location_id to integers
                tenant_id = int(tenant_id)
                location_id = int(location_id)
                # Execute appropriate function based on location_type
                if location_type == "rest":
                    result = gen_availability_venue(tenant_id=tenant_id, location_id=location_id, location_tz=timezone)
                else:
                    result = gen_availability(tenant_id=tenant_id, location_id=location_id, location_tz=timezone)
            except Exception as e:
                result = None  # Treat exceptions as failure

    return render_template("availability.html", locations=locations, result=result)

# Allowed extension check
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() == "csv"

@app.route("/venue", methods=["GET", "POST"])
@restrict_ip
def venue_generator():
    from io import StringIO
    import csv

    db_url = os.getenv("DATABASE_URL")
    selected_location = None
    locations = []
    availabilities = []
    templates = []
    parsed_rows = []
    error_message = ""
    success_message = ""

    # Determine current step
    if request.method == "POST":
        step = request.form.get("step", "1")
    else:
        step = "1"

    with psycopg2.connect(db_url, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            # Load locations
            cur.execute("""
                SELECT tenant_id, location_id, name
                FROM locations
                WHERE is_active = true AND location_type = 'rest'
                ORDER BY name ASC
            """)
            locations = cur.fetchall()

            # Step 1: Just render dropdown
            if step == "1":
                return render_template("venue.html", step=1, locations=locations)

            # Step 2: Display all availability or template options
            elif step == "2":
                tenant_id = request.form.get("tenant_id")
                location_id = request.form.get("location_id")
                selected_location = {
                    "tenant_id": tenant_id,
                    "location_id": location_id
                }

                # Fetch location availability (all types)
                cur.execute("""
                    SELECT type, day_of_week, specific_date, start_time, end_time, is_closed, service_duration
                    FROM location_availability
                    WHERE tenant_id = %s AND location_id = %s AND is_active = true
                    ORDER BY type, day_of_week, specific_date, start_time
                """, (tenant_id, location_id))
                availabilities = cur.fetchall()

                # Fetch availability templates for tenant
                cur.execute("""
                    SELECT template_id, template_name
                    FROM availability_template
                    WHERE tenant_id = %s
                    ORDER BY template_name ASC
                """, (tenant_id,))
                templates = cur.fetchall()

                return render_template(
                    "venue.html",
                    step=2,
                    locations=locations,
                    selected_location=selected_location,
                    availabilities=availabilities,
                    templates=templates
                )

            # Step 3: Upload and parse CSV
            elif step == "3":
                tenant_id = request.form.get("tenant_id")
                location_id = request.form.get("location_id")
                availability_source = request.form.get("availability_source", "location")
                selected_template_id = request.form.get("selected_template_id", "")
                selected_location = {
                    "tenant_id": tenant_id,
                    "location_id": location_id
                }

                if "file" in request.files:
                    file = request.files["file"]
                    if file.filename == "":
                        error_message = "No file selected."
                    elif not file.filename.lower().endswith(".csv"):
                        error_message = "Only CSV files are allowed."
                    else:
                        try:
                            content = file.read().decode("utf-8")
                            stream = StringIO(content)
                            reader = csv.DictReader(stream)
                            for row in reader:
                                cleaned_row = {k.strip().lower().replace('\ufeff', ''): v.strip() for k, v in row.items()}
                                parsed_rows.append(cleaned_row)
                            success_message = f"Uploaded {len(parsed_rows)} venue rows successfully."
                        except Exception as e:
                            error_message = f"Failed to read CSV: {str(e)}"

                return render_template(
                    "venue.html",
                    step=3,
                    selected_location=selected_location,
                    parsed_rows=parsed_rows,
                    error_message=error_message,
                    success_message=success_message,
                    availability_source=availability_source,
                    selected_template_id=selected_template_id
                )

            # Step 4: Display uploaded result
            elif step == "4" and request.form.get("action") == "generate":
                tenant_id = request.form.get("tenant_id")
                location_id = request.form.get("location_id")
                availability_source = request.form.get("availability_source", "location")
                selected_template_id = request.form.get("selected_template_id", "")
                selected_location = {
                    "tenant_id": tenant_id,
                    "location_id": location_id
                }

                # Reconstruct parsed rows
                from flask import Markup
                import json

                csv_rows = request.form.getlist("csv_data")
                parsed_rows = [json.loads(unescape(row)) for row in csv_rows]

                inserted_units = []

                with psycopg2.connect(db_url, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        # Fetch availabilities based on source
                        if availability_source == "template" and selected_template_id:
                            cur.execute("""
                                SELECT type, day_of_week, specific_date, start_time, end_time, is_active, service_duration
                                FROM availability_template_details
                                WHERE tenant_id = %s AND template_id = %s AND is_active = true
                                ORDER BY type, day_of_week, specific_date, start_time
                            """, (tenant_id, selected_template_id))
                            availabilities = cur.fetchall()
                        else:
                            cur.execute("""
                                SELECT type, day_of_week, specific_date, start_time, end_time, is_closed, service_duration
                                FROM location_availability
                                WHERE tenant_id = %s AND location_id = %s AND is_active = true
                                ORDER BY type, day_of_week, specific_date, start_time
                            """, (tenant_id, location_id))
                            availabilities = cur.fetchall()

                        for row in parsed_rows:
                            # Insert into venue_unit
                            cur.execute("""
                                INSERT INTO venue_unit (tenant_id, location_id, name, venue_unit_type, capacity, min_capacity)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                RETURNING venue_unit_id
                            """, (
                                tenant_id,
                                location_id,
                                row["name"],
                                row["venue_unit_type"],
                                int(row["capacity"]),
                                int(row["min_capacity"])
                            ))
                            venue_unit_id = cur.fetchone()["venue_unit_id"]
                            inserted_units.append({**row, "venue_unit_id": venue_unit_id})

                            # Create availability for this unit
                            for avail in availabilities:
                                # Use service_duration from the source, defaulting to 60 if NULL
                                service_duration = avail.get("service_duration", 60) or 60
                                cur.execute("""
                                    INSERT INTO venue_availability (
                                        tenant_id, venue_unit_id, location_id, type,
                                        day_of_week, specific_date, start_time, end_time,
                                        is_active, service_duration
                                    ) VALUES (
                                        %s, %s, %s, %s,
                                        %s, %s, %s, %s,
                                        true, %s
                                    )
                                """, (
                                    tenant_id,
                                    venue_unit_id,
                                    location_id,
                                    avail["type"],
                                    avail["day_of_week"],
                                    avail.get("specific_date"),
                                    avail["start_time"],
                                    avail["end_time"],
                                    service_duration
                                ))

                    conn.commit()

                return render_template("venue.html",
                                    step=4,
                                    selected_location=selected_location,
                                    inserted_units=inserted_units)

# ----------------------------
# Voice Agent Creator Page
# ----------------------------
@app.route("/agent", methods=["GET", "POST"])
@restrict_ip
def voice_agent_creator():
    if request.method == "POST":
        # Handle form submission for creating voice agents
        # This is a placeholder for future implementation
        agent_name = request.form.get("agent_name", "")
        agent_voice = request.form.get("agent_voice", "")
        agent_prompt = request.form.get("agent_prompt", "")
        
        # For now, just return success message
        success_message = f"Voice agent '{agent_name}' created successfully!"
        return render_template("agent.html", success_message=success_message)
    
    return render_template("agent.html")

# Helper functions for booking page
def allowed_image_file(filename):
    """Check if file is an allowed image type"""
    allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

def validate_alias(alias):
    """Validate alias format: alphanumeric + hyphens only"""
    import re
    if not alias:
        return False, "Alias is required"
    if not re.match(r'^[a-zA-Z0-9-]+$', alias):
        return False, "Alias can only contain letters, numbers, and hyphens"
    if len(alias) < 3:
        return False, "Alias must be at least 3 characters long"
    if len(alias) > 50:
        return False, "Alias must be less than 50 characters long"
    return True, ""

def generate_booking_filename(tenant_id, location_id, file_type, file_extension):
    """Generate versioned filename for booking page files"""
    import hashlib
    import time
    # Create hash from tenant_id, location_id, file_type, and current timestamp
    hash_input = f"{tenant_id}_{location_id}_{file_type}_{int(time.time())}"
    file_hash = hashlib.md5(hash_input.encode()).hexdigest()[:8]
    return f"{tenant_id}_{location_id}_{file_type}_{file_hash}{file_extension}"

def upload_to_r2(file, tenant_id, location_id, file_type):
    """Upload file to R2 and return URL"""
    if not r2_client:
        raise Exception("R2 storage not configured properly")
    
    # Validate file
    if not allowed_image_file(file.filename):
        raise Exception("Only image files (PNG, JPG, JPEG, GIF, WEBP) are allowed")
    
    # Check file size (5MB limit)
    file.seek(0, 2)  # Seek to end
    file_size = file.tell()
    file.seek(0)  # Reset to beginning
    
    if file_size > 5 * 1024 * 1024:  # 5MB
        raise Exception("File size must be less than 5MB")
    
    # Generate filename
    file_extension = os.path.splitext(secure_filename(file.filename))[1].lower()
    unique_filename = generate_booking_filename(tenant_id, location_id, file_type, file_extension)
    
    # Define R2 path
    file_key = f"booking_pages/{file_type}s/{unique_filename}"
    
    # Get file content
    file_content = file.read()
    content_type = file.content_type or 'application/octet-stream'
    
    # Upload to R2
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=file_key,
        Body=file_content,
        ContentType=content_type,
        Metadata={
            'original_filename': file.filename,
            'upload_timestamp': datetime.now().isoformat(),
            'tenant_id': str(tenant_id),
            'location_id': str(location_id),
            'file_type': file_type
        }
    )
    
    # Generate public URL using custom domain
    file_url = f"https://assets.speako.ai/{file_key}"
    
    return {
        'url': file_url,
        'key': file_key,
        'filename': unique_filename,
        'size': len(file_content)
    }

# ----------------------------
# Booking Page with File Upload to R2
# ----------------------------
@app.route("/booking_page", methods=["GET", "POST"])
@restrict_ip
def booking_page():
    db_url = os.getenv("DATABASE_URL")
    
    # Initialize variables
    locations = []
    selected_location = None
    existing_booking_page = None
    error_message = ""
    success_message = ""
    uploaded_files = {}
    
    # Determine current step
    if request.method == "POST":
        step = request.form.get("step", "1")
    else:
        step = "1"
    
    with psycopg2.connect(db_url, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            # Load all active locations
            cur.execute("""
                SELECT tenant_id, location_id, name, location_type
                FROM locations 
                WHERE is_active = true 
                ORDER BY name ASC
            """)
            locations = cur.fetchall()
            
            # Step 1: Location selection
            if step == "1":
                return render_template("booking_page.html", 
                                     step=1, 
                                     locations=locations)
            
            # Step 2: Show booking page form (create or update)
            elif step == "2":
                tenant_id = request.form.get("tenant_id")
                location_id = request.form.get("location_id")
                
                if not tenant_id or not location_id:
                    error_message = "Please select a location"
                    return render_template("booking_page.html", 
                                         step=1, 
                                         locations=locations,
                                         error_message=error_message)
                
                selected_location = {
                    "tenant_id": int(tenant_id),
                    "location_id": int(location_id)
                }
                
                # Get location name
                location_info = next((loc for loc in locations if 
                                    loc['tenant_id'] == selected_location['tenant_id'] and 
                                    loc['location_id'] == selected_location['location_id']), None)
                if location_info:
                    selected_location['name'] = location_info['name']
                
                # Check if booking page already exists
                cur.execute("""
                    SELECT alias, logo_url, banner_url, created_at
                    FROM booking_page 
                    WHERE tenant_id = %s AND location_id = %s AND is_active = true
                """, (tenant_id, location_id))
                existing_booking_page = cur.fetchone()
                
                return render_template("booking_page.html",
                                     step=2,
                                     locations=locations,
                                     selected_location=selected_location,
                                     existing_booking_page=existing_booking_page)
            
            # Step 3: Process form submission and file uploads
            elif step == "3":
                tenant_id = int(request.form.get("tenant_id"))
                location_id = int(request.form.get("location_id"))
                alias = request.form.get("alias", "").strip()
                is_update = request.form.get("is_update") == "true"
                
                selected_location = {
                    "tenant_id": tenant_id,
                    "location_id": location_id
                }
                
                # Get location name
                location_info = next((loc for loc in locations if 
                                    loc['tenant_id'] == tenant_id and 
                                    loc['location_id'] == location_id), None)
                if location_info:
                    selected_location['name'] = location_info['name']
                
                try:
                    # For new booking pages, validate alias
                    if not is_update:
                        is_valid, validation_error = validate_alias(alias)
                        if not is_valid:
                            raise Exception(validation_error)
                        
                        # Check if alias already exists
                        cur.execute("SELECT alias FROM booking_page WHERE LOWER(alias) = LOWER(%s)", (alias,))
                        if cur.fetchone():
                            raise Exception("Alias already exists. Please choose a different one.")
                    
                    # Process file uploads
                    logo_result = None
                    banner_result = None
                    
                    if 'logo_file' in request.files and request.files['logo_file'].filename:
                        logo_result = upload_to_r2(request.files['logo_file'], tenant_id, location_id, 'logo')
                        uploaded_files['logo'] = logo_result
                    
                    if 'banner_file' in request.files and request.files['banner_file'].filename:
                        banner_result = upload_to_r2(request.files['banner_file'], tenant_id, location_id, 'banner')
                        uploaded_files['banner'] = banner_result
                    
                    # Database operations
                    if is_update:
                        # Update existing booking page
                        update_fields = []
                        update_values = []
                        
                        if logo_result:
                            update_fields.append("logo_url = %s")
                            update_values.append(logo_result['url'])
                        
                        if banner_result:
                            update_fields.append("banner_url = %s")
                            update_values.append(banner_result['url'])
                        
                        if update_fields:
                            update_fields.append("updated_at = CURRENT_TIMESTAMP")
                            update_values.extend([tenant_id, location_id])
                            
                            update_query = f"""
                                UPDATE booking_page 
                                SET {', '.join(update_fields)}
                                WHERE tenant_id = %s AND location_id = %s AND is_active = true
                            """
                            cur.execute(update_query, update_values)
                            
                        # Get updated booking page
                        cur.execute("""
                            SELECT alias, logo_url, banner_url
                            FROM booking_page 
                            WHERE tenant_id = %s AND location_id = %s AND is_active = true
                        """, (tenant_id, location_id))
                        existing_booking_page = cur.fetchone()
                        
                        success_message = "Booking page updated successfully!"
                        
                    else:
                        # Create new booking page
                        cur.execute("""
                            INSERT INTO booking_page (
                                alias, tenant_id, location_id, logo_url, banner_url, is_active
                            ) VALUES (%s, %s, %s, %s, %s, true)
                        """, (
                            alias,
                            tenant_id, 
                            location_id,
                            logo_result['url'] if logo_result else None,
                            banner_result['url'] if banner_result else None
                        ))
                        
                        existing_booking_page = {
                            'alias': alias,
                            'logo_url': logo_result['url'] if logo_result else None,
                            'banner_url': banner_result['url'] if banner_result else None
                        }
                        
                        success_message = f"Booking page '{alias}' created successfully!"
                    
                    conn.commit()
                    
                except Exception as e:
                    conn.rollback()
                    error_message = str(e)
                
                return render_template("booking_page.html",
                                     step=3,
                                     locations=locations,
                                     selected_location=selected_location,
                                     existing_booking_page=existing_booking_page,
                                     uploaded_files=uploaded_files,
                                     error_message=error_message,
                                     success_message=success_message)

# ----------------------------
# Helper Function
# ----------------------------
def construct_key(tenant_id, location_id, start_date=None):
    key = f"availability:tenant_{tenant_id}:location_{location_id}"
    if start_date:
        key += f":start_date_{start_date}"
    return key

# Helper function for direct booking uploads
def upload_file_to_booking_uploads(file):
    """Upload file directly to booking_uploads folder in R2"""
    if not r2_client:
        raise Exception("R2 storage not configured properly")
    
    # Validate file
    if not allowed_image_file(file.filename):
        raise Exception("Only image files (PNG, JPG, JPEG, GIF, WEBP) are allowed")
    
    # Check file size (5MB limit)
    file.seek(0, 2)  # Seek to end
    file_size = file.tell()
    file.seek(0)  # Reset to beginning
    
    if file_size > 5 * 1024 * 1024:  # 5MB
        raise Exception("File size must be less than 5MB")
    
    # Generate unique filename
    file_extension = os.path.splitext(secure_filename(file.filename))[1].lower()
    timestamp = int(time.time())
    file_hash = hashlib.md5(f"{file.filename}_{timestamp}".encode()).hexdigest()[:8]
    unique_filename = f"{timestamp}_{file_hash}_{secure_filename(file.filename)}"
    
    # Define R2 path
    file_key = f"booking_uploads/{unique_filename}"
    
    # Get file content
    file_content = file.read()
    content_type = file.content_type or 'application/octet-stream'
    
    # Upload to R2
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=file_key,
        Body=file_content,
        ContentType=content_type,
        Metadata={
            'original_filename': file.filename,
            'upload_timestamp': datetime.now().isoformat()
        }
    )
    
    # Generate public URL using custom domain
    file_url = f"https://assets.speako.ai/{file_key}"
    
    return {
        'url': file_url,
        'key': file_key,
        'filename': unique_filename,
        'original_filename': file.filename,
        'size': len(file_content)
    }

# ----------------------------
# Booking Uploads - Direct File Upload to R2
# ----------------------------
@app.route("/booking_uploads", methods=["GET", "POST"])
@restrict_ip
def booking_uploads():
    error_message = ""
    success_message = ""
    uploaded_files = []
    
    if request.method == "POST":
        try:
            # Get all uploaded files
            files = request.files.getlist('upload_files')
            
            if not files or all(f.filename == '' for f in files):
                raise Exception("Please select at least one file to upload")
            
            # Filter out empty files and check limit
            valid_files = [f for f in files if f.filename != '']
            if len(valid_files) > 5:
                raise Exception("Maximum 5 files allowed per upload")
            
            # Upload each file
            for file in valid_files:
                result = upload_file_to_booking_uploads(file)
                uploaded_files.append(result)
            
            success_message = f"Successfully uploaded {len(uploaded_files)} file(s)!"
            
        except Exception as e:
            error_message = str(e)
    
    return render_template("booking_uploads.html",
                         error_message=error_message,
                         success_message=success_message,
                         uploaded_files=uploaded_files)

# ----------------------------
# App Entrypoint for Local Dev (Render uses gunicorn)
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)