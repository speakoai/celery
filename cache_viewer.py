from flask import Flask, request, jsonify, render_template, redirect, url_for
from werkzeug.utils import secure_filename
import redis
import os
from dotenv import load_dotenv
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from html import unescape
from tasks.availability import gen_availability, gen_availability_venue
from functools import wraps

load_dotenv()

app = Flask(__name__)

# Redis setup
REDIS_URL = os.getenv("REDIS_URL")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

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

# ----------------------------
# Helper Function
# ----------------------------
def construct_key(tenant_id, location_id, start_date=None):
    key = f"availability:tenant_{tenant_id}:location_{location_id}"
    if start_date:
        key += f":start_date_{start_date}"
    return key

# ----------------------------
# App Entrypoint for Local Dev (Render uses gunicorn)
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)