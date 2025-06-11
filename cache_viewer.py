from flask import Flask, request, jsonify, render_template, redirect, url_for
from werkzeug.utils import secure_filename
import redis
import os
from dotenv import load_dotenv
import json
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

app = Flask(__name__)

# Redis setup
REDIS_URL = os.getenv("REDIS_URL")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# ----------------------------
# Home Page with 3 Buttons
# ----------------------------
@app.route("/")
def home():
    return render_template("home.html")


# ----------------------------
# Cache Viewer Page
# ----------------------------
@app.route("/cache")
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


# Allowed extension check
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() == "csv"

@app.route("/venue", methods=["GET", "POST"])
def venue_generator():
    from io import StringIO
    import csv

    db_url = os.getenv("DATABASE_URL")
    selected_location = None
    locations = []
    availabilities = []
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
            # Step 1: load location dropdown (filtered by type = 'rest')
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

            # Step 2: Display recurring availability for selected location
            elif step == "2":
                tenant_id = request.form.get("tenant_id")
                location_id = request.form.get("location_id")
                selected_location = {
                    "tenant_id": tenant_id,
                    "location_id": location_id
                }

                cur.execute("""
                    SELECT day_of_week, start_time, end_time, is_closed
                    FROM location_availability
                    WHERE tenant_id = %s AND location_id = %s
                      AND type = 'recurring' AND is_active = true
                    ORDER BY day_of_week, start_time
                """, (tenant_id, location_id))
                availabilities = cur.fetchall()

                return render_template(
                    "venue.html",
                    step=2,
                    locations=locations,
                    selected_location=selected_location,
                    availabilities=availabilities
                )

            # Step 3: Upload and parse CSV
            elif step == "3":
                tenant_id = request.form.get("tenant_id")
                location_id = request.form.get("location_id")
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
                                parsed_rows.append(row)

                            success_message = f"Uploaded {len(parsed_rows)} venue rows successfully."
                        except Exception as e:
                            error_message = f"Failed to read CSV: {str(e)}"

                return render_template(
                    "venue.html",
                    step=3,
                    selected_location=selected_location,
                    parsed_rows=parsed_rows,
                    error_message=error_message,
                    success_message=success_message
                )

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
