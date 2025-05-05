# cache_viewer.py

from flask import Flask, request, jsonify, render_template_string
import redis
import os
from dotenv import load_dotenv
import json

load_dotenv()

app = Flask(__name__)
REDIS_URL = os.getenv("REDIS_URL")

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><title>Cache Viewer</title></head>
<body>
  <h1>Redis Cache Viewer</h1>
  <form method="get">
    Tenant ID: <input type="text" name="tenant_id" value="{{ tenant_id }}">
    Location ID: <input type="text" name="location_id" value="{{ location_id }}">
    <button type="submit">Fetch</button>
  </form>
  {% if value %}
    <h2>Value for key: {{ key }}</h2>
    <pre>{{ value | safe }}</pre>
  {% elif tenant_id and location_id %}
    <p><strong>No value found for key: {{ key }}</strong></p>
  {% endif %}
</body>
</html>
"""

def construct_key(tenant_id, location_id):
    """Construct Redis key in the format availability:tenant_<tenant_id>:location_<location_id>"""
    return f"availability:tenant_{tenant_id}:location_{location_id}"

@app.route("/")
def index():
    tenant_id = request.args.get("tenant_id", "")
    location_id = request.args.get("location_id", "")
    value = None
    key = None
    if tenant_id and location_id:
        key = construct_key(tenant_id, location_id)
        raw_value = redis_client.get(key)
        if raw_value:
            try:
                parsed = json.loads(raw_value)
                value = json.dumps(parsed, indent=2, ensure_ascii=False)
            except json.JSONDecodeError:
                value = raw_value  # fallback to raw value
    return render_template_string(HTML_TEMPLATE, tenant_id=tenant_id, location_id=location_id, key=key, value=value)

@app.route("/api")
def api():
    tenant_id = request.args.get("tenant_id")
    location_id = request.args.get("location_id")
    if not tenant_id or not location_id:
        return jsonify({"error": "Missing ?tenant_id= or ?location_id= parameter"}), 400
    key = construct_key(tenant_id, location_id)
    value = redis_client.get(key)
    if value:
        return jsonify({"key": key, "value": value})
    return jsonify({"error": "Key not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)