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
    <h2>Value for Key: {{ cache_key }}</h2>
    <pre>{{ value | safe }}</pre>
  {% elif cache_key %}
    <p><strong>No value found for key: {{ cache_key }}</strong></p>
  {% endif %}
</body>
</html>
"""

@app.route("/")
def index():
    tenant_id = request.args.get("tenant_id")
    location_id = request.args.get("location_id")
    cache_key = None
    value = None

    if tenant_id and location_id:
        cache_key = f"availability_v1:tenant_{tenant_id}_location_{location_id}"
        raw_value = redis_client.get(cache_key)
        if raw_value:
            try:
                parsed = json.loads(raw_value)
                value = json.dumps(parsed, indent=2, ensure_ascii=False)
            except json.JSONDecodeError:
                value = raw_value  # fallback to raw value

    return render_template_string(
        HTML_TEMPLATE,
        tenant_id=tenant_id or '',
        location_id=location_id or '',
        cache_key=cache_key,
        value=value
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
