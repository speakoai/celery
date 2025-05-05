# cache_viewer.py

from flask import Flask, request, jsonify, render_template_string
import redis
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
REDIS_URL = os.getenv("REDIS_URL")

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, ssl=True)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><title>Cache Viewer</title></head>
<body>
  <h1>Redis Cache Viewer</h1>
  <form method="get">
    Key: <input type="text" name="key" value="{{ key }}">
    <button type="submit">Fetch</button>
  </form>
  {% if value %}
    <h2>Value:</h2>
    <pre>{{ value }}</pre>
  {% elif key %}
    <p><strong>No value found for this key.</strong></p>
  {% endif %}
</body>
</html>
"""

@app.route("/")
def index():
    key = request.args.get("key")
    value = None
    if key:
        value = redis_client.get(key)
    return render_template_string(HTML_TEMPLATE, key=key, value=value)

@app.route("/api")
def api():
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "Missing ?key= parameter"}), 400
    value = redis_client.get(key)
    if value:
        return jsonify({"key": key, "value": value})
    return jsonify({"error": "Key not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
