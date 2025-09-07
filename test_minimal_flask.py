#!/usr/bin/env python3
"""
Minimal Flask app to test basic connectivity.
This helps isolate whether the issue is with Flask itself or with the complex imports.
"""

import os
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/api/health', methods=['GET'])
def health():
    """Simple health check."""
    return jsonify({
        'status': 'healthy',
        'service': 'minimal-test-app',
        'message': 'Basic Flask connectivity working'
    }), 200

@app.route('/', methods=['GET'])
def root():
    """Root endpoint."""
    return jsonify({
        'message': 'Minimal Flask test app is running',
        'endpoints': ['/api/health']
    }), 200

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    print(f"🚀 Starting minimal Flask app on port {port}...")
    print(f"   Health check: http://localhost:{port}/api/health")
    print(f"   Root endpoint: http://localhost:{port}/")
    print()
    
    try:
        app.run(host='0.0.0.0', port=port, debug=True)
    except Exception as e:
        print(f"❌ Failed to start Flask app: {e}")
