#!/usr/bin/env python3
"""
Simple connectivity test script to debug Flask app connection issues.
This script helps identify whether the problem is with:
1. Flask app not running
2. Network connectivity
3. Python requests library issues
4. Environment/dependency problems
"""

import sys
import os
import time
import socket
from urllib.parse import urlparse

def test_port_connectivity(host='localhost', port=5000, timeout=5):
    """Test if a port is open and accepting connections."""
    print(f"🔍 Testing TCP connectivity to {host}:{port}...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✅ Port {port} is open and accepting connections")
            return True
        else:
            print(f"❌ Port {port} is not accessible (error code: {result})")
            return False
            
    except socket.gaierror as e:
        print(f"❌ DNS resolution failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Socket error: {e}")
        return False

def test_http_with_urllib(url, timeout=10):
    """Test HTTP connectivity using urllib (Python built-in)."""
    print(f"🔍 Testing HTTP with urllib: {url}")
    
    try:
        import urllib.request
        import urllib.error
        
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Python-urllib/test')
        
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status_code = response.getcode()
            data = response.read().decode('utf-8')
            
            print(f"✅ urllib SUCCESS: HTTP {status_code}")
            print(f"   Response: {data[:200]}...")
            return True
            
    except urllib.error.HTTPError as e:
        print(f"❌ urllib HTTP Error: {e.code} {e.reason}")
        return False
    except urllib.error.URLError as e:
        print(f"❌ urllib URL Error: {e.reason}")
        return False
    except Exception as e:
        print(f"❌ urllib Unexpected Error: {e}")
        return False

def test_http_with_requests(url, timeout=10):
    """Test HTTP connectivity using requests library."""
    print(f"🔍 Testing HTTP with requests: {url}")
    
    try:
        import requests
        
        response = requests.get(url, timeout=timeout)
        
        print(f"✅ requests SUCCESS: HTTP {response.status_code}")
        print(f"   Response: {response.text[:200]}...")
        return True
        
    except ImportError:
        print(f"❌ requests library not available")
        return False
    except Exception as e:
        try:
            import requests
            if isinstance(e, requests.exceptions.Timeout):
                print(f"❌ requests TIMEOUT: Request timed out after {timeout} seconds")
            elif isinstance(e, requests.exceptions.ConnectionError):
                print(f"❌ requests CONNECTION ERROR: {e}")
            elif isinstance(e, requests.exceptions.RequestException):
                print(f"❌ requests REQUEST ERROR: {e}")
            else:
                print(f"❌ requests UNEXPECTED ERROR: {e}")
        except ImportError:
            print(f"❌ requests UNEXPECTED ERROR: {e}")
        return False

def check_flask_process():
    """Check if Flask process is running."""
    print(f"🔍 Checking for Flask processes...")
    
    try:
        if os.name == 'nt':  # Windows
            import subprocess
            result = subprocess.run(['tasklist', '/fi', 'imagename eq python.exe'], 
                                  capture_output=True, text=True)
            if 'python.exe' in result.stdout:
                print(f"✅ Python processes found")
                # Count lines to estimate number of processes
                lines = [line for line in result.stdout.split('\n') if 'python.exe' in line]
                print(f"   Found {len(lines)} Python processes")
            else:
                print(f"❌ No Python processes found")
        else:  # Unix-like
            import subprocess
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            flask_processes = [line for line in result.stdout.split('\n') if 'flask' in line.lower() or 'app.py' in line]
            if flask_processes:
                print(f"✅ Flask-related processes found:")
                for proc in flask_processes:
                    print(f"   {proc.strip()}")
            else:
                print(f"❌ No Flask processes found")
                
    except Exception as e:
        print(f"⚠️  Could not check processes: {e}")

def main():
    """Run comprehensive connectivity tests."""
    
    print("🚀 Simple Connectivity Test Script")
    print("=" * 60)
    print("This script helps debug Flask app connectivity issues")
    print()
    
    # Test configuration
    base_url = 'http://localhost:5000'
    health_url = f'{base_url}/api/health'
    
    # Parse URL
    parsed = urlparse(base_url)
    host = parsed.hostname or 'localhost'
    port = parsed.port or 5000
    
    print(f"🎯 Target: {base_url}")
    print(f"   Host: {host}")
    print(f"   Port: {port}")
    print()
    
    # Test 1: Check for Flask processes
    check_flask_process()
    print()
    
    # Test 2: TCP port connectivity
    port_open = test_port_connectivity(host, port)
    print()
    
    if not port_open:
        print("❌ Port is not accessible. Possible issues:")
        print("   1. Flask app is not running")
        print("   2. Flask app is running on a different port")
        print("   3. Firewall blocking the connection")
        print("   4. Flask app crashed during startup")
        print()
        print("💡 Try these steps:")
        print("   1. Run: python app.py")
        print("   2. Check for error messages")
        print("   3. Verify the port in the Flask startup message")
        print("   4. Try a different port with: PORT=5001 python app.py")
        return 1
    
    # Test 3: HTTP with urllib (built-in)
    urllib_success = test_http_with_urllib(health_url)
    print()
    
    # Test 4: HTTP with requests
    requests_success = test_http_with_requests(health_url)
    print()
    
    # Summary
    print("📊 Test Results Summary:")
    print("-" * 30)
    print(f"{'✅' if port_open else '❌'} TCP Port Connectivity")
    print(f"{'✅' if urllib_success else '❌'} HTTP with urllib")
    print(f"{'✅' if requests_success else '❌'} HTTP with requests")
    print()
    
    if port_open and urllib_success and not requests_success:
        print("🔍 Analysis: Port is open and urllib works, but requests fails")
        print("This suggests an issue with the requests library configuration.")
        print()
        print("💡 Possible solutions:")
        print("   1. Check virtual environment activation")
        print("   2. Reinstall requests: pip install --upgrade requests")
        print("   3. Check for proxy settings interfering with requests")
        print("   4. Try using urllib in your test script instead")
        
    elif port_open and not urllib_success and not requests_success:
        print("🔍 Analysis: Port is open but HTTP requests fail")
        print("This suggests the Flask app is accepting connections but not responding properly.")
        print()
        print("💡 Possible solutions:")
        print("   1. Check Flask app console for errors")
        print("   2. Verify the health endpoint is properly configured")
        print("   3. Check if the Flask app is stuck during startup")
        
    elif not port_open:
        print("🔍 Analysis: Port is not accessible")
        print("The Flask app is likely not running or not listening on this port.")
        
    else:
        print("✅ All connectivity tests passed!")
        print("The Flask app appears to be working correctly.")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
