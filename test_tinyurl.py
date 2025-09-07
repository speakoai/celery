#!/usr/bin/env python3
"""
Test script for TinyURL API functionality
This script will help debug issues with the create_tiny_url function
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_create_tiny_url(long_url: str) -> str:
    """
    Test version of create_tiny_url with detailed debugging output
    """
    print(f"[DEBUG] Testing TinyURL creation for: {long_url}")
    
    try:
        # Check if API token exists
        api_token = os.getenv("TINYURL_API_TOKEN")
        if not api_token:
            print("[ERROR] TINYURL_API_TOKEN not found in environment variables")
            print("[INFO] Available environment variables:")
            for key in os.environ.keys():
                if 'TINY' in key.upper() or 'URL' in key.upper():
                    print(f"  - {key}: {'***set***' if os.getenv(key) else 'not set'}")
            return long_url
        
        print(f"[DEBUG] API token found (length: {len(api_token)})")
        print(f"[DEBUG] Token starts with: {api_token[:10]}...")
        
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        
        data = {
            "url": long_url
        }
        
        print(f"[DEBUG] Request headers: {headers}")
        print(f"[DEBUG] Request data: {data}")
        
        print("[DEBUG] Making API request to TinyURL...")
        response = requests.post(
            "https://api.tinyurl.com/create",
            headers=headers,
            json=data,
            timeout=10
        )
        
        print(f"[DEBUG] Response status code: {response.status_code}")
        print(f"[DEBUG] Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"[DEBUG] Response JSON: {result}")
            
            tiny_url = result.get("data", {}).get("tiny_url")
            if tiny_url:
                print(f"[SUCCESS] TinyURL created successfully: {long_url} -> {tiny_url}")
                return tiny_url
            else:
                print(f"[ERROR] No tiny_url in response data")
                print(f"[DEBUG] Available keys in data: {list(result.get('data', {}).keys())}")
                return long_url
        else:
            print(f"[ERROR] API error {response.status_code}")
            print(f"[DEBUG] Response text: {response.text}")
            
            # Try to parse error details
            try:
                error_json = response.json()
                print(f"[DEBUG] Error JSON: {error_json}")
            except:
                print("[DEBUG] Could not parse response as JSON")
            
            return long_url
            
    except requests.exceptions.Timeout:
        print(f"[ERROR] Request timed out")
        return long_url
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Connection error - check internet connection")
        return long_url
    except Exception as e:
        print(f"[ERROR] Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return long_url

def test_api_authentication():
    """
    Test API authentication with a simple request
    """
    print("\n" + "="*60)
    print("TESTING API AUTHENTICATION")
    print("="*60)
    
    api_token = os.getenv("TINYURL_API_TOKEN")
    if not api_token:
        print("[ERROR] No API token found")
        return False
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Test with a simple endpoint (if available)
        print("[DEBUG] Testing authentication with a simple URL...")
        test_url = "https://www.google.com"
        response = requests.post(
            "https://api.tinyurl.com/create",
            headers=headers,
            json={"url": test_url},
            timeout=10
        )
        
        print(f"[DEBUG] Auth test response: {response.status_code}")
        if response.status_code == 401:
            print("[ERROR] Authentication failed - invalid API key")
            return False
        elif response.status_code == 403:
            print("[ERROR] Forbidden - API key may not have proper permissions")
            return False
        elif response.status_code in [200, 201]:
            print("[SUCCESS] Authentication successful")
            return True
        else:
            print(f"[WARNING] Unexpected status code: {response.status_code}")
            print(f"[DEBUG] Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Auth test failed: {e}")
        return False

def main():
    print("TinyURL API Test Script")
    print("="*50)
    
    # Test 1: Check environment setup
    print("\n1. ENVIRONMENT CHECK")
    print("-" * 30)
    api_token = os.getenv("TINYURL_API_TOKEN")
    if api_token:
        print(f"✓ TINYURL_API_TOKEN found (length: {len(api_token)})")
    else:
        print("✗ TINYURL_API_TOKEN not found")
        print("\nPlease set your TinyURL API token:")
        print("1. Get your API token from: https://tinyurl.com/app/settings/api")
        print("2. Add it to your .env file: TINYURL_API_TOKEN=your_token_here")
        return
    
    # Test 2: Authentication
    if not test_api_authentication():
        print("\n[ERROR] Authentication failed. Please check your API token.")
        return
    
    # Test 3: URL shortening with various URLs
    print("\n" + "="*60)
    print("TESTING URL SHORTENING")
    print("="*60)
    
    test_urls = [
        "https://speako.ai/en-US/customer/booking/demo-restaurant/view?token=abc123",
        "https://www.google.com",
        "https://github.com/speakoai/celery",
        "https://example.com/very/long/path/with/many/segments/and/parameters?param1=value1&param2=value2&token=verylongtoken123456789"
    ]
    
    for i, url in enumerate(test_urls, 1):
        print(f"\nTest {i}: {url}")
        print("-" * 50)
        result = test_create_tiny_url(url)
        if result != url:
            print(f"✓ SUCCESS: Shortened to {result}")
        else:
            print("✗ FAILED: No URL shortening occurred")
    
    print("\n" + "="*60)
    print("TEST COMPLETED")
    print("="*60)

if __name__ == "__main__":
    main()
