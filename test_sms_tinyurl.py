#!/usr/bin/env python3
"""
Test script specifically for SMS functions with TinyURL integration
This will help debug the actual SMS function context
"""

import os
import sys
import psycopg2
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_tiny_url_debug(long_url: str) -> str:
    """
    Debug version of create_tiny_url with detailed logging
    """
    print(f"[SMS-DEBUG] Starting TinyURL creation for: {long_url}")
    
    try:
        api_token = os.getenv("TINYURL_API_TOKEN")
        if not api_token:
            print("[SMS-DEBUG] API token not found, returning original URL")
            return long_url
        
        print(f"[SMS-DEBUG] API token found, making request...")
        
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        
        data = {
            "url": long_url
        }
        
        response = requests.post(
            "https://api.tinyurl.com/create",
            headers=headers,
            json=data,
            timeout=10
        )
        
        print(f"[SMS-DEBUG] Response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            tiny_url = result.get("data", {}).get("tiny_url")
            if tiny_url:
                print(f"[SMS-DEBUG] Successfully shortened: {long_url} -> {tiny_url}")
                return tiny_url
            else:
                print(f"[SMS-DEBUG] No tiny_url in response, returning original URL")
                return long_url
        else:
            print(f"[SMS-DEBUG] API error {response.status_code}: {response.text}")
            return long_url
            
    except Exception as e:
        print(f"[SMS-DEBUG] Error creating short URL: {e}")
        return long_url

def simulate_sms_function():
    """
    Simulate the SMS function context to test TinyURL creation
    """
    print("="*60)
    print("SIMULATING SMS FUNCTION CONTEXT")
    print("="*60)
    
    # Simulate booking data
    booking_page_alias = "demo-restaurant"
    booking_access_token = "test-token-123456789"
    booking_id = 12345
    
    # Construct manage booking URL (same as in SMS functions)
    manage_booking_url = ""
    if booking_page_alias and booking_page_alias.strip():
        if booking_access_token and booking_access_token.strip():
            # Construct URL with token parameter
            manage_booking_url = f"https://speako.ai/en-US/customer/booking/{booking_page_alias.strip()}/view?token={booking_access_token.strip()}"
        else:
            # Fallback URL without token
            manage_booking_url = f"https://speako.ai/en-US/customer/booking/{booking_page_alias.strip()}/view"
    
    print(f"[SMS-DEBUG] Constructed URL: {manage_booking_url}")
    
    # Test TinyURL creation
    if manage_booking_url:
        print(f"[SMS-DEBUG] Creating shortened URL...")
        tiny_url = create_tiny_url_debug(manage_booking_url)
        print(f"[SMS-DEBUG] Result: {tiny_url}")
        
        # Simulate message construction
        message = f"Hi John, your booking has been confirmed. Manage your booking: {tiny_url}"
        print(f"[SMS-DEBUG] Final message: {message}")
        
        return tiny_url != manage_booking_url  # Return True if shortening worked
    else:
        print("[SMS-DEBUG] No manage_booking_url generated")
        return False

def test_environment_variables():
    """
    Test all environment variables that might affect SMS functionality
    """
    print("="*60)
    print("ENVIRONMENT VARIABLES CHECK")
    print("="*60)
    
    required_vars = [
        "TINYURL_API_TOKEN",
        "TWILIO_ACCOUNT_SID", 
        "TWILIO_AUTH_TOKEN",
        "TWILIO_SEND_SMS_NUMBER",
        "DATABASE_URL"
    ]
    
    all_good = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if "TOKEN" in var or "KEY" in var or "SID" in var:
                print(f"✓ {var}: {'*' * min(len(value), 10)}... (length: {len(value)})")
            else:
                print(f"✓ {var}: {value}")
        else:
            print(f"✗ {var}: NOT SET")
            all_good = False
    
    return all_good

def test_database_connection():
    """
    Test database connectivity (without actually querying booking data)
    """
    print("="*60)
    print("DATABASE CONNECTION TEST")
    print("="*60)
    
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        
        # Simple test query
        cur.execute("SELECT 1 as test")
        result = cur.fetchone()
        
        if result and result[0] == 1:
            print("✓ Database connection successful")
            
            # Test booking-related tables exist
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('bookings', 'booking_access_tokens', 'booking_page')
            """)
            tables = cur.fetchall()
            
            expected_tables = {'bookings', 'booking_access_tokens', 'booking_page'}
            found_tables = {table[0] for table in tables}
            
            for table in expected_tables:
                if table in found_tables:
                    print(f"✓ Table '{table}' exists")
                else:
                    print(f"✗ Table '{table}' missing")
            
            return True
        else:
            print("✗ Database query failed")
            return False
            
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def main():
    print("SMS FUNCTION TINYURL DEBUG TEST")
    print("="*60)
    
    # Test 1: Environment variables
    print("\n1. CHECKING ENVIRONMENT VARIABLES")
    env_ok = test_environment_variables()
    
    if not env_ok:
        print("\n[ERROR] Some environment variables are missing. Please check your .env file.")
        return
    
    # Test 2: Database connection
    print("\n2. TESTING DATABASE CONNECTION")
    db_ok = test_database_connection()
    
    if not db_ok:
        print("\n[WARNING] Database connection failed. SMS functions may not work properly.")
    
    # Test 3: TinyURL in SMS context
    print("\n3. TESTING TINYURL IN SMS CONTEXT")
    sms_ok = simulate_sms_function()
    
    if sms_ok:
        print("\n✓ SUCCESS: TinyURL creation works in SMS context")
    else:
        print("\n✗ FAILED: TinyURL creation failed in SMS context")
    
    print("\n" + "="*60)
    print("DIAGNOSIS COMPLETE")
    print("="*60)
    
    if env_ok and sms_ok:
        print("✓ All tests passed - TinyURL should work in SMS functions")
    else:
        print("✗ Some tests failed - check the output above for issues")

if __name__ == "__main__":
    main()
