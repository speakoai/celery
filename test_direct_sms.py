#!/usr/bin/env python3
"""
Direct test of SMS functions to check TinyURL integration
This will test the actual SMS functions to see if TinyURL is working
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the tasks directory to the path so we can import the SMS functions
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

# Import the actual SMS functions
try:
    from tasks.sms import create_tiny_url
    print("✓ Successfully imported create_tiny_url from tasks.sms")
except ImportError as e:
    print(f"✗ Failed to import create_tiny_url: {e}")
    sys.exit(1)

def test_create_tiny_url_direct():
    """
    Test the actual create_tiny_url function from sms.py
    """
    print("="*60)
    print("TESTING ACTUAL create_tiny_url FUNCTION")
    print("="*60)
    
    test_urls = [
        "https://speako.ai/en-US/customer/booking/demo-restaurant/view?token=abc123",
        "https://www.google.com"
    ]
    
    for i, url in enumerate(test_urls, 1):
        print(f"\nTest {i}: {url}")
        print("-" * 50)
        
        try:
            result = create_tiny_url(url)
            if result != url:
                print(f"✓ SUCCESS: {result}")
            else:
                print("✗ FAILED: No shortening occurred")
        except Exception as e:
            print(f"✗ ERROR: {e}")

def find_sample_booking():
    """
    Find a real booking from the database to test with
    """
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        
        # Find a recent booking with all required fields
        cur.execute("""
            SELECT 
                b.booking_id,
                b.tenant_id,
                b.customer_name,
                b.booking_ref,
                bp.alias AS booking_page_alias
            FROM bookings b
            LEFT JOIN booking_page bp
              ON b.tenant_id = bp.tenant_id AND b.location_id = bp.location_id AND bp.is_active = true
            WHERE b.customer_name IS NOT NULL
            AND b.booking_ref IS NOT NULL
            ORDER BY b.booking_id DESC
            LIMIT 5
        """)
        
        bookings = cur.fetchall()
        
        if bookings:
            print("="*60)
            print("SAMPLE BOOKINGS FROM DATABASE")
            print("="*60)
            
            for booking in bookings:
                booking_id, tenant_id, customer_name, booking_ref, booking_page_alias = booking
                print(f"Booking ID: {booking_id}")
                print(f"Customer: {customer_name}")
                print(f"Ref: {booking_ref}")
                print(f"Booking Page Alias: {booking_page_alias or 'None'}")
                print("-" * 30)
            
            return bookings[0]  # Return the first booking for testing
        else:
            print("No bookings found in database")
            return None
            
    except Exception as e:
        print(f"Error finding bookings: {e}")
        return None
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def test_url_construction():
    """
    Test the URL construction logic with real booking data
    """
    print("="*60)
    print("TESTING URL CONSTRUCTION WITH REAL DATA")
    print("="*60)
    
    booking = find_sample_booking()
    if not booking:
        print("Cannot test URL construction - no booking data available")
        return
    
    booking_id, tenant_id, customer_name, booking_ref, booking_page_alias = booking
    
    print(f"Testing with booking {booking_id} (Customer: {customer_name})")
    
    # Test URL construction logic (same as in SMS functions)
    if booking_page_alias and booking_page_alias.strip():
        # Try to get booking access token
        try:
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
            cur = conn.cursor()
            
            cur.execute("""
                SELECT token_id 
                FROM booking_access_tokens 
                WHERE tenant_id = %s AND booking_id = %s AND purpose = 'view'
                ORDER BY created_at DESC
                LIMIT 1
            """, (tenant_id, booking_id))
            
            token_row = cur.fetchone()
            booking_access_token = str(token_row[0]) if token_row else None
            
            # Construct manage booking URL
            if booking_access_token and booking_access_token.strip():
                manage_booking_url = f"https://speako.ai/en-US/customer/booking/{booking_page_alias.strip()}/view?token={booking_access_token.strip()}"
            else:
                manage_booking_url = f"https://speako.ai/en-US/customer/booking/{booking_page_alias.strip()}/view"
            
            print(f"Constructed URL: {manage_booking_url}")
            
            # Test TinyURL creation
            print("Testing TinyURL creation...")
            tiny_url = create_tiny_url(manage_booking_url)
            
            if tiny_url != manage_booking_url:
                print(f"✓ SUCCESS: URL shortened to {tiny_url}")
            else:
                print("✗ FAILED: URL was not shortened")
                
        except Exception as e:
            print(f"Error during URL construction test: {e}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()
    else:
        print("Cannot construct URL - no booking page alias available")

def main():
    print("DIRECT SMS FUNCTION TINYURL TEST")
    print("="*60)
    
    # Test 1: Direct function test
    test_create_tiny_url_direct()
    
    # Test 2: URL construction with real data
    test_url_construction()
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)

if __name__ == "__main__":
    main()
