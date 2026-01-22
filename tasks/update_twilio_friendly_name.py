"""
Update Twilio Phone Number Friendly Name Task

Updates the friendly name of a Twilio phone number to match the location name.

Flow:
1. Query locations table to get location name and twilio_phone_number
2. Query twilio_phone_numbers table to get the twilio_sid
3. Update twilio_phone_numbers.friendly_name in database
4. Update Twilio API with new friendly_name
"""

import os
from pathlib import Path

# Load .env from the project root (celery directory)
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

import psycopg2
from psycopg2.extras import RealDictCursor
from twilio.rest import Client

from tasks.celery_app import app

# Environment variables
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')


def get_twilio_client():
    """Initialize and return Twilio client."""
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        raise ValueError("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN must be set in environment")
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def get_db_connection():
    """Get database connection."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL must be set in environment")
    return psycopg2.connect(DATABASE_URL)


@app.task(bind=True, name='tasks.update_twilio_friendly_name')
def update_twilio_friendly_name(self, location_id: str) -> dict:
    """
    Update Twilio phone number friendly name to match location name.
    
    Args:
        location_id: The location ID to update friendly name for
        
    Returns:
        Dictionary with update results including:
        - success: bool
        - location_id: str
        - location_name: str
        - phone_number: str
        - twilio_sid: str
        - previous_friendly_name: str
        - new_friendly_name: str
        - db_updated: bool
        - twilio_updated: bool
        - error: str (if failed)
    """
    print("=" * 70)
    print("UPDATE TWILIO FRIENDLY NAME TASK")
    print("=" * 70)
    print(f"Location ID: {location_id}")
    print()
    
    result = {
        'success': False,
        'location_id': location_id,
        'location_name': None,
        'phone_number': None,
        'twilio_sid': None,
        'previous_friendly_name': None,
        'new_friendly_name': None,
        'db_updated': False,
        'twilio_updated': False,
        'error': None
    }
    
    conn = None
    
    try:
        # Step 1: Get location details from locations table
        print("[Step 1] Fetching location details...")
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT location_id, name, twilio_phone_number
            FROM locations
            WHERE location_id = %s
            LIMIT 1
        """, (location_id,))
        
        location_row = cur.fetchone()
        
        if not location_row:
            error_msg = f"Location not found: location_id={location_id}"
            print(f"❌ {error_msg}")
            result['error'] = error_msg
            return result
        
        location_name = location_row['name']
        twilio_phone_number = location_row['twilio_phone_number']
        
        print(f"   Location Name: {location_name}")
        print(f"   Twilio Phone Number: {twilio_phone_number}")
        
        result['location_name'] = location_name
        result['phone_number'] = twilio_phone_number
        
        if not twilio_phone_number:
            error_msg = f"Location {location_id} does not have a twilio_phone_number assigned"
            print(f"❌ {error_msg}")
            result['error'] = error_msg
            return result
        
        # Step 2: Get twilio_sid from twilio_phone_numbers table
        print("\n[Step 2] Fetching Twilio SID from twilio_phone_numbers...")
        
        cur.execute("""
            SELECT phone_number_id, phone_number, friendly_name, twilio_sid
            FROM twilio_phone_numbers
            WHERE phone_number = %s
            LIMIT 1
        """, (twilio_phone_number,))
        
        phone_row = cur.fetchone()
        
        if not phone_row:
            error_msg = f"Phone number {twilio_phone_number} not found in twilio_phone_numbers table"
            print(f"❌ {error_msg}")
            result['error'] = error_msg
            return result
        
        twilio_sid = phone_row['twilio_sid']
        previous_friendly_name = phone_row['friendly_name']
        phone_number_id = phone_row['phone_number_id']
        
        print(f"   Phone Number ID: {phone_number_id}")
        print(f"   Twilio SID: {twilio_sid}")
        print(f"   Current Friendly Name: {previous_friendly_name}")
        
        result['twilio_sid'] = twilio_sid
        result['previous_friendly_name'] = previous_friendly_name
        result['new_friendly_name'] = location_name
        
        if not twilio_sid:
            error_msg = f"Phone number {twilio_phone_number} does not have a twilio_sid"
            print(f"❌ {error_msg}")
            result['error'] = error_msg
            return result
        
        # Step 3: Update friendly_name in twilio_phone_numbers table
        print("\n[Step 3] Updating friendly_name in database...")
        
        cur.execute("""
            UPDATE twilio_phone_numbers
            SET friendly_name = %s, updated_at = CURRENT_TIMESTAMP
            WHERE phone_number = %s
        """, (location_name, twilio_phone_number))
        
        conn.commit()
        result['db_updated'] = True
        print(f"   ✅ Database updated: friendly_name = '{location_name}'")
        
        # Step 4: Update Twilio API
        print("\n[Step 4] Updating Twilio API...")
        
        try:
            client = get_twilio_client()
            
            updated_number = client.incoming_phone_numbers(twilio_sid).update(
                friendly_name=location_name
            )
            
            result['twilio_updated'] = True
            print(f"   ✅ Twilio API updated!")
            print(f"      SID: {updated_number.sid}")
            print(f"      Friendly Name: {updated_number.friendly_name}")
            
        except Exception as twilio_error:
            error_msg = f"Failed to update Twilio API: {twilio_error}"
            print(f"   ⚠️  {error_msg}")
            # Note: DB was already updated, so we report partial success
            result['error'] = error_msg
            # Don't return here - we still want to report partial success
        
        # Final result
        result['success'] = result['db_updated'] and result['twilio_updated']
        
        print("\n" + "=" * 70)
        if result['success']:
            print("✅ UPDATE COMPLETED SUCCESSFULLY")
        elif result['db_updated']:
            print("⚠️  PARTIAL SUCCESS: Database updated but Twilio API failed")
        else:
            print("❌ UPDATE FAILED")
        print("=" * 70)
        
        return result
        
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(f"\n❌ {error_msg}")
        result['error'] = error_msg
        
        # Rollback if connection exists
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        
        return result
        
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
