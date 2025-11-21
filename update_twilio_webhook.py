"""
Script to update Twilio phone number voice webhook URL.
Updates the VoiceUrl configuration for a specific Twilio phone number.
"""

import os
from dotenv import load_dotenv
from twilio.rest import Client

# Load environment variables
load_dotenv()

# Twilio credentials from .env
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')

# Hard-coded configuration
# Replace these with your actual values
PHONE_NUMBER = "+61250163718"  # The Twilio number to update (E.164 format, e.g., +15551234567)
NEW_WEBHOOK_URL = "https://your-domain.com/voice/webhook"  # The new webhook URL

def update_twilio_voice_webhook():
    """Update the voice webhook URL for a Twilio phone number."""
    
    print(f"Connecting to Twilio Account: {TWILIO_ACCOUNT_SID}")
    
    # Initialize Twilio client
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    
    try:
        # Update the phone number configuration
        print(f"\nUpdating phone number: {PHONE_NUMBER}")
        print(f"New webhook URL: {NEW_WEBHOOK_URL}")
        
        incoming_phone_number = client.incoming_phone_numbers.list(
            phone_number=PHONE_NUMBER
        )
        
        if not incoming_phone_number:
            print(f"❌ Error: Phone number {PHONE_NUMBER} not found in your Twilio account")
            return False
        
        # Get the phone number SID
        phone_number_sid = incoming_phone_number[0].sid
        
        if not phone_number_sid:
            print(f"❌ Error: Could not retrieve SID for phone number {PHONE_NUMBER}")
            return False
            
        print(f"Found phone number SID: {phone_number_sid}")
        
        # Update the voice webhook
        updated_number = client.incoming_phone_numbers(phone_number_sid).update(
            voice_url=NEW_WEBHOOK_URL,
            voice_method='POST'  # Standard method for webhooks
        )
        
        print(f"\n✅ Successfully updated webhook configuration!")
        print(f"Phone Number: {updated_number.phone_number}")
        print(f"Voice URL: {updated_number.voice_url}")
        print(f"Voice Method: {updated_number.voice_method}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error updating webhook: {str(e)}")
        return False

def list_twilio_numbers():
    """List all Twilio phone numbers in the account with their current configuration."""
    
    print(f"Fetching all phone numbers from Twilio Account: {TWILIO_ACCOUNT_SID}\n")
    
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    
    try:
        incoming_phone_numbers = client.incoming_phone_numbers.list()
        
        if not incoming_phone_numbers:
            print("No phone numbers found in your Twilio account")
            return
        
        print(f"Found {len(incoming_phone_numbers)} phone number(s):\n")
        
        for number in incoming_phone_numbers:
            print(f"📞 Phone Number: {number.phone_number}")
            print(f"   SID: {number.sid}")
            print(f"   Friendly Name: {number.friendly_name}")
            print(f"   Voice URL: {number.voice_url or 'Not set'}")
            print(f"   Voice Method: {number.voice_method or 'Not set'}")
            print(f"   SMS URL: {number.sms_url or 'Not set'}")
            print("-" * 70)
            
    except Exception as e:
        print(f"❌ Error fetching phone numbers: {str(e)}")

if __name__ == "__main__":
    print("=" * 70)
    print("TWILIO PHONE NUMBER WEBHOOK UPDATER")
    print("=" * 70)
    
    # First, list all numbers to help identify which one to update
    print("\n📋 Current Twilio Phone Numbers:")
    print("=" * 70)
    list_twilio_numbers()
    
    # Then update the specified number
    print("\n🔧 Updating Voice Webhook Configuration:")
    print("=" * 70)
    update_twilio_voice_webhook()
