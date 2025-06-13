from tasks.sms import send_email_confirmation_new_rest, send_email_confirmation_new, send_email_confirmation_mod_rest, send_email_confirmation_mod

if __name__ == "__main__":
    result = send_email_confirmation_mod(booking_id=10374, original_booking_id=10372)
    if result:
        print("✅ Task completed with result:")
    else:
        print("❌ No result returned")
