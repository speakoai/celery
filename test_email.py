from tasks.sms import send_email_confirmation_new_rest, send_email_confirmation_new, send_email_confirmation_mod_rest, send_email_confirmation_mod, send_email_confirmation_can_rest, send_email_confirmation_can

if __name__ == "__main__":
    result = send_email_confirmation_can(booking_id=10328)
    if result:
        print("✅ Task completed with result:")
    else:
        print("❌ No result returned")
