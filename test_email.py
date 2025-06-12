from tasks.sms import send_email_confirmation_new_rest, send_email_confirmation_new

if __name__ == "__main__":
    result = send_email_confirmation_new(booking_id=10353, host_email='waterfallbay@gmail.com')
    if result:
        print("✅ Task completed with result:")
    else:
        print("❌ No result returned")
