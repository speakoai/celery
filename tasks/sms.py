from celery_app import celery_app
from twilio.rest import Client
import psycopg2
import os

@celery_app.task
def send_sms_confirmation_new(booking_id: int):
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DB_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT customer_name, customer_phone, start_time, service_name, staff_name
            FROM bookings
            WHERE booking_id = %s
        """, (booking_id,))
        row = cur.fetchone()

        if not row:
            print(f"[SMS] Booking {booking_id} not found.")
            return

        name, phone, date, service, staff = row

        message = (
            f"Hi {name}, your booking is confirmed on {date.strftime('%Y-%m-%d %H:%M')} "
            f"with {staff} for {service}. Thank you!"
        )

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_="+61238213524",
            to=phone
        )

        print(f"[SMS] Sent to {phone}")

    except Exception as e:
        print(f"[SMS] Error: {e}")
