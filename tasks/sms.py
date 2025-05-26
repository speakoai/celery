from tasks.celery_app import app
from twilio.rest import Client
import psycopg2
import os


from twilio.rest import Client
import psycopg2
import os

from twilio.rest import Client
import psycopg2
import os

from twilio.rest import Client
import psycopg2
import os

@app.task
def send_sms_confirmation_new(booking_id: int):
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[SMS] Booking {booking_id} not found.")
            return

        (
            customer_name,
            start_time,
            booking_ref,
            party_num,
            customer_phone,
            location_name,
            location_type,
            staff_name,
            service_name
        ) = row

        if location_type == "rest":
            message = (
                f"Hi {customer_name}, your booking (Ref: {booking_ref}) for {party_num} "
                f"is confirmed at {location_name} on {start_time.strftime('%Y-%m-%d %H:%M')}."
            )
        else:
            message = (
                f"Hi {customer_name}, your booking (Ref: {booking_ref}) "
                f"is confirmed at {location_name} on {start_time.strftime('%Y-%m-%d %H:%M')} "
                f"with {staff_name} for {service_name}."
            )

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_="+61489266149",
            to=customer_phone
        )

        print(f"[SMS] Sent to {customer_phone}: {message}")

    except Exception as e:
        print(f"[SMS] Error: {e}")


@app.task
def send_sms_confirmation_mod(booking_id: int):
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[SMS] Booking {booking_id} not found.")
            return

        (
            customer_name,
            start_time,
            booking_ref,
            party_num,
            customer_phone,
            location_name,
            location_type,
            staff_name,
            service_name
        ) = row

        if location_type == "rest":
            message = (
                f"Hi {customer_name}, your booking (Ref: {booking_ref}) for {party_num} "
                f"has been successfully updated at {location_name} to {start_time.strftime('%Y-%m-%d %H:%M')}."
            )
        else:
            message = (
                f"Hi {customer_name}, your booking (Ref: {booking_ref}) "
                f"has been successfully updated at {location_name} to {start_time.strftime('%Y-%m-%d %H:%M')} "
                f"with {staff_name} for {service_name}."
            )

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_="+61489266149",
            to=customer_phone
        )

        print(f"[SMS] Sent to {customer_phone}: {message}")

    except Exception as e:
        print(f"[SMS] Error: {e}")