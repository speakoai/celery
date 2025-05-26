from tasks.celery_app import app
from twilio.rest import Client
import psycopg2
import os

@app.task
def send_sms_confirmation_new(booking_id: int):
    print(f"[SMS] Booking {booking_id} – task triggered successfully.")
