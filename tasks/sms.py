from dotenv import load_dotenv
load_dotenv()

import urllib.parse
from tasks.celery_app import app
from twilio.rest import Client
import psycopg2
import os
import re
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from tasks.email_template_utils import render_booking_confirmation_template, render_customer_booking_confirmation_template, format_time_12hour

def create_tiny_url(long_url: str) -> str:
    """
    Create a shortened URL using TinyURL API.
    Returns the original URL if shortening fails.
    """
    try:
        api_token = os.getenv("TINYURL_API_TOKEN")
        if not api_token:
            print("[TinyURL] API token not found, returning original URL")
            return long_url
        
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
        
        if response.status_code == 200:
            result = response.json()
            tiny_url = result.get("data", {}).get("tiny_url")
            if tiny_url:
                print(f"[TinyURL] Successfully shortened URL: {long_url} -> {tiny_url}")
                return tiny_url
            else:
                print(f"[TinyURL] No tiny_url in response, returning original URL")
                return long_url
        else:
            print(f"[TinyURL] API error {response.status_code}: {response.text}")
            return long_url
            
    except Exception as e:
        print(f"[TinyURL] Error creating short URL: {e}")
        return long_url

@app.task
def send_sms_confirmation_new(booking_id: int):
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                b.tenant_id,
                b.customer_name,
                b.start_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name,
                bp.alias AS booking_page_alias
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN booking_page bp
              ON b.tenant_id = bp.tenant_id AND b.location_id = bp.location_id AND bp.is_active = true
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[SMS] Booking {booking_id} not found.")
            return

        (
            tenant_id,
            customer_name,
            start_time,
            booking_ref,
            party_num,
            customer_phone,
            location_name,
            location_type,
            staff_name,
            service_name,
            booking_page_alias
        ) = row
        
        # Get booking access token for manage booking URL
        booking_access_token = None
        cur.execute("""
            SELECT token_id 
            FROM booking_access_tokens 
            WHERE tenant_id = %s AND booking_id = %s AND purpose = 'view'
            ORDER BY created_at DESC
            LIMIT 1
        """, (tenant_id, booking_id))
        
        token_row = cur.fetchone()
        if token_row:
            booking_access_token = str(token_row[0])

        # Construct manage booking URL
        manage_booking_url = ""
        if booking_page_alias and booking_page_alias.strip():
            if booking_access_token and booking_access_token.strip():
                # Construct URL with token parameter
                manage_booking_url = f"{os.getenv('BOOKING_LINK_BASE_URL', 'https://speako.ai')}/customer/booking/{booking_page_alias.strip()}/view?token={booking_access_token.strip()}"
            else:
                # Fallback URL without token
                manage_booking_url = f"{os.getenv('BOOKING_LINK_BASE_URL', 'https://speako.ai')}/customer/booking/{booking_page_alias.strip()}/view"
        
        clean_ref = booking_ref[3:] if booking_ref.startswith("REF") else booking_ref

        if location_type == "rest":
            message = (
                f"Hi {customer_name}, your booking (Ref: {clean_ref}) for {party_num} "
                f"is confirmed at {location_name} on {start_time.strftime('%Y-%m-%d %H:%M')}."
            )
        else:
            message = (
                f"Hi {customer_name}, your booking (Ref: {clean_ref}) "
                f"is confirmed at {location_name} on {start_time.strftime('%Y-%m-%d %H:%M')} "
                f"with {staff_name} for {service_name}."
            )

        # Append manage booking link if available
        if manage_booking_url:
            # Create shortened URL for SMS
            tiny_url = create_tiny_url(manage_booking_url)
            message += f" Manage your booking: {tiny_url}"

        # Add Speako AI signature
        message += " [Speako AI]"

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_=os.getenv("TWILIO_SEND_SMS_NUMBER"),
            to=customer_phone
        )

        print(f"[SMS] Sent to {customer_phone}: {message}")

    except Exception as e:
        print(f"[SMS] Error: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


@app.task
def send_sms_confirmation_mod(booking_id: int):
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                b.tenant_id,
                b.customer_name,
                b.start_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name,
                bp.alias AS booking_page_alias
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN booking_page bp
              ON b.tenant_id = bp.tenant_id AND b.location_id = bp.location_id AND bp.is_active = true
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[SMS] Booking {booking_id} not found.")
            return

        (
            tenant_id,
            customer_name,
            start_time,
            booking_ref,
            party_num,
            customer_phone,
            location_name,
            location_type,
            staff_name,
            service_name,
            booking_page_alias
        ) = row
        
        # Get booking access token for manage booking URL
        booking_access_token = None
        cur.execute("""
            SELECT token_id 
            FROM booking_access_tokens 
            WHERE tenant_id = %s AND booking_id = %s AND purpose = 'view'
            ORDER BY created_at DESC
            LIMIT 1
        """, (tenant_id, booking_id))
        
        token_row = cur.fetchone()
        if token_row:
            booking_access_token = str(token_row[0])

        # Construct manage booking URL
        manage_booking_url = ""
        if booking_page_alias and booking_page_alias.strip():
            if booking_access_token and booking_access_token.strip():
                # Construct URL with token parameter
                manage_booking_url = f"{os.getenv('BOOKING_LINK_BASE_URL', 'https://speako.ai')}/customer/booking/{booking_page_alias.strip()}/view?token={booking_access_token.strip()}"
            else:
                # Fallback URL without token
                manage_booking_url = f"{os.getenv('BOOKING_LINK_BASE_URL', 'https://speako.ai')}/customer/booking/{booking_page_alias.strip()}/view"
        
        clean_ref = booking_ref[3:] if booking_ref.startswith("REF") else booking_ref

        if location_type == "rest":
            message = (
                f"Hi {customer_name}, your booking (Ref: {clean_ref}) for {party_num} "
                f"has been successfully updated at {location_name} to {start_time.strftime('%Y-%m-%d %H:%M')}."
            )
        else:
            message = (
                f"Hi {customer_name}, your booking (Ref: {clean_ref}) "
                f"has been successfully updated at {location_name} to {start_time.strftime('%Y-%m-%d %H:%M')} "
                f"with {staff_name} for {service_name}."
            )

        # Append manage booking link if available
        if manage_booking_url:
            # Create shortened URL for SMS
            tiny_url = create_tiny_url(manage_booking_url)
            message += f" Manage your booking: {tiny_url}"

        # Add Speako AI signature
        message += " [Speako AI]"

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_=os.getenv("TWILIO_SEND_SMS_NUMBER"),
            to=customer_phone
        )

        print(f"[SMS] Sent to {customer_phone}: {message}")

    except Exception as e:
        print(f"[SMS] Error: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        
@app.task
def send_sms_confirmation_can(booking_id: int):
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                b.tenant_id,
                b.customer_name,
                b.start_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name,
                bp.alias AS booking_page_alias
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN booking_page bp
              ON b.tenant_id = bp.tenant_id AND b.location_id = bp.location_id AND bp.is_active = true
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[SMS] Booking {booking_id} not found.")
            return

        (
            tenant_id,
            customer_name,
            start_time,
            booking_ref,
            party_num,
            customer_phone,
            location_name,
            location_type,
            staff_name,
            service_name,
            booking_page_alias
        ) = row
        
        # Get booking access token for manage booking URL
        booking_access_token = None
        cur.execute("""
            SELECT token_id 
            FROM booking_access_tokens 
            WHERE tenant_id = %s AND booking_id = %s AND purpose = 'view'
            ORDER BY created_at DESC
            LIMIT 1
        """, (tenant_id, booking_id))
        
        token_row = cur.fetchone()
        if token_row:
            booking_access_token = str(token_row[0])

        # Construct manage booking URL
        manage_booking_url = ""
        if booking_page_alias and booking_page_alias.strip():
            if booking_access_token and booking_access_token.strip():
                # Construct URL with token parameter
                manage_booking_url = f"{os.getenv('BOOKING_LINK_BASE_URL', 'https://speako.ai')}/customer/booking/{booking_page_alias.strip()}/view?token={booking_access_token.strip()}"
            else:
                # Fallback URL without token
                manage_booking_url = f"{os.getenv('BOOKING_LINK_BASE_URL', 'https://speako.ai')}/customer/booking/{booking_page_alias.strip()}/view"
        
        clean_ref = booking_ref[3:] if booking_ref.startswith("REF") else booking_ref

        if location_type == "rest":
            message = (
                f"Hi {customer_name}, your booking (Ref: {booking_ref}) for {party_num} "
                f"at {location_name} on {start_time.strftime('%Y-%m-%d %H:%M')} has been cancelled."
            )
        else:
            message = (
                f"Hi {customer_name}, your booking (Ref: {clean_ref}) "
                f"at {location_name} on {start_time.strftime('%Y-%m-%d %H:%M')} "
                f"with {staff_name} for {service_name} has been cancelled."
            )

        # Append manage booking link if available
        if manage_booking_url:
            # Create shortened URL for SMS
            tiny_url = create_tiny_url(manage_booking_url)
            message += f" View details: {tiny_url}"

        # Add Speako AI signature
        message += " [Speako AI]"

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_=os.getenv("TWILIO_SEND_SMS_NUMBER"),
            to=customer_phone
        )

        print(f"[SMS] Sent to {customer_phone}: {message}")

    except Exception as e:
        print(f"[SMS] Error: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# Simple email validation regex
EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

@app.task
def send_email_confirmation_new_rest(booking_id: int) -> str:
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.venue_unit_id,
                l.name AS location_name,
                l.booking_email_recipients,
                vu.name AS venue_unit_name
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[EMAIL] Booking {booking_id} not found.")
            return "failed"

        (
            customer_name,
            start_time,
            end_time,
            booking_ref,
            party_num,
            customer_phone,
            venue_unit_id,
            location_name,
            booking_email_recipients,
            venue_unit_name
        ) = row

        # Parse email recipients
        if not booking_email_recipients:
            fallback_email = os.getenv("FALLBACK_EMAIL")
            if not fallback_email:
                print(f"[EMAIL] No email recipients or fallback email for booking {booking_id}.")
                return "failed"
            to_emails = [fallback_email]
        else:
            # Split by comma or semicolon, strip whitespace, filter valid emails
            to_emails = []
            for email in re.split('[,;]', booking_email_recipients):
                email = email.strip()
                if email and re.match(EMAIL_REGEX, email):
                    to_emails.append(email)
                else:
                    print(f"[EMAIL] Invalid email skipped: {email}")

            if not to_emails:
                fallback_email = os.getenv("FALLBACK_EMAIL")
                if not fallback_email:
                    print(f"[EMAIL] No valid email recipients or fallback email for booking {booking_id}.")
                    return "failed"
        print(f"[EMAIL] Will send to: {to_emails}")

        # Construct plain text email as fallback
        plain_text_body = (
            "Dear Host,\n\n"
            "A new booking has been confirmed with the following details:\n\n"
            f"Location: {location_name}\n"
            f"Booking Ref.: {booking_ref}\n"
            f"Customer Name: {customer_name}\n"
            f"Customer Phone: {customer_phone}\n"
            f"Party Size: {party_num}\n"
            f"Date: {start_time.strftime('%Y-%m-%d')}\n"
            f"Start Time: {start_time.strftime('%H:%M')}\n"
            f"End Time: {end_time.strftime('%H:%M')}\n"
            f"Table/Venue Name: {venue_unit_name or 'Not Assigned'}\n"
            f"Table/Venue ID: {venue_unit_id or 'Not Assigned'}\n\n"
            "Please ensure all arrangements are in place.\n\n"
            "Best regards,\n"
            "Speako AI Booking System"
        )

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="New Booking Confirmation",
            email_message="",
            location_name=location_name,
            booking_ref=booking_ref,
            customer_name=customer_name,
            customer_phone=customer_phone,
            party_num=party_num,
            booking_date=start_time.strftime('%Y-%m-%d'),
            start_time=start_time.strftime('%H:%M'),
            end_time=end_time.strftime('%H:%M'),
            closing_message="Please ensure all arrangements are in place.",
            venue_unit_name=venue_unit_name,
            venue_unit_id=venue_unit_id
        )

        if not html_template:
            print("[EMAIL] Failed to generate HTML template, falling back to plain text only")
            # Set up SendGrid email with plain text only
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"New Booking Confirmation (Ref: {booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[EMAIL] HTML template generated successfully")
            # Set up SendGrid email with both HTML and plain text
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"New Booking Confirmation (Ref: {booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: HTML email with booking confirmation")
        print(f"[EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            
@app.task
def send_email_confirmation_new(booking_id: int) -> str:
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.staff_id,
                b.service_id,
                l.name AS location_name,
                l.booking_email_recipients,
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
            print(f"[EMAIL] Booking {booking_id} not found.")
            return "failed"

        (
            customer_name,
            start_time,
            end_time,
            booking_ref,
            party_num,
            customer_phone,
            staff_id,
            service_id,
            location_name,
            booking_email_recipients,
            staff_name,
            service_name
        ) = row

        # Parse email recipients
        if not booking_email_recipients:
            fallback_email = os.getenv("FALLBACK_EMAIL")
            if not fallback_email:
                print(f"[EMAIL] No email recipients or fallback email for booking {booking_id}.")
                return "failed"
            to_emails = [fallback_email]
        else:
            # Split by comma or semicolon, strip whitespace, filter valid emails
            to_emails = []
            for email in re.split('[,;]', booking_email_recipients):
                email = email.strip()
                if email and re.match(EMAIL_REGEX, email):
                    to_emails.append(email)
                else:
                    print(f"[EMAIL] Invalid email skipped: {email}")

            if not to_emails:
                fallback_email = os.getenv("FALLBACK_EMAIL")
                if not fallback_email:
                    print(f"[EMAIL] No valid email recipients or fallback email for booking {booking_id}.")
                    return "failed"
                to_emails = [fallback_email]

        # Construct plain text email as fallback
        plain_text_body = (
            "Dear Host,\n\n"
            "A new booking has been confirmed with the following details:\n\n"
            f"Location: {location_name}\n"
            f"Booking Ref.: {booking_ref}\n"
            f"Customer Name: {customer_name}\n"
            f"Customer Phone: {customer_phone}\n"
            f"Party Size: {party_num}\n"
            f"Date: {start_time.strftime('%Y-%m-%d')}\n"
            f"Start Time: {start_time.strftime('%H:%M')}\n"
            f"End Time: {end_time.strftime('%H:%M')}\n"
            f"Staff Name: {staff_name or 'Not Assigned'}\n"
            f"Staff ID: {staff_id or 'Not Assigned'}\n"
            f"Service Name: {service_name or 'Not Assigned'}\n"
            f"Service ID: {service_id or 'Not Assigned'}\n\n"
            "Please ensure all arrangements are in place.\n\n"
            "Best regards,\n"
            "Speako AI Booking System"
        )

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="New Booking Confirmation",
            email_message="",
            location_name=location_name,
            booking_ref=booking_ref,
            customer_name=customer_name,
            customer_phone=customer_phone,
            party_num=party_num,
            booking_date=start_time.strftime('%Y-%m-%d'),
            start_time=start_time.strftime('%H:%M'),
            end_time=end_time.strftime('%H:%M'),
            closing_message="Please ensure all arrangements are in place.",
            staff_name=staff_name,
            staff_id=staff_id,
            service_name=service_name,
            service_id=service_id
        )

        if not html_template:
            print("[EMAIL] Failed to generate HTML template, falling back to plain text only")
            # Set up SendGrid email with plain text only
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"New Booking Confirmation (Ref: {booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[EMAIL] HTML template generated successfully")
            # Set up SendGrid email with both HTML and plain text
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"New Booking Confirmation (Ref: {booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: HTML email with booking confirmation")
        print(f"[EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.task
def send_email_confirmation_mod_rest(booking_id: int, original_booking_id: int) -> str:
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # Fetch new booking details
        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.venue_unit_id,
                l.name AS location_name,
                l.booking_email_recipients,
                vu.name AS venue_unit_name
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s AND b.status = 'confirmed'
        """, (booking_id,))
        
        new_booking = cur.fetchone()

        if not new_booking:
            print(f"[EMAIL] Confirmed booking {booking_id} not found or not in 'confirmed' status.")
            return "failed"

        (
            new_customer_name,
            new_start_time,
            new_end_time,
            new_booking_ref,
            new_party_num,
            new_customer_phone,
            new_venue_unit_id,
            new_location_name,
            booking_email_recipients,
            new_venue_unit_name
        ) = new_booking

        # Fetch original booking details
        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.venue_unit_id,
                vu.name AS venue_unit_name
            FROM bookings b
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s AND b.status = 'modified'
        """, (original_booking_id,))
        
        original_booking = cur.fetchone()

        # Parse email recipients
        if not booking_email_recipients:
            fallback_email = os.getenv("FALLBACK_EMAIL")
            if not fallback_email:
                print(f"[EMAIL] No email recipients or fallback email for booking {booking_id}.")
                return "failed"
            to_emails = [fallback_email]
        else:
            # Split by comma or semicolon, strip whitespace, filter valid emails
            to_emails = []
            for email in re.split('[,;]', booking_email_recipients):
                email = email.strip()
                if email and re.match(EMAIL_REGEX, email):
                    to_emails.append(email)
                else:
                    print(f"[EMAIL] Invalid email skipped: {email}")

            if not to_emails:
                fallback_email = os.getenv("FALLBACK_EMAIL")
                if not fallback_email:
                    print(f"[EMAIL] No valid email recipients or fallback email for booking {booking_id}.")
                    return "failed"
                to_emails = [fallback_email]

        # Construct plain text email as fallback
        new_booking_details = (
            f"Location: {new_location_name}\n"
            f"Booking Ref.: {new_booking_ref}\n"
            f"Customer Name: {new_customer_name}\n"
            f"Customer Phone: {new_customer_phone}\n"
            f"Party Size: {new_party_num}\n"
            f"Date: {new_start_time.strftime('%Y-%m-%d')}\n"
            f"Start Time: {new_start_time.strftime('%H:%M')}\n"
            f"End Time: {new_end_time.strftime('%H:%M')}\n"
            f"Table/Venue Name: {new_venue_unit_name or 'Not Assigned'}\n"
            f"Table/Venue ID: {new_venue_unit_id or 'Not Assigned'}"
        )

        if original_booking:
            (
                orig_customer_name,
                orig_start_time,
                orig_end_time,
                orig_booking_ref,
                orig_party_num,
                orig_customer_phone,
                orig_venue_unit_id,
                orig_venue_unit_name
            ) = original_booking

            original_booking_details = (
                f"Location: {new_location_name}\n"  # Assuming same location
                f"Booking Ref.: {orig_booking_ref}\n"
                f"Customer Name: {orig_customer_name}\n"
                f"Customer Phone: {orig_customer_phone}\n"
                f"Party Size: {orig_party_num}\n"
                f"Date: {orig_start_time.strftime('%Y-%m-%d')}\n"
                f"Start Time: {orig_start_time.strftime('%H:%M')}\n"
                f"End Time: {orig_end_time.strftime('%H:%M')}\n"
                f"Table/Venue Name: {orig_venue_unit_name or 'Not Assigned'}\n"
                f"Table/Venue ID: {orig_venue_unit_id or 'Not Assigned'}"
            )

            plain_text_body = (
                "Dear Host,\n\n"
                "A booking has been modified. Here are the updated details:\n\n"
                f"{new_booking_details}\n\n"
                "The original booking was modified, with the following details:\n\n"
                f"{original_booking_details}\n\n"
                "Please ensure all arrangements are updated accordingly.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )

            # Create email message with original booking context
            email_message_with_original = (
                "<div style='background-color: #f8f9fa; padding: 15px; border-left: 4px solid #6c757d; margin: 15px 0; font-size: 16px; color: #495057; border-radius: 8px;'>"
                "<strong style='color: #212529; font-size: 17px;'>📋 Original Booking Details:</strong><br>"
                f"<span style='font-weight: 600;'>Date:</span> {orig_start_time.strftime('%Y-%m-%d')} | "
                f"<span style='font-weight: 600;'>Time:</span> {orig_start_time.strftime('%H:%M')} - {orig_end_time.strftime('%H:%M')}<br>"
                f"<span style='font-weight: 600;'>Party Size:</span> {orig_party_num} | "
                f"<span style='font-weight: 600;'>Table:</span> {orig_venue_unit_name or 'Not Assigned'}"
                "</div>"
            )
        else:
            plain_text_body = (
                "Dear Host,\n\n"
                "A booking has been modified. Here are the updated details:\n\n"
                f"{new_booking_details}\n\n"
                f"No original booking was found for ID {original_booking_id}.\n\n"
                "Please ensure all arrangements are updated accordingly.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )

            email_message_with_original = (
                "<div style='background-color: #f8f9fa; padding: 10px; border-left: 4px solid #6c757d; margin: 10px 0; font-size: 16px; color: #495057; border-radius: 8px;'>"
                f"<strong style='font-size: 17px;'>⚠️ Note:</strong> Original booking details not available (ID: {original_booking_id})"
                "</div>"
            )

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="Booking Modification Confirmation",
            email_message_mod=email_message_with_original,
            location_name=new_location_name,
            booking_ref=new_booking_ref,
            customer_name=new_customer_name,
            customer_phone=new_customer_phone,
            party_num=new_party_num,
            booking_date=new_start_time.strftime('%Y-%m-%d'),
            start_time=new_start_time.strftime('%H:%M'),
            end_time=new_end_time.strftime('%H:%M'),
            closing_message="Please ensure all arrangements are updated accordingly.",
            venue_unit_name=new_venue_unit_name,
            venue_unit_id=new_venue_unit_id,
            is_modification=True
        )

        if not html_template:
            print("[EMAIL] Failed to generate HTML template, falling back to plain text only")
            # Set up SendGrid email with plain text only
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Modification Confirmation (Ref: {new_booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[EMAIL] HTML template generated successfully")
            # Set up SendGrid email with both HTML and plain text
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Modification Confirmation (Ref: {new_booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: HTML email with booking modification confirmation")
        print(f"[EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.task
def send_email_confirmation_mod(booking_id: int, original_booking_id: int) -> str:
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # Fetch new booking details
        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.staff_id,
                b.service_id,
                l.name AS location_name,
                l.booking_email_recipients,
                s.name AS staff_name,
                sv.name AS service_name
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            WHERE b.booking_id = %s AND b.status = 'confirmed'
        """, (booking_id,))
        
        new_booking = cur.fetchone()

        if not new_booking:
            print(f"[EMAIL] Confirmed booking {booking_id} not found or not in 'confirmed' status.")
            return "failed"

        (
            new_customer_name,
            new_start_time,
            new_end_time,
            new_booking_ref,
            new_party_num,
            new_customer_phone,
            new_staff_id,
            new_service_id,
            new_location_name,
            booking_email_recipients,
            new_staff_name,
            new_service_name
        ) = new_booking

        # Fetch original booking details
        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.staff_id,
                b.service_id,
                s.name AS staff_name,
                sv.name AS service_name
            FROM bookings b
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            WHERE b.booking_id = %s AND b.status = 'modified'
        """, (original_booking_id,))
        
        original_booking = cur.fetchone()

        # Parse email recipients
        if not booking_email_recipients:
            fallback_email = os.getenv("FALLBACK_EMAIL")
            if not fallback_email:
                print(f"[EMAIL] No email recipients or fallback email for booking {booking_id}.")
                return "failed"
            to_emails = [fallback_email]
        else:
            # Split by comma or semicolon, strip whitespace, filter valid emails
            to_emails = []
            for email in re.split('[,;]', booking_email_recipients):
                email = email.strip()
                if email and re.match(EMAIL_REGEX, email):
                    to_emails.append(email)
                else:
                    print(f"[EMAIL] Invalid email skipped: {email}")

            if not to_emails:
                fallback_email = os.getenv("FALLBACK_EMAIL")
                if not fallback_email:
                    print(f"[EMAIL] No valid email recipients or fallback email for booking {booking_id}.")
                    return "failed"
                to_emails = [fallback_email]

        # Construct plain text email as fallback
        new_booking_details = (
            f"Location: {new_location_name}\n"
            f"Booking Ref.: {new_booking_ref}\n"
            f"Customer Name: {new_customer_name}\n"
            f"Customer Phone: {new_customer_phone}\n"
            f"Party Size: {new_party_num}\n"
            f"Date: {new_start_time.strftime('%Y-%m-%d')}\n"
            f"Start Time: {new_start_time.strftime('%H:%M')}\n"
            f"End Time: {new_end_time.strftime('%H:%M')}\n"
            f"Staff Name: {new_staff_name or 'Not Assigned'}\n"
            f"Staff ID: {new_staff_id or 'Not Assigned'}\n"
            f"Service Name: {new_service_name or 'Not Assigned'}\n"
            f"Service ID: {new_service_id or 'Not Assigned'}"
        )

        if original_booking:
            (
                orig_customer_name,
                orig_start_time,
                orig_end_time,
                orig_booking_ref,
                orig_party_num,
                orig_customer_phone,
                orig_staff_id,
                orig_service_id,
                orig_staff_name,
                orig_service_name
            ) = original_booking

            original_booking_details = (
                f"Location: {new_location_name}\n"  # Assuming same location
                f"Booking Ref.: {orig_booking_ref}\n"
                f"Customer Name: {orig_customer_name}\n"
                f"Customer Phone: {orig_customer_phone}\n"
                f"Party Size: {orig_party_num}\n"
                f"Date: {orig_start_time.strftime('%Y-%m-%d')}\n"
                f"Start Time: {orig_start_time.strftime('%H:%M')}\n"
                f"End Time: {orig_end_time.strftime('%H:%M')}\n"
                f"Staff Name: {orig_staff_name or 'Not Assigned'}\n"
                f"Staff ID: {orig_staff_id or 'Not Assigned'}\n"
                f"Service Name: {orig_service_name or 'Not Assigned'}\n"
                f"Service ID: {orig_service_id or 'Not Assigned'}"
            )

            plain_text_body = (
                "Dear Host,\n\n"
                "A booking has been modified. Here are the updated details:\n\n"
                f"{new_booking_details}\n\n"
                "The original booking was modified, with the following details:\n\n"
                f"{original_booking_details}\n\n"
                "Please ensure all arrangements are updated accordingly.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )

            # Create email message with original booking context
            email_message_with_original = (
                "<div style='background-color: #f8f9fa; padding: 15px; border-left: 4px solid #6c757d; margin: 15px 0; font-size: 16px; color: #495057; border-radius: 8px;'>"
                "<strong style='color: #212529; font-size: 17px;'>📋 Original Booking Details:</strong><br>"
                f"<span style='font-weight: 600;'>Date:</span> {orig_start_time.strftime('%Y-%m-%d')} | "
                f"<span style='font-weight: 600;'>Time:</span> {orig_start_time.strftime('%H:%M')} - {orig_end_time.strftime('%H:%M')}<br>"
                f"<span style='font-weight: 600;'>Party Size:</span> {orig_party_num} | "
                f"<span style='font-weight: 600;'>Staff:</span> {orig_staff_name or 'Not Assigned'}<br>"
                f"<span style='font-weight: 600;'>Service:</span> {orig_service_name or 'Not Assigned'}"
                "</div>"
            )
        else:
            plain_text_body = (
                "Dear Host,\n\n"
                "A booking has been modified. Here are the updated details:\n\n"
                f"{new_booking_details}\n\n"
                f"No original booking was found for ID {original_booking_id}.\n\n"
                "Please ensure all arrangements are updated accordingly.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )

            email_message_with_original = (
                "<div style='background-color: #f8f9fa; padding: 10px; border-left: 4px solid #6c757d; margin: 10px 0; font-size: 16px; color: #495057; border-radius: 8px;'>"
                f"<strong style='font-size: 17px;'>⚠️ Note:</strong> Original booking details not available (ID: {original_booking_id})"
                "</div>"
            )

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="Booking Modification Confirmation",
            email_message_mod=email_message_with_original,
            location_name=new_location_name,
            booking_ref=new_booking_ref,
            customer_name=new_customer_name,
            customer_phone=new_customer_phone,
            party_num=new_party_num,
            booking_date=new_start_time.strftime('%Y-%m-%d'),
            start_time=new_start_time.strftime('%H:%M'),
            end_time=new_end_time.strftime('%H:%M'),
            closing_message="Please ensure all arrangements are updated accordingly.",
            staff_name=new_staff_name,
            staff_id=new_staff_id,
            service_name=new_service_name,
            service_id=new_service_id,
            is_modification=True
        )

        if not html_template:
            print("[EMAIL] Failed to generate HTML template, falling back to plain text only")
            # Set up SendGrid email with plain text only
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Modification Confirmation (Ref: {new_booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[EMAIL] HTML template generated successfully")
            # Set up SendGrid email with both HTML and plain text
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Modification Confirmation (Ref: {new_booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: HTML email with booking modification confirmation")
        print(f"[EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            
@app.task
def send_email_confirmation_can_rest(booking_id: int) -> str:
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # Fetch cancelled booking details
        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.venue_unit_id,
                l.name AS location_name,
                l.booking_email_recipients,
                vu.name AS venue_unit_name
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s AND b.status = 'cancelled'
        """, (booking_id,))
        
        booking = cur.fetchone()

        if not booking:
            print(f"[EMAIL] Cancelled booking {booking_id} not found or not in 'cancelled' status.")
            return "failed"

        (
            customer_name,
            start_time,
            end_time,
            booking_ref,
            party_num,
            customer_phone,
            venue_unit_id,
            location_name,
            booking_email_recipients,
            venue_unit_name
        ) = booking

        # Parse email recipients
        if not booking_email_recipients:
            fallback_email = os.getenv("FALLBACK_EMAIL")
            if not fallback_email:
                print(f"[EMAIL] No email recipients or fallback email for booking {booking_id}.")
                return "failed"
            to_emails = [fallback_email]
        else:
            # Split by comma or semicolon, strip whitespace, filter valid emails
            to_emails = []
            for email in re.split('[,;]', booking_email_recipients):
                email = email.strip()
                if email and re.match(EMAIL_REGEX, email):
                    to_emails.append(email)
                else:
                    print(f"[EMAIL] Invalid email skipped: {email}")

            if not to_emails:
                fallback_email = os.getenv("FALLBACK_EMAIL")
                if not fallback_email:
                    print(f"[EMAIL] No valid email recipients or fallback email for booking {booking_id}.")
                    return "failed"
                to_emails = [fallback_email]

        # Construct email message with cancelled booking details
        plain_text_body = (
            "Dear Host,\n\n"
            "A booking has been cancelled with the following details:\n\n"
            f"Location: {location_name}\n"
            f"Booking Ref.: {booking_ref}\n"
            f"Customer Name: {customer_name}\n"
            f"Customer Phone: {customer_phone}\n"
            f"Party Size: {party_num}\n"
            f"Date: {start_time.strftime('%Y-%m-%d')}\n"
            f"Start Time: {start_time.strftime('%H:%M')}\n"
            f"End Time: {end_time.strftime('%H:%M')}\n"
            f"Table/Venue Name: {venue_unit_name or 'Not Assigned'}\n"
            f"Table/Venue ID: {venue_unit_id or 'Not Assigned'}\n\n"
            "Please update your records accordingly.\n\n"
            "Best regards,\n"
            "Speako AI Booking System"
        )

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="Booking Cancellation Notification",
            email_message="",
            location_name=location_name,
            booking_ref=booking_ref,
            customer_name=customer_name,
            customer_phone=customer_phone,
            party_num=party_num,
            booking_date=start_time.strftime('%Y-%m-%d'),
            start_time=start_time.strftime('%H:%M'),
            end_time=end_time.strftime('%H:%M'),
            closing_message="Please update your records accordingly.",
            venue_unit_name=venue_unit_name,
            venue_unit_id=venue_unit_id,
            is_cancellation=True
        )

        if not html_template:
            print("[EMAIL] Failed to generate HTML template, falling back to plain text only")
            # Set up SendGrid email with plain text only
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Cancellation Notification (Ref: {booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[EMAIL] HTML template generated successfully")
            # Set up SendGrid email with both HTML and plain text
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Cancellation Notification (Ref: {booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: HTML email with booking cancellation notification")
        print(f"[EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.task
def send_email_confirmation_can(booking_id: int) -> str:
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # Fetch cancelled booking details
        cur.execute("""
            SELECT 
                b.customer_name,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.staff_id,
                b.service_id,
                l.name AS location_name,
                l.booking_email_recipients,
                s.name AS staff_name,
                sv.name AS service_name
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            WHERE b.booking_id = %s AND b.status = 'cancelled'
        """, (booking_id,))
        
        booking = cur.fetchone()

        if not booking:
            print(f"[EMAIL] Cancelled booking {booking_id} not found or not in 'cancelled' status.")
            return "failed"

        (
            customer_name,
            start_time,
            end_time,
            booking_ref,
            party_num,
            customer_phone,
            staff_id,
            service_id,
            location_name,
            booking_email_recipients,
            staff_name,
            service_name
        ) = booking

        # Parse email recipients
        if not booking_email_recipients:
            fallback_email = os.getenv("FALLBACK_EMAIL")
            if not fallback_email:
                print(f"[EMAIL] No email recipients or fallback email for booking {booking_id}.")
                return "failed"
            to_emails = [fallback_email]
        else:
            # Split by comma or semicolon, strip whitespace, filter valid emails
            to_emails = []
            for email in re.split('[,;]', booking_email_recipients):
                email = email.strip()
                if email and re.match(EMAIL_REGEX, email):
                    to_emails.append(email)
                else:
                    print(f"[EMAIL] Invalid email skipped: {email}")

            if not to_emails:
                fallback_email = os.getenv("FALLBACK_EMAIL")
                if not fallback_email:
                    print(f"[EMAIL] No valid email recipients or fallback email for booking {booking_id}.")
                    return "failed"
                to_emails = [fallback_email]

        # Construct email message with cancelled booking details
        plain_text_body = (
            "Dear Host,\n\n"
            "A booking has been cancelled with the following details:\n\n"
            f"Location: {location_name}\n"
            f"Booking Ref.: {booking_ref}\n"
            f"Customer Name: {customer_name}\n"
            f"Customer Phone: {customer_phone}\n"
            f"Party Size: {party_num}\n"
            f"Date: {start_time.strftime('%Y-%m-%d')}\n"
            f"Start Time: {start_time.strftime('%H:%M')}\n"
            f"End Time: {end_time.strftime('%H:%M')}\n"
            f"Staff Name: {staff_name or 'Not Assigned'}\n"
            f"Staff ID: {staff_id or 'Not Assigned'}\n"
            f"Service Name: {service_name or 'Not Assigned'}\n"
            f"Service ID: {service_id or 'Not Assigned'}\n\n"
            "Please update your records accordingly.\n\n"
            "Best regards,\n"
            "Speako AI Booking System"
        )

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="Booking Cancellation Notification",
            email_message="",
            location_name=location_name,
            booking_ref=booking_ref,
            customer_name=customer_name,
            customer_phone=customer_phone,
            party_num=party_num,
            booking_date=start_time.strftime('%Y-%m-%d'),
            start_time=start_time.strftime('%H:%M'),
            end_time=end_time.strftime('%H:%M'),
            closing_message="Please update your records accordingly.",
            staff_name=staff_name,
            staff_id=staff_id,
            service_name=service_name,
            service_id=service_id,
            is_cancellation=True
        )

        if not html_template:
            print("[EMAIL] Failed to generate HTML template, falling back to plain text only")
            # Set up SendGrid email with plain text only
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Cancellation Notification (Ref: {booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[EMAIL] HTML template generated successfully")
            # Set up SendGrid email with both HTML and plain text
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=to_emails,
                subject=f"Booking Cancellation Notification (Ref: {booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: HTML email with booking cancellation notification")
        print(f"[EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.task
def send_email_confirmation_customer_new(booking_id: int) -> str:
    """
    Send booking confirmation email to customer (not merchant).
    Email recipient priority:
    1. Check bookings.customer_email
    2. If empty/null, check if customer_id exists
    3. If customer_id exists, query customers.email
    4. If both are empty/null, skip sending
    """
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # First query: Get booking details and check for direct customer email
        cur.execute("""
            SELECT 
                b.tenant_id,
                b.location_id,
                b.customer_name,
                b.customer_email,
                b.customer_id,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.staff_id,
                b.service_id,
                b.venue_unit_id,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name,
                vu.zone_tag_ids,
                bp.logo_url,
                bp.banner_url,
                bp.alias,
                li.address AS location_address,
                li.phone_with_country_code AS location_phone,
                li.website_url AS location_website
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            LEFT JOIN booking_page bp
              ON b.tenant_id = bp.tenant_id AND b.location_id = bp.location_id AND bp.is_active = true
            LEFT JOIN location_info li
              ON b.tenant_id = li.tenant_id AND b.location_id = li.location_id
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[CUSTOMER_EMAIL] Booking {booking_id} not found.")
            return "failed"

        (
            tenant_id,
            location_id,
            customer_name,
            customer_email,
            customer_id,
            start_time,
            end_time,
            booking_ref,
            party_num,
            customer_phone,
            staff_id,
            service_id,
            venue_unit_id,
            location_name,
            location_type,
            staff_name,
            service_name,
            zone_tag_ids,
            logo_url,
            banner_url,
            booking_page_alias,
            location_address,
            location_phone,
            location_website
        ) = row

        # Get booking access token for manage booking URL
        booking_access_token = None
        cur.execute("""
            SELECT token_id 
            FROM booking_access_tokens 
            WHERE tenant_id = %s AND booking_id = %s AND purpose = 'view'
            ORDER BY created_at DESC
            LIMIT 1
        """, (tenant_id, booking_id))
        
        token_row = cur.fetchone()
        if token_row:
            booking_access_token = str(token_row[0])

        # Get zone information for restaurant bookings
        zone_names = []
        if location_type == "rest" and zone_tag_ids:
            # Query location_tag table to get zone names
            zone_ids_tuple = tuple(zone_tag_ids)
            if zone_ids_tuple:
                cur.execute("""
                    SELECT name 
                    FROM location_tag 
                    WHERE tenant_id = %s AND tag_id = ANY(%s)
                    ORDER BY name
                """, (tenant_id, zone_tag_ids))
                
                zone_results = cur.fetchall()
                zone_names = [zone[0] for zone in zone_results]

        # Determine recipient email with fallback logic
        recipient_email = None
        
        # Priority 1: Check customer_email field in bookings
        if customer_email and customer_email.strip() and re.match(EMAIL_REGEX, customer_email.strip()):
            recipient_email = customer_email.strip()
            print(f"[CUSTOMER_EMAIL] Using customer_email from booking: {recipient_email}")
        
        # Priority 2: Check customers table if customer_id exists
        elif customer_id:
            cur.execute("""
                SELECT email 
                FROM customers 
                WHERE tenant_id = %s AND customer_id = %s AND email IS NOT NULL AND email != ''
            """, (tenant_id, customer_id))
            
            customer_row = cur.fetchone()
            if customer_row and customer_row[0]:
                customer_table_email = customer_row[0].strip()
                if re.match(EMAIL_REGEX, customer_table_email):
                    recipient_email = customer_table_email
                    print(f"[CUSTOMER_EMAIL] Using email from customers table: {recipient_email}")
                else:
                    print(f"[CUSTOMER_EMAIL] Invalid email in customers table: {customer_table_email}")
        
        # If no valid email found, skip sending
        if not recipient_email:
            print(f"[CUSTOMER_EMAIL] No valid customer email found for booking {booking_id}. Skipping email send.")
            return "skipped"

        # Prepare email content based on location type
        if location_type == "rest":
            # Restaurant booking - use zone names as table information
            table_info_text = ""
            if zone_names:
                zone_text = ", ".join(zone_names)
                table_info_text = f"Table: {zone_text}\n"

            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            # Prepare location information section
            location_info_text = ""
            if location_address or location_phone or location_website:
                location_info_text = "\nLocation Information:\n"
                if location_phone:
                    location_info_text += f"Tel: {location_phone}\n"
                if location_address:
                    location_info_text += f"Address: {location_address}\n"
                if location_website:
                    location_info_text += f"Website: {location_website}\n"

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your booking has been confirmed! Here are your booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Party Size: {party_num} people\n"
                f"{table_info_text}"
                f"{location_info_text}"
                "\nWe look forward to welcoming you!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Booking is Confirmed!",
                email_message="Great news! Your reservation has been successfully confirmed.",
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="We can't wait to see you! Please arrive on time for your reservation.",
                zone_names=zone_names,
                logo_url=logo_url,
                banner_url=banner_url,
                booking_page_alias=booking_page_alias,
                booking_access_token=booking_access_token,
                location_address=location_address,
                location_phone=location_phone,
                location_website=location_website,
                button_color_start="#28a745",
                button_color_end="#20c997"
            )
        else:
            # Service booking
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            # Prepare location information section
            location_info_text = ""
            if location_address or location_phone or location_website:
                location_info_text = "\nLocation Information:\n"
                if location_phone:
                    location_info_text += f"Tel: {location_phone}\n"
                if location_address:
                    location_info_text += f"Address: {location_address}\n"
                if location_website:
                    location_info_text += f"Website: {location_website}\n"

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your appointment has been confirmed! Here are your booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Staff Member: {staff_name or 'To be assigned'}\n"
                f"Service: {service_name or 'General service'}\n"
                f"{location_info_text}"
                "\nWe look forward to serving you!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Appointment is Confirmed!",
                email_message="Excellent! Your appointment has been successfully booked.",
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="We're excited to serve you! Please arrive a few minutes early.",
                staff_name=staff_name,
                staff_id=staff_id,
                service_name=service_name,
                service_id=service_id,
                logo_url=logo_url,
                banner_url=banner_url,
                booking_page_alias=booking_page_alias,
                booking_access_token=booking_access_token,
                location_address=location_address,
                location_phone=location_phone,
                location_website=location_website,
                button_color_start="#28a745",
                button_color_end="#20c997"
            )

        # Create and send email
        if not html_template:
            print("[CUSTOMER_EMAIL] Failed to generate HTML template, falling back to plain text only")
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=[recipient_email],
                subject=f"Booking Confirmation for {location_name} (Ref: {booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[CUSTOMER_EMAIL] HTML template generated successfully")
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=[recipient_email],
                subject=f"Booking Confirmation for {location_name} (Ref: {booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[CUSTOMER_EMAIL] Sent to {recipient_email}: Customer booking confirmation email")
        print(f"[CUSTOMER_EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[CUSTOMER_EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[CUSTOMER_EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.task
def send_email_confirmation_customer_mod(booking_id: int, original_booking_id: int) -> str:
    """
    Send booking modification confirmation email to customer (not merchant).
    Email recipient priority:
    1. Check bookings.customer_email
    2. If empty/null, check if customer_id exists
    3. If customer_id exists, query customers.email
    4. If both are empty/null, skip sending
    """
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # First query: Get new booking details and check for direct customer email
        cur.execute("""
            SELECT 
                b.tenant_id,
                b.location_id,
                b.customer_name,
                b.customer_email,
                b.customer_id,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.staff_id,
                b.service_id,
                b.venue_unit_id,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name,
                vu.zone_tag_ids,
                bp.logo_url,
                bp.banner_url,
                bp.alias,
                li.address,
                li.phone_with_country_code,
                li.website_url
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            LEFT JOIN booking_page bp
              ON b.tenant_id = bp.tenant_id AND b.location_id = bp.location_id AND bp.is_active = true
            LEFT JOIN location_info li
              ON b.tenant_id = li.tenant_id AND b.location_id = li.location_id
            WHERE b.booking_id = %s AND b.status = 'confirmed'
        """, (booking_id,))
        
        new_booking = cur.fetchone()

        if not new_booking:
            print(f"[CUSTOMER_EMAIL] Confirmed booking {booking_id} not found or not in 'confirmed' status.")
            return "failed"

        (
            tenant_id,
            location_id,
            customer_name,
            customer_email,
            customer_id,
            start_time,
            end_time,
            booking_ref,
            party_num,
            customer_phone,
            staff_id,
            service_id,
            venue_unit_id,
            location_name,
            location_type,
            staff_name,
            service_name,
            zone_tag_ids,
            logo_url,
            banner_url,
            booking_page_alias,
            location_address,
            location_phone,
            location_website
        ) = new_booking

        # Get booking access token for manage booking URL
        booking_access_token = None
        cur.execute("""
            SELECT token_id 
            FROM booking_access_tokens 
            WHERE tenant_id = %s AND booking_id = %s AND purpose = 'view'
            ORDER BY created_at DESC
            LIMIT 1
        """, (tenant_id, booking_id))
        
        token_row = cur.fetchone()
        if token_row:
            booking_access_token = str(token_row[0])

        # Get zone information for restaurant bookings
        zone_names = []
        if location_type == "rest" and zone_tag_ids:
            # Query location_tag table to get zone names
            zone_ids_tuple = tuple(zone_tag_ids)
            if zone_ids_tuple:
                cur.execute("""
                    SELECT name 
                    FROM location_tag 
                    WHERE tenant_id = %s AND tag_id = ANY(%s)
                    ORDER BY name
                """, (tenant_id, zone_tag_ids))
                
                zone_results = cur.fetchall()
                zone_names = [zone[0] for zone in zone_results]

        # Fetch original booking details for context
        cur.execute("""
            SELECT 
                b.start_time,
                b.end_time,
                b.party_num,
                b.staff_id,
                b.service_id,
                b.venue_unit_id,
                s.name AS staff_name,
                sv.name AS service_name,
                vu.name AS venue_unit_name
            FROM bookings b
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s AND b.status = 'modified'
        """, (original_booking_id,))
        
        original_booking = cur.fetchone()

        # Determine recipient email with fallback logic
        recipient_email = None
        
        # Priority 1: Check customer_email field in bookings
        if customer_email and customer_email.strip() and re.match(EMAIL_REGEX, customer_email.strip()):
            recipient_email = customer_email.strip()
            print(f"[CUSTOMER_EMAIL] Using customer_email from booking: {recipient_email}")
        
        # Priority 2: Check customers table if customer_id exists
        elif customer_id:
            cur.execute("""
                SELECT email 
                FROM customers 
                WHERE tenant_id = %s AND customer_id = %s AND email IS NOT NULL AND email != ''
            """, (tenant_id, customer_id))
            
            customer_row = cur.fetchone()
            if customer_row and customer_row[0]:
                customer_table_email = customer_row[0].strip()
                if re.match(EMAIL_REGEX, customer_table_email):
                    recipient_email = customer_table_email
                    print(f"[CUSTOMER_EMAIL] Using email from customers table: {recipient_email}")
                else:
                    print(f"[CUSTOMER_EMAIL] Invalid email in customers table: {customer_table_email}")
        
        # If no valid email found, skip sending
        if not recipient_email:
            print(f"[CUSTOMER_EMAIL] No valid customer email found for booking {booking_id}. Skipping email send.")
            return "skipped"

        # Prepare original booking context for email
        original_details_message = ""
        orig_booking_date = None
        orig_start_time_formatted = None
        orig_party_num = None
        orig_zone_names = []
        orig_staff_name = None
        orig_service_name = None
        
        if original_booking:
            (
                orig_start_time,
                orig_end_time,
                orig_party_num,
                orig_staff_id,
                orig_service_id,
                orig_venue_unit_id,
                orig_staff_name,
                orig_service_name,
                orig_venue_unit_name
            ) = original_booking

            # Set original booking variables for template
            orig_booking_date = orig_start_time.strftime('%Y-%m-%d')
            orig_start_time_formatted = format_time_12hour(orig_start_time)
            # orig_party_num is already set from the tuple
            orig_staff_name = orig_staff_name  # From tuple
            orig_service_name = orig_service_name  # From tuple
            
            # Get original zone information for restaurant bookings
            if location_type == "rest" and orig_venue_unit_id:
                # Query venue_unit to get zone_tag_ids for original booking
                cur.execute("""
                    SELECT zone_tag_ids 
                    FROM venue_unit
                    WHERE tenant_id = %s AND venue_unit_id = %s
                """, (tenant_id, orig_venue_unit_id))
                
                orig_venue_unit = cur.fetchone()
                if orig_venue_unit and orig_venue_unit[0]:
                    orig_zone_tag_ids = orig_venue_unit[0]
                    
                    # Get zone names
                    cur.execute("""
                        SELECT name 
                        FROM location_tag 
                        WHERE tenant_id = %s AND tag_id = ANY(%s)
                        ORDER BY name
                    """, (tenant_id, orig_zone_tag_ids))
                    
                    orig_zone_results = cur.fetchall()
                    orig_zone_names = [zone[0] for zone in orig_zone_results]

            if location_type == "rest":
                original_details_message = (
                    f"Your original booking was scheduled for {orig_start_time.strftime('%Y-%m-%d')} "
                    f"at {orig_start_time_formatted} "
                    f"for {orig_party_num} people."
                )
            else:
                original_details_message = (
                    f"Your original appointment was scheduled for {orig_start_time.strftime('%Y-%m-%d')} "
                    f"at {orig_start_time_formatted} "
                    f"with {orig_staff_name or 'staff member'} for {orig_service_name or 'service'}."
                )

        # Prepare email content based on location type
        if location_type == "rest":
            # Restaurant booking modification - use zone names as table information
            table_info_text = ""
            if zone_names:
                zone_text = ", ".join(zone_names)
                table_info_text = f"Table: {zone_text}\n"

            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            # Prepare location information section
            location_info_text = ""
            if location_address or location_phone or location_website:
                location_info_text = "\nLocation Information:\n"
                if location_address:
                    location_info_text += f"Address: {location_address}\n"
                if location_phone:
                    location_info_text += f"Phone: {location_phone}\n"
                if location_website:
                    location_info_text += f"Website: {location_website}\n"

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your booking has been successfully updated! Here are your new booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Party Size: {party_num} people\n"
                f"{table_info_text}"
                f"{location_info_text}"
                f"\n{original_details_message}\n\n"
                "We look forward to welcoming you at your updated time!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Booking Has Been Updated!",
                email_message="Your reservation has been successfully modified with the new details below.",
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="We're excited to see you at your updated time!",
                zone_names=zone_names,
                logo_url=logo_url,
                banner_url=banner_url,
                is_modification=True,
                original_booking_date=orig_booking_date,
                original_start_time=orig_start_time_formatted,
                original_party_num=orig_party_num,
                original_zone_names=orig_zone_names,
                original_staff_name=orig_staff_name,
                original_service_name=orig_service_name,
                booking_page_alias=booking_page_alias,
                booking_access_token=booking_access_token,
                location_address=location_address,
                location_phone=location_phone,
                location_website=location_website,
                button_color_start="#ffc107",
                button_color_end="#fd7e14"
            )
        else:
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            # Prepare location information section
            location_info_text = ""
            if location_address or location_phone or location_website:
                location_info_text = "\nLocation Information:\n"
                if location_address:
                    location_info_text += f"Address: {location_address}\n"
                if location_phone:
                    location_info_text += f"Phone: {location_phone}\n"
                if location_website:
                    location_info_text += f"Website: {location_website}\n"

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your appointment has been successfully updated! Here are your new booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Staff Member: {staff_name or 'To be assigned'}\n"
                f"Service: {service_name or 'General service'}\n"
                f"{location_info_text}"
                f"\n{original_details_message}\n\n"
                "We look forward to serving you at your updated appointment time!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Appointment Has Been Updated!",
                email_message="Great news! Your appointment has been successfully rescheduled.",
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="We're looking forward to serving you at your new appointment time!",
                staff_name=staff_name,
                staff_id=staff_id,
                service_name=service_name,
                service_id=service_id,
                logo_url=logo_url,
                banner_url=banner_url,
                is_modification=True,
                original_booking_date=orig_booking_date,
                original_start_time=orig_start_time_formatted,
                original_party_num=orig_party_num,
                original_zone_names=orig_zone_names,
                original_staff_name=orig_staff_name,
                original_service_name=orig_service_name,
                booking_page_alias=booking_page_alias,
                booking_access_token=booking_access_token,
                location_address=location_address,
                location_phone=location_phone,
                location_website=location_website,
                button_color_start="#ffc107",
                button_color_end="#fd7e14"
            )

        # Create and send email
        if not html_template:
            print("[CUSTOMER_EMAIL] Failed to generate HTML template, falling back to plain text only")
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=[recipient_email],
                subject=f"Booking Update for {location_name} (Ref: {booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[CUSTOMER_EMAIL] HTML template generated successfully")
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=[recipient_email],
                subject=f"Booking Update for {location_name} (Ref: {booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[CUSTOMER_EMAIL] Sent to {recipient_email}: Customer booking modification confirmation email")
        print(f"[CUSTOMER_EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[CUSTOMER_EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[CUSTOMER_EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.task
def send_email_confirmation_customer_can(booking_id: int) -> str:
    """
    Send booking cancellation confirmation email to customer (not merchant).
    Email recipient priority:
    1. Check bookings.customer_email
    2. If empty/null, check if customer_id exists
    3. If customer_id exists, query customers.email
    4. If both are empty/null, skip sending
    """
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # Query: Get cancelled booking details and check for direct customer email
        cur.execute("""
            SELECT 
                b.tenant_id,
                b.location_id,
                b.customer_name,
                b.customer_email,
                b.customer_id,
                b.start_time,
                b.end_time,
                b.booking_ref,
                b.party_num,
                b.customer_phone,
                b.staff_id,
                b.service_id,
                b.venue_unit_id,
                l.name AS location_name,
                l.location_type,
                s.name AS staff_name,
                sv.name AS service_name,
                vu.zone_tag_ids,
                bp.logo_url,
                bp.banner_url,
                bp.alias,
                li.address AS location_address,
                li.phone_with_country_code AS location_phone,
                li.website_url AS location_website
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            LEFT JOIN booking_page bp
              ON b.tenant_id = bp.tenant_id AND b.location_id = bp.location_id AND bp.is_active = true
            LEFT JOIN location_info li
              ON b.tenant_id = li.tenant_id AND b.location_id = li.location_id
            WHERE b.booking_id = %s AND b.status = 'cancelled'
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[CUSTOMER_EMAIL] Cancelled booking {booking_id} not found or not in 'cancelled' status.")
            return "failed"

        (
            tenant_id,
            location_id,
            customer_name,
            customer_email,
            customer_id,
            start_time,
            end_time,
            booking_ref,
            party_num,
            customer_phone,
            staff_id,
            service_id,
            venue_unit_id,
            location_name,
            location_type,
            staff_name,
            service_name,
            zone_tag_ids,
            logo_url,
            banner_url,
            booking_page_alias,
            location_address,
            location_phone,
            location_website
        ) = row

        # Get booking access token for manage booking URL
        booking_access_token = None
        cur.execute("""
            SELECT token_id 
            FROM booking_access_tokens 
            WHERE tenant_id = %s AND booking_id = %s AND purpose = 'view'
            ORDER BY created_at DESC
            LIMIT 1
        """, (tenant_id, booking_id))
        
        token_row = cur.fetchone()
        if token_row:
            booking_access_token = str(token_row[0])

        # Get zone information for restaurant bookings
        zone_names = []
        if location_type == "rest" and zone_tag_ids:
            # Query location_tag table to get zone names
            zone_ids_tuple = tuple(zone_tag_ids)
            if zone_ids_tuple:
                cur.execute("""
                    SELECT name 
                    FROM location_tag 
                    WHERE tenant_id = %s AND tag_id = ANY(%s)
                    ORDER BY name
                """, (tenant_id, zone_tag_ids))
                
                zone_results = cur.fetchall()
                zone_names = [zone[0] for zone in zone_results]

        # Determine recipient email with fallback logic
        recipient_email = None
        
        # Priority 1: Check customer_email field in bookings
        if customer_email and customer_email.strip() and re.match(EMAIL_REGEX, customer_email.strip()):
            recipient_email = customer_email.strip()
            print(f"[CUSTOMER_EMAIL] Using customer_email from booking: {recipient_email}")
        
        # Priority 2: Check customers table if customer_id exists
        elif customer_id:
            cur.execute("""
                SELECT email 
                FROM customers 
                WHERE tenant_id = %s AND customer_id = %s AND email IS NOT NULL AND email != ''
            """, (tenant_id, customer_id))
            
            customer_row = cur.fetchone()
            if customer_row and customer_row[0]:
                customer_table_email = customer_row[0].strip()
                if re.match(EMAIL_REGEX, customer_table_email):
                    recipient_email = customer_table_email
                    print(f"[CUSTOMER_EMAIL] Using email from customers table: {recipient_email}")
                else:
                    print(f"[CUSTOMER_EMAIL] Invalid email in customers table: {customer_table_email}")
        
        # If no valid email found, skip sending
        if not recipient_email:
            print(f"[CUSTOMER_EMAIL] No valid customer email found for booking {booking_id}. Skipping email send.")
            return "skipped"

        # Prepare email content based on location type
        if location_type == "rest":
            # Restaurant booking cancellation - use zone names as table information
            table_info_text = ""
            if zone_names:
                zone_text = ", ".join(zone_names)
                table_info_text = f"Table: {zone_text}\n"

            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            # Prepare location information section
            location_info_text = ""
            if location_address or location_phone or location_website:
                location_info_text = "\nLocation Information:\n"
                if location_phone:
                    location_info_text += f"Tel: {location_phone}\n"
                if location_address:
                    location_info_text += f"Address: {location_address}\n"
                if location_website:
                    location_info_text += f"Website: {location_website}\n"

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your booking has been cancelled. Here are the details of the cancelled booking:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Party Size: {party_num} people\n"
                f"{table_info_text}"
                f"{location_info_text}"
                "\nIf you'd like to make a new reservation, please contact us.\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Booking Has Been Cancelled",
                email_message="This email confirms that your reservation has been cancelled.",
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="We hope to welcome you again in the future. Feel free to contact us for new reservations.",
                zone_names=zone_names,
                logo_url=logo_url,
                banner_url=banner_url,
                is_cancellation=True,
                booking_page_alias=booking_page_alias,
                booking_access_token=booking_access_token,
                location_address=location_address,
                location_phone=location_phone,
                location_website=location_website,
                button_color_start="#dc3545",
                button_color_end="#e83e8c"
            )
        else:
            # Service booking cancellation
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            # Prepare location information section
            location_info_text = ""
            if location_address or location_phone or location_website:
                location_info_text = "\nLocation Information:\n"
                if location_phone:
                    location_info_text += f"Tel: {location_phone}\n"
                if location_address:
                    location_info_text += f"Address: {location_address}\n"
                if location_website:
                    location_info_text += f"Website: {location_website}\n"

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your appointment has been cancelled. Here are the details of the cancelled appointment:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Staff Member: {staff_name or 'Not specified'}\n"
                f"Service: {service_name or 'General service'}\n"
                f"{location_info_text}"
                "\nIf you'd like to schedule a new appointment, please contact us.\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Appointment Has Been Cancelled",
                email_message="This email confirms that your appointment has been cancelled.",
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="We hope to serve you again in the future. Feel free to contact us for new appointments.",
                staff_name=staff_name,
                staff_id=staff_id,
                service_name=service_name,
                service_id=service_id,
                logo_url=logo_url,
                banner_url=banner_url,
                is_cancellation=True,
                booking_page_alias=booking_page_alias,
                booking_access_token=booking_access_token,
                location_address=location_address,
                location_phone=location_phone,
                location_website=location_website,
                button_color_start="#dc3545",
                button_color_end="#e83e8c"
            )

        # Create and send email
        if not html_template:
            print("[CUSTOMER_EMAIL] Failed to generate HTML template, falling back to plain text only")
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=[recipient_email],
                subject=f"Booking Cancellation for {location_name} (Ref: {booking_ref})",
                plain_text_content=plain_text_body
            )
        else:
            print("[CUSTOMER_EMAIL] HTML template generated successfully")
            message = Mail(
                from_email=os.getenv("SENDGRID_FROM_EMAIL"),
                to_emails=[recipient_email],
                subject=f"Booking Cancellation for {location_name} (Ref: {booking_ref})",
                html_content=html_template,
                plain_text_content=plain_text_body
            )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[CUSTOMER_EMAIL] Sent to {recipient_email}: Customer booking cancellation confirmation email")
        print(f"[CUSTOMER_EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[CUSTOMER_EMAIL] Error: {e}")
        if hasattr(e, 'body'):
            print(f"[CUSTOMER_EMAIL] SendGrid Response: {e.body}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
