from dotenv import load_dotenv
load_dotenv()

from tasks.celery_app import app
from twilio.rest import Client
import psycopg2
import os
import re
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from tasks.email_template_utils import render_booking_confirmation_template

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

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_=os.getenv("TWILIO_SEND_SMS_NUMBER"),
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

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_=os.getenv("TWILIO_SEND_SMS_NUMBER"),
            to=customer_phone
        )

        print(f"[SMS] Sent to {customer_phone}: {message}")

    except Exception as e:
        print(f"[SMS] Error: {e}")
        
@app.task
def send_sms_confirmation_can(booking_id: int):
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

        client = Client(os.getenv("TWILIO_ACCOUNT_SID"), os.getenv("TWILIO_AUTH_TOKEN"))
        client.messages.create(
            body=message,
            from_=os.getenv("TWILIO_SEND_SMS_NUMBER"),
            to=customer_phone
        )

        print(f"[SMS] Sent to {customer_phone}: {message}")

    except Exception as e:
        print(f"[SMS] Error: {e}")

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
            email_message="A new booking has been confirmed with the following details:",
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
            email_message="A new booking has been confirmed with the following details:",
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

        # Construct email message with new and original booking details
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

            email_body = (
                "Dear Host,\n\n"
                "A new booking has been confirmed with the following details:\n\n"
                f"{new_booking_details}\n\n"
                "The original booking was modified, with the following details:\n\n"
                f"{original_booking_details}\n\n"
                "Please ensure all arrangements are updated accordingly.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )
        else:
            email_body = (
                "Dear Host,\n\n"
                "A new booking has been confirmed with the following details:\n\n"
                f"{new_booking_details}\n\n"
                f"No original booking was found for ID {original_booking_id}.\n\n"
                "Please ensure all arrangements are in place.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )

        # Set up SendGrid email
        message = Mail(
            from_email=os.getenv("SENDGRID_FROM_EMAIL"),
            to_emails=to_emails,
            subject=f"Booking Modification Confirmation (Ref: {new_booking_ref})",
            plain_text_content=email_body
        )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: {email_body}")
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

        # Construct email message with new and original booking details
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

            email_body = (
                "Dear Host,\n\n"
                "A new booking has been confirmed with the following details:\n\n"
                f"{new_booking_details}\n\n"
                "The original booking was modified, with the following details:\n\n"
                f"{original_booking_details}\n\n"
                "Please ensure all arrangements are updated accordingly.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )
        else:
            email_body = (
                "Dear Host,\n\n"
                "A new booking has been confirmed with the following details:\n\n"
                f"{new_booking_details}\n\n"
                f"No original booking was found for ID {original_booking_id}.\n\n"
                "Please ensure all arrangements are in place.\n\n"
                "Best regards,\n"
                "Speako AI Booking System"
            )

        # Set up SendGrid email
        message = Mail(
            from_email=os.getenv("SENDGRID_FROM_EMAIL"),
            to_emails=to_emails,
            subject=f"Booking Modification Confirmation (Ref: {new_booking_ref})",
            plain_text_content=email_body
        )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: {email_body}")
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
        email_body = (
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

        # Set up SendGrid email
        message = Mail(
            from_email=os.getenv("SENDGRID_FROM_EMAIL"),
            to_emails=to_emails,
            subject=f"Booking Cancellation Notification (Ref: {booking_ref})",
            plain_text_content=email_body
        )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: {email_body}")
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
        email_body = (
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

        # Set up SendGrid email
        message = Mail(
            from_email=os.getenv("SENDGRID_FROM_EMAIL"),
            to_emails=to_emails,
            subject=f"Booking Cancellation Notification (Ref: {booking_ref})",
            plain_text_content=email_body
        )

        # Send email via SendGrid
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)

        print(f"[EMAIL] Sent to {to_emails}: {email_body}")
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
