#!/usr/bin/env python3
"""
Test script for HTML email functionality
Usage: python test_html_email.py
"""

import sys
import os
import psycopg2
import re
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Add the tasks directory to the Python path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tasks'))

# Import our template utility
try:
    from email_template_utils import render_booking_confirmation_template
except ImportError:
    print("Warning: Could not import email_template_utils. HTML templates will not work.")
    def render_booking_confirmation_template(*args, **kwargs):
        return ""

# Simple email validation regex
EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

def send_email_confirmation_new_rest_test(booking_id: int) -> str:
    """Test version of the email function without Celery decorator."""
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

        print(f"[EMAIL] Found booking: {booking_ref} for {customer_name}")

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
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def send_email_confirmation_new_test(booking_id: int) -> str:
    """Test version of the service booking email function without Celery decorator."""
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

        print(f"[EMAIL] Found service booking: {booking_ref} for {customer_name}")
        print(f"[EMAIL] Service: {service_name} with {staff_name}")

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

        print(f"[EMAIL] Sent to {to_emails}: HTML email with service booking confirmation")
        print(f"[EMAIL] SendGrid response status: {response.status_code}")
        return "success"

    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def send_email_confirmation_mod_rest_test(booking_id: int, original_booking_id: int) -> str:
    """Test version of the modification email function without Celery decorator."""
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

        print(f"[EMAIL] Found new booking: {new_booking_ref} for {new_customer_name}")
        print(f"[DEBUG] New booking details:")
        print(f"  - Date: {new_start_time.strftime('%Y-%m-%d')}")
        print(f"  - Time: {new_start_time.strftime('%H:%M')} - {new_end_time.strftime('%H:%M')}")
        print(f"  - Party Size: {new_party_num}")
        print(f"  - Table: {new_venue_unit_name or 'Not Assigned'}")
        print(f"  - Table ID: {new_venue_unit_id or 'Not Assigned'}")

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
            print(f"[EMAIL] Found original booking: {orig_booking_ref} for {orig_customer_name}")
            print(f"[DEBUG] Original booking details:")
            print(f"  - Date: {orig_start_time.strftime('%Y-%m-%d')}")
            print(f"  - Time: {orig_start_time.strftime('%H:%M')} - {orig_end_time.strftime('%H:%M')}")
            print(f"  - Party Size: {orig_party_num}")
            print(f"  - Table: {orig_venue_unit_name or 'Not Assigned'}")
            print(f"  - Table ID: {orig_venue_unit_id or 'Not Assigned'}")
        else:
            print(f"[EMAIL] Original booking {original_booking_id} not found or not in 'modified' status")

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
            print(f"[DEBUG] Email message with original booking created:")
            print(f"[DEBUG] {email_message_with_original}")
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
                "<div style='background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; font-size: 16px; color: #856404; border-radius: 8px;'>"
                f"<strong style='font-size: 17px;'>⚠️ Note:</strong> Original booking details not available (ID: {original_booking_id})"
                "</div>"
            )
            print(f"[DEBUG] No original booking found - using fallback message")
            print(f"[DEBUG] {email_message_with_original}")

        # Create HTML email template using template file
        print(f"[DEBUG] Creating HTML template with:")
        print(f"  - email_title: 'Booking Modification Confirmation'")
        print(f"  - location_name: {new_location_name}")
        print(f"  - booking_ref: {new_booking_ref}")
        print(f"  - customer_name: {new_customer_name}")
        print(f"  - party_num: {new_party_num}")
        print(f"  - venue_unit_name: {new_venue_unit_name}")
        
        html_template = render_booking_confirmation_template(
            email_title="Booking Modification Confirmation",
            email_message=email_message_with_original,
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
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def send_email_confirmation_mod_test(booking_id: int, original_booking_id: int) -> str:
    """Test version of send_email_confirmation_mod function for service bookings."""
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

        print(f"[EMAIL] Found new booking: {new_booking_ref} for {new_customer_name}")
        print(f"[DEBUG] New booking details:")
        print(f"  - Date: {new_start_time.strftime('%Y-%m-%d')}")
        print(f"  - Time: {new_start_time.strftime('%H:%M')} - {new_end_time.strftime('%H:%M')}")
        print(f"  - Party Size: {new_party_num}")
        print(f"  - Staff: {new_staff_name}")
        print(f"  - Service: {new_service_name}")
        print(f"  - Staff ID: {new_staff_id}")
        print(f"  - Service ID: {new_service_id}")

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

        # Use fallback email from environment variable for testing
        fallback_email = os.getenv("FALLBACK_EMAIL")
        if not fallback_email:
            print(f"[EMAIL] No fallback email configured.")
            return "failed"
        
        # For testing, also send to a known test email if available
        to_emails = [fallback_email]
        pedro_email = "pedro@surpass.com.au"
        if pedro_email not in to_emails:
            to_emails.append(pedro_email)

        print(f"[EMAIL] Will send to: {to_emails}")

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

            print(f"[EMAIL] Found original booking: {orig_booking_ref} for {orig_customer_name}")
            print(f"[DEBUG] Original booking details:")
            print(f"  - Date: {orig_start_time.strftime('%Y-%m-%d')}")
            print(f"  - Time: {orig_start_time.strftime('%H:%M')} - {orig_end_time.strftime('%H:%M')}")
            print(f"  - Party Size: {orig_party_num}")
            print(f"  - Staff: {orig_staff_name}")
            print(f"  - Service: {orig_service_name}")
            print(f"  - Staff ID: {orig_staff_id}")
            print(f"  - Service ID: {orig_service_id}")

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
            print(f"[DEBUG] Email message with original booking created:")
            print(f"[DEBUG] {email_message_with_original}")
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
                "<div style='background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; font-size: 16px; color: #856404; border-radius: 8px;'>"
                f"<strong style='font-size: 17px;'>⚠️ Note:</strong> Original booking details not available (ID: {original_booking_id})"
                "</div>"
            )
            print(f"[DEBUG] No original booking found - using fallback message")
            print(f"[DEBUG] {email_message_with_original}")

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="Booking Modification Confirmation",
            email_message=email_message_with_original,
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

        print(f"[DEBUG] Creating HTML template with:")
        print(f"  - email_title: 'Booking Modification Confirmation'")
        print(f"  - location_name: {new_location_name}")
        print(f"  - booking_ref: {new_booking_ref}")
        print(f"  - customer_name: {new_customer_name}")
        print(f"  - party_num: {new_party_num}")
        print(f"  - staff_name: {new_staff_name}")
        print(f"  - service_name: {new_service_name}")

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
        return "failed"
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def send_email_confirmation_can_rest_test(booking_id: int) -> str:
    """Test version of the restaurant cancellation email function without Celery decorator."""
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

        print(f"[EMAIL] Found cancelled booking: {booking_ref} for {customer_name}")
        print(f"[DEBUG] Booking details:")
        print(f"  - Date: {start_time.strftime('%Y-%m-%d')}")
        print(f"  - Time: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
        print(f"  - Party Size: {party_num}")
        print(f"  - Table: {venue_unit_name}")
        print(f"  - Table ID: {venue_unit_id}")

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

        print(f"[EMAIL] Will send to: {to_emails}")

        # Construct plain text email as fallback
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

        print(f"[DEBUG] Creating HTML template with:")
        print(f"  - email_title: 'Booking Cancellation Notification'")
        print(f"  - location_name: {location_name}")
        print(f"  - booking_ref: {booking_ref}")
        print(f"  - customer_name: {customer_name}")
        print(f"  - party_num: {party_num}")
        print(f"  - venue_unit_name: {venue_unit_name}")

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="Booking Cancellation Notification",
            email_message="A booking has been cancelled with the following details:",
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

def send_email_confirmation_can_test(booking_id: int) -> str:
    """Test version of the service cancellation email function without Celery decorator."""
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

        print(f"[EMAIL] Found cancelled booking: {booking_ref} for {customer_name}")
        print(f"[DEBUG] Booking details:")
        print(f"  - Date: {start_time.strftime('%Y-%m-%d')}")
        print(f"  - Time: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
        print(f"  - Party Size: {party_num}")
        print(f"  - Staff: {staff_name}")
        print(f"  - Service: {service_name}")
        print(f"  - Staff ID: {staff_id}")
        print(f"  - Service ID: {service_id}")

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

        print(f"[EMAIL] Will send to: {to_emails}")

        # Construct plain text email as fallback
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

        print(f"[DEBUG] Creating HTML template with:")
        print(f"  - email_title: 'Booking Cancellation Notification'")
        print(f"  - location_name: {location_name}")
        print(f"  - booking_ref: {booking_ref}")
        print(f"  - customer_name: {customer_name}")
        print(f"  - party_num: {party_num}")
        print(f"  - staff_name: {staff_name}")
        print(f"  - service_name: {service_name}")

        # Create HTML email template using template file
        html_template = render_booking_confirmation_template(
            email_title="Booking Cancellation Notification",
            email_message="A booking has been cancelled with the following details:",
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

def test_html_email():
    """Test the HTML email function with specific booking IDs."""
    
    # Load environment variables
    load_dotenv()
    
    print("Available test options:")
    print("1. Restaurant booking (ID: 23208) - Table assignment")
    print("2. Service booking (ID: 23207) - Staff and service details")
    print("3. Restaurant booking modification (New ID: 23194, Original ID: 23193)")
    print("4. Service booking modification (New ID: 23084, Original ID: 23082)")
    print("5. Restaurant booking cancellation (ID: 22985)")
    print("6. Service booking cancellation (ID: 23083)")
    print("7. Custom booking ID")
    print()
    
    choice = input("Choose test option (1/2/3/4/5/6/7): ").strip()
    
    if choice == "1":
        booking_id = 23208
        test_function = send_email_confirmation_new_rest_test
        booking_type = "restaurant"
    elif choice == "2":
        booking_id = 23207
        test_function = send_email_confirmation_new_test
        booking_type = "service"
    elif choice == "3":
        # Restaurant booking modification test
        new_booking_id = 23194
        original_booking_id = 23193
        booking_type = "restaurant modification"
        print(f"\nTesting {booking_type} HTML email")
        print(f"New booking ID: {new_booking_id}")
        print(f"Original booking ID: {original_booking_id}")
        print("-" * 50)
        
        try:
            result = send_email_confirmation_mod_rest_test(new_booking_id, original_booking_id)
            
            print(f"Email send result: {result}")
            
            if result == "success":
                print("✅ Modification email sent successfully!")
                print("Check your email inbox to see the HTML email.")
                print("📧 Template type: restaurant booking modification")
                print("🔄 Should show: Updated booking details with subtle original booking info")
            else:
                print("❌ Failed to send modification email.")
                print("Check the console output above for error details.")
                
        except Exception as e:
            print(f"❌ Error during modification email test: {e}")
        
        return
        
    elif choice == "4":
        # Service booking modification test
        new_booking_id = 23084
        original_booking_id = 23082
        booking_type = "service modification"
        print(f"\nTesting {booking_type} HTML email")
        print(f"New booking ID: {new_booking_id}")
        print(f"Original booking ID: {original_booking_id}")
        print("-" * 50)
        
        try:
            result = send_email_confirmation_mod_test(new_booking_id, original_booking_id)
            
            print(f"Email send result: {result}")
            
            if result == "success":
                print("✅ Service modification email sent successfully!")
                print("Check your email inbox to see the HTML email.")
                print("📧 Template type: service booking modification")
                print("🔄 Should show: Updated booking details with subtle original booking info")
            else:
                print("❌ Failed to send service modification email.")
                print("Check the console output above for error details.")
                
        except Exception as e:
            print(f"❌ Error during service modification email test: {e}")
        
        return
        
    elif choice == "5":
        # Restaurant booking cancellation test
        booking_id = 22985
        booking_type = "restaurant cancellation"
        print(f"\nTesting {booking_type} HTML email")
        print(f"Booking ID: {booking_id}")
        print("-" * 50)
        
        try:
            result = send_email_confirmation_can_rest_test(booking_id)
            
            print(f"Email send result: {result}")
            
            if result == "success":
                print("✅ Restaurant cancellation email sent successfully!")
                print("Check your email inbox to see the HTML email.")
                print("📧 Template type: restaurant booking cancellation")
                print("🔴 Should show: Red/pale red header with cancellation details")
            else:
                print("❌ Failed to send restaurant cancellation email.")
                print("Check the console output above for error details.")
                
        except Exception as e:
            print(f"❌ Error during restaurant cancellation email test: {e}")
        
        return
        
    elif choice == "6":
        # Service booking cancellation test
        booking_id = 23083
        booking_type = "service cancellation"
        print(f"\nTesting {booking_type} HTML email")
        print(f"Booking ID: {booking_id}")
        print("-" * 50)
        
        try:
            result = send_email_confirmation_can_test(booking_id)
            
            print(f"Email send result: {result}")
            
            if result == "success":
                print("✅ Service cancellation email sent successfully!")
                print("Check your email inbox to see the HTML email.")
                print("📧 Template type: service booking cancellation")
                print("🔴 Should show: Red/pale red header with cancellation details")
            else:
                print("❌ Failed to send service cancellation email.")
                print("Check the console output above for error details.")
                
        except Exception as e:
            print(f"❌ Error during service cancellation email test: {e}")
        
        return
        
    elif choice == "7":
        try:
            booking_id = int(input("Enter booking ID: ").strip())
            print("Is this a restaurant or service booking?")
            print("1. Restaurant (table assignment)")
            print("2. Service (staff and service)")
            type_choice = input("Choose type (1/2): ").strip()
            
            if type_choice == "1":
                test_function = send_email_confirmation_new_rest_test
                booking_type = "restaurant"
            elif type_choice == "2":
                test_function = send_email_confirmation_new_test
                booking_type = "service"
            else:
                print("Invalid choice. Defaulting to restaurant booking.")
                test_function = send_email_confirmation_new_rest_test
                booking_type = "restaurant"
        except ValueError:
            print("Invalid booking ID. Using default service booking 23207.")
            booking_id = 23207
            test_function = send_email_confirmation_new_test
            booking_type = "service"
    else:
        print("Invalid choice. Using default service booking 23207.")
        booking_id = 23207
        test_function = send_email_confirmation_new_test
        booking_type = "service"
    
    print(f"\nTesting {booking_type} HTML email for booking ID: {booking_id}")
    print("-" * 50)
    
    try:
        # Call the appropriate email function
        result = test_function(booking_id)
        
        print(f"Email send result: {result}")
        
        if result == "success":
            print("✅ Email sent successfully!")
            print("Check your email inbox to see the HTML email.")
            print(f"📧 Template type: {booking_type} booking")
            if booking_type == "restaurant":
                print("🍽️ Should show: Table assignment section")
            else:
                print("👨‍💼 Should show: Staff and service details section")
        else:
            print("❌ Email sending failed.")
            
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        print(f"Error type: {type(e).__name__}")

def check_environment():
    """Check if required environment variables are set."""
    
    print("Checking environment variables...")
    print("-" * 50)
    
    required_vars = [
        "DATABASE_URL",
        "SENDGRID_API_KEY", 
        "SENDGRID_FROM_EMAIL",
        "FALLBACK_EMAIL"
    ]
    
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive values
            if "API_KEY" in var or "URL" in var:
                masked = value[:8] + "..." if len(value) > 8 else "***"
                print(f"✅ {var}: {masked}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NOT SET")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n⚠️  Missing environment variables: {', '.join(missing_vars)}")
        print("Please set them in your .env file or environment.")
        return False
    
    print("\n✅ All required environment variables are set!")
    return True

def check_dependencies():
    """Check if required Python packages are installed."""
    
    print("Checking Python dependencies...")
    print("-" * 50)
    
    required_packages = [
        ("psycopg2", "psycopg2"),
        ("sendgrid", "sendgrid"),
        ("python-dotenv", "dotenv")
    ]
    
    missing_packages = []
    
    for package_name, import_name in required_packages:
        try:
            __import__(import_name)
            print(f"✅ {package_name}: installed")
        except ImportError:
            print(f"❌ {package_name}: NOT INSTALLED")
            missing_packages.append(package_name)
    
    if missing_packages:
        print(f"\n⚠️  Missing packages: {', '.join(missing_packages)}")
        print("Install them with:")
        for package in missing_packages:
            print(f"  pip install {package}")
        return False
    
    print("\n✅ All required packages are installed!")
    return True

def main():
    """Main function to run the test."""
    
    print("HTML Email Test Script")
    print("=" * 50)
    
    # Load environment variables first
    load_dotenv()
    
    # Check dependencies first
    if not check_dependencies():
        print("\n❌ Dependency check failed. Please install the missing packages.")
        return 1
    
    print()
    
    # Check environment
    if not check_environment():
        print("\n❌ Environment check failed. Please fix the issues above.")
        return 1
    
    print()
    
    # Test the email function
    test_html_email()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
