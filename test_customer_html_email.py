#!/usr/bin/env python3
"""
Test script for Customer HTML email functionality
Usage: python test_customer_html_email.py

This test script validates customer-facing email confirmations for:
- New bookings (restaurant and service)
- Booking modifications (restaurant and service)
- Booking cancellations (restaurant and service)

The emails are sent directly to customers using the customer email template.
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

# Import our template utilities
try:
    from email_template_utils import render_customer_booking_confirmation_template, format_time_12hour
except ImportError:
    print("Warning: Could not import email_template_utils. HTML templates will not work.")
    def render_customer_booking_confirmation_template(*args, **kwargs):
        return ""
    def format_time_12hour(time_obj):
        return str(time_obj)

# Simple email validation regex
EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

def send_email_confirmation_customer_new_test(booking_id: int) -> str:
    """
    Test version of the customer email function without Celery decorator.
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
                vu.zone_tag_ids
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        row = cur.fetchone()

        if not row:
            print(f"[CUSTOMER_EMAIL] Booking {booking_id} not found.")
            return "failed"

        (
            tenant_id,
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
            zone_tag_ids
        ) = row

        print(f"[CUSTOMER_EMAIL] Found booking: {booking_ref} for {customer_name}")
        print(f"[CUSTOMER_EMAIL] Location Type: {location_type}")
        print(f"[CUSTOMER_EMAIL] Zone Tag IDs: {zone_tag_ids}")
        print(f"[CUSTOMER_EMAIL] Zone Tag IDs Type: {type(zone_tag_ids)}")

        # Get zone names for table information (restaurant bookings only)
        zone_names = []
        if location_type == "rest" and zone_tag_ids:
            print(f"[CUSTOMER_EMAIL] Restaurant booking detected, fetching zone names...")
            try:
                print(f"[CUSTOMER_EMAIL] Executing zone query with tenant_id={tenant_id}, zone_tag_ids={zone_tag_ids}")
                cur.execute("""
                    SELECT name FROM venue_tag 
                    WHERE tenant_id = %s AND tag_id = ANY(%s)
                    ORDER BY name
                """, (tenant_id, zone_tag_ids))
                
                zone_rows = cur.fetchall()
                zone_names = [row[0] for row in zone_rows]
                print(f"[CUSTOMER_EMAIL] Zone query returned {len(zone_rows)} rows")
                print(f"[CUSTOMER_EMAIL] Found zone names: {zone_names}")
            except Exception as e:
                print(f"[CUSTOMER_EMAIL] Error fetching zone names: {e}")
                print(f"[CUSTOMER_EMAIL] Error details: {type(e).__name__}: {str(e)}")
                zone_names = []
        elif location_type == "rest":
            print(f"[CUSTOMER_EMAIL] Restaurant booking but no zone_tag_ids found")
        else:
            print(f"[CUSTOMER_EMAIL] Not a restaurant booking, skipping zone lookup")

        # Format zone names for display
        zone_names_display = ", ".join(zone_names) if zone_names else "Will be assigned upon arrival"
        print(f"[CUSTOMER_EMAIL] Final zone_names_display: '{zone_names_display}'")

        # Determine recipient email with fallback logic
        recipient_email = None
        
        # Priority 1: Check customer_email field in bookings
        if customer_email and customer_email.strip() and re.match(EMAIL_REGEX, customer_email.strip()):
            recipient_email = customer_email.strip()
            print(f"[CUSTOMER_EMAIL] Using customer_email from booking: {recipient_email}")
        
        # Priority 2: Check customers table if customer_id exists
        elif customer_id:
            print(f"[CUSTOMER_EMAIL] No customer_email in booking, checking customers table for customer_id: {customer_id}")
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
            # Restaurant booking
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your booking has been confirmed! Here are your booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Party Size: {party_num} people\n"
                f"Table: {zone_names_display}\n\n"
                "We look forward to welcoming you!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Booking is Confirmed! 🎉",
                email_message="Great news! Your reservation has been successfully confirmed.",
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="We can't wait to see you! Please arrive on time for your reservation.",
                zone_names=zone_names_display,
                venue_unit_id=venue_unit_id
            )
            print(f"[CUSTOMER_EMAIL] Template called with zone_names='{zone_names_display}'")
        else:
            # Service booking
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your appointment has been confirmed! Here are your booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Staff Member: {staff_name or 'To be assigned'}\n"
                f"Service: {service_name or 'General service'}\n\n"
                "We look forward to serving you!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Appointment is Confirmed! ✅",
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
                service_id=service_id
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

def send_email_confirmation_customer_mod_test(booking_id: int, original_booking_id: int) -> str:
    """
    Test version of customer booking modification email function.
    """
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # Get new booking details
        cur.execute("""
            SELECT 
                b.tenant_id,
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
                vu.zone_tag_ids
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        new_booking = cur.fetchone()

        if not new_booking:
            print(f"[CUSTOMER_EMAIL] New booking {booking_id} not found.")
            return "failed"

        (
            tenant_id,
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
            zone_tag_ids
        ) = new_booking

        print(f"[CUSTOMER_EMAIL] Found modified booking: {booking_ref} for {customer_name}")
        print(f"[CUSTOMER_EMAIL] Location Type: {location_type}")
        print(f"[CUSTOMER_EMAIL] Zone Tag IDs: {zone_tag_ids}")
        print(f"[CUSTOMER_EMAIL] Zone Tag IDs Type: {type(zone_tag_ids)}")

        # Get zone names for table information (restaurant bookings only)
        zone_names = []
        if location_type == "rest" and zone_tag_ids:
            print(f"[CUSTOMER_EMAIL] Restaurant booking detected, fetching zone names...")
            try:
                print(f"[CUSTOMER_EMAIL] Executing zone query with tenant_id={tenant_id}, zone_tag_ids={zone_tag_ids}")
                cur.execute("""
                    SELECT name FROM venue_tag 
                    WHERE tenant_id = %s AND tag_id = ANY(%s)
                    ORDER BY name
                """, (tenant_id, zone_tag_ids))
                
                zone_rows = cur.fetchall()
                zone_names = [row[0] for row in zone_rows]
                print(f"[CUSTOMER_EMAIL] Zone query returned {len(zone_rows)} rows")
                print(f"[CUSTOMER_EMAIL] Found zone names: {zone_names}")
            except Exception as e:
                print(f"[CUSTOMER_EMAIL] Error fetching zone names: {e}")
                print(f"[CUSTOMER_EMAIL] Error details: {type(e).__name__}: {str(e)}")
                zone_names = []
        elif location_type == "rest":
            print(f"[CUSTOMER_EMAIL] Restaurant booking but no zone_tag_ids found")
        else:
            print(f"[CUSTOMER_EMAIL] Not a restaurant booking, skipping zone lookup")

        # Format zone names for display
        zone_names_display = ", ".join(zone_names) if zone_names else "Will be assigned upon arrival"
        print(f"[CUSTOMER_EMAIL] Final zone_names_display: '{zone_names_display}'")

        # Get original booking details for comparison
        cur.execute("""
            SELECT 
                b.start_time,
                b.end_time,
                b.party_num,
                s.name AS staff_name,
                sv.name AS service_name,
                vu.zone_tag_ids
            FROM bookings b
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s
        """, (original_booking_id,))
        
        original_booking = cur.fetchone()

        # Determine recipient email with fallback logic
        recipient_email = None
        
        if customer_email and customer_email.strip() and re.match(EMAIL_REGEX, customer_email.strip()):
            recipient_email = customer_email.strip()
            print(f"[CUSTOMER_EMAIL] Using customer_email from booking: {recipient_email}")
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

        if not recipient_email:
            print(f"[CUSTOMER_EMAIL] No valid customer email found for booking {booking_id}. Skipping email send.")
            return "skipped"

        # Create modification message with original booking context
        if original_booking:
            orig_start_time, orig_end_time, orig_party_num, orig_staff_name, orig_service_name, orig_zone_tag_ids = original_booking
            original_info = (
                f"<div style='background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 15px 0; font-size: 16px; color: #856404; border-radius: 8px;'>"
                "<strong style='color: #6c5700; font-size: 17px;'>📅 Previous Details:</strong><br>"
                f"<span style='font-weight: 600;'>Date:</span> {orig_start_time.strftime('%Y-%m-%d')} | "
                f"<span style='font-weight: 600;'>Time:</span> {orig_start_time.strftime('%H:%M')} - {orig_end_time.strftime('%H:%M')}"
                "</div>"
            )
        else:
            original_info = ""

        # Prepare email content based on location type
        if location_type == "rest":
            email_message = f"Your reservation has been successfully updated!{original_info}"
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your reservation has been successfully updated! Here are your new booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Party Size: {party_num} people\n"
                f"Table: {zone_names_display}\n\n"
                "We look forward to welcoming you with the updated arrangements!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Booking Has Been Updated! 📝",
                email_message=email_message,
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="Thank you for updating your reservation. We can't wait to see you!",
                zone_names=zone_names_display,
                venue_unit_id=venue_unit_id,
                is_modification=True
            )
            print(f"[CUSTOMER_EMAIL] Template called with zone_names='{zone_names_display}'")
        else:
            email_message = f"Your appointment has been successfully updated!{original_info}"
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your appointment has been successfully updated! Here are your new booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Staff Member: {staff_name or 'To be assigned'}\n"
                f"Service: {service_name or 'General service'}\n\n"
                "We look forward to serving you with the updated arrangements!\n\n"
                "Best regards,\n"
                f"{location_name}"
            )

            html_template = render_customer_booking_confirmation_template(
                email_title="Your Appointment Has Been Updated! 🔄",
                email_message=email_message,
                location_name=location_name,
                booking_ref=booking_ref,
                customer_name=customer_name,
                customer_phone=customer_phone,
                party_num=party_num,
                booking_date=start_time.strftime('%Y-%m-%d'),
                start_time=start_time_formatted,
                closing_message="Thank you for updating your appointment. We're excited to serve you!",
                staff_name=staff_name,
                staff_id=staff_id,
                service_name=service_name,
                service_id=service_id,
                is_modification=True
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

        print(f"[CUSTOMER_EMAIL] Sent to {recipient_email}: Customer booking modification email")
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

def send_email_confirmation_customer_can_test(booking_id: int) -> str:
    """
    Test version of customer booking cancellation email function.
    """
    try:
        # Connect to database
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # Get cancelled booking details
        cur.execute("""
            SELECT 
                b.tenant_id,
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
                vu.zone_tag_ids
            FROM bookings b
            JOIN locations l
              ON b.tenant_id = l.tenant_id AND b.location_id = l.location_id
            LEFT JOIN staff s
              ON b.tenant_id = s.tenant_id AND b.staff_id = s.staff_id
            LEFT JOIN services sv
              ON b.tenant_id = sv.tenant_id AND b.service_id = sv.service_id
            LEFT JOIN venue_unit vu
              ON b.tenant_id = vu.tenant_id AND b.venue_unit_id = vu.venue_unit_id
            WHERE b.booking_id = %s
        """, (booking_id,))
        
        booking = cur.fetchone()

        if not booking:
            print(f"[CUSTOMER_EMAIL] Booking {booking_id} not found.")
            return "failed"

        (
            tenant_id,
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
            zone_tag_ids
        ) = booking

        print(f"[CUSTOMER_EMAIL] Found cancelled booking: {booking_ref} for {customer_name}")
        print(f"[CUSTOMER_EMAIL] Location Type: {location_type}")
        print(f"[CUSTOMER_EMAIL] Zone Tag IDs: {zone_tag_ids}")
        print(f"[CUSTOMER_EMAIL] Zone Tag IDs Type: {type(zone_tag_ids)}")

        # Get zone names for table information (restaurant bookings only)
        zone_names = []
        if location_type == "rest" and zone_tag_ids:
            print(f"[CUSTOMER_EMAIL] Restaurant booking detected, fetching zone names...")
            try:
                print(f"[CUSTOMER_EMAIL] Executing zone query with tenant_id={tenant_id}, zone_tag_ids={zone_tag_ids}")
                cur.execute("""
                    SELECT name FROM venue_tag 
                    WHERE tenant_id = %s AND tag_id = ANY(%s)
                    ORDER BY name
                """, (tenant_id, zone_tag_ids))
                
                zone_rows = cur.fetchall()
                zone_names = [row[0] for row in zone_rows]
                print(f"[CUSTOMER_EMAIL] Zone query returned {len(zone_rows)} rows")
                print(f"[CUSTOMER_EMAIL] Found zone names: {zone_names}")
            except Exception as e:
                print(f"[CUSTOMER_EMAIL] Error fetching zone names: {e}")
                print(f"[CUSTOMER_EMAIL] Error details: {type(e).__name__}: {str(e)}")
                zone_names = []
        elif location_type == "rest":
            print(f"[CUSTOMER_EMAIL] Restaurant booking but no zone_tag_ids found")
        else:
            print(f"[CUSTOMER_EMAIL] Not a restaurant booking, skipping zone lookup")

        # Format zone names for display
        zone_names_display = ", ".join(zone_names) if zone_names else "Not assigned"
        print(f"[CUSTOMER_EMAIL] Final zone_names_display: '{zone_names_display}'")

        # Determine recipient email with fallback logic
        recipient_email = None
        
        if customer_email and customer_email.strip() and re.match(EMAIL_REGEX, customer_email.strip()):
            recipient_email = customer_email.strip()
            print(f"[CUSTOMER_EMAIL] Using customer_email from booking: {recipient_email}")
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

        if not recipient_email:
            print(f"[CUSTOMER_EMAIL] No valid customer email found for booking {booking_id}. Skipping email send.")
            return "skipped"

        # Prepare email content based on location type
        if location_type == "rest":
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your reservation has been cancelled. Here were the booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Party Size: {party_num} people\n"
                f"Table: {zone_names_display}\n\n"
                "We hope to welcome you again in the future.\n\n"
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
                closing_message="We hope to have the opportunity to serve you in the future.",
                zone_names=zone_names_display,
                venue_unit_id=venue_unit_id,
                is_cancellation=True
            )
            print(f"[CUSTOMER_EMAIL] Template called with zone_names='{zone_names_display}'")
        else:
            # Format start time to 12-hour format
            start_time_formatted = format_time_12hour(start_time)

            plain_text_body = (
                f"Dear {customer_name},\n\n"
                "Your appointment has been cancelled. Here were the booking details:\n\n"
                f"Location: {location_name}\n"
                f"Booking Reference: {booking_ref}\n"
                f"Date: {start_time.strftime('%Y-%m-%d')}\n"
                f"Time: {start_time_formatted}\n"
                f"Staff Member: {staff_name or 'Not assigned'}\n"
                f"Service: {service_name or 'General service'}\n\n"
                "We hope to serve you again in the future.\n\n"
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
                closing_message="We hope to have the opportunity to serve you in the future.",
                staff_name=staff_name,
                staff_id=staff_id,
                service_name=service_name,
                service_id=service_id,
                is_cancellation=True
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

        print(f"[CUSTOMER_EMAIL] Sent to {recipient_email}: Customer booking cancellation email")
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

def test_customer_html_email():
    """Test the customer HTML email functions with specific booking IDs."""
    
    # Load environment variables
    load_dotenv()
    
    print("🎯 Customer Email Test Options:")
    print("=" * 50)
    print("1. 📧 New Restaurant Booking (customer confirmation)")
    print("2. 📧 New Service Booking (customer confirmation)")
    print("3. 🔄 Restaurant Booking Modification (customer notification)")
    print("4. 🔄 Service Booking Modification (customer notification)")
    print("5. ❌ Restaurant Booking Cancellation (customer notification)")
    print("6. ❌ Service Booking Cancellation (customer notification)")
    print("7. 🎲 Custom Booking ID (specify your own)")
    print("8. 🧪 Test All Scenarios (run multiple tests)")
    print()
    
    choice = input("Choose test option (1-8): ").strip()
    
    if choice == "1":
        # Restaurant booking confirmation
        booking_id = input("Enter restaurant booking ID (or press Enter for default 23208): ").strip() or "23208"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 23208.")
            booking_id = 23208
            
        print(f"\n🍽️ Testing Restaurant Customer Confirmation Email")
        print(f"Booking ID: {booking_id}")
        print("-" * 50)
        
        result = send_email_confirmation_customer_new_test(booking_id)
        
        if result == "success":
            print("✅ Customer restaurant confirmation email sent successfully!")
            print("📧 Template: Customer-facing restaurant booking confirmation")
            print("🎨 Design: Green theme with table assignment details")
        elif result == "skipped":
            print("⚠️ Email skipped - no customer email found in database")
        else:
            print("❌ Failed to send customer email")
            
    elif choice == "2":
        # Service booking confirmation
        booking_id = input("Enter service booking ID (or press Enter for default 23207): ").strip() or "23207"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 23207.")
            booking_id = 23207
            
        print(f"\n👨‍💼 Testing Service Customer Confirmation Email")
        print(f"Booking ID: {booking_id}")
        print("-" * 50)
        
        result = send_email_confirmation_customer_new_test(booking_id)
        
        if result == "success":
            print("✅ Customer service confirmation email sent successfully!")
            print("📧 Template: Customer-facing service booking confirmation")
            print("🎨 Design: Green theme with staff and service details")
        elif result == "skipped":
            print("⚠️ Email skipped - no customer email found in database")
        else:
            print("❌ Failed to send customer email")
            
    elif choice == "3":
        # Restaurant modification
        new_id = input("Enter new restaurant booking ID (or press Enter for default 23194): ").strip() or "23194"
        original_id = input("Enter original booking ID (or press Enter for default 23193): ").strip() or "23193"
        
        try:
            new_id = int(new_id)
            original_id = int(original_id)
        except ValueError:
            print("Invalid booking IDs. Using defaults.")
            new_id, original_id = 23194, 23193
            
        print(f"\n🔄 Testing Restaurant Customer Modification Email")
        print(f"New Booking ID: {new_id}, Original ID: {original_id}")
        print("-" * 50)
        
        result = send_email_confirmation_customer_mod_test(new_id, original_id)
        
        if result == "success":
            print("✅ Customer restaurant modification email sent successfully!")
            print("📧 Template: Customer-facing modification with original booking context")
            print("🎨 Design: Orange/yellow theme showing changes")
        elif result == "skipped":
            print("⚠️ Email skipped - no customer email found in database")
        else:
            print("❌ Failed to send customer modification email")
            
    elif choice == "4":
        # Service modification
        new_id = input("Enter new service booking ID (or press Enter for default 23084): ").strip() or "23084"
        original_id = input("Enter original booking ID (or press Enter for default 23082): ").strip() or "23082"
        
        try:
            new_id = int(new_id)
            original_id = int(original_id)
        except ValueError:
            print("Invalid booking IDs. Using defaults.")
            new_id, original_id = 23084, 23082
            
        print(f"\n🔄 Testing Service Customer Modification Email")
        print(f"New Booking ID: {new_id}, Original ID: {original_id}")
        print("-" * 50)
        
        result = send_email_confirmation_customer_mod_test(new_id, original_id)
        
        if result == "success":
            print("✅ Customer service modification email sent successfully!")
            print("📧 Template: Customer-facing modification with staff/service changes")
            print("🎨 Design: Orange/yellow theme showing appointment changes")
        elif result == "skipped":
            print("⚠️ Email skipped - no customer email found in database")
        else:
            print("❌ Failed to send customer modification email")
            
    elif choice == "5":
        # Restaurant cancellation
        booking_id = input("Enter cancelled restaurant booking ID (or press Enter for default 22985): ").strip() or "22985"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 22985.")
            booking_id = 22985
            
        print(f"\n❌ Testing Restaurant Customer Cancellation Email")
        print(f"Booking ID: {booking_id}")
        print("-" * 50)
        
        result = send_email_confirmation_customer_can_test(booking_id)
        
        if result == "success":
            print("✅ Customer restaurant cancellation email sent successfully!")
            print("📧 Template: Customer-facing cancellation notification")
            print("🎨 Design: Red theme with sympathetic messaging")
        elif result == "skipped":
            print("⚠️ Email skipped - no customer email found in database")
        else:
            print("❌ Failed to send customer cancellation email")
            
    elif choice == "6":
        # Service cancellation
        booking_id = input("Enter cancelled service booking ID (or press Enter for default 23083): ").strip() or "23083"
        try:
            booking_id = int(booking_id)
        except ValueError:
            print("Invalid booking ID. Using default 23083.")
            booking_id = 23083
            
        print(f"\n❌ Testing Service Customer Cancellation Email")
        print(f"Booking ID: {booking_id}")
        print("-" * 50)
        
        result = send_email_confirmation_customer_can_test(booking_id)
        
        if result == "success":
            print("✅ Customer service cancellation email sent successfully!")
            print("📧 Template: Customer-facing appointment cancellation")
            print("🎨 Design: Red theme with sympathetic messaging")
        elif result == "skipped":
            print("⚠️ Email skipped - no customer email found in database")
        else:
            print("❌ Failed to send customer cancellation email")
            
    elif choice == "7":
        # Custom booking ID
        try:
            booking_id = int(input("Enter booking ID: ").strip())
            print("What type of test?")
            print("1. New booking confirmation")
            print("2. Booking modification (need original ID too)")
            print("3. Booking cancellation")
            
            test_type = input("Choose test type (1/2/3): ").strip()
            
            if test_type == "1":
                print(f"\n🎯 Testing Custom Customer Confirmation Email")
                print(f"Booking ID: {booking_id}")
                print("-" * 50)
                result = send_email_confirmation_customer_new_test(booking_id)
            elif test_type == "2":
                original_id = int(input("Enter original booking ID: ").strip())
                print(f"\n🎯 Testing Custom Customer Modification Email")
                print(f"New Booking ID: {booking_id}, Original ID: {original_id}")
                print("-" * 50)
                result = send_email_confirmation_customer_mod_test(booking_id, original_id)
            elif test_type == "3":
                print(f"\n🎯 Testing Custom Customer Cancellation Email")
                print(f"Booking ID: {booking_id}")
                print("-" * 50)
                result = send_email_confirmation_customer_can_test(booking_id)
            else:
                print("Invalid choice. Testing as new booking.")
                result = send_email_confirmation_customer_new_test(booking_id)
                
            if result == "success":
                print("✅ Custom customer email sent successfully!")
            elif result == "skipped":
                print("⚠️ Email skipped - no customer email found in database")
            else:
                print("❌ Failed to send custom customer email")
                
        except ValueError:
            print("Invalid booking ID.")
            return
            
    elif choice == "8":
        # Test all scenarios
        print("\n🧪 Running All Customer Email Tests")
        print("=" * 50)
        
        tests = [
            ("Restaurant Confirmation", lambda: send_email_confirmation_customer_new_test(23208)),
            ("Service Confirmation", lambda: send_email_confirmation_customer_new_test(23207)),
            ("Restaurant Modification", lambda: send_email_confirmation_customer_mod_test(23194, 23193)),
            ("Service Modification", lambda: send_email_confirmation_customer_mod_test(23084, 23082)),
            ("Restaurant Cancellation", lambda: send_email_confirmation_customer_can_test(22985)),
            ("Service Cancellation", lambda: send_email_confirmation_customer_can_test(23083)),
        ]
        
        results = {}
        for test_name, test_func in tests:
            print(f"\n🔄 Testing {test_name}...")
            try:
                result = test_func()
                results[test_name] = result
                if result == "success":
                    print(f"✅ {test_name}: SUCCESS")
                elif result == "skipped":
                    print(f"⚠️ {test_name}: SKIPPED (no customer email)")
                else:
                    print(f"❌ {test_name}: FAILED")
            except Exception as e:
                results[test_name] = "error"
                print(f"💥 {test_name}: ERROR - {e}")
        
        print("\n📊 Test Results Summary:")
        print("-" * 30)
        for test_name, result in results.items():
            status_icon = "✅" if result == "success" else "⚠️" if result == "skipped" else "❌"
            print(f"{status_icon} {test_name}: {result.upper()}")
            
    else:
        print("Invalid choice. Please run the script again.")
        return
    
    print("\n" + "=" * 50)
    print("🎯 Customer Email Test Complete!")
    print("Check your customer email inbox to see the results.")
    print("💡 Note: Emails are sent to customer emails found in the database.")

def check_environment():
    """Check if required environment variables are set."""
    
    print("Checking environment variables...")
    print("-" * 50)
    
    required_vars = [
        "DATABASE_URL",
        "SENDGRID_API_KEY", 
        "SENDGRID_FROM_EMAIL"
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
    """Main function to run the customer email tests."""
    
    print("📧 Customer HTML Email Test Script")
    print("=" * 50)
    print("This script tests customer-facing email confirmations")
    print("(different from merchant/host emails)")
    print()
    
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
    
    # Test the customer email functions
    test_customer_html_email()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
