#!/usr/bin/env python3
"""
Email template utilities for rendering HTML email templates.
"""

import os
import re
from datetime import datetime

def format_time_12hour(time_obj) -> str:
    """
    Format time object to 12-hour format with am/pm.
    
    Args:
        time_obj: datetime object or time string
    
    Returns:
        str: Formatted time (e.g., "3:30pm", "9:30am")
    """
    if isinstance(time_obj, str):
        # If it's already a string, try to parse it
        try:
            time_obj = datetime.strptime(time_obj, '%H:%M').time()
        except:
            return time_obj  # Return as-is if parsing fails
    
    if hasattr(time_obj, 'time'):
        # It's a datetime object, extract time
        time_obj = time_obj.time()
    
    # Format to 12-hour with am/pm
    hour = time_obj.hour
    minute = time_obj.minute
    
    if hour == 0:
        return f"12:{minute:02d}am"
    elif hour < 12:
        return f"{hour}:{minute:02d}am"
    elif hour == 12:
        return f"12:{minute:02d}pm"
    else:
        return f"{hour-12}:{minute:02d}pm"

def load_email_template(template_name: str) -> str:
    """
    Load an email template from the email_templates directory.
    
    Args:
        template_name (str): Name of the template file (e.g., 'booking_confirmation.html')
    
    Returns:
        str: Template content as a string, or empty string if not found
    """
    try:
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(current_dir, 'email_templates', template_name)
        
        with open(template_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        print(f"[EMAIL_TEMPLATE] Template file not found: {template_name}")
        return ""
    except Exception as e:
        print(f"[EMAIL_TEMPLATE] Error loading template {template_name}: {e}")
        return ""

def render_booking_confirmation_template(**kwargs) -> str:
    """
    Render the booking confirmation email template with provided data.
    
    Args:
        **kwargs: Template variables including:
            - email_title: Title of the email
            - email_message: Main message text
            - location_name: Name of the location
            - booking_ref: Booking reference number
            - customer_name: Customer's name
            - customer_phone: Customer's phone number
            - party_num: Number of people in the party
            - booking_date: Date of the booking
            - start_time: Start time
            - end_time: End time
            - closing_message: Closing message
            - venue_unit_name: Table/venue name (optional)
            - venue_unit_id: Table/venue ID (optional)
            - staff_name: Staff name (optional, for services)
            - staff_id: Staff ID (optional, for services)
            - service_name: Service name (optional, for services)
            - service_id: Service ID (optional, for services)
    
    Returns:
        str: Rendered HTML template, or empty string if template loading fails
    """
    try:
        # Load the base template
        template = load_email_template('booking_confirmation.html')
        
        if not template:
            return ""
        
        # Replace all placeholders with provided values
        for key, value in kwargs.items():
            placeholder = f"{{{{{key}}}}}"
            
            # Convert None values to empty string or appropriate default
            if value is None:
                if key in ['venue_unit_name', 'staff_name', 'service_name']:
                    value = 'Not Assigned'
                elif key in ['venue_unit_id', 'staff_id', 'service_id']:
                    value = 'Not Assigned'
                else:
                    value = ''
            
            # Convert to string
            template = template.replace(placeholder, str(value))
        
        # Handle optional sections based on presence of venue/staff data
        if kwargs.get('venue_unit_name') or kwargs.get('venue_unit_id'):
            # This is a restaurant booking - show venue section and party size, hide staff section
            template = template.replace('{{#venue_section}}', '')
            template = template.replace('{{/venue_section}}', '')
            template = template.replace('{{#party_section}}', '')
            template = template.replace('{{/party_section}}', '')
            template = re.sub(r'{{#staff_section}}.*?{{/staff_section}}', '', template, flags=re.DOTALL)
        elif kwargs.get('staff_name') or kwargs.get('service_name'):
            # This is a service booking - show staff section, hide venue section and party size
            template = template.replace('{{#staff_section}}', '')
            template = template.replace('{{/staff_section}}', '')
            template = re.sub(r'{{#venue_section}}.*?{{/venue_section}}', '', template, flags=re.DOTALL)
            template = re.sub(r'{{#party_section}}.*?{{/party_section}}', '', template, flags=re.DOTALL)
        else:
            # No specific venue or staff info - hide all optional sections
            template = re.sub(r'{{#venue_section}}.*?{{/venue_section}}', '', template, flags=re.DOTALL)
            template = re.sub(r'{{#staff_section}}.*?{{/staff_section}}', '', template, flags=re.DOTALL)
            template = re.sub(r'{{#party_section}}.*?{{/party_section}}', '', template, flags=re.DOTALL)
        
        # Handle modification styling
        if kwargs.get('is_modification'):
            template = template.replace('{{#is_modification}}', '')
            template = template.replace('{{/is_modification}}', '')
        else:
            template = re.sub(r'{{#is_modification}}.*?{{/is_modification}}', '', template, flags=re.DOTALL)
        
        # Handle cancellation styling
        if kwargs.get('is_cancellation'):
            template = template.replace('{{#is_cancellation}}', '')
            template = template.replace('{{/is_cancellation}}', '')
        else:
            template = re.sub(r'{{#is_cancellation}}.*?{{/is_cancellation}}', '', template, flags=re.DOTALL)
        
        # Handle original booking section (for modifications)
        if kwargs.get('is_modification') and (kwargs.get('original_booking_date') or kwargs.get('original_start_time')):
            template = template.replace('{{#original_booking_section}}', '')
            template = template.replace('{{/original_booking_section}}', '')
        else:
            template = re.sub(r'{{#original_booking_section}}.*?{{/original_booking_section}}', '', template, flags=re.DOTALL)
        
        # Handle manage booking section
        booking_page_alias = kwargs.get('booking_page_alias')
        if booking_page_alias and booking_page_alias.strip():
            template = template.replace('{{#manage_booking_section}}', '')
            template = template.replace('{{/manage_booking_section}}', '')
        else:
            template = re.sub(r'{{#manage_booking_section}}.*?{{/manage_booking_section}}', '', template, flags=re.DOTALL)
        
        # Clean up any remaining placeholder sections
        template = re.sub(r'{{#\w+}}', '', template)
        template = re.sub(r'{{/\w+}}', '', template)
        
        # Clean up any remaining placeholders that weren't provided
        template = re.sub(r'{{[^}]+}}', '', template)
        
        return template
        
    except Exception as e:
        print(f"[EMAIL_TEMPLATE] Error rendering template: {e}")
        return ""

def render_customer_booking_confirmation_template(**kwargs) -> str:
    """
    Render the customer booking confirmation email template with provided data.
    This template is specifically designed for customer-facing emails.
    
    Args:
        **kwargs: Template variables including:
            - email_title: Title of the email
            - email_message: Main message text
            - location_name: Name of the location
            - booking_ref: Booking reference number
            - customer_name: Customer's name
            - customer_phone: Customer's phone number
            - party_num: Number of people in the party
            - booking_date: Date of the booking
            - start_time: Start time
            - end_time: End time
            - closing_message: Closing message
            - zone_names: Zone/table names (for restaurants)
            - venue_unit_name: Table/venue name (optional, deprecated - use zone_names)
            - venue_unit_id: Table/venue ID (optional)
            - staff_name: Staff name (optional, for services)
            - staff_id: Staff ID (optional, for services)
            - service_name: Service name (optional, for services)
            - service_id: Service ID (optional, for services)
            - is_modification: Boolean indicating if this is a modification email
            - is_cancellation: Boolean indicating if this is a cancellation email
            - logo_url: URL of the location's logo (optional)
            - banner_url: URL of the location's banner (optional)
            - original_booking_date: Original booking date (for modifications)
            - original_start_time: Original start time (for modifications)
            - original_party_num: Original party size (for modifications)
            - original_staff_name: Original staff name (for modifications)
            - original_service_name: Original service name (for modifications)
            - original_zone_names: Original zone/table names (for modifications)
            - booking_page_alias: Booking page alias for manage booking URL (optional)
    
    Returns:
        str: Rendered HTML template, or empty string if template loading fails
    """
    try:
        # Load the customer template
        template = load_email_template('customer_booking_confirmation.html')
        
        if not template:
            return ""
        
        # Replace all placeholders with provided values
        for key, value in kwargs.items():
            placeholder = f"{{{{{key}}}}}"
            
            # Convert None values to empty string or appropriate default
            if value is None:
                if key in ['zone_names', 'venue_unit_name', 'staff_name', 'service_name']:
                    value = 'Not Assigned'
                elif key in ['venue_unit_id', 'staff_id', 'service_id']:
                    value = 'Not Assigned'
                elif key in ['logo_url', 'banner_url']:
                    value = ''
                else:
                    value = ''
            
            # Handle zone_names special case - convert list to string
            if key == 'zone_names' and isinstance(value, list):
                if value:
                    value = ', '.join(str(zone) for zone in value)
                else:
                    value = 'Not Assigned'
            
            # Handle original_zone_names special case - convert list to string
            if key == 'original_zone_names' and isinstance(value, list):
                if value:
                    value = ', '.join(str(zone) for zone in value)
                else:
                    value = 'Not Assigned'
            
            # Convert to string
            template = template.replace(placeholder, str(value))
        
        # Handle manage booking URL construction
        booking_page_alias = kwargs.get('booking_page_alias')
        booking_access_token = kwargs.get('booking_access_token')
        
        if booking_page_alias and booking_page_alias.strip():
            if booking_access_token and booking_access_token.strip():
                # Construct URL with token parameter
                manage_booking_url = f"https://speako.ai/en-US/customer/booking/{booking_page_alias.strip()}/view?token={booking_access_token.strip()}"
            else:
                # Fallback URL without token
                manage_booking_url = f"https://speako.ai/en-US/customer/booking/{booking_page_alias.strip()}/view"
            template = template.replace('{{manage_booking_url}}', manage_booking_url)
        else:
            template = template.replace('{{manage_booking_url}}', '')
        
        # Handle button color defaults
        if 'button_color_start' not in kwargs or not kwargs.get('button_color_start'):
            # Default to green gradient for new bookings
            template = template.replace('{{button_color_start}}', '#28a745')
        if 'button_color_end' not in kwargs or not kwargs.get('button_color_end'):
            # Default to green gradient for new bookings
            template = template.replace('{{button_color_end}}', '#20c997')
        
        # Handle optional sections based on presence of venue/staff data
        if kwargs.get('zone_names') or kwargs.get('venue_unit_name') or kwargs.get('venue_unit_id'):
            # This is a restaurant booking - show venue section and party size, hide staff section
            template = template.replace('{{#venue_section}}', '')
            template = template.replace('{{/venue_section}}', '')
            template = template.replace('{{#party_section}}', '')
            template = template.replace('{{/party_section}}', '')
            template = re.sub(r'{{#staff_section}}.*?{{/staff_section}}', '', template, flags=re.DOTALL)
        elif kwargs.get('staff_name') or kwargs.get('service_name'):
            # This is a service booking - show staff section, hide venue section and party size
            template = template.replace('{{#staff_section}}', '')
            template = template.replace('{{/staff_section}}', '')
            template = re.sub(r'{{#venue_section}}.*?{{/venue_section}}', '', template, flags=re.DOTALL)
            template = re.sub(r'{{#party_section}}.*?{{/party_section}}', '', template, flags=re.DOTALL)
        else:
            # No specific venue or staff info - hide all optional sections
            template = re.sub(r'{{#venue_section}}.*?{{/venue_section}}', '', template, flags=re.DOTALL)
            template = re.sub(r'{{#staff_section}}.*?{{/staff_section}}', '', template, flags=re.DOTALL)
            template = re.sub(r'{{#party_section}}.*?{{/party_section}}', '', template, flags=re.DOTALL)
        
        # Handle logo section
        if kwargs.get('logo_url') and kwargs.get('logo_url').strip():
            template = template.replace('{{#logo_section}}', '')
            template = template.replace('{{/logo_section}}', '')
        else:
            template = re.sub(r'{{#logo_section}}.*?{{/logo_section}}', '', template, flags=re.DOTALL)
        
        # Handle banner section
        if kwargs.get('banner_url') and kwargs.get('banner_url').strip():
            template = template.replace('{{#banner_section}}', '')
            template = template.replace('{{/banner_section}}', '')
        else:
            template = re.sub(r'{{#banner_section}}.*?{{/banner_section}}', '', template, flags=re.DOTALL)
        
        # Handle modification styling
        if kwargs.get('is_modification'):
            template = template.replace('{{#is_modification}}', '')
            template = template.replace('{{/is_modification}}', '')
        else:
            template = re.sub(r'{{#is_modification}}.*?{{/is_modification}}', '', template, flags=re.DOTALL)
        
        # Handle cancellation styling
        if kwargs.get('is_cancellation'):
            template = template.replace('{{#is_cancellation}}', '')
            template = template.replace('{{/is_cancellation}}', '')
        else:
            template = re.sub(r'{{#is_cancellation}}.*?{{/is_cancellation}}', '', template, flags=re.DOTALL)
        
        # Handle original booking section (for modifications)
        if kwargs.get('is_modification') and (kwargs.get('original_booking_date') or kwargs.get('original_start_time')):
            template = template.replace('{{#original_booking_section}}', '')
            template = template.replace('{{/original_booking_section}}', '')
        else:
            template = re.sub(r'{{#original_booking_section}}.*?{{/original_booking_section}}', '', template, flags=re.DOTALL)
        
        # Handle manage booking section
        booking_page_alias = kwargs.get('booking_page_alias')
        if booking_page_alias and booking_page_alias.strip():
            template = template.replace('{{#manage_booking_section}}', '')
            template = template.replace('{{/manage_booking_section}}', '')
        else:
            template = re.sub(r'{{#manage_booking_section}}.*?{{/manage_booking_section}}', '', template, flags=re.DOTALL)
        
        # Clean up any remaining placeholder sections
        template = re.sub(r'{{#\w+}}', '', template)
        template = re.sub(r'{{/\w+}}', '', template)
        
        # Clean up any remaining placeholders that weren't provided
        template = re.sub(r'{{[^}]+}}', '', template)
        
        return template
        
    except Exception as e:
        print(f"[EMAIL_TEMPLATE] Error rendering customer template: {e}")
        return ""

def render_template_with_data(template_name: str, **kwargs) -> str:
    """
    Generic function to render any template with provided data.
    
    Args:
        template_name (str): Name of the template file
        **kwargs: Template variables
    
    Returns:
        str: Rendered HTML template
    """
    try:
        template = load_email_template(template_name)
        
        if not template:
            return ""
        
        # Replace all placeholders
        for key, value in kwargs.items():
            placeholder = f"{{{{{key}}}}}"
            template = template.replace(placeholder, str(value) if value is not None else '')
        
        return template
        
    except Exception as e:
        print(f"[EMAIL_TEMPLATE] Error rendering template {template_name}: {e}")
        return ""
