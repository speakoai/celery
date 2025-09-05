#!/usr/bin/env python3
"""
Email template utilities for rendering HTML email templates.
"""

import os
import re

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
        
        # Clean up any remaining placeholder sections
        template = re.sub(r'{{#\w+}}', '', template)
        template = re.sub(r'{{/\w+}}', '', template)
        
        # Clean up any remaining placeholders that weren't provided
        template = re.sub(r'{{[^}]+}}', '', template)
        
        return template
        
    except Exception as e:
        print(f"[EMAIL_TEMPLATE] Error rendering template: {e}")
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
