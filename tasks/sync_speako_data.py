import os
import time
import json
"""
Sync knowledge data from Speako's internal database.

ARCHITECTURE:
This module uses a DATABASE-DRIVEN approach where:
1. Raw data is queried from PostgreSQL
2. Minimal formatting is applied (time conversions, data grouping)
3. Raw data is packaged and ready for OpenAI processing
4. OpenAI generates BOTH json_data and markdown_data based on ai_prompt from ai_knowledge_types table
5. No hardcoded markdown templates - all formatting controlled via database

This eliminates the need for code redeployment when templates change.
Just update ai_knowledge_types.ai_prompt in the database!

TODO: Implement OpenAI integration to replace placeholder markdown generation.
"""

from datetime import datetime

from celery.utils.log import get_task_logger
from tasks.celery_app import app

from .utils.task_db import mark_task_running, mark_task_failed, mark_task_succeeded, upsert_tenant_integration_param

logger = get_task_logger(__name__)

# Database connection
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    _psycopg2_available = True
except ImportError:
    _psycopg2_available = False
    logger.warning("psycopg2 not available - database sync will not work")


def _get_db_connection():
    """Get PostgreSQL database connection."""
    if not _psycopg2_available:
        raise RuntimeError("psycopg2 is not installed")
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise RuntimeError('DATABASE_URL environment variable not set')
    
    conn = psycopg2.connect(database_url)
    return conn


def _format_time(time_obj) -> str:
    """
    Format time object to HH:MM string.
    Handles both datetime.time and string formats.
    """
    if time_obj is None:
        return ""
    if isinstance(time_obj, str):
        # Already a string, parse and reformat if needed
        return time_obj[:5] if len(time_obj) >= 5 else time_obj
    else:
        # datetime.time object
        return time_obj.strftime('%H:%M')


def _extract_duration_minutes(interval_obj) -> int:
    """
    Convert PostgreSQL interval/timedelta to total minutes.
    
    Args:
        interval_obj: timedelta or interval from psycopg2
    
    Returns:
        int: Total minutes (e.g., 45, 90, 120)
    """
    if interval_obj is None:
        return 0
    
    # timedelta.total_seconds() returns float
    total_seconds = interval_obj.total_seconds()
    return int(total_seconds / 60)


def _format_recurring_hours(hours_data: list) -> dict:
    """
    Convert recurring availability into weekly schedule.
    
    Args:
        hours_data: list with day_of_week, start_time, end_time, slot_name
    
    Returns:
        dict: {"mon": ["09:00–17:00", ...], "tue": [...], ...}
    """
    # PostgreSQL day_of_week: 0=Sunday, 1=Monday, ..., 6=Saturday
    day_map = {
        0: 'sun', 1: 'mon', 2: 'tue', 3: 'wed',
        4: 'thu', 5: 'fri', 6: 'sat'
    }
    
    # Initialize all days as empty (closed)
    hours = {day: [] for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']}
    
    # Group by day and format times
    for row in hours_data:
        day_num = row['day_of_week']
        day_name = day_map.get(day_num)
        if not day_name:
            continue
            
        start = _format_time(row['start_time'])
        end = _format_time(row['end_time'])
        slot = f"{start}–{end}"
        
        if row.get('slot_name'):
            slot += f" ({row['slot_name']})"
        
        hours[day_name].append(slot)
    
    return hours


def _format_onetime_hours(hours_data: list) -> list:
    """
    Convert one-time availability into exceptions list.
    
    Returns:
        list of dicts with date, type, holiday_name, status, hours
    """
    exceptions = {}
    
    for row in hours_data:
        date_str = row['specific_date'].strftime('%Y-%m-%d') if row['specific_date'] else None
        if not date_str:
            continue
        
        if date_str not in exceptions:
            # Determine exception type
            if row['is_closed'] and row.get('holiday_id'):
                exception_type = 'public_holiday'
            elif row['is_closed']:
                exception_type = 'closure'
            else:
                exception_type = 'special_hours'
            
            exceptions[date_str] = {
                'date': date_str,
                'type': exception_type,
                'holiday_name': row.get('holiday_name'),
                'status': 'closed' if row['is_closed'] else 'open',
                'hours': []
            }
        
        # Add time slots if not closed
        if not row['is_closed']:
            start = _format_time(row['start_time'])
            end = _format_time(row['end_time'])
            slot = f"{start}–{end}"
            
            if row.get('slot_name'):
                slot += f" ({row['slot_name']})"
            
            exceptions[date_str]['hours'].append(slot)
    
    return sorted(exceptions.values(), key=lambda x: x['date'])


# ============================================================================
# Markdown Generation Helpers for business_info
# ============================================================================

def _format_day_hours_markdown(hours_array: list) -> str:
    """Format hours array for a single day into markdown string."""
    if not hours_array:
        return "Closed"
    return ", ".join(hours_array)


def _format_week_schedule_markdown(recurring_dict: dict) -> str:
    """Format weekly recurring hours into markdown bullet list."""
    days_order = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
    day_names = {
        'mon': 'Monday',
        'tue': 'Tuesday', 
        'wed': 'Wednesday',
        'thu': 'Thursday',
        'fri': 'Friday',
        'sat': 'Saturday',
        'sun': 'Sunday'
    }
    
    lines = []
    for day_key in days_order:
        day_name = day_names[day_key]
        hours = recurring_dict.get(day_key, [])
        hours_text = _format_day_hours_markdown(hours)
        lines.append(f"- **{day_name}**: {hours_text}")
    
    return "\n".join(lines)


def _format_exceptions_markdown(exceptions_array: list) -> str:
    """Format exceptions (special hours/closures) into markdown list."""
    if not exceptions_array:
        return ""
    
    lines = []
    for exc in exceptions_array:
        date = exc.get('date', '')
        status = exc.get('status', '')
        exc_type = exc.get('type', '')
        holiday_name = exc.get('holiday_name', '')
        hours = exc.get('hours', [])
        
        # Build description
        if status == 'closed':
            if holiday_name:
                desc = f"Closed ({holiday_name})"
            else:
                desc = "Closed"
        else:
            hours_text = ", ".join(hours) if hours else "Open"
            if holiday_name:
                desc = f"{hours_text} ({holiday_name})"
            elif exc_type == 'special_hours':
                desc = f"{hours_text} (Special Hours)"
            else:
                desc = hours_text
        
        lines.append(f"- **{date}**: {desc}")
    
    return "\n".join(lines)


def _build_business_info_markdown(json_data: dict) -> str:
    """Build markdown content from business_info JSON data."""
    data = json_data.get('data', {})
    
    sections = []
    
    # Header: Company Name
    company_name = data.get('company_name', '')
    if company_name:
        sections.append(f"# {company_name}")
    
    # Tagline
    tagline = data.get('tagline', '')
    if tagline:
        sections.append(f"\n{tagline}")
    
    # About Us Section
    description = data.get('description', '')
    philosophy = data.get('philosophy', '')
    
    if description or philosophy:
        sections.append("\n## About Us")
        if description:
            sections.append(f"\n{description}")
        if philosophy:
            sections.append(f"\n{philosophy}")
    
    # Contact Information Section
    contacts = data.get('contacts', {})
    email = contacts.get('email', '')
    phone = contacts.get('phone', '')
    website = data.get('website', '')
    
    if email or phone or website:
        sections.append("\n## Contact Information")
        contact_lines = []
        if email:
            contact_lines.append(f"- **Email**: {email}")
        if phone:
            contact_lines.append(f"- **Phone**: {phone}")
        if website:
            contact_lines.append(f"- **Website**: {website}")
        sections.append("\n" + "\n".join(contact_lines))
    
    # Social Media Section
    social = data.get('social', {})
    instagram = social.get('instagram', '')
    facebook = social.get('facebook', '')
    
    if instagram or facebook:
        sections.append("\n## Social Media")
        social_lines = []
        if instagram:
            social_lines.append(f"- **Instagram**: {instagram}")
        if facebook:
            social_lines.append(f"- **Facebook**: {facebook}")
        sections.append("\n" + "\n".join(social_lines))
    
    # Locations Section
    locations = data.get('locations', [])
    if locations:
        sections.append("\n## Our Locations")
        
        for loc in locations:
            loc_name = loc.get('location_name', 'Unknown Location')
            sections.append(f"\n### {loc_name}")
            
            hours = loc.get('hours', {})
            recurring = hours.get('recurring', {})
            exceptions = hours.get('exceptions', [])
            
            # Regular Hours
            if recurring:
                sections.append("\n**Regular Hours:**\n")
                sections.append(_format_week_schedule_markdown(recurring))
            
            # Special Hours & Closures
            if exceptions:
                sections.append("\n\n**Special Hours & Closures:**\n")
                sections.append(_format_exceptions_markdown(exceptions))
    
    return "\n".join(sections)


def _query_business_info(tenant_id: str, location_id: str) -> dict:
    """
    Query business info from database including ALL locations' hours for the tenant.
    
    Note: location_id parameter represents dashboard context but we query ALL locations.
    
    Returns:
        dict with keys:
        - 'business_data': dict from tenants + tenant_info
        - 'locations': list of dicts with location_id, name, recurring_hours, onetime_hours
    """
    conn = _get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Query 1: Get business basic info (tenants + tenant_info)
        query_business = """
            SELECT 
                t.name as company_name,
                ti.company_legal_name as legal_name,
                ti.tagline,
                ti.description,
                ti.philosophy,
                ti.contact_email,
                ti.contact_phone,
                ti.website_url as website,
                ti.instagram,
                ti.facebook,
                ti.primary_color,
                t.country_code
            FROM tenants t
            LEFT JOIN tenant_info ti ON t.tenant_id = ti.tenant_id
            WHERE t.tenant_id = %s
        """
        cursor.execute(query_business, (tenant_id,))
        business_data = cursor.fetchone()
        
        if not business_data:
            raise ValueError(f"No tenant found with tenant_id={tenant_id}")
        
        # Query 2: Get ALL locations for this tenant
        query_locations = """
            SELECT location_id, name
            FROM locations
            WHERE tenant_id = %s
            ORDER BY location_id
        """
        cursor.execute(query_locations, (tenant_id,))
        locations_list = cursor.fetchall()
        
        if not locations_list:
            logger.warning(f"No locations found for tenant_id={tenant_id}")
            locations_list = []
        
        # Query 3: Get recurring hours for ALL locations
        query_recurring = """
            SELECT 
                location_id,
                day_of_week,
                start_time,
                end_time,
                slot_name
            FROM location_availability
            WHERE tenant_id = %s 
              AND type = 'recurring'
              AND is_active = true
              AND is_closed = false
            ORDER BY location_id, day_of_week, start_time
        """
        cursor.execute(query_recurring, (tenant_id,))
        all_recurring_hours = cursor.fetchall()
        
        # Query 4: Get one-time availability for ALL locations with public holiday info
        query_onetime = """
            SELECT 
                la.location_id,
                la.specific_date,
                la.start_time,
                la.end_time,
                la.is_closed,
                la.slot_name,
                la.holiday_id,
                ph.name as holiday_name
            FROM location_availability la
            LEFT JOIN public_holidays ph ON la.holiday_id = ph.holiday_id
            WHERE la.tenant_id = %s 
              AND la.type = 'one_time'
              AND la.is_active = true
              AND la.specific_date >= CURRENT_DATE
            ORDER BY la.location_id, la.specific_date, la.start_time
        """
        cursor.execute(query_onetime, (tenant_id,))
        all_onetime_hours = cursor.fetchall()
        
        # Group hours by location_id
        locations_data = []
        for loc in locations_list:
            loc_id = str(loc['location_id'])
            loc_name = loc['name']
            
            # Filter recurring hours for this location
            recurring_for_loc = [
                dict(row) for row in all_recurring_hours 
                if str(row['location_id']) == loc_id
            ]
            
            # Filter onetime hours for this location
            onetime_for_loc = [
                dict(row) for row in all_onetime_hours 
                if str(row['location_id']) == loc_id
            ]
            
            locations_data.append({
                'location_id': loc_id,
                'location_name': loc_name,
                'recurring_hours': recurring_for_loc,
                'onetime_hours': onetime_for_loc
            })
        
        return {
            'business_data': dict(business_data),
            'locations': locations_data
        }
        
    finally:
        cursor.close()
        conn.close()


def _format_business_info(raw_data: dict) -> tuple[dict, str]:
    """
    Format business info data into structured JSON and markdown.
    
    Args:
        raw_data: dict with business_data and locations (list)
    
    Returns:
        tuple: (json_output, markdown_content)
    """
    
    business_data = raw_data['business_data']
    locations_raw = raw_data['locations']
    
    # Derive locale from country_code
    country_code = business_data.get('country_code', 'AU')
    locale_map = {
        'AU': 'en-AU',
        'US': 'en-US',
        'GB': 'en-GB',
        'NZ': 'en-NZ',
        'CA': 'en-CA'
    }
    locale = locale_map.get(country_code, 'en-AU')
    
    # Format hours for each location
    locations_formatted = []
    for loc in locations_raw:
        recurring_hours = _format_recurring_hours(loc['recurring_hours'])
        onetime_hours = _format_onetime_hours(loc['onetime_hours'])
        
        locations_formatted.append({
            'location_id': loc['location_id'],
            'location_name': loc['location_name'],
            'hours': {
                'recurring': recurring_hours,
                'exceptions': onetime_hours
            }
        })
    
    # Package formatted data
    json_output = {
        "version": 1,
        "source": "sync_speako_data",
        "analysis_artifact_url": "",
        "locale": locale,
        "data": {
            "company_name": business_data.get('company_name') or '',
            "legal_name": business_data.get('legal_name') or '',
            "tagline": business_data.get('tagline') or '',
            "description": business_data.get('description') or '',
            "philosophy": business_data.get('philosophy') or '',
            "contacts": {
                "email": business_data.get('contact_email') or '',
                "phone": business_data.get('contact_phone') or ''
            },
            "website": business_data.get('website') or '',
            "social": {
                "instagram": business_data.get('instagram') or '',
                "facebook": business_data.get('facebook') or ''
            },
            "branding": {
                "logo_url": '',  # Not in schema yet
                "primary_color": business_data.get('primary_color') or ''
            },
            "locations": locations_formatted
        }
    }
    
    # Generate markdown from JSON data
    markdown = _build_business_info_markdown(json_output)
    
    return json_output, markdown


# ============================================================================
# SERVICE MENU Knowledge Type Helpers
# ============================================================================

def _query_service_menu(tenant_id: str, location_id: str) -> dict:
    """
    Fetch service categories, services, locations, and service-modifier links for tenant.
    
    Args:
        tenant_id: The tenant identifier
        location_id: Primary location identifier (for location-centric output)
    
    Returns:
        dict with:
        - categories: List[dict] - Category tags from location_tag (category_id=4)
        - locations: List[dict] - All locations for tenant
        - services: List[dict] - All active services
        - location_services: Dict[int, List[int]] - location_id → [service_ids]
        - service_modifiers: Dict[int, List[dict]] - service_id → list of modifiers
    """
    conn = _get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Query 1: Fetch service category tags (category_id=4, is_active=true)
        cursor.execute("""
            SELECT tag_id, name, slug, tag_colour
            FROM location_tag
            WHERE tenant_id = %s 
              AND category_id = 4 
              AND is_active = true
            ORDER BY name
        """, (tenant_id,))
        categories = cursor.fetchall()
        
        # Query 2: Fetch all locations for tenant
        cursor.execute("""
            SELECT location_id, name
            FROM locations
            WHERE tenant_id = %s
            ORDER BY name
        """, (tenant_id,))
        locations = cursor.fetchall()
        
        # Query 3: Fetch location-service relationships
        cursor.execute("""
            SELECT location_id, service_id
            FROM location_services
            WHERE tenant_id = %s
        """, (tenant_id,))
        location_service_links = cursor.fetchall()
        
        # Build location_services map: location_id → [service_ids]
        location_services = {}
        for link in location_service_links:
            loc_id = link['location_id']
            svc_id = link['service_id']
            if loc_id not in location_services:
                location_services[loc_id] = []
            location_services[loc_id].append(svc_id)
        
        # Query 4: Fetch all active services
        cursor.execute("""
            SELECT 
                service_id, name, description, duration, price, 
                category_tag_ids, is_active
            FROM services
            WHERE tenant_id = %s 
              AND is_active = true
            ORDER BY name
        """, (tenant_id,))
        services = cursor.fetchall()
        
        # Query 5: Fetch service-modifier links with modifier details (JOIN)
        # Query 5: Fetch service-modifier links with modifier details (JOIN)
        cursor.execute("""
            SELECT 
                sml.service_id,
                sml.modifier_id,
                sml.price_override,
                sml.is_required,
                sml.default_selected,
                sml.sort_order,
                m.name as modifier_name,
                m.description as modifier_description,
                m.base_price as modifier_base_price
            FROM service_modifier_links sml
            INNER JOIN modifiers m 
                ON sml.tenant_id = m.tenant_id 
                AND sml.modifier_id = m.modifier_id
            WHERE sml.tenant_id = %s
              AND m.active = true
            ORDER BY sml.service_id, sml.sort_order NULLS LAST, m.name
        """, (tenant_id,))
        modifier_links = cursor.fetchall()
        
        # Group modifiers by service_id
        service_modifiers = {}
        for link in modifier_links:
            sid = link['service_id']
            if sid not in service_modifiers:
                service_modifiers[sid] = []
            
            # Use price_override if available, else base_price
            price = link['price_override'] if link['price_override'] is not None else link['modifier_base_price']
            
            service_modifiers[sid].append({
                'id': str(link['modifier_id']),
                'name': link['modifier_name'],
                'description': link['modifier_description'] or '',
                'price': float(price) if price is not None else 0.0,
                'is_required': link['is_required'] or False,
                'default_selected': link['default_selected'] or False
            })
        
        return {
            'categories': categories,
            'locations': locations,
            'services': services,
            'location_services': location_services,
            'service_modifiers': service_modifiers
        }
    
    finally:
        cursor.close()
        conn.close()


def _build_location_categories(location_service_ids: list, 
                                all_services: list, 
                                category_map: dict, 
                                service_modifiers: dict) -> list:
    """
    Build categories array for a specific location based on available services.
    
    Args:
        location_service_ids: List of service_ids available at this location
        all_services: List of all services (from query)
        category_map: Dict mapping tag_id → category info
        service_modifiers: Dict mapping service_id → modifiers list
    
    Returns:
        list: Categories array with items filtered to location's services
    """
    # Filter services to only those available at this location
    location_services = [s for s in all_services if s['service_id'] in location_service_ids]
    
    # Build category → services mapping
    category_services = {}  # tag_id → [services]
    uncategorized_services = []
    
    for service in location_services:
        category_tag_ids = service['category_tag_ids'] or []
        
        if not category_tag_ids:
            # No categories assigned
            uncategorized_services.append(service)
        else:
            # Add service to each valid category
            for tag_id in category_tag_ids:
                if tag_id in category_map:
                    if tag_id not in category_services:
                        category_services[tag_id] = []
                    category_services[tag_id].append(service)
    
    # Build categories output
    categories_output = []
    
    # Process each category (already sorted by name from query)
    for tag_id, cat_info in category_map.items():
        if tag_id not in category_services:
            # Skip empty categories for this location
            continue
        
        items = []
        for service in category_services[tag_id]:
            service_id = service['service_id']
            addons = service_modifiers.get(service_id, [])
            
            items.append({
                'id': str(service_id),
                'name': service['name'],
                'description': service['description'] or '',
                'duration_min': _extract_duration_minutes(service['duration']),
                'price': {
                    'currency': 'AUD',
                    'amount': float(service['price']) if service['price'] is not None else 0.0
                },
                'addons': addons
            })
        
        categories_output.append({
            'id': cat_info['id'],
            'name': cat_info['name'],
            'items': items
        })
    
    # Add uncategorized if any
    if uncategorized_services:
        items = []
        for service in uncategorized_services:
            service_id = service['service_id']
            addons = service_modifiers.get(service_id, [])
            
            items.append({
                'id': str(service_id),
                'name': service['name'],
                'description': service['description'] or '',
                'duration_min': _extract_duration_minutes(service['duration']),
                'price': {
                    'currency': 'AUD',
                    'amount': float(service['price']) if service['price'] is not None else 0.0
                },
                'addons': addons
            })
        
        categories_output.append({
            'id': '0',
            'name': 'Uncategorized',
            'items': items
        })
    
    return categories_output


# ============================================================================
# Markdown Generation Helpers for service_menu
# ============================================================================

def _build_service_menu_markdown(json_data: dict) -> str:
    """Build markdown content from service_menu JSON data."""
    data = json_data.get('data', {})
    sections = []
    
    # Header
    sections.append("# Service Menu")
    
    # Primary Location
    primary_location = data.get('primary_location', {})
    if primary_location:
        loc_name = primary_location.get('location_name', 'Primary Location')
        categories = primary_location.get('categories', [])
        
        sections.append(f"\n## {loc_name}")
        
        for category in categories:
            cat_name = category.get('name', 'Uncategorized')
            items = category.get('items', [])
            
            sections.append(f"\n### {cat_name}")
            
            for item in items:
                item_name = item.get('name', '')
                description = item.get('description', '')
                duration = item.get('duration_min', 0)
                price = item.get('price', {})
                amount = price.get('amount', 0)
                currency = price.get('currency', 'AUD')
                addons = item.get('addons', [])
                
                # Service title with price
                sections.append(f"\n**{item_name}** — {currency} {amount:.2f}")
                
                # Duration
                if duration > 0:
                    sections.append(f"  \n*Duration: {duration} minutes*")
                
                # Description
                if description:
                    sections.append(f"  \n{description}")
                
                # Addons/Modifiers
                if addons:
                    sections.append("\n  \n*Available Add-ons:*")
                    for addon in addons:
                        addon_name = addon.get('name', '')
                        addon_price = addon.get('price', 0)
                        addon_desc = addon.get('description', '')
                        
                        addon_line = f"  - {addon_name} (+{currency} {addon_price:.2f})"
                        if addon_desc:
                            addon_line += f" - {addon_desc}"
                        sections.append(addon_line)
    
    # Other Locations
    other_locations = data.get('other_locations', [])
    if other_locations:
        sections.append("\n---\n")
        sections.append("## Other Locations\n")
        
        for loc in other_locations:
            loc_name = loc.get('location_name', 'Location')
            categories = loc.get('categories', [])
            
            sections.append(f"\n### {loc_name}")
            
            # Just list categories and service counts for other locations
            for category in categories:
                cat_name = category.get('name', '')
                items = category.get('items', [])
                item_count = len(items)
                
                service_names = [item.get('name', '') for item in items]
                sections.append(f"\n**{cat_name}** ({item_count} service{'s' if item_count != 1 else ''})")
                sections.append(f"  \n{', '.join(service_names)}")
    
    return "\n".join(sections)


def _format_service_menu(raw_data: dict, primary_location_id: int) -> tuple[dict, str]:
    """
    Prepare raw DB data for OpenAI processing.
    OpenAI will structure and format the data based on ai_prompt from database.
    
    Args:
        raw_data: Dict with categories, locations, services, location_services, service_modifiers
        primary_location_id: The location that triggered this sync (shown first)
    
    Returns:
        tuple: (raw_data_dict, empty_markdown_placeholder)
    """
    from .utils.task_db import get_ai_knowledge_type_by_key
    
    categories_list = raw_data['categories']
    locations_list = raw_data['locations']
    services_list = raw_data['services']
    location_services = raw_data['location_services']
    service_modifiers = raw_data['service_modifiers']
    
    # Build category map: tag_id → category info
    category_map = {
        cat['tag_id']: {
            'id': str(cat['tag_id']),
            'name': cat['name'],
            'slug': cat['slug']
        }
        for cat in categories_list
    }
    
    # Build locations map: location_id → name
    locations_map = {loc['location_id']: loc['name'] for loc in locations_list}
    
    # Build primary location data
    primary_location_name = locations_map.get(primary_location_id, f'Location {primary_location_id}')
    primary_service_ids = location_services.get(primary_location_id, [])
    primary_categories = _build_location_categories(
        primary_service_ids,
        services_list,
        category_map,
        service_modifiers
    )
    
    primary_location_data = {
        'location_id': str(primary_location_id),
        'location_name': primary_location_name,
        'categories': primary_categories
    }
    
    # Build other locations data
    other_locations_data = []
    for loc_id in sorted(locations_map.keys()):
        if loc_id == primary_location_id:
            continue  # Skip primary location
        
        loc_service_ids = location_services.get(loc_id, [])
        if not loc_service_ids:
            continue  # Skip locations with no services
        
        loc_categories = _build_location_categories(
            loc_service_ids,
            services_list,
            category_map,
            service_modifiers
        )
        
        if not loc_categories:
            continue  # Skip if no categories after filtering
        
        other_locations_data.append({
            'location_id': str(loc_id),
            'location_name': locations_map[loc_id],
            'categories': loc_categories
        })
    
    # Package formatted data
    json_data = {
        'version': 1,
        'source': 'sync_speako_data',
        'analysis_artifact_url': '',
        'locale': 'en-AU',
        'data': {
            'primary_location': primary_location_data,
            'other_locations': other_locations_data
        }
    }
    
    # Generate markdown from JSON data
    markdown_content = _build_service_menu_markdown(json_data)
    
    return json_data, markdown_content




# ============================================================================
# LOCATIONS Knowledge Type Helpers
# ============================================================================

def _parse_address(address_text: str, state_name: str = '', country_code: str = 'AU') -> dict:
    """
    Parse address text into structured components.
    
    Args:
        address_text: Address string (may be multi-line or comma-separated)
        state_name: State name from states table
        country_code: Country code from locations table
    
    Returns:
        dict with: line1, line2, city, state, postcode, country
    """
    if not address_text:
        return {
            'line1': '',
            'line2': '',
            'city': '',
            'state': state_name or '',
            'postcode': '',
            'country': country_code or 'AU'
        }
    
    # Try to parse address intelligently
    # Common formats:
    # "123 Main St, Sydney NSW 2000"
    # "123 Main St\nSuite 5\nSydney NSW 2000"
    
    lines = [line.strip() for line in address_text.replace('\n', ',').split(',') if line.strip()]
    
    line1 = lines[0] if len(lines) > 0 else ''
    line2 = ''
    city = ''
    postcode = ''
    
    # Last line often contains city, state, postcode
    if len(lines) >= 2:
        last_line = lines[-1]
        # Try to extract postcode (4 digits in Australia)
        import re
        postcode_match = re.search(r'\b(\d{4})\b', last_line)
        if postcode_match:
            postcode = postcode_match.group(1)
            # Remove postcode from last line to get city
            city = last_line.replace(postcode, '').replace(state_name, '').strip(', ')
        else:
            city = last_line.replace(state_name, '').strip(', ')
        
        # If there are 3+ lines, middle ones are line2
        if len(lines) >= 3:
            line2 = ', '.join(lines[1:-1])
    
    return {
        'line1': line1,
        'line2': line2,
        'city': city,
        'state': state_name or '',
        'postcode': postcode,
        'country': country_code or 'AU'
    }


def _query_locations(tenant_id: str, location_id: str) -> dict:
    """
    Fetch all location data including locations, info, hours, services, states.
    
    Args:
        tenant_id: The tenant identifier
        location_id: Primary location context
    
    Returns:
        dict with:
        - locations: List of location records
        - location_info: Dict mapping location_id → info
        - recurring_hours: Dict mapping location_id → hours list
        - exceptions: Dict mapping location_id → exceptions list
        - location_services: Dict mapping location_id → [service_ids]
        - services_names: Dict mapping service_id → name
        - states: Dict mapping state_id → name
    """
    conn = _get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Query 1: Fetch all active locations for tenant
        cursor.execute("""
            SELECT 
                location_id, name, timezone, country_code, state_id,
                location_type, twilio_phone_number, human_phone_number,
                booking_email_recipients, min_advance_booking_minutes, 
                slot_interval_minutes, is_active
            FROM locations
            WHERE tenant_id = %s AND is_active = true
            ORDER BY name
        """, (tenant_id,))
        locations = cursor.fetchall()
        
        # Query 2: Fetch location_info for all locations
        cursor.execute("""
            SELECT 
                location_id, address, phone_with_country_code, 
                email, website_url, order_link, opening_hours, ai_prompt
            FROM location_info
            WHERE tenant_id = %s
        """, (tenant_id,))
        location_info_list = cursor.fetchall()
        location_info = {info['location_id']: info for info in location_info_list}
        
        # Query 3: Fetch recurring availability for all locations
        cursor.execute("""
            SELECT 
                location_id, day_of_week, start_time, end_time, 
                slot_name, is_active, is_closed
            FROM location_availability
            WHERE tenant_id = %s 
              AND type = 'recurring' 
              AND is_active = true 
              AND is_closed = false
            ORDER BY location_id, day_of_week, start_time
        """, (tenant_id,))
        recurring_list = cursor.fetchall()
        
        # Group recurring hours by location_id
        recurring_hours = {}
        for row in recurring_list:
            loc_id = row['location_id']
            if loc_id not in recurring_hours:
                recurring_hours[loc_id] = []
            recurring_hours[loc_id].append(row)
        
        # Query 4: Fetch one-time availability (exceptions) for all locations
        cursor.execute("""
            SELECT 
                la.location_id, la.specific_date, la.start_time, la.end_time,
                la.is_closed, la.slot_name, la.holiday_id,
                ph.name as holiday_name
            FROM location_availability la
            LEFT JOIN public_holidays ph ON la.holiday_id = ph.holiday_id
            WHERE la.tenant_id = %s 
              AND la.type = 'one_time' 
              AND la.is_active = true
            ORDER BY la.location_id, la.specific_date
        """, (tenant_id,))
        exceptions_list = cursor.fetchall()
        
        # Group exceptions by location_id
        exceptions = {}
        for row in exceptions_list:
            loc_id = row['location_id']
            if loc_id not in exceptions:
                exceptions[loc_id] = []
            exceptions[loc_id].append(row)
        
        # Query 5: Fetch location-service relationships
        cursor.execute("""
            SELECT location_id, service_id
            FROM location_services
            WHERE tenant_id = %s
        """, (tenant_id,))
        location_service_links = cursor.fetchall()
        
        # Build location_services map
        location_services = {}
        for link in location_service_links:
            loc_id = link['location_id']
            svc_id = link['service_id']
            if loc_id not in location_services:
                location_services[loc_id] = []
            location_services[loc_id].append(svc_id)
        
        # Query 6: Fetch all active service names
        cursor.execute("""
            SELECT service_id, name
            FROM services
            WHERE tenant_id = %s AND is_active = true
        """, (tenant_id,))
        services_list = cursor.fetchall()
        services_names = {svc['service_id']: svc['name'] for svc in services_list}
        
        # Query 7: Fetch state names for locations' state_ids
        state_ids = [loc['state_id'] for loc in locations if loc.get('state_id')]
        states = {}
        if state_ids:
            cursor.execute("""
                SELECT state_id, name
                FROM states
                WHERE state_id = ANY(%s)
            """, (state_ids,))
            states_list = cursor.fetchall()
            states = {st['state_id']: st['name'] for st in states_list}
        
        return {
            'locations': locations,
            'location_info': location_info,
            'recurring_hours': recurring_hours,
            'exceptions': exceptions,
            'location_services': location_services,
            'services_names': services_names,
            'states': states
        }
    
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# Markdown Generation Helpers for locations
# ============================================================================

def _build_locations_markdown(json_data: dict) -> str:
    """Build markdown content from locations JSON data."""
    data = json_data.get('data', {})
    sections = []
    
    # Header
    sections.append("# Our Locations")
    
    # Primary Location
    primary_location = data.get('primary_location', {})
    if primary_location:
        sections.append("\n## Primary Location\n")
        sections.append(_format_location_details_markdown(primary_location))
    
    # Other Locations
    other_locations = data.get('other_locations', [])
    if other_locations:
        sections.append("\n---\n")
        sections.append("## Additional Locations\n")
        
        for loc in other_locations:
            sections.append(_format_location_details_markdown(loc))
            sections.append("\n---\n")
    
    return "\n".join(sections)


def _format_location_details_markdown(location: dict) -> str:
    """Format a single location's details into markdown."""
    sections = []
    
    # Location Name
    name = location.get('name', 'Unknown Location')
    sections.append(f"### {name}")
    
    # Address
    address = location.get('address', {})
    line1 = address.get('line1', '')
    line2 = address.get('line2', '')
    city = address.get('city', '')
    state = address.get('state', '')
    postcode = address.get('postcode', '')
    country = address.get('country', '')
    
    if line1:
        sections.append("\n**Address:**")
        address_parts = [line1]
        if line2:
            address_parts.append(line2)
        if city or state or postcode:
            address_parts.append(f"{city} {state} {postcode}".strip())
        if country:
            address_parts.append(country)
        sections.append("  \n" + "  \n".join(address_parts))
    
    # Contact
    contact = location.get('contact', {})
    phone = contact.get('phone', '')
    email = contact.get('email', '')
    website = contact.get('website', '')
    
    if phone or email or website:
        sections.append("\n**Contact:**")
        if phone:
            sections.append(f"  \n📞 {phone}")
        if email:
            sections.append(f"  \n📧 {email}")
        if website:
            sections.append(f"  \n🌐 {website}")
    
    # Hours
    hours = location.get('hours', {})
    recurring = hours.get('recurring', {})
    exceptions = hours.get('exceptions', [])
    
    if recurring:
        sections.append("\n**Regular Hours:**\n")
        sections.append(_format_week_schedule_markdown(recurring))
    
    if exceptions:
        sections.append("\n**Special Hours & Closures:**\n")
        sections.append(_format_exceptions_markdown(exceptions))
    
    # Services Available
    services = location.get('services_available', [])
    if services:
        sections.append(f"\n**Services Available:** {len(services)} service{'s' if len(services) != 1 else ''}")
        sections.append("  \n" + ", ".join(services))
    
    # Booking Info
    booking_info = location.get('booking_info', {})
    min_advance = booking_info.get('min_advance_minutes', 0)
    slot_interval = booking_info.get('slot_interval_minutes', 0)
    
    if min_advance > 0 or slot_interval > 0:
        sections.append("\n**Booking Information:**")
        if min_advance > 0:
            hours = min_advance // 60
            mins = min_advance % 60
            if hours > 0:
                sections.append(f"  \n- Minimum advance booking: {hours}h {mins}min" if mins else f"  \n- Minimum advance booking: {hours} hour{'s' if hours != 1 else ''}")
            else:
                sections.append(f"  \n- Minimum advance booking: {mins} minutes")
        if slot_interval > 0:
            sections.append(f"  \n- Booking slots every: {slot_interval} minutes")
    
    # Notes
    notes = location.get('notes', '')
    if notes:
        sections.append(f"\n**Notes:**  \n{notes}")
    
    return "\n".join(sections)


def _build_location_details(
    location_id: int,
    locations_map: dict,
    location_info_map: dict,
    recurring_hours: dict,
    exceptions: dict,
    location_services_map: dict,
    services_names: dict,
    states_map: dict
) -> dict:
    """
    Build complete location details dictionary.
    
    Args:
        location_id: Location identifier
        locations_map: Dict of location_id → location record
        location_info_map: Dict of location_id → location_info record
        recurring_hours: Dict of location_id → hours list
        exceptions: Dict of location_id → exceptions list
        location_services_map: Dict of location_id → [service_ids]
        services_names: Dict of service_id → service name
        states_map: Dict of state_id → state name
    
    Returns:
        dict: Complete location details
    """
    loc = locations_map.get(location_id)
    if not loc:
        return None
    
    info = location_info_map.get(location_id, {})
    hours_recurring = recurring_hours.get(location_id, [])
    hours_exceptions = exceptions.get(location_id, [])
    service_ids = location_services_map.get(location_id, [])
    
    # Get state name
    state_name = states_map.get(loc.get('state_id'), '')
    
    # Parse address
    address = _parse_address(
        info.get('address', ''),
        state_name,
        loc.get('country_code', 'AU')
    )
    
    # Format hours using existing helpers
    hours_formatted = _format_recurring_hours(hours_recurring)
    exceptions_formatted = _format_onetime_hours(hours_exceptions)
    
    # Get service names
    services_available = [
        services_names.get(svc_id, f'Service {svc_id}')
        for svc_id in service_ids
        if svc_id in services_names
    ]
    services_available.sort()  # Alphabetical order
    
    # Contact info - prefer location_info, fallback to location fields
    phone = info.get('phone_with_country_code') or loc.get('human_phone_number') or ''
    email = info.get('email') or ''
    website = info.get('website_url') or ''
    
    return {
        'id': str(location_id),
        'name': loc['name'],
        'address': address,
        'contact': {
            'phone': phone,
            'email': email,
            'website': website
        },
        'geo': {
            'lat': 0,  # Not available in schema - placeholder
            'lng': 0   # Not available in schema - placeholder
        },
        'timezone': loc.get('timezone', ''),
        'location_type': loc.get('location_type', ''),
        'hours': {
            'recurring': hours_formatted,
            'exceptions': exceptions_formatted
        },
        'services_available': services_available,
        'booking_info': {
            'min_advance_minutes': loc.get('min_advance_booking_minutes', 0) or 0,
            'slot_interval_minutes': loc.get('slot_interval_minutes', 0) or 0,
            'booking_email': loc.get('booking_email_recipients', '') or ''
        },
        'notes': info.get('ai_prompt', '') or ''
    }


def _format_locations(raw_data: dict, primary_location_id: int) -> tuple[dict, str]:
    """
    Format locations data into structured JSON and markdown.
    
    Args:
        raw_data: Dict with locations, location_info, hours, services, states
        primary_location_id: The location that triggered this sync
    
    Returns:
        tuple: (json_output, markdown_content)
    """
    
    locations_list = raw_data['locations']
    location_info = raw_data['location_info']
    recurring_hours = raw_data['recurring_hours']
    exceptions = raw_data['exceptions']
    location_services = raw_data['location_services']
    services_names = raw_data['services_names']
    states = raw_data['states']
    
    # Build locations map
    locations_map = {loc['location_id']: loc for loc in locations_list}
    
    # Build primary location details
    primary_location_data = _build_location_details(
        primary_location_id,
        locations_map,
        location_info,
        recurring_hours,
        exceptions,
        location_services,
        services_names,
        states
    )
    
    # Build other locations details
    other_locations_data = []
    for loc_id in sorted(locations_map.keys()):
        if loc_id == primary_location_id:
            continue  # Skip primary location
        
        loc_details = _build_location_details(
            loc_id,
            locations_map,
            location_info,
            recurring_hours,
            exceptions,
            location_services,
            services_names,
            states
        )
        
        if loc_details:
            other_locations_data.append(loc_details)
    
    # Package formatted data
    json_data = {
        'version': 1,
        'source': 'sync_speako_data',
        'analysis_artifact_url': '',
        'locale': 'en-AU',
        'data': {
            'primary_location': primary_location_data,
            'other_locations': other_locations_data
        }
    }
    
    # Generate markdown from JSON data
    markdown_content = _build_locations_markdown(json_data)
    
    return json_data, markdown_content


# ============================================================================
# STAFF Knowledge Type Helpers
# ============================================================================

def _query_staff(tenant_id: str, location_id: str) -> dict:
    """
    Fetch staff members, their titles, services, and availability for tenant.
    
    Args:
        tenant_id: The tenant identifier
        location_id: Location identifier (for location context)
    
    Returns:
        dict with:
        - staff_list: List of staff records
        - title_tags: Dict mapping tag_id → title info
        - recurring_availability: List of recurring availability records
        - onetime_availability: List of one-time availability records
        - staff_services: Dict mapping staff_id → [service_ids]
        - services: Dict mapping service_id → service details
        - locations: Dict mapping location_id → location name
    """
    conn = _get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Query 1: Get all active staff for tenant
        cursor.execute("""
            SELECT staff_id, name, title_tag_ids, default_location_id,
                   staff_img_url, phone_number, bio, email
            FROM staff
            WHERE tenant_id = %s AND is_active = true
            ORDER BY name
        """, (tenant_id,))
        staff_list = cursor.fetchall()
        
        # Query 2: Get staff title tags (category_id = 3)
        cursor.execute("""
            SELECT tag_id, name, slug, tag_colour
            FROM location_tag
            WHERE tenant_id = %s AND category_id = 3 AND is_active = true
        """, (tenant_id,))
        title_tags_list = cursor.fetchall()
        title_tags = {tag['tag_id']: dict(tag) for tag in title_tags_list}
        
        # Query 3: Get staff recurring availability
        cursor.execute("""
            SELECT staff_id, location_id, day_of_week, start_time, end_time, is_closed
            FROM staff_availability
            WHERE tenant_id = %s AND type = 'recurring' AND is_active = true AND is_closed = false
            ORDER BY staff_id, location_id, day_of_week, start_time
        """, (tenant_id,))
        recurring_availability = cursor.fetchall()
        
        # Query 4: Get staff one-time availability
        cursor.execute("""
            SELECT staff_id, location_id, specific_date, start_time, end_time, is_closed
            FROM staff_availability
            WHERE tenant_id = %s AND type = 'one_time' AND is_active = true
            ORDER BY staff_id, location_id, specific_date, start_time
        """, (tenant_id,))
        onetime_availability = cursor.fetchall()
        
        # Query 5: Get staff-service relationships
        cursor.execute("""
            SELECT staff_id, service_id
            FROM staff_services
            WHERE tenant_id = %s
        """, (tenant_id,))
        staff_service_links = cursor.fetchall()
        
        # Build staff_services map: staff_id → [service_ids]
        staff_services = {}
        for link in staff_service_links:
            sid = link['staff_id']
            svc_id = link['service_id']
            if sid not in staff_services:
                staff_services[sid] = []
            staff_services[sid].append(svc_id)
        
        # Query 6: Get active service details
        cursor.execute("""
            SELECT service_id, name, description, duration, price
            FROM services
            WHERE tenant_id = %s AND is_active = true
        """, (tenant_id,))
        services_list = cursor.fetchall()
        services = {svc['service_id']: dict(svc) for svc in services_list}
        
        # Query 7: Get location names
        cursor.execute("""
            SELECT location_id, name
            FROM locations
            WHERE tenant_id = %s AND is_active = true
        """, (tenant_id,))
        locations_list = cursor.fetchall()
        locations = {loc['location_id']: loc['name'] for loc in locations_list}
        
        return {
            'staff_list': staff_list,
            'title_tags': title_tags,
            'recurring_availability': recurring_availability,
            'onetime_availability': onetime_availability,
            'staff_services': staff_services,
            'services': services,
            'locations': locations
        }
    
    finally:
        cursor.close()
        conn.close()


def _group_staff_availability_by_location(staff_id: int, recurring_avail: list, onetime_avail: list) -> dict:
    """
    Group availability data by location for a specific staff member.
    
    Args:
        staff_id: The staff member's ID
        recurring_avail: List of all recurring availability records
        onetime_avail: List of all one-time availability records
    
    Returns:
        dict: location_id → {'recurring': hours_data, 'onetime': hours_data}
    """
    location_availability = {}
    
    # Group recurring hours by location
    for row in recurring_avail:
        if row['staff_id'] != staff_id:
            continue
        
        loc_id = row['location_id']
        if loc_id not in location_availability:
            location_availability[loc_id] = {'recurring': [], 'onetime': []}
        
        location_availability[loc_id]['recurring'].append(dict(row))
    
    # Group one-time hours by location
    for row in onetime_avail:
        if row['staff_id'] != staff_id:
            continue
        
        loc_id = row['location_id']
        if loc_id not in location_availability:
            location_availability[loc_id] = {'recurring': [], 'onetime': []}
        
        location_availability[loc_id]['onetime'].append(dict(row))
    
    return location_availability


# ============================================================================
# Markdown Generation Helpers for staff
# ============================================================================

def _format_staff_day_hours_markdown(hours_array: list) -> str:
    """Format hours array for a single day into markdown string (staff context)."""
    if not hours_array:
        return "Off Duty"
    return ", ".join(hours_array)


def _format_staff_week_schedule_markdown(recurring_dict: dict) -> str:
    """Format weekly recurring hours into markdown bullet list (staff context)."""
    days_order = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
    day_names = {
        'mon': 'Monday',
        'tue': 'Tuesday', 
        'wed': 'Wednesday',
        'thu': 'Thursday',
        'fri': 'Friday',
        'sat': 'Saturday',
        'sun': 'Sunday'
    }
    
    lines = []
    for day_key in days_order:
        day_name = day_names[day_key]
        hours = recurring_dict.get(day_key, [])
        hours_text = _format_staff_day_hours_markdown(hours)
        lines.append(f"- **{day_name}**: {hours_text}")
    
    return "\n".join(lines)


def _build_staff_markdown(json_data: dict) -> str:
    """Build markdown content from staff JSON data."""
    summary = json_data.get('summary', {})
    data = json_data.get('data', {})
    sections = []
    
    # Header with summary
    sections.append("# Our Team")
    if summary:
        total_staff = summary.get('total_staff', 0)
        total_locations = summary.get('total_locations', 0)
        loc_text = 'location' if total_locations == 1 else 'locations'
        sections.append(f"\n**Total Staff:** {total_staff} across {total_locations} {loc_text}\n")
    
    # Primary Location
    primary_location = data.get('primary_location', {})
    if primary_location:
        loc_name = primary_location.get('location_name', 'Primary Location')
        staff_count = primary_location.get('staff_count', 0)
        staff_list = primary_location.get('staff', [])
        
        staff_text = 'staff member' if staff_count == 1 else 'staff members'
        sections.append(f"\n## Staff at {loc_name} ({staff_count} {staff_text})")
        
        for staff in staff_list:
            sections.append("\n" + _format_staff_member_markdown(staff, is_primary=True))
            sections.append("\n---")
    
    # Other Locations
    other_locations = data.get('other_locations', [])
    if other_locations:
        sections.append("\n## Staff at Other Locations\n")
        
        for loc in other_locations:
            loc_name = loc.get('location_name', 'Location')
            staff_count = loc.get('staff_count', 0)
            staff_list = loc.get('staff', [])
            
            staff_text = 'staff member' if staff_count == 1 else 'staff members'
            sections.append(f"\n### {loc_name} ({staff_count} {staff_text})\n")
            
            for staff in staff_list:
                sections.append(_format_staff_member_markdown(staff, is_primary=False))
                sections.append("\n---")
    
    return "\n".join(sections)


def _format_staff_member_markdown(staff: dict, is_primary: bool = True) -> str:
    """Format a single staff member's details into markdown."""
    sections = []
    
    # Name and Titles
    name = staff.get('name', 'Unknown Staff')
    titles = staff.get('titles', [])
    
    sections.append(f"### {name}")
    if titles:
        sections.append(f"**{', '.join(titles)}**")
    
    # Bio
    bio = staff.get('bio', '')
    if bio:
        sections.append(f"\n{bio}")
    
    # Contact (only for primary location or if provided)
    contact = staff.get('contact', {})
    email = contact.get('email', '')
    phone = contact.get('phone', '')
    
    if is_primary and (email or phone):
        sections.append("\n**Contact:**")
        if email:
            sections.append(f"- 📧 {email}")
        if phone:
            sections.append(f"- 📞 {phone}")
    
    # Services
    services = staff.get('services', [])
    if services:
        sections.append("\n**Services Offered:**")
        for svc_name in services:
            sections.append(f"- {svc_name}")
    
    # Availability
    availability = staff.get('availability', {})
    this_location = availability.get('this_location', {})
    recurring = this_location.get('recurring', {})
    exceptions = this_location.get('exceptions', [])
    
    if recurring or exceptions:
        sections.append("\n**Availability at This Location:**")
        
        if recurring:
            sections.append("\n*Regular Hours:*\n")
            sections.append(_format_staff_week_schedule_markdown(recurring))
        
        if exceptions:
            sections.append("\n*Special Hours:*\n")
            sections.append(_format_exceptions_markdown(exceptions))
    
    # Other locations where staff works
    other_locs = availability.get('other_locations', [])
    if other_locs and is_primary:
        sections.append("\n**Also Available At:**")
        for loc in other_locs:
            loc_name = loc.get('location_name', '')
            loc_recurring = loc.get('recurring', {})
            
            # Summarize availability
            days_available = [day for day, hours in loc_recurring.items() if hours]
            if days_available:
                day_names = {
                    'mon': 'Mon', 'tue': 'Tue', 'wed': 'Wed', 'thu': 'Thu',
                    'fri': 'Fri', 'sat': 'Sat', 'sun': 'Sun'
                }
                days_str = ', '.join([day_names.get(d, d.title()) for d in days_available])
                
                # Get first time range as example
                first_day_hours = loc_recurring[days_available[0]]
                hours_str = first_day_hours[0] if first_day_hours else ''
                
                sections.append(f"- **{loc_name}**: {days_str} {hours_str}")
    
    return "\n".join(sections)


def _format_staff(raw_data: dict, primary_location_id: int) -> tuple[dict, str]:
    """
    Format staff data into structured JSON and markdown.
    
    Args:
        raw_data: Dict with staff_list, title_tags, availability, services, locations
        primary_location_id: The location that triggered this sync (shown first)
    
    Returns:
        tuple: (json_output, markdown_content)
    """
    
    staff_list = raw_data['staff_list']
    title_tags = raw_data['title_tags']
    recurring_availability = raw_data['recurring_availability']
    onetime_availability = raw_data['onetime_availability']
    staff_services = raw_data['staff_services']
    services = raw_data['services']
    locations_map = raw_data['locations']
    
    # Helper to build staff member data
    def build_staff_data(staff):
        staff_id = staff['staff_id']
        
        # Resolve titles from title_tag_ids
        title_ids = staff['title_tag_ids'] or []
        titles = [
            title_tags[tag_id]['name']
            for tag_id in title_ids
            if tag_id in title_tags
        ]
        
        # Get services for this staff
        service_ids = staff_services.get(staff_id, [])
        staff_services_list = []
        for svc_id in service_ids:
            if svc_id not in services:
                continue
            svc = services[svc_id]
            staff_services_list.append(svc['name'])
        
        # Sort services by name
        staff_services_list.sort()
        
        # Get availability grouped by location
        loc_availability = _group_staff_availability_by_location(
            staff_id,
            recurring_availability,
            onetime_availability
        )
        
        # Determine primary location availability
        primary_loc_avail = loc_availability.get(primary_location_id, {'recurring': [], 'onetime': []})
        primary_recurring = _format_recurring_hours(primary_loc_avail['recurring'])
        primary_exceptions = _format_onetime_hours(primary_loc_avail['onetime'])
        
        # Get other locations where staff works
        other_locs_avail = []
        for loc_id, avail_data in loc_availability.items():
            if loc_id == primary_location_id:
                continue
            
            loc_name = locations_map.get(loc_id, f'Location {loc_id}')
            recurring = _format_recurring_hours(avail_data['recurring'])
            exceptions = _format_onetime_hours(avail_data['onetime'])
            
            other_locs_avail.append({
                'location_id': str(loc_id),
                'location_name': loc_name,
                'recurring': recurring,
                'exceptions': exceptions
            })
        
        # Sort other locations by name
        other_locs_avail.sort(key=lambda x: x['location_name'])
        
        return {
            'staff_id': str(staff_id),
            'name': staff['name'],
            'titles': titles,
            'bio': staff['bio'] or '',
            'contact': {
                'email': staff['email'] or '',
                'phone': staff['phone_number'] or ''
            },
            'image_url': staff['staff_img_url'] or '',
            'default_location_id': str(staff['default_location_id']) if staff['default_location_id'] else '',
            'services': staff_services_list,
            'availability': {
                'this_location': {
                    'recurring': primary_recurring,
                    'exceptions': primary_exceptions
                },
                'other_locations': other_locs_avail
            }
        }
    
    # Separate staff by primary vs other locations
    primary_location_staff = []
    other_locations_staff = {}
    
    for staff in staff_list:
        default_loc_id = staff['default_location_id']
        
        if default_loc_id == primary_location_id:
            # Staff belongs to primary location
            primary_location_staff.append(build_staff_data(staff))
        elif default_loc_id and default_loc_id in locations_map:
            # Staff belongs to another location
            if default_loc_id not in other_locations_staff:
                other_locations_staff[default_loc_id] = []
            other_locations_staff[default_loc_id].append(build_staff_data(staff))
    
    # Sort staff by name within each location
    primary_location_staff.sort(key=lambda x: x['name'])
    for loc_id in other_locations_staff:
        other_locations_staff[loc_id].sort(key=lambda x: x['name'])
    
    # Build primary location data
    primary_location_name = locations_map.get(primary_location_id, f'Location {primary_location_id}')
    primary_location_data = {
        'location_id': str(primary_location_id),
        'location_name': primary_location_name,
        'staff_count': len(primary_location_staff),
        'staff': primary_location_staff
    }
    
    # Build other locations data
    other_locations_data = []
    for loc_id in sorted(other_locations_staff.keys()):
        loc_name = locations_map.get(loc_id, f'Location {loc_id}')
        other_locations_data.append({
            'location_id': str(loc_id),
            'location_name': loc_name,
            'staff_count': len(other_locations_staff[loc_id]),
            'staff': other_locations_staff[loc_id]
        })
    
    # Sort other locations by name
    other_locations_data.sort(key=lambda x: x['location_name'])
    
    # Build summary
    total_staff = len(raw_data['staff_list'])
    total_locations = (1 if primary_location_data else 0) + len(other_locations_data)
    
    summary = {
        'total_staff': total_staff,
        'total_locations': total_locations
    }
    
    # Package formatted data
    json_data = {
        'version': 1,
        'source': 'sync_speako_data',
        'analysis_artifact_url': '',
        'locale': 'en-AU',
        'summary': summary,
        'data': {
            'primary_location': primary_location_data,
            'other_locations': other_locations_data
        }
    }
    
    # Generate markdown from JSON data
    markdown_content = _build_staff_markdown(json_data)
    
    return json_data, markdown_content




@app.task(bind=True)
def sync_speako_data(self, *, 
                     tenant_id: str, 
                     location_id: str, 
                     knowledge_type: str,
                     speako_task_id: str | None = None,
                     tenant_integration_param: dict | None = None) -> dict:
    """
    Sync knowledge data directly from Speako's internal database.
    
    This task fetches data from Speako's database based on tenant_id, location_id,
    and knowledge_type, then processes and stores it for AI agent consumption.
    
    Args:
        tenant_id: Tenant identifier
        location_id: Location identifier
        knowledge_type: Type of knowledge to sync (business_info, service_menu, locations, staff)
        speako_task_id: Optional task correlation ID
        tenant_integration_param: Optional integration metadata
    
    Returns:
        dict: Task result with success status and data details
    """
    start_ts = time.time()
    started_at = datetime.utcnow().isoformat() + 'Z'

    # Log task start
    logger.info(f"🔄 [sync_speako_data] Started sync for tenant={tenant_id}, location={location_id}, knowledge_type={knowledge_type}")
    
    if tenant_integration_param:
        logger.info(f"📋 [sync_speako_data] tenantIntegrationParam received: {tenant_integration_param}")
    
    # Mark task as running in DB (best-effort)
    if speako_task_id:
        try:
            mark_task_running(
                task_id=str(speako_task_id), 
                celery_task_id=str(self.request.id),
                message='Sync started', 
                details={
                    'tenant_id': tenant_id,
                    'location_id': location_id,
                    'knowledge_type': knowledge_type
                }, 
                actor='celery'
            )
        except Exception as db_e:
            logger.warning(f"mark_task_running failed: {db_e}")

    try:
        json_output = None
        markdown_output = None
        ai_description = None
        
        # Route to appropriate sync handler based on knowledge_type
        if knowledge_type == 'business_info':
            logger.info(f"📊 [sync_speako_data] Syncing business_info for tenant={tenant_id}, location={location_id}")
            
            # Query database
            try:
                raw_data = _query_business_info(tenant_id, location_id)
                num_locations = len(raw_data.get('locations', []))
                logger.info(f"✅ [sync_speako_data] Retrieved business data: company={raw_data['business_data'].get('company_name')}, locations_count={num_locations}")
            except Exception as query_e:
                logger.error(f"❌ [sync_speako_data] Database query failed: {query_e}")
                raise
            
            # Format into JSON + Markdown
            try:
                json_output, markdown_output = _format_business_info(raw_data)
                logger.info(f"✅ [sync_speako_data] Formatted business_info: {len(json.dumps(json_output))} bytes JSON, {len(markdown_output)} bytes Markdown")
            except Exception as format_e:
                logger.error(f"❌ [sync_speako_data] Data formatting failed: {format_e}")
                raise
            
            # Generate AI description
            try:
                company_name = json_output['data'].get('company_name', 'Business')
                num_locations = len(json_output['data'].get('locations', []))
                location_plural = 'location' if num_locations == 1 else 'locations'
                ai_description = f"Business information for {company_name} with {num_locations} {location_plural} including operating hours and contact details"
                logger.info(f"📝 [sync_speako_data] Generated AI description: {ai_description}")
            except Exception as desc_e:
                logger.warning(f"⚠️ [sync_speako_data] Failed to generate AI description: {desc_e}")
        
        elif knowledge_type == 'service_menu':
            logger.info(f"🛍️ [sync_speako_data] Syncing service_menu for tenant={tenant_id}, location={location_id}")
            
            # Query database
            try:
                raw_data = _query_service_menu(tenant_id, location_id)
                num_categories = len(raw_data.get('categories', []))
                num_services = len(raw_data.get('services', []))
                num_locations = len(raw_data.get('locations', []))
                location_services = raw_data.get('location_services', {})
                
                # DEBUG: Log detailed service distribution
                logger.info(f"✅ [sync_speako_data] Retrieved service menu data: {num_locations} locations, {num_categories} categories, {num_services} services")
                logger.info(f"🔍 [DEBUG] Categories found: {[c['name'] for c in raw_data.get('categories', [])]}")
                logger.info(f"🔍 [DEBUG] Services found: {[s['name'] for s in raw_data.get('services', [])]}")
                logger.info(f"🔍 [DEBUG] Location-Service mapping: {dict((loc_id, len(svc_ids)) for loc_id, svc_ids in location_services.items())}")
                logger.info(f"🔍 [DEBUG] Primary location_id={location_id}, has {len(location_services.get(int(location_id), []))} services assigned")
                
                # DEBUG: Check if services have category_tag_ids
                for svc in raw_data.get('services', [])[:5]:  # Log first 5 services
                    logger.info(f"🔍 [DEBUG] Service '{svc['name']}'(id={svc['service_id']}): category_tag_ids={svc.get('category_tag_ids', [])}, is_active={svc.get('is_active')}")
                    
            except Exception as query_e:
                logger.error(f"❌ [sync_speako_data] Database query failed: {query_e}")
                raise
            
            # Format into JSON + Markdown (location-centric)
            try:
                json_output, markdown_output = _format_service_menu(raw_data, int(location_id))
                
                # Count services per location
                primary_loc = json_output['data'].get('primary_location', {})
                primary_services = sum(len(cat.get('items', [])) for cat in primary_loc.get('categories', []))
                other_locs = json_output['data'].get('other_locations', [])
                other_loc_count = len(other_locs)
                
                # DEBUG: Log what categories ended up in the output
                logger.info(f"✅ [sync_speako_data] Formatted service_menu: primary_location has {primary_services} services, {other_loc_count} other locations, {len(json.dumps(json_output))} bytes JSON, {len(markdown_output)} bytes Markdown")
                logger.info(f"🔍 [DEBUG] Primary location '{primary_loc.get('location_name')}' categories in output: {[cat.get('name') for cat in primary_loc.get('categories', [])]}")
                for cat in primary_loc.get('categories', []):
                    logger.info(f"🔍 [DEBUG] Category '{cat.get('name')}' has {len(cat.get('items', []))} items")
                
            except Exception as format_e:
                logger.error(f"❌ [sync_speako_data] Data formatting failed: {format_e}")
                raise
            
            # Generate AI description
            try:
                primary_loc = json_output['data'].get('primary_location', {})
                primary_name = primary_loc.get('location_name', 'Primary Location')
                primary_services = sum(len(cat.get('items', [])) for cat in primary_loc.get('categories', []))
                other_locs = json_output['data'].get('other_locations', [])
                num_other_locs = len(other_locs)
                
                service_plural = 'service' if primary_services == 1 else 'services'
                if num_other_locs > 0:
                    loc_plural = 'location' if num_other_locs == 1 else 'locations'
                    ai_description = f"Service menu for {primary_name} ({primary_services} {service_plural}) and {num_other_locs} other {loc_plural}"
                else:
                    ai_description = f"Service menu for {primary_name} with {primary_services} {service_plural}"
                
                logger.info(f"📝 [sync_speako_data] Generated AI description: {ai_description}")
            except Exception as desc_e:
                logger.warning(f"⚠️ [sync_speako_data] Failed to generate AI description: {desc_e}")
        
        elif knowledge_type == 'locations':
            logger.info(f"📍 [sync_speako_data] Syncing locations for tenant={tenant_id}, location={location_id}")
            
            # Query database
            try:
                raw_data = _query_locations(tenant_id, location_id)
                num_locations = len(raw_data.get('locations', []))
                num_services = len(raw_data.get('services_names', {}))
                logger.info(f"✅ [sync_speako_data] Retrieved location data: {num_locations} locations, {num_services} services available")
            except Exception as query_e:
                logger.error(f"❌ [sync_speako_data] Database query failed: {query_e}")
                raise
            
            # Format into JSON + Markdown (location-centric)
            try:
                json_output, markdown_output = _format_locations(raw_data, int(location_id))
                
                # Count services per location
                primary_loc = json_output['data'].get('primary_location', {})
                primary_name = primary_loc.get('name', 'Primary Location')
                primary_services_count = len(primary_loc.get('services_available', []))
                other_locs = json_output['data'].get('other_locations', [])
                other_loc_count = len(other_locs)
                
                logger.info(f"✅ [sync_speako_data] Formatted locations: primary_location='{primary_name}' has {primary_services_count} services, {other_loc_count} other locations, {len(json.dumps(json_output))} bytes JSON, {len(markdown_output)} bytes Markdown")
            except Exception as format_e:
                logger.error(f"❌ [sync_speako_data] Data formatting failed: {format_e}")
                raise
            
            # Generate AI description
            try:
                primary_loc = json_output['data'].get('primary_location', {})
                primary_name = primary_loc.get('name', 'Primary Location')
                primary_services_count = len(primary_loc.get('services_available', []))
                other_locs = json_output['data'].get('other_locations', [])
                num_other_locs = len(other_locs)
                
                service_plural = 'service' if primary_services_count == 1 else 'services'
                if num_other_locs > 0:
                    loc_plural = 'location' if num_other_locs == 1 else 'locations'
                    ai_description = f"Location details for {primary_name} ({primary_services_count} {service_plural}) and {num_other_locs} other {loc_plural}"
                else:
                    ai_description = f"Location details for {primary_name} with {primary_services_count} {service_plural}"
                
                logger.info(f"📝 [sync_speako_data] Generated AI description: {ai_description}")
            except Exception as desc_e:
                logger.warning(f"⚠️ [sync_speako_data] Failed to generate AI description: {desc_e}")
        
        elif knowledge_type in ['staff']:
            logger.info(f"📋 [sync_speako_data] Querying staff data for tenant_id={tenant_id}")
            staff_data = _query_staff(tenant_id, location_id)
            
            logger.info(f"🔄 [sync_speako_data] Formatting staff data")
            json_output, markdown_output = _format_staff(staff_data, int(location_id))
            
            logger.info(f"📊 [sync_speako_data] Staff data formatted: {len(str(json_output))} bytes JSON, {len(markdown_output)} bytes Markdown")
            
            # Generate AI description
            try:
                total_staff = json_output['summary']['total_staff']
                primary_loc = json_output['data'].get('primary_location', {})
                primary_name = primary_loc.get('name', 'Primary Location')
                primary_staff_count = len(primary_loc.get('staff', []))
                other_locs = json_output['data'].get('other_locations', [])
                num_other_locs = len(other_locs)
                
                staff_plural = 'staff member' if primary_staff_count == 1 else 'staff members'
                if num_other_locs > 0:
                    loc_plural = 'location' if num_other_locs == 1 else 'locations'
                    ai_description = f"Team roster: {total_staff} total staff - {primary_name} ({primary_staff_count} {staff_plural}) and {num_other_locs} other {loc_plural}"
                else:
                    ai_description = f"Team roster for {primary_name} with {primary_staff_count} {staff_plural}"
                
                logger.info(f"📝 [sync_speako_data] Generated AI description: {ai_description}")
            except Exception as desc_e:
                logger.warning(f"⚠️ [sync_speako_data] Failed to generate AI description: {desc_e}")
        
        else:
            raise ValueError(f"Unsupported knowledge_type: {knowledge_type}")
        
        # Save to database
        if json_output is not None:
            try:
                param_id = upsert_tenant_integration_param(
                    tenant_integration_param=tenant_integration_param,
                    analysis_result=json_output,
                    value_text=markdown_output or "",
                    ai_description=ai_description
                )
                if param_id:
                    logger.info(f"✅ [sync_speako_data] Saved to tenant_integration_param (param_id={param_id})")
                else:
                    logger.warning(f"⚠️ [sync_speako_data] Failed to save - no param_id returned")
            except Exception as save_e:
                logger.error(f"❌ [sync_speako_data] Database save failed: {save_e}")
                raise
        
        # Mark succeeded
        if speako_task_id:
            try:
                mark_task_succeeded(
                    task_id=str(speako_task_id), 
                    celery_task_id=str(self.request.id),
                    details={
                        'tenant_id': tenant_id,
                        'location_id': location_id,
                        'knowledge_type': knowledge_type,
                        'status': 'success'
                    },
                    actor='celery', 
                    progress=100
                )
            except Exception as db_e:
                logger.warning(f"mark_task_succeeded failed: {db_e}")
        
        return {
            'success': True,
            'tenant_id': tenant_id,
            'location_id': location_id,
            'knowledge_type': knowledge_type,
            'status': 'success',
            'message': f'Successfully synced {knowledge_type} data from Speako database',
            'data': {
                'json_size': len(json.dumps(json_output)) if json_output else 0,
                'markdown_size': len(markdown_output) if markdown_output else 0,
                'ai_description': ai_description
            },
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }

    except Exception as e:
        error_msg = f"Sync failed for tenant={tenant_id}, location={location_id}, knowledge_type={knowledge_type}"
        logger.error(f"❌ {error_msg} - {type(e).__name__}: {str(e)}")
        logger.exception("Full traceback:")
        
        if speako_task_id:
            try:
                mark_task_failed(
                    task_id=str(speako_task_id), 
                    celery_task_id=str(self.request.id),
                    error_code='sync_error', 
                    error_message=str(e),
                    details={
                        'tenant_id': tenant_id,
                        'location_id': location_id,
                        'knowledge_type': knowledge_type,
                        'error_type': type(e).__name__
                    }, 
                    actor='celery'
                )
            except Exception as db_e:
                logger.warning(f"mark_task_failed failed: {db_e}")
        
        return {
            'success': False,
            'error': f'Sync failed - {type(e).__name__}',
            'error_type': 'sync_error',
            'tenant_id': tenant_id,
            'location_id': location_id,
            'knowledge_type': knowledge_type,
            'job': {
                'task_id': self.request.id,
                'speako_task_id': speako_task_id,
                'started_at': started_at,
                'completed_at': datetime.utcnow().isoformat() + 'Z',
                'duration_ms': int((time.time() - start_ts) * 1000),
            }
        }
