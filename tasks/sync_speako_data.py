import os
import time
import json
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


def _build_business_markdown(data: dict, locations_data: list) -> str:
    """Build comprehensive markdown with all business info and all locations' availability."""
    lines = []
    
    # Company/Brand Name
    lines.append("## Company Name / Trading Name / Brand Name")
    lines.append(data.get('company_name', 'N/A'))
    lines.append("")
    
    # Legal Name
    lines.append("## Legal Name")
    lines.append(data.get('legal_name', 'N/A'))
    lines.append("")
    
    # Tagline
    lines.append("## Tagline")
    lines.append(data.get('tagline', 'N/A'))
    lines.append("")
    
    # Description
    lines.append("## Description")
    lines.append(data.get('description', 'N/A'))
    lines.append("")
    
    # Philosophy
    if data.get('philosophy'):
        lines.append("## Philosophy")
        lines.append(data['philosophy'])
        lines.append("")
    
    # Contacts
    lines.append("## Contacts")
    email = data.get('contacts', {}).get('email', '')
    phone = data.get('contacts', {}).get('phone', '')
    if email:
        lines.append(f"- Email: {email}")
    if phone:
        lines.append(f"- Phone: {phone}")
    if not email and not phone:
        lines.append("- N/A")
    lines.append("")
    
    # Website
    lines.append("## Website")
    lines.append(data.get('website', 'N/A'))
    lines.append("")
    
    # Social
    lines.append("## Social")
    instagram = data.get('social', {}).get('instagram', '')
    facebook = data.get('social', {}).get('facebook', '')
    if instagram:
        lines.append(f"- Instagram: {instagram}")
    if facebook:
        lines.append(f"- Facebook: {facebook}")
    if not instagram and not facebook:
        lines.append("- N/A")
    lines.append("")
    
    # Branding
    lines.append("## Branding")
    logo = data.get('branding', {}).get('logo_url', '')
    color = data.get('branding', {}).get('primary_color', '')
    if logo:
        lines.append(f"- Logo URL: {logo}")
    if color:
        lines.append(f"- Primary Color: {color}")
    if not logo and not color:
        lines.append("- N/A")
    lines.append("")
    
    # All locations' opening hours
    lines.append("## Locations & Opening Hours")
    lines.append("")
    
    for idx, location in enumerate(locations_data):
        location_name = location.get('location_name', 'Unknown Location')
        location_id = location.get('location_id', '')
        recurring_hours = location.get('hours', {}).get('recurring', {})
        onetime_hours = location.get('hours', {}).get('exceptions', [])
        
        # Location header
        lines.append(f"### {location_name} (Location ID: {location_id})")
        lines.append("")
        
        # Regular opening hours
        lines.append("#### Regular Schedule")
        for day in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
            day_key = day.lower()
            slots = recurring_hours.get(day_key, [])
            if slots:
                lines.append(f"- {day}: {', '.join(slots)}")
            else:
                lines.append(f"- {day}: Closed")
        lines.append("")
        
        # Categorize one-time hours
        public_holidays = [e for e in onetime_hours if e['type'] == 'public_holiday']
        special_hours = [e for e in onetime_hours if e['type'] == 'special_hours']
        other_closures = [e for e in onetime_hours if e['type'] == 'closure']
        
        # Public holiday closures
        if public_holidays:
            lines.append("#### Public Holiday Closures")
            for entry in public_holidays:
                holiday_info = f" ({entry['holiday_name']})" if entry['holiday_name'] else ""
                lines.append(f"- {entry['date']}{holiday_info}: Closed")
            lines.append("")
        
        # Special hours
        if special_hours:
            lines.append("#### Special Hours & Exceptions")
            for entry in special_hours:
                hours_str = ', '.join(entry['hours'])
                lines.append(f"- {entry['date']}: {hours_str}")
            lines.append("")
        
        # Other closures
        if other_closures:
            lines.append("#### Other Closures")
            for entry in other_closures:
                lines.append(f"- {entry['date']}: Closed")
            lines.append("")
        
        # Add separator between locations (except for the last one)
        if idx < len(locations_data) - 1:
            lines.append("---")
            lines.append("")
    
    return "\n".join(lines)


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
    Format raw DB data into JSON and Markdown for ALL locations.
    
    Args:
        raw_data: dict with business_data and locations (list)
    
    Returns:
        tuple: (json_dict, markdown_string)
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
    
    # Build JSON structure
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
    
    # Build comprehensive markdown
    markdown = _build_business_markdown(
        json_output['data'],
        locations_formatted
    )
    
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


def _format_service_menu(raw_data: dict, primary_location_id: int) -> tuple[dict, str]:
    """
    Transform raw DB data into location-centric JSON + Markdown format.
    Shows primary location at top, other locations grouped separately.
    
    Args:
        raw_data: Dict with categories, locations, services, location_services, service_modifiers
        primary_location_id: The location that triggered this sync (shown first)
    
    Returns:
        tuple: (json_dict, markdown_string)
    """
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
    
    # Build JSON
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
    
    # Build Markdown
    markdown_content = _build_service_menu_markdown(json_data['data'])
    
    return json_data, markdown_content


def _build_service_menu_markdown(data: dict) -> str:
    """
    Build comprehensive Markdown from location-centric formatted data.
    
    Args:
        data: Dict with primary_location and other_locations
    
    Returns:
        str: Markdown content
    """
    lines = []
    lines.append("> Group services by **Category**, then list **Items** with duration and price.")
    lines.append("")
    lines.append("# Service Menu")
    lines.append("")
    
    # Primary location
    primary_loc = data.get('primary_location')
    if primary_loc:
        lines.append(f"## Primary Location: {primary_loc['location_name']}")
        lines.append("")
        
        for category in primary_loc.get('categories', []):
            lines.append(f"### Category: {category['name']}")
            
            for item in category['items']:
                lines.append(f"#### Service: {item['name']}")
                lines.append("**Description:**  ")
                lines.append(item['description'] or 'No description available')
                lines.append("")
                lines.append(f"- Duration: {item['duration_min']} min")
                lines.append(f"- Price: ${item['price']['amount']:.2f} {item['price']['currency']}")
                
                # Format addons
                if item['addons']:
                    addon_parts = []
                    for addon in item['addons']:
                        addon_str = f"{addon['name']} (${addon['price']:.2f})"
                        
                        # Add indicators
                        indicators = []
                        if addon.get('is_required'):
                            indicators.append('Required')
                        if addon.get('default_selected'):
                            indicators.append('Default')
                        
                        if indicators:
                            addon_str += f" [{']['.join(indicators)}]"
                        
                        addon_parts.append(addon_str)
                    
                    lines.append(f"- Add-ons: {', '.join(addon_parts)}")
                else:
                    lines.append("- Add-ons: None")
                
                lines.append("")
            
            lines.append("")
    
    # Other locations
    other_locations = data.get('other_locations', [])
    if other_locations:
        lines.append("---")
        lines.append("")
        lines.append("## Other Locations")
        lines.append("")
        
        for location in other_locations:
            lines.append(f"### Location: {location['location_name']}")
            lines.append("")
            
            for category in location.get('categories', []):
                lines.append(f"#### Category: {category['name']}")
                
                for item in category['items']:
                    lines.append(f"##### Service: {item['name']}")
                    lines.append("**Description:**  ")
                    lines.append(item['description'] or 'No description available')
                    lines.append("")
                    lines.append(f"- Duration: {item['duration_min']} min")
                    lines.append(f"- Price: ${item['price']['amount']:.2f} {item['price']['currency']}")
                    
                    # Format addons
                    if item['addons']:
                        addon_parts = []
                        for addon in item['addons']:
                            addon_str = f"{addon['name']} (${addon['price']:.2f})"
                            
                            # Add indicators
                            indicators = []
                            if addon.get('is_required'):
                                indicators.append('Required')
                            if addon.get('default_selected'):
                                indicators.append('Default')
                            
                            if indicators:
                                addon_str += f" [{']['.join(indicators)}]"
                            
                            addon_parts.append(addon_str)
                        
                        lines.append(f"- Add-ons: {', '.join(addon_parts)}")
                    else:
                        lines.append("- Add-ons: None")
                    
                    lines.append("")
                
                lines.append("")
            
            lines.append("")
    
    return '\n'.join(lines)


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
    Transform raw DB data into location-centric JSON + Markdown format.
    
    Args:
        raw_data: Dict with locations, location_info, hours, services, states
        primary_location_id: The location that triggered this sync
    
    Returns:
        tuple: (json_dict, markdown_string)
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
    
    # Build JSON
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
    
    # Build Markdown
    markdown_content = _build_locations_markdown(json_data['data'])
    
    return json_data, markdown_content


def _build_locations_markdown(data: dict) -> str:
    """
    Build comprehensive Markdown from location data.
    
    Args:
        data: Dict with primary_location and other_locations
    
    Returns:
        str: Markdown content
    """
    lines = []
    
    # Header / Instructions
    lines.append("# How to use this document")
    lines.append("")
    lines.append("This is a simple text form. You can type normally — no special tech skills needed.")
    lines.append("")
    lines.append("**You can:**")
    lines.append("- **Delete** any section you don't need.")
    lines.append("- **Add** more sections or items by copying a block and pasting it below.")
    lines.append("- **Rename** headings if it helps.")
    lines.append("- **Write naturally** in sentences or bullet points. Both are fine.")
    lines.append("")
    lines.append("**Tips**")
    lines.append("- Dates: use **YYYY-MM-DD** (e.g., 2025-10-31) if possible.")
    lines.append("- Times: 09:00–17:00 (24-hour) or 9am–5pm (either is okay).")
    lines.append("- Optional fields are truly optional — fill what you have and skip the rest.")
    lines.append("")
    lines.append("When you're done, just save. You can always return and edit later.")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("# Locations")
    lines.append("")
    
    # Primary location
    primary_loc = data.get('primary_location')
    if primary_loc:
        lines.append(f"## Primary Location: {primary_loc['name']}")
        lines.append("")
        
        # Address
        lines.append("**Address**  ")
        addr = primary_loc['address']
        if addr['line1']:
            lines.append(addr['line1'])
        if addr['line2']:
            lines.append(addr['line2'])
        city_state_post = ', '.join(filter(None, [addr['city'], addr['state'], addr['postcode']]))
        if city_state_post:
            lines.append(f"{city_state_post}, {addr['country']}")
        elif addr['country']:
            lines.append(addr['country'])
        lines.append("")
        
        # Contact
        lines.append("**Contact**")
        contact = primary_loc['contact']
        if contact['phone']:
            lines.append(f"- Phone: {contact['phone']}")
        if contact['email']:
            lines.append(f"- Email: {contact['email']}")
        if contact['website']:
            lines.append(f"- Website: {contact['website']}")
        if not any([contact['phone'], contact['email'], contact['website']]):
            lines.append("- No contact information available")
        lines.append("")
        
        # Location Details
        lines.append("**Location Details**")
        if primary_loc['location_type']:
            lines.append(f"- Type: {primary_loc['location_type']}")
        if primary_loc['timezone']:
            lines.append(f"- Timezone: {primary_loc['timezone']}")
        if primary_loc['geo']['lat'] != 0 or primary_loc['geo']['lng'] != 0:
            lines.append(f"- Coordinates: lat {primary_loc['geo']['lat']}, lng {primary_loc['geo']['lng']}")
        lines.append("")
        
        # Opening Hours (recurring)
        lines.append("**Opening Hours**")
        hours = primary_loc['hours']['recurring']
        day_order = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        day_labels = {'mon': 'Mon', 'tue': 'Tue', 'wed': 'Wed', 'thu': 'Thu', 'fri': 'Fri', 'sat': 'Sat', 'sun': 'Sun'}
        for day in day_order:
            day_hours = hours.get(day, [])
            if day_hours:
                lines.append(f"- {day_labels[day]}: {', '.join(day_hours)}")
            else:
                lines.append(f"- {day_labels[day]}: Closed")
        lines.append("")
        
        # Exceptions
        exceptions = primary_loc['hours']['exceptions']
        if exceptions:
            lines.append("**Exceptions/Special Hours**")
            for exc in exceptions:
                lines.append(f"- {exc['date']}: {exc['status']} ({exc['type']})")
            lines.append("")
        
        # Services Available
        services = primary_loc['services_available']
        if services:
            lines.append("**Services Available**")
            for svc in services:
                lines.append(f"- {svc}")
            lines.append(f"(Total: {len(services)} services)")
        else:
            lines.append("**Services Available**")
            lines.append("- No services configured")
        lines.append("")
        
        # Booking Information
        booking = primary_loc['booking_info']
        if any([booking['min_advance_minutes'], booking['slot_interval_minutes'], booking['booking_email']]):
            lines.append("**Booking Information**")
            if booking['min_advance_minutes']:
                lines.append(f"- Minimum advance booking: {booking['min_advance_minutes']} minutes")
            if booking['slot_interval_minutes']:
                lines.append(f"- Booking slot intervals: {booking['slot_interval_minutes']} minutes")
            if booking['booking_email']:
                lines.append(f"- Booking notifications sent to: {booking['booking_email']}")
            lines.append("")
        
        # Notes
        if primary_loc['notes']:
            lines.append("**Notes**  ")
            lines.append(primary_loc['notes'])
            lines.append("")
    
    # Other locations
    other_locations = data.get('other_locations', [])
    if other_locations:
        lines.append("---")
        lines.append("")
        lines.append("## Other Locations")
        lines.append("")
        
        for location in other_locations:
            lines.append(f"### Location: {location['name']}")
            lines.append("")
            
            # Address
            lines.append("**Address**  ")
            addr = location['address']
            if addr['line1']:
                lines.append(addr['line1'])
            if addr['line2']:
                lines.append(addr['line2'])
            city_state_post = ', '.join(filter(None, [addr['city'], addr['state'], addr['postcode']]))
            if city_state_post:
                lines.append(f"{city_state_post}, {addr['country']}")
            elif addr['country']:
                lines.append(addr['country'])
            lines.append("")
            
            # Contact
            lines.append("**Contact**")
            contact = location['contact']
            if contact['phone']:
                lines.append(f"- Phone: {contact['phone']}")
            if contact['email']:
                lines.append(f"- Email: {contact['email']}")
            if contact['website']:
                lines.append(f"- Website: {contact['website']}")
            if not any([contact['phone'], contact['email'], contact['website']]):
                lines.append("- No contact information available")
            lines.append("")
            
            # Location Details
            lines.append("**Location Details**")
            if location['location_type']:
                lines.append(f"- Type: {location['location_type']}")
            if location['timezone']:
                lines.append(f"- Timezone: {location['timezone']}")
            lines.append("")
            
            # Opening Hours
            lines.append("**Opening Hours**")
            hours = location['hours']['recurring']
            for day in day_order:
                day_hours = hours.get(day, [])
                if day_hours:
                    lines.append(f"- {day_labels[day]}: {', '.join(day_hours)}")
                else:
                    lines.append(f"- {day_labels[day]}: Closed")
            lines.append("")
            
            # Exceptions
            exceptions = location['hours']['exceptions']
            if exceptions:
                lines.append("**Exceptions/Special Hours**")
                for exc in exceptions:
                    lines.append(f"- {exc['date']}: {exc['status']} ({exc['type']})")
                lines.append("")
            
            # Services Available
            services = location['services_available']
            if services:
                lines.append("**Services Available**")
                for svc in services:
                    lines.append(f"- {svc}")
                lines.append(f"(Total: {len(services)} services)")
            else:
                lines.append("**Services Available**")
                lines.append("- No services configured")
            lines.append("")
            
            # Booking Information
            booking = location['booking_info']
            if any([booking['min_advance_minutes'], booking['slot_interval_minutes'], booking['booking_email']]):
                lines.append("**Booking Information**")
                if booking['min_advance_minutes']:
                    lines.append(f"- Minimum advance booking: {booking['min_advance_minutes']} minutes")
                if booking['slot_interval_minutes']:
                    lines.append(f"- Booking slot intervals: {booking['slot_interval_minutes']} minutes")
                if booking['booking_email']:
                    lines.append(f"- Booking notifications sent to: {booking['booking_email']}")
                lines.append("")
            
            # Notes
            if location['notes']:
                lines.append("**Notes**  ")
                lines.append(location['notes'])
                lines.append("")
            
            lines.append("")
    
    return '\n'.join(lines)


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
                logger.info(f"✅ [sync_speako_data] Retrieved service menu data: {num_locations} locations, {num_categories} categories, {num_services} services")
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
                
                logger.info(f"✅ [sync_speako_data] Formatted service_menu: primary_location has {primary_services} services, {other_loc_count} other locations, {len(json.dumps(json_output))} bytes JSON, {len(markdown_output)} bytes Markdown")
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
            # TODO: Implement staff knowledge type
            logger.warning(f"⚠️ [sync_speako_data] Knowledge type '{knowledge_type}' not yet implemented")
            raise NotImplementedError(f"Knowledge type '{knowledge_type}' sync not yet implemented")
        
        else:
            raise ValueError(f"Unsupported knowledge_type: {knowledge_type}")
        
        # Save to database
        if json_output and markdown_output:
            try:
                param_id = upsert_tenant_integration_param(
                    tenant_integration_param=tenant_integration_param,
                    analysis_result=json_output,
                    value_text=markdown_output,
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
