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
                logger.info(f"✅ [sync_speako_data] Retrieved business data: company={raw_data['business_data'].get('company_name')}, location={raw_data['location_name']}")
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
        
        elif knowledge_type in ['service_menu', 'locations', 'staff']:
            # TODO: Implement other knowledge types
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
