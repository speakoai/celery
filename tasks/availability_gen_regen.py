from dotenv import load_dotenv
load_dotenv()

from tasks.celery_app import app
from celery.utils.log import get_task_logger
from tasks.utils.availability_helpers import reconstruct_staff_availability, reconstruct_venue_availability

import os
import psycopg2
import redis
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil import parser
import time

print(f"[DEBUG] DATABASE_URL: {os.getenv('DATABASE_URL')}")
print(f"[DEBUG] REDIS_URL: {os.getenv('REDIS_URL')}")

load_dotenv()
logger = get_task_logger(__name__)

def resolve_tag_names(zone_tag_ids, venue_tags_lookup):
    """Convert zone_tag_ids array to comma-separated tag names"""
    if not zone_tag_ids:
        return ""
    
    tag_names = []
    for tag_id in zone_tag_ids:
        if tag_id in venue_tags_lookup:
            tag_names.append(venue_tags_lookup[tag_id])
    
    return ", ".join(sorted(tag_names))

@app.task
def fetch_sample_data():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set")
        return

    logger.info(f"[PRODUCTION] Connecting to PostgreSQL at: {db_url}")

    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' LIMIT 3;")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        logger.info(f"[PRODUCTION] Successfully fetched {len(rows)} tables.")
        return rows
    except Exception as e:
        logger.error(f"[PRODUCTION] Database error: {e}")
        return None

@app.task
def gen_availability(tenant_id, location_id, location_tz, affected_date=None):
    logger.info(f"[LOCAL TEST] Generating availability for tenant={tenant_id}, location={location_id}")

    db_url = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")

    if not db_url or not redis_url:
        logger.error("Missing DATABASE_URL or REDIS_URL in .env")
        return

    try:
        pg_conn = psycopg2.connect(db_url)
        logger.info("✅ Connected to PostgreSQL")
        logger.debug(f"🔍 Using DB URL: {os.getenv('DATABASE_URL')}")

        valkey_client = redis.Redis.from_url(redis_url, decode_responses=True)
        logger.info("[DEBUG] Connected to Redis")

        cur = pg_conn.cursor()
        db_start = time.time()
        chunk_size = 3
        is_regen = affected_date is not None

        if is_regen:
            
            dt = parser.parse(affected_date)
            affected_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            # Ensure affected_dt has the same timezone as location_tz
            if affected_dt.tzinfo is None:
                affected_dt = affected_dt.replace(tzinfo=ZoneInfo(location_tz))
            # Compute current midnight in location_tz for chunk calculation
            now_local = datetime.now(ZoneInfo(location_tz))
            current_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            day_offset = (affected_dt - current_start).days
            if day_offset < 0:
                logger.info(f"[SKIP] Affected date {affected_date} is in the past for tenant={tenant_id}, location={location_id}")
                return {"status": "skipped"}
            chunk_index = day_offset // chunk_size
            chunk_start_offset = chunk_index * chunk_size
            chunk_start_day = current_start + timedelta(days=chunk_start_offset)
            start_date = chunk_start_day
            days_range = chunk_size  # 3 days for regen
            start_date = start_date.replace(tzinfo=ZoneInfo(location_tz))
            logger.info(f"[REGEN] Regenerating chunk starting {chunk_start_day.date().isoformat()} for affected_date={affected_date}")
        else:
            start_date = datetime.now(ZoneInfo(location_tz)).replace(hour=0, minute=0, second=0, microsecond=0)
            days_range = 60
            logger.info(f"[FULL] Generating full availability starting from {start_date.date().isoformat()}")

        # Preload services once
        services = []
        cur.execute("""
            SELECT s.service_id, s.name, EXTRACT(EPOCH FROM s.duration)/60
            FROM location_services ls
            JOIN services s ON ls.tenant_id = s.tenant_id AND ls.service_id = s.service_id
            WHERE ls.tenant_id = %s AND ls.location_id = %s
            ORDER BY s.service_id
        """, (tenant_id, location_id))
        for row in cur.fetchall():
            services.append({
                "id": row[0],
                "name": row[1],
                "duration": int(row[2])
            })

        staff_services, location_services = {}, set()
        # Preload services on first day
        cur.execute("SELECT staff_id, service_id FROM staff_services WHERE tenant_id = %s", (tenant_id,))
        for sid, svc_id in cur.fetchall():
            staff_services.setdefault(sid, []).append(svc_id)

        cur.execute("SELECT service_id FROM location_services WHERE tenant_id = %s AND location_id = %s", (tenant_id, location_id))
        location_services = {r[0] for r in cur.fetchall()}

        for chunk_start in range(0, days_range, chunk_size):
            response = {
                "tenant_id": tenant_id,
                "location_id": location_id,
                "services": services,
                "availabilities": []
            }

            for day_offset in range(chunk_start, min(chunk_start + chunk_size, days_range)):
                current_date = start_date + timedelta(days=day_offset)
                current_date_str = current_date.strftime("%Y-%m-%d")
                python_day = current_date.weekday()
                db_day = (python_day + 1) % 7

                availability = {
                    "date": current_date_str,
                    "staff": [],
                    "holiday": None,
                    "is_open": True,
                    "open_hours": []
                }

                # Step 1: Get one-time availability entries for this specific date
                cur.execute("""
                    SELECT s.staff_id, s.name, sa.start_time, sa.end_time, sa.is_closed
                    FROM staff s
                    JOIN staff_availability sa ON s.tenant_id = sa.tenant_id AND s.staff_id = sa.staff_id
                    WHERE s.tenant_id = %s AND sa.location_id = %s AND sa.type = 'one_time'
                    AND sa.specific_date = %s AND sa.is_active = TRUE
                """, (tenant_id, location_id, current_date_str))
                one_time_staff_rows = cur.fetchall()
                
                # Get staff IDs that have one-time entries
                staff_with_one_time = {row[0] for row in one_time_staff_rows}
                
                # Step 2: Get recurring availability for staff who don't have one-time entries
                if staff_with_one_time:
                    cur.execute("""
                        SELECT s.staff_id, s.name, sa.start_time, sa.end_time
                        FROM staff s
                        JOIN staff_availability sa ON s.tenant_id = sa.tenant_id AND s.staff_id = sa.staff_id
                        WHERE s.tenant_id = %s AND sa.location_id = %s AND sa.type = 'recurring'
                        AND sa.day_of_week = %s AND (sa.specific_date IS NULL OR sa.specific_date <> %s)
                        AND sa.is_active = TRUE
                        AND s.staff_id NOT IN %s
                    """, (tenant_id, location_id, db_day, current_date_str, tuple(staff_with_one_time)))
                else:
                    cur.execute("""
                        SELECT s.staff_id, s.name, sa.start_time, sa.end_time
                        FROM staff s
                        JOIN staff_availability sa ON s.tenant_id = sa.tenant_id AND s.staff_id = sa.staff_id
                        WHERE s.tenant_id = %s AND sa.location_id = %s AND sa.type = 'recurring'
                        AND sa.day_of_week = %s AND (sa.specific_date IS NULL OR sa.specific_date <> %s)
                        AND sa.is_active = TRUE
                    """, (tenant_id, location_id, db_day, current_date_str))
                recurring_staff_rows = cur.fetchall()

                cur.execute("""
                    SELECT staff_id, customer_id, start_time, end_time
                    FROM bookings
                    WHERE tenant_id = %s AND location_id = %s
                    AND start_time >= %s AND start_time < %s::date + INTERVAL '1 day'
                    AND status = 'confirmed'
                """, (tenant_id, location_id, current_date_str, current_date_str))
                booking_rows = cur.fetchall()
                bookings = [{"staff_id": r[0], "customer_id": r[1], "start_time": r[2].strftime("%Y-%m-%d %H:%M:%S"), "end_time": r[3].strftime("%Y-%m-%d %H:%M:%S")} for r in booking_rows]

                staff_dict = {}
                
                # Process one-time availability first (highest priority)
                for sid, name, start, end, is_closed in one_time_staff_rows:
                    if not is_closed:  # Only add if not closed for the day
                        staff_dict.setdefault(sid, {
                            "id": sid,
                            "name": name,
                            "service": [svc for svc in staff_services.get(sid, []) if svc in location_services],
                            "slots": []
                        })["slots"].append({"start": str(start), "end": str(end)})
                    # If is_closed = true, staff is completely unavailable (don't add to staff_dict)
                
                # Process recurring availability for staff without one-time entries
                for sid, name, start, end in recurring_staff_rows:
                    staff_dict.setdefault(sid, {
                        "id": sid,
                        "name": name,
                        "service": [svc for svc in staff_services.get(sid, []) if svc in location_services],
                        "slots": []
                    })["slots"].append({"start": str(start), "end": str(end)})

                updated_staff_dict = reconstruct_staff_availability(bookings, staff_dict)
                availability["staff"] = list(updated_staff_dict.values())

                cur.execute("""
                    SELECT COUNT(*)
                    FROM location_availability
                    WHERE tenant_id = %s AND location_id = %s AND type = 'one_time'
                    AND specific_date = %s AND is_active = true AND is_closed = true
                """, (tenant_id, location_id, current_date_str))
                if cur.fetchone()[0] > 0:
                    availability["holiday"] = True
                    availability["is_open"] = False
                else:
                    cur.execute("""
                        SELECT start_time, end_time
                        FROM location_availability
                        WHERE tenant_id = %s AND location_id = %s AND is_active = true AND is_closed = false
                        AND ((type = 'recurring' AND day_of_week = %s) OR (type = 'one_time' AND specific_date = %s))
                        ORDER BY start_time
                    """, (tenant_id, location_id, db_day, current_date_str))
                    hours = cur.fetchall()
                    for s, e in hours:
                        availability["open_hours"].append({"start": s.strftime("%H:%M"), "end": e.strftime("%H:%M")})
                    if not availability["open_hours"]:
                        availability["is_open"] = False

                response["availabilities"].append(availability)

            # Cache per 3-day chunk
            # Get the chunk's start date
            chunk_start_date = start_date + timedelta(days=chunk_start)
            chunk_start_date_str = chunk_start_date.strftime("%Y-%m-%d")
            cache_key = f"availability:tenant_{tenant_id}:location_{location_id}:start_date_{chunk_start_date_str}"

            if not is_regen:
                # ➖ Delete the previous day's key
                prev_day = chunk_start_date - timedelta(days=1)
                prev_day_key = f"availability:tenant_{tenant_id}:location_{location_id}:start_date_{prev_day.strftime('%Y-%m-%d')}"
                deleted = valkey_client.delete(prev_day_key)
                if deleted:
                    logger.info(f"[LOCAL TEST] Deleted previous cache key: {prev_day_key}")
                else:
                    logger.info(f"[LOCAL TEST] No previous cache key to delete: {prev_day_key}")

            # ✅ Set current chunk's key
            valkey_client.set(cache_key, json.dumps(response))
            logger.info(f"🗝️ [CACHE WRITE] Writing cache key: {cache_key}")

        cur.close()
        db_end = time.time()
        logger.info(f"[INFO] DB fetch duration: {db_end - db_start:.2f}s")
        logger.info(f"[DEBUG] JSON generated and cached for tenant_id={tenant_id}, location_id={location_id}")

        return {"status": "success"}

    except Exception as e:
        import traceback
        logger.error(f"[LOCAL TEST] Exception occurred: {e}")
        traceback.print_exc()
        return None


@app.task
def gen_availability_venue(tenant_id, location_id, location_tz, affected_date=None):
    logger.info(f"[LOCAL TEST] Generating availability for tenant={tenant_id}, location={location_id}")

    db_url = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")

    if not db_url or not redis_url:
        logger.error("Missing DATABASE_URL or REDIS_URL in .env")
        return

    try:
        pg_conn = psycopg2.connect(db_url)
        logger.info("✅ Connected to PostgreSQL")
        valkey_client = redis.Redis.from_url(redis_url, decode_responses=True)
        logger.info("[DEBUG] Connected to Redis")

        cur = pg_conn.cursor()
        chunk_size = 3
        is_regen = affected_date is not None

        if is_regen:
            from dateutil import parser
            dt = parser.parse(affected_date)
            affected_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            # Ensure affected_dt has the same timezone as location_tz
            if affected_dt.tzinfo is None:
                affected_dt = affected_dt.replace(tzinfo=ZoneInfo(location_tz))
            # Compute current midnight in location_tz for chunk calculation
            now_local = datetime.now(ZoneInfo(location_tz))
            current_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            day_offset = (affected_dt - current_start).days
            if day_offset < 0:
                logger.info(f"[SKIP] Affected date {affected_date} is in the past for tenant={tenant_id}, location={location_id}")
                return {"status": "skipped"}
            chunk_index = day_offset // chunk_size
            chunk_start_offset = chunk_index * chunk_size
            chunk_start_day = current_start + timedelta(days=chunk_start_offset)
            start_date = chunk_start_day
            days_range = chunk_size  # 3 days for regen
            start_date = start_date.replace(tzinfo=ZoneInfo(location_tz))
            logger.info(f"[REGEN] Regenerating chunk starting {chunk_start_day.date().isoformat()} for affected_date={affected_date}")
        else:
            start_date = datetime.now(ZoneInfo(location_tz)).replace(hour=0, minute=0, second=0, microsecond=0)
            days_range = 60
            logger.info(f"[FULL] Generating full availability starting from {start_date.date().isoformat()}")

        # Preload services
        services = []
        cur.execute("""
            SELECT s.service_id, s.name, EXTRACT(EPOCH FROM s.duration)/60
            FROM location_services ls
            JOIN services s ON ls.tenant_id = s.tenant_id AND ls.service_id = s.service_id
            WHERE ls.tenant_id = %s AND ls.location_id = %s
            ORDER BY s.service_id
        """, (tenant_id, location_id))
        for row in cur.fetchall():
            services.append({
                "id": row[0],
                "name": row[1],
                "duration": int(row[2])
            })

        # Preload service mappings
        venue_unit_services = {}
        location_services = set()
        cur.execute("SELECT venue_unit_id, service_id FROM venue_unit_services WHERE tenant_id = %s", (tenant_id,))
        for vuid, svc_id in cur.fetchall():
            venue_unit_services.setdefault(vuid, []).append(svc_id)

        cur.execute("SELECT service_id FROM location_services WHERE tenant_id = %s AND location_id = %s", (tenant_id, location_id))
        location_services = {r[0] for r in cur.fetchall()}

        # Preload venue tags for this location
        venue_tags = {}
        location_zone_tags = []
        cur.execute("""
            SELECT tag_id, name, slug
            FROM location_tag 
            WHERE tenant_id = %s AND location_id = %s AND is_active = TRUE AND category_id = 1
            ORDER BY name
        """, (tenant_id, location_id))
        for tag_id, tag_name, tag_slug in cur.fetchall():
            venue_tags[tag_id] = tag_name  # Keep for lookup
            location_zone_tags.append({
                "id": tag_id,
                "name": tag_name,
                "slug": tag_slug
            })

        for chunk_start in range(0, days_range, chunk_size):
            response = {
                "tenant_id": tenant_id,
                "location_id": location_id,
                "services": services,
                "location_zone_tags": location_zone_tags,
                "availabilities": []
            }

            for day_offset in range(chunk_start, min(chunk_start + chunk_size, days_range)):
                current_date = start_date + timedelta(days=day_offset)
                current_date_str = current_date.strftime("%Y-%m-%d")
                python_day = current_date.weekday()
                db_day = (python_day + 1) % 7

                availability = {
                    "date": current_date_str,
                    "holiday": None,
                    "is_open": True,
                    "open_hours": []
                }

                # Step 1: Get one-time venue availability entries for this specific date
                cur.execute("""
                    SELECT vu.venue_unit_id, vu.name, vu.venue_unit_type, vu.capacity, vu.min_capacity, va.service_duration, va.start_time, va.end_time, va.availability_id, va.is_closed, vu.zone_tag_ids
                    FROM venue_unit vu
                    JOIN venue_availability va ON vu.tenant_id = va.tenant_id AND vu.venue_unit_id = va.venue_unit_id
                    WHERE vu.tenant_id = %s AND va.location_id = %s AND va.type = 'one_time'
                    AND va.specific_date = %s AND va.is_active = TRUE
                """, (tenant_id, location_id, current_date_str))
                one_time_venue_rows = cur.fetchall()
                
                # Get venue unit IDs that have one-time entries
                venues_with_one_time = {row[0] for row in one_time_venue_rows}
                
                # Step 2: Get recurring availability for venues who don't have one-time entries
                if venues_with_one_time:
                    cur.execute("""
                        SELECT vu.venue_unit_id, vu.name, vu.venue_unit_type, vu.capacity, vu.min_capacity, va.service_duration, va.start_time, va.end_time, va.availability_id, vu.zone_tag_ids
                        FROM venue_unit vu
                        JOIN venue_availability va ON vu.tenant_id = va.tenant_id AND vu.venue_unit_id = va.venue_unit_id
                        WHERE vu.tenant_id = %s AND va.location_id = %s AND va.type = 'recurring'
                        AND va.day_of_week = %s AND (va.specific_date IS NULL OR va.specific_date <> %s)
                        AND va.is_active = TRUE
                        AND vu.venue_unit_id NOT IN %s
                    """, (tenant_id, location_id, db_day, current_date_str, tuple(venues_with_one_time)))
                else:
                    cur.execute("""
                        SELECT vu.venue_unit_id, vu.name, vu.venue_unit_type, vu.capacity, vu.min_capacity, va.service_duration, va.start_time, va.end_time, va.availability_id, vu.zone_tag_ids
                        FROM venue_unit vu
                        JOIN venue_availability va ON vu.tenant_id = va.tenant_id AND vu.venue_unit_id = va.venue_unit_id
                        WHERE vu.tenant_id = %s AND va.location_id = %s AND va.type = 'recurring'
                        AND va.day_of_week = %s AND (va.specific_date IS NULL OR va.specific_date <> %s)
                        AND va.is_active = TRUE
                    """, (tenant_id, location_id, db_day, current_date_str))
                recurring_venue_rows = cur.fetchall()

                cur.execute("""
                    SELECT venue_unit_id, customer_id, start_time, end_time
                    FROM bookings
                    WHERE tenant_id = %s AND location_id = %s
                    AND start_time >= %s AND start_time < %s::date + INTERVAL '1 day'
                    AND status = 'confirmed'
                """, (tenant_id, location_id, current_date_str, current_date_str))
                booking_rows = cur.fetchall()
                bookings = [{"venue_unit_id": r[0], "customer_id": r[1], "start_time": r[2].strftime("%Y-%m-%d %H:%M:%S"), "end_time": r[3].strftime("%Y-%m-%d %H:%M:%S")} for r in booking_rows]

                venue_dict = {}
                is_dining_table = False

                # Process one-time venue availability first (highest priority)
                for vuid, name, venue_unit_type, capacity, min_capacity, service_duration, start, end, va_availability_id, is_closed, zone_tag_ids in one_time_venue_rows:
                    if venue_unit_type == "dining_table":
                        is_dining_table = True
                    if not is_closed:  # Only add if not closed for the day
                        zone_tags = resolve_tag_names(zone_tag_ids, venue_tags)
                        venue_dict.setdefault(vuid, {
                            "id": vuid,
                            "name": name,
                            "capacity": capacity,
                            "min_capacity": min_capacity,
                            "service": [svc for svc in venue_unit_services.get(vuid, []) if svc in location_services],
                            "zone_tags": zone_tags,
                            "zone_tag_ids": zone_tag_ids or [],
                            "slots": []
                        })["slots"].append({"start": str(start), "end": str(end), "service_duration": str(service_duration)})
                    # If is_closed = true, venue is completely unavailable (don't add to venue_dict)

                # Process recurring availability for venues without one-time entries
                for vuid, name, venue_unit_type, capacity, min_capacity, service_duration, start, end, va_availability_id, zone_tag_ids in recurring_venue_rows:
                    if venue_unit_type == "dining_table":
                        is_dining_table = True
                    zone_tags = resolve_tag_names(zone_tag_ids, venue_tags)
                    venue_dict.setdefault(vuid, {
                        "id": vuid,
                        "name": name,
                        "capacity": capacity,
                        "min_capacity": min_capacity,
                        "service": [svc for svc in venue_unit_services.get(vuid, []) if svc in location_services],
                        "zone_tags": zone_tags,
                        "zone_tag_ids": zone_tag_ids or [],
                        "slots": []
                    })["slots"].append({"start": str(start), "end": str(end), "service_duration": str(service_duration)})

                updated_venue_dict = reconstruct_venue_availability(bookings, venue_dict)
                venue_key_name = "tables" if is_dining_table else "venue_units"
                availability[venue_key_name] = list(updated_venue_dict.values())

                cur.execute("""
                    SELECT COUNT(*)
                    FROM location_availability
                    WHERE tenant_id = %s AND location_id = %s AND type = 'one_time'
                    AND specific_date = %s AND is_active = true AND is_closed = true
                """, (tenant_id, location_id, current_date_str))
                if cur.fetchone()[0] > 0:
                    availability["holiday"] = True
                    availability["is_open"] = False
                else:
                    cur.execute("""
                        SELECT start_time, end_time
                        FROM location_availability
                        WHERE tenant_id = %s AND location_id = %s AND is_active = true AND is_closed = false
                        AND ((type = 'recurring' AND day_of_week = %s) OR (type = 'one_time' AND specific_date = %s))
                        ORDER BY start_time
                    """, (tenant_id, location_id, db_day, current_date_str))
                    hours = cur.fetchall()
                    for s, e in hours:
                        availability["open_hours"].append({"start": s.strftime("%H:%M"), "end": e.strftime("%H:%M")})
                    if not availability["open_hours"]:
                        availability["is_open"] = False

                response["availabilities"].append(availability)

            chunk_start_date = start_date + timedelta(days=chunk_start)
            chunk_start_date_str = chunk_start_date.strftime("%Y-%m-%d")
            cache_key = f"availability:tenant_{tenant_id}:location_{location_id}:start_date_{chunk_start_date_str}"

            if not is_regen:
                prev_day = chunk_start_date - timedelta(days=1)
                prev_day_key = f"availability:tenant_{tenant_id}:location_{location_id}:start_date_{prev_day.strftime('%Y-%m-%d')}"
                deleted = valkey_client.delete(prev_day_key)
                if deleted:
                    logger.info(f"[LOCAL TEST] Deleted previous cache key: {prev_day_key}")
                else:
                    logger.info(f"[LOCAL TEST] No previous cache key to delete: {prev_day_key}")

            valkey_client.set(cache_key, json.dumps(response))
            logger.info(f"🗝️ [CACHE WRITE] Writing cache key: {cache_key}")

        cur.close()
        logger.info(f"[DEBUG] All chunks cached successfully for tenant={tenant_id}, location={location_id}")
        return {"status": "success"}

    except Exception as e:
        import traceback
        logger.error(f"[LOCAL TEST] Exception occurred: {e}")
        traceback.print_exc()
        return None
