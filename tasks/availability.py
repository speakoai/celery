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
import time


print(f"[DEBUG] DATABASE_URL: {os.getenv('DATABASE_URL')}")
print(f"[DEBUG] REDIS_URL: {os.getenv('REDIS_URL')}")

load_dotenv()
logger = get_task_logger(__name__)

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
def gen_availability(tenant_id, location_id, location_tz="UTC"):
    logger.info(f"[LOCAL TEST] Generating availability for tenant={tenant_id}, location={location_id}")

    db_url = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")

    if not db_url or not redis_url:
        logger.error("Missing DATABASE_URL or REDIS_URL in .env")
        return

    try:

        pg_conn = psycopg2.connect(db_url)
        print("✅ Connected to PostgreSQL")
        print(f"🔍 Using DB URL: {os.getenv('DATABASE_URL')}")

        valkey_client = redis.Redis.from_url(redis_url, decode_responses=True)
        print("[DEBUG] Connected to Redis")

        cur = pg_conn.cursor()
        db_start = time.time() #record the current time of db_start for benchmarking
        start_date = datetime.now(ZoneInfo(location_tz)).replace(hour=0, minute=0, second=0, microsecond=0)
        days_range = 60

        response = {
            "tenant_id": tenant_id,
            "location_id": location_id,
            "services": [],
            "availabilities": []
        }

        cur.execute("""
            SELECT s.service_id, s.name, EXTRACT(EPOCH FROM s.duration)/60
            FROM location_services ls
            JOIN services s ON ls.tenant_id = s.tenant_id AND ls.service_id = s.service_id
            WHERE ls.tenant_id = %s AND ls.location_id = %s
            ORDER BY s.service_id
        """, (tenant_id, location_id))
        for row in cur.fetchall():
            response["services"].append({
                "id": row[0],
                "name": row[1],
                "duration": int(row[2])
            })

        staff_services, location_services = {}, set()
        for day_offset in range(days_range):
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

            cur.execute("""
                SELECT s.staff_id, s.name, sa.start_time, sa.end_time
                FROM staff s
                JOIN staff_availability sa ON s.tenant_id = sa.tenant_id AND s.staff_id = sa.staff_id
                WHERE s.tenant_id = %s AND sa.location_id = %s AND sa.type = 'recurring'
                AND sa.day_of_week = %s AND (sa.specific_date IS NULL OR sa.specific_date <> %s)
                AND sa.is_active = TRUE
            """, (tenant_id, location_id, db_day, current_date_str))
            staff_rows = cur.fetchall()

            if day_offset == 0:
                cur.execute("SELECT staff_id, service_id FROM staff_services WHERE tenant_id = %s", (tenant_id,))
                for sid, svc_id in cur.fetchall():
                    staff_services.setdefault(sid, []).append(svc_id)

                cur.execute("SELECT service_id FROM location_services WHERE tenant_id = %s AND location_id = %s", (tenant_id, location_id))
                location_services = {r[0] for r in cur.fetchall()}

            cur.execute("""
                SELECT staff_id, customer_id, start_time, end_time
                FROM bookings
                WHERE tenant_id = %s AND location_id = %s
                AND start_time >= %s AND start_time < %s::date + INTERVAL '1 day'
            """, (tenant_id, location_id, current_date_str, current_date_str))
            booking_rows = cur.fetchall()
            bookings = [{"staff_id": r[0], "customer_id": r[1], "start_time": r[2].strftime("%Y-%m-%d %H:%M:%S"), "end_time": r[3].strftime("%Y-%m-%d %H:%M:%S")} for r in booking_rows]

            staff_dict = {}
            for sid, name, start, end in staff_rows:
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

        cache_key = f"availability:tenant_{tenant_id}:location_{location_id}"
        valkey_client.set(cache_key, json.dumps(response))
        logger.info(f"[LOCAL TEST] Cached key: {cache_key}")
        cur.close()

        db_end = time.time() #record the current time of db_end for benchmarking
        print(f"[INFO] DB fetch duration: {db_end - db_start:.2f}s") #print the time difference

        print(f"[DEBUG] JSON generated and cached for tenant_id={tenant_id}, location_id={location_id}")

        return response

    except Exception as e:
        import traceback
        logger.error(f"[LOCAL TEST] Exception occurred: {e}")
        traceback.print_exc()
        return None

@app.task
def gen_availability_venue(tenant_id, location_id, location_tz="UTC"):
    logger.info(f"[LOCAL TEST] Generating availability for tenant={tenant_id}, location={location_id}")

    db_url = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")

    if not db_url or not redis_url:
        logger.error("Missing DATABASE_URL or REDIS_URL in .env")
        return

    try:
        pg_conn = psycopg2.connect(db_url)
        print("✅ Connected to PostgreSQL")
        print(f"🔍 Using DB URL: {os.getenv('DATABASE_URL')}")

        valkey_client = redis.Redis.from_url(redis_url, decode_responses=True)
        print("[DEBUG] Connected to Redis")

        cur = pg_conn.cursor()
        db_start = time.time()
        start_date = datetime.now(ZoneInfo(location_tz)).replace(hour=0, minute=0, second=0, microsecond=0)
        days_range = 60

        response = {
            "tenant_id": tenant_id,
            "location_id": location_id,
            "services": [],
            "availabilities": []
        }

        cur.execute("""
            SELECT s.service_id, s.name, EXTRACT(EPOCH FROM s.duration)/60
            FROM location_services ls
            JOIN services s ON ls.tenant_id = s.tenant_id AND ls.service_id = s.service_id
            WHERE ls.tenant_id = %s AND ls.location_id = %s
            ORDER BY s.service_id
        """, (tenant_id, location_id))
        for row in cur.fetchall():
            response["services"].append({
                "id": row[0],
                "name": row[1],
                "duration": int(row[2])
            })

        venue_unit_services, location_services = {}, set()
        for day_offset in range(days_range):
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

            cur.execute("""
                SELECT vu.venue_unit_id, vu.name, vu.venue_unit_type, vu.capacity, va.start_time, va.end_time
                FROM venue_unit vu
                JOIN venue_availability va ON vu.tenant_id = va.tenant_id AND vu.venue_unit_id = va.venue_unit_id
                WHERE vu.tenant_id = %s AND va.location_id = %s AND va.type = 'recurring'
                AND va.day_of_week = %s AND (va.specific_date IS NULL OR va.specific_date <> %s)
                AND va.is_active = TRUE
            """, (tenant_id, location_id, db_day, current_date_str))
            venue_rows = cur.fetchall()

            if day_offset == 0:
                cur.execute("SELECT venue_unit_id, service_id FROM venue_unit_services WHERE tenant_id = %s", (tenant_id,))
                for vuid, svc_id in cur.fetchall():
                    venue_unit_services.setdefault(vuid, []).append(svc_id)

                cur.execute("SELECT service_id FROM location_services WHERE tenant_id = %s AND location_id = %s", (tenant_id, location_id))
                location_services = {r[0] for r in cur.fetchall()}

            cur.execute("""
                SELECT venue_unit_id, customer_id, start_time, end_time
                FROM bookings
                WHERE tenant_id = %s AND location_id = %s
                AND start_time >= %s AND start_time < %s::date + INTERVAL '1 day'
            """, (tenant_id, location_id, current_date_str, current_date_str))
            booking_rows = cur.fetchall()
            bookings = [{"venue_unit_id": r[0], "customer_id": r[1], "start_time": r[2].strftime("%Y-%m-%d %H:%M:%S"), "end_time": r[3].strftime("%Y-%m-%d %H:%M:%S")} for r in booking_rows]

            venue_dict = {}
            is_dining_table = False

            for vuid, name, venue_unit_type, capacity, start, end in venue_rows:
                if venue_unit_type == "dining_table":
                    is_dining_table = True
                venue_dict.setdefault(vuid, {
                    "id": vuid,
                    "name": name,
                    "capacity": capacity,
                    "service": [svc for svc in venue_unit_services.get(vuid, []) if svc in location_services],
                    "slots": []
                })["slots"].append({"start": str(start), "end": str(end)})

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

        cache_key = f"availability:tenant_{tenant_id}:location_{location_id}"
        valkey_client.set(cache_key, json.dumps(response))
        logger.info(f"[LOCAL TEST] Cached key: {cache_key}")
        cur.close()

        db_end = time.time()
        print(f"[INFO] DB fetch duration: {db_end - db_start:.2f}s")
        print(f"[DEBUG] JSON generated and cached for tenant_id={tenant_id}, location_id={location_id}")

        return response

    except Exception as e:
        import traceback
        logger.error(f"[LOCAL TEST] Exception occurred: {e}")
        traceback.print_exc()
        return None
