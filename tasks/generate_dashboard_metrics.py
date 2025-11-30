"""
Generate aggregated dashboard metrics for tenants.

This task collects business operation data from the database and generates
a comprehensive JSON report including bookings, customers, calls, and trends.
"""

from dotenv import load_dotenv
load_dotenv()

from tasks.celery_app import app
from celery.utils.log import get_task_logger

import os
import psycopg2
import psycopg2.extras
import redis
import json
from datetime import datetime, timedelta
from decimal import Decimal

logger = get_task_logger(__name__)


# ============================================================================
# Database Connection
# ============================================================================

def get_db_connection():
    """Get PostgreSQL database connection."""
    try:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


# ============================================================================
# Metrics Collection Functions
# ============================================================================

def collect_summary_metrics(tenant_id, location_ids):
    """
    Collect summary metrics for all locations and by location.
    
    Uses optimized queries with FILTER clauses to minimize DB load.
    Calculates growth percentages by comparing current vs previous 30-day periods.
    
    Returns:
        dict: Summary section of the metrics JSON
    """
    logger.info(f"[Tenant {tenant_id}] Collecting summary metrics...")
    
    conn = get_db_connection()
    
    try:
        # Initialize structure
        summary = {
            "all_locations": {
                "bookings_last_30_days": 0,
                "bookings_growth_pct": 0.0,
                "customers_total": 0,
                "customers_growth_pct": 0.0,
                "calls_last_30_days": 0,
                "calls_growth_pct": 0.0,
                "avg_call_duration_seconds": 0,
                "call_duration_growth_pct": 0.0
            },
            "by_location": {}
        }
        
        # Get location names
        location_names = {}
        with conn.cursor() as cur:
            cur.execute("""
                SELECT location_id, name
                FROM locations
                WHERE tenant_id = %s
                  AND location_id = ANY(%s)
            """, (tenant_id, location_ids))
            
            for row in cur.fetchall():
                location_names[row[0]] = row[1]
        
        # Query 1: Bookings (current + previous 30 days)
        bookings_data = {}
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    location_id,
                    COUNT(*) FILTER (WHERE start_time >= NOW() - INTERVAL '30 days') as current_count,
                    COUNT(*) FILTER (WHERE start_time >= NOW() - INTERVAL '60 days' 
                                     AND start_time < NOW() - INTERVAL '30 days') as previous_count
                FROM bookings
                WHERE tenant_id = %s 
                  AND location_id = ANY(%s)
                  AND start_time >= NOW() - INTERVAL '60 days'
                  AND status = 'confirmed'
                GROUP BY location_id
            """, (tenant_id, location_ids))
            
            for row in cur.fetchall():
                loc_id = row[0]
                current = row[1] or 0
                previous = row[2] or 0
                growth = calculate_growth_pct(current, previous)
                
                bookings_data[loc_id] = {
                    "current": current,
                    "growth_pct": growth
                }
        
        # Query 2: Customers (tenant-level active customer count)
        total_customers = 0
        customers_growth_pct = 0.0
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE is_active = true) as current_total,
                    COUNT(*) FILTER (WHERE is_active = true AND created_at < NOW() - INTERVAL '30 days') as previous_total
                FROM customers
                WHERE tenant_id = %s
            """, (tenant_id,))
            
            row = cur.fetchone()
            if row:
                current = row[0] or 0
                previous = row[1] or 0
                total_customers = current
                customers_growth_pct = calculate_growth_pct(current, previous)
        
        # Query 3: Calls (current + previous 30 days with avg duration)
        calls_data = {}
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    location_id,
                    COUNT(*) FILTER (WHERE call_start_time >= NOW() - INTERVAL '30 days') as current_count,
                    COUNT(*) FILTER (WHERE call_start_time >= NOW() - INTERVAL '60 days' 
                                     AND call_start_time < NOW() - INTERVAL '30 days') as previous_count,
                    AVG(call_duration_secs) FILTER (WHERE call_start_time >= NOW() - INTERVAL '30 days') as current_avg_duration,
                    AVG(call_duration_secs) FILTER (WHERE call_start_time >= NOW() - INTERVAL '60 days' 
                                                    AND call_start_time < NOW() - INTERVAL '30 days') as previous_avg_duration
                FROM location_conversations
                WHERE tenant_id = %s 
                  AND location_id = ANY(%s)
                  AND call_start_time >= NOW() - INTERVAL '60 days'
                  AND call_successful = true
                GROUP BY location_id
            """, (tenant_id, location_ids))
            
            for row in cur.fetchall():
                loc_id = row[0]
                current_count = row[1] or 0
                previous_count = row[2] or 0
                current_avg = int(row[3]) if row[3] else 0
                previous_avg = int(row[4]) if row[4] else 0
                
                calls_growth = calculate_growth_pct(current_count, previous_count)
                duration_growth = calculate_growth_pct(current_avg, previous_avg)
                
                calls_data[loc_id] = {
                    "current_count": current_count,
                    "calls_growth_pct": calls_growth,
                    "current_avg_duration": current_avg,
                    "duration_growth_pct": duration_growth
                }
        
        # Aggregate by location
        for loc_id in location_ids:
            bookings = bookings_data.get(loc_id, {"current": 0, "growth_pct": 0.0})
            calls = calls_data.get(loc_id, {
                "current_count": 0,
                "calls_growth_pct": 0.0,
                "current_avg_duration": 0,
                "duration_growth_pct": 0.0
            })
            
            summary["by_location"][str(loc_id)] = {
                "location_name": location_names.get(loc_id, f"Location {loc_id}"),
                "bookings_last_30_days": bookings["current"],
                "bookings_growth_pct": bookings["growth_pct"],
                "calls_last_30_days": calls["current_count"],
                "calls_growth_pct": calls["calls_growth_pct"],
                "avg_call_duration_seconds": calls["current_avg_duration"],
                "call_duration_growth_pct": calls["duration_growth_pct"]
            }
        
        # Aggregate all_locations totals
        summary["all_locations"]["bookings_last_30_days"] = sum(
            loc["bookings_last_30_days"] for loc in summary["by_location"].values()
        )
        summary["all_locations"]["customers_total"] = total_customers
        summary["all_locations"]["customers_growth_pct"] = customers_growth_pct
        summary["all_locations"]["calls_last_30_days"] = sum(
            loc["calls_last_30_days"] for loc in summary["by_location"].values()
        )
        
        # Calculate weighted average call duration
        total_calls = summary["all_locations"]["calls_last_30_days"]
        if total_calls > 0:
            weighted_duration = sum(
                loc["avg_call_duration_seconds"] * loc["calls_last_30_days"]
                for loc in summary["by_location"].values()
            )
            summary["all_locations"]["avg_call_duration_seconds"] = int(weighted_duration / total_calls)
        
        # Calculate all_locations growth percentages by reverse-deriving previous period values
        all_bookings_prev_sum = 0
        all_customers_prev_sum = 0
        all_calls_prev_sum = 0
        all_duration_prev_sum = 0
        
        for loc_id in location_ids:
            # Bookings: Derive previous value from current and growth%
            b = bookings_data.get(loc_id, {"current": 0, "growth_pct": 0.0})
            if b["growth_pct"] != 0 and b["growth_pct"] != -100:
                # previous = current / (1 + growth% / 100)
                all_bookings_prev_sum += int(b["current"] / (1 + b["growth_pct"] / 100))
            elif b["growth_pct"] == -100:
                # Special case: went from some value to 0, can't reverse calculate
                # Use 1 as minimum to avoid complete loss of signal
                all_bookings_prev_sum += max(1, b["current"])
            else:
                # No growth, previous = current
                all_bookings_prev_sum += b["current"]
            

            
            # Calls
            ca = calls_data.get(loc_id, {"current_count": 0, "calls_growth_pct": 0.0})
            if ca["calls_growth_pct"] != 0 and ca["calls_growth_pct"] != -100:
                all_calls_prev_sum += int(ca["current_count"] / (1 + ca["calls_growth_pct"] / 100))
            elif ca["calls_growth_pct"] == -100:
                all_calls_prev_sum += max(1, ca["current_count"])
            else:
                all_calls_prev_sum += ca["current_count"]
            
            # Call Duration
            cd = calls_data.get(loc_id, {"current_avg_duration": 0, "duration_growth_pct": 0.0})
            if cd["duration_growth_pct"] != 0 and cd["duration_growth_pct"] != -100:
                all_duration_prev_sum += int(cd["current_avg_duration"] / (1 + cd["duration_growth_pct"] / 100))
            elif cd["duration_growth_pct"] == -100:
                all_duration_prev_sum += max(1, cd["current_avg_duration"])
            else:
                all_duration_prev_sum += cd["current_avg_duration"]
        
        summary["all_locations"]["bookings_growth_pct"] = calculate_growth_pct(
            summary["all_locations"]["bookings_last_30_days"], 
            all_bookings_prev_sum
        )
        summary["all_locations"]["customers_growth_pct"] = calculate_growth_pct(
            summary["all_locations"]["customers_total"], 
            all_customers_prev_sum
        )
        summary["all_locations"]["calls_growth_pct"] = calculate_growth_pct(
            summary["all_locations"]["calls_last_30_days"], 
            all_calls_prev_sum
        )
        summary["all_locations"]["call_duration_growth_pct"] = calculate_growth_pct(
            summary["all_locations"]["avg_call_duration_seconds"], 
            all_duration_prev_sum
        )
        
        logger.info(f"[Tenant {tenant_id}] ✓ Summary metrics collected")
        return summary
        
    except Exception as e:
        logger.error(f"[Tenant {tenant_id}] Failed to collect summary metrics: {e}")
        raise
    finally:
        conn.close()


def get_redis_client():
    """Get Redis client connection."""
    redis_url = os.environ.get('REDIS_URL')
    if not redis_url:
        raise ValueError("REDIS_URL environment variable not set")
    return redis.from_url(redis_url, decode_responses=True)


def get_cached_trends(tenant_id):
    """
    Retrieve cached trends data from Redis.
    
    Args:
        tenant_id (int): Tenant ID
        
    Returns:
        tuple: (trends_data dict, last_update_date str) or (None, None) if not cached
    """
    try:
        redis_client = get_redis_client()
        
        # Get cached data
        cache_key = f"dashboard_metrics:{tenant_id}:trends_data"
        last_update_key = f"dashboard_metrics:{tenant_id}:last_update"
        
        cached_json = redis_client.get(cache_key)
        last_update = redis_client.get(last_update_key)
        
        if cached_json and last_update:
            trends_data = json.loads(str(cached_json))
            logger.info(f"[Tenant {tenant_id}] Found cached trends data (last update: {last_update})")
            return trends_data, str(last_update)
        
        return None, None
        
    except Exception as e:
        logger.warning(f"[Tenant {tenant_id}] Failed to get cached trends: {e}")
        return None, None


def save_trends_cache(tenant_id, trends_data):
    """
    Save trends data to Redis cache.
    
    Args:
        tenant_id (int): Tenant ID
        trends_data (dict): Complete trends data structure
    """
    try:
        redis_client = get_redis_client()
        today_str = datetime.now().date().strftime("%Y-%m-%d")
        
        cache_key = f"dashboard_metrics:{tenant_id}:trends_data"
        last_update_key = f"dashboard_metrics:{tenant_id}:last_update"
        
        # Save data with 95-day TTL
        redis_client.setex(cache_key, 95 * 24 * 60 * 60, json.dumps(trends_data))
        redis_client.setex(last_update_key, 95 * 24 * 60 * 60, today_str)
        
        logger.info(f"[Tenant {tenant_id}] Cached trends data (date: {today_str})")
        
    except Exception as e:
        logger.warning(f"[Tenant {tenant_id}] Failed to cache trends: {e}")


def collect_trends(tenant_id, location_ids, location_names):
    """
    Collect booking trends for 7/30/90 day time windows.
    
    Uses incremental updates with Redis caching:
    - First run: Full 90-day query
    - Subsequent runs: Only query yesterday's data, append to cache
    - Slides the 90-day window automatically
    
    Separates bookings by source: AI calls (voice-ai) vs Web (web/dashboard).
    
    Args:
        tenant_id (int): Tenant ID
        location_ids (list): List of location IDs
        location_names (dict): Map of location_id to location name
    
    Returns:
        dict: Trends section of the metrics JSON
    """
    logger.info(f"[Tenant {tenant_id}] Collecting booking trends...")
    
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    # Try to get cached data
    cached_trends, last_update_str = get_cached_trends(tenant_id)
    
    # Check if we can use incremental update
    if cached_trends and last_update_str:
        last_update_date = datetime.strptime(last_update_str, "%Y-%m-%d").date()
        
        # If already updated today, return cached data
        if last_update_date >= today:
            logger.info(f"[Tenant {tenant_id}] Using today's cached trends data")
            return cached_trends
        
        # If updated yesterday, do incremental update
        if last_update_date == yesterday:
            logger.info(f"[Tenant {tenant_id}] Performing incremental update (adding today's data)")
            updated_trends = incremental_trends_update(
                tenant_id, location_ids, location_names, cached_trends, today
            )
            save_trends_cache(tenant_id, updated_trends)
            return updated_trends
    
    # Cache miss or too old - do full 90-day query
    logger.info(f"[Tenant {tenant_id}] Cache miss or stale - performing full 90-day query")
    full_trends = full_trends_query(tenant_id, location_ids, location_names)
    save_trends_cache(tenant_id, full_trends)
    return full_trends


def incremental_trends_update(tenant_id, location_ids, location_names, cached_trends, today):
    """
    Update trends with yesterday's data only.
    
    Args:
        tenant_id (int): Tenant ID
        location_ids (list): List of location IDs
        location_names (dict): Map of location_id to location name
        cached_trends (dict): Existing cached trends data
        today (date): Today's date
        
    Returns:
        dict: Updated trends data
    """
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    
    conn = get_db_connection()
    
    try:
        # Query only yesterday's data (using location timezone for date extraction)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    b.location_id,
                    COUNT(*) FILTER (WHERE b.source = 'voice-ai') as ai_count,
                    COUNT(*) FILTER (WHERE b.source IN ('web', 'dashboard', 'onboarding')) as web_count
                FROM bookings b
                WHERE b.tenant_id = %s
                  AND b.location_id = ANY(%s)
                  AND DATE(b.start_time) = %s
                  AND b.status = 'confirmed'
                GROUP BY b.location_id
            """, (tenant_id, location_ids, yesterday))
            
            yesterday_data = {}
            for row in cur.fetchall():
                loc_id = str(row[0])
                yesterday_data[loc_id] = {
                    "ai": row[1] or 0,
                    "web": row[2] or 0
                }
        
        # Update each location's data
        for loc_id_str in cached_trends["by_location"].keys():
            loc_data = cached_trends["by_location"][loc_id_str]
            
            # Get yesterday's counts (0 if no bookings)
            new_ai = yesterday_data.get(loc_id_str, {}).get("ai", 0)
            new_web = yesterday_data.get(loc_id_str, {}).get("web", 0)
            
            # Update 90_days data (append + slide window)
            loc_data["90_days"]["dates"].append(yesterday_str)
            loc_data["90_days"]["dates"].pop(0)  # Remove oldest day
            
            loc_data["90_days"]["bookings_ai"].append(new_ai)
            loc_data["90_days"]["bookings_ai"].pop(0)
            
            loc_data["90_days"]["bookings_web"].append(new_web)
            loc_data["90_days"]["bookings_web"].pop(0)
            
            # Update 30_days and 7_days (slices from 90_days)
            loc_data["30_days"]["dates"] = loc_data["90_days"]["dates"][-30:]
            loc_data["30_days"]["bookings_ai"] = loc_data["90_days"]["bookings_ai"][-30:]
            loc_data["30_days"]["bookings_web"] = loc_data["90_days"]["bookings_web"][-30:]
            
            loc_data["7_days"]["dates"] = loc_data["90_days"]["dates"][-7:]
            loc_data["7_days"]["bookings_ai"] = loc_data["90_days"]["bookings_ai"][-7:]
            loc_data["7_days"]["bookings_web"] = loc_data["90_days"]["bookings_web"][-7:]
        
        # Handle new locations that weren't in cache
        for loc_id in location_ids:
            loc_id_str = str(loc_id)
            if loc_id_str not in cached_trends["by_location"]:
                logger.info(f"[Tenant {tenant_id}] New location {loc_id} detected - adding to trends")
                # For new locations, we need to backfill - use full query
                conn_backfill = get_db_connection()
                try:
                    with conn_backfill.cursor() as cur:
                        cur.execute("""
                            SELECT 
                                DATE(b.start_time) as booking_date,
                                COUNT(*) FILTER (WHERE b.source = 'voice-ai') as ai_count,
                                COUNT(*) FILTER (WHERE b.source IN ('web', 'dashboard', 'onboarding')) as web_count
                            FROM bookings b
                            WHERE b.tenant_id = %s
                              AND b.location_id = %s
                              AND b.start_time >= CURRENT_DATE - INTERVAL '89 days'
                              AND b.status = 'confirmed'
                            GROUP BY DATE(b.start_time)
                        """, (tenant_id, loc_id))
                        
                        # Build 90-day arrays
                        date_range_90 = [(today - timedelta(days=i)) for i in range(89, -1, -1)]
                        date_strings_90 = [d.strftime("%Y-%m-%d") for d in date_range_90]
                        bookings_ai_90 = [0] * 90
                        bookings_web_90 = [0] * 90
                        
                        for row in cur.fetchall():
                            date_str = row[0].strftime("%Y-%m-%d")
                            if date_str in date_strings_90:
                                idx = date_strings_90.index(date_str)
                                bookings_ai_90[idx] = row[1] or 0
                                bookings_web_90[idx] = row[2] or 0
                        
                        cached_trends["by_location"][loc_id_str] = {
                            "location_name": location_names.get(loc_id, f"Location {loc_id}"),
                            "7_days": {
                                "dates": date_strings_90[-7:],
                                "bookings_ai": bookings_ai_90[-7:],
                                "bookings_web": bookings_web_90[-7:]
                            },
                            "30_days": {
                                "dates": date_strings_90[-30:],
                                "bookings_ai": bookings_ai_90[-30:],
                                "bookings_web": bookings_web_90[-30:]
                            },
                            "90_days": {
                                "dates": date_strings_90,
                                "bookings_ai": bookings_ai_90,
                                "bookings_web": bookings_web_90
                            }
                        }
                finally:
                    conn_backfill.close()
        
        # Recalculate all_locations aggregation
        all_locations_ai_90 = [0] * 90
        all_locations_web_90 = [0] * 90
        
        for loc_data in cached_trends["by_location"].values():
            for i in range(90):
                all_locations_ai_90[i] += loc_data["90_days"]["bookings_ai"][i]
                all_locations_web_90[i] += loc_data["90_days"]["bookings_web"][i]
        
        # Update all_locations
        cached_trends["all_locations"]["90_days"]["bookings_ai"] = all_locations_ai_90
        cached_trends["all_locations"]["90_days"]["bookings_web"] = all_locations_web_90
        
        cached_trends["all_locations"]["30_days"]["dates"] = cached_trends["all_locations"]["90_days"]["dates"][-30:]
        cached_trends["all_locations"]["30_days"]["bookings_ai"] = all_locations_ai_90[-30:]
        cached_trends["all_locations"]["30_days"]["bookings_web"] = all_locations_web_90[-30:]
        
        cached_trends["all_locations"]["7_days"]["dates"] = cached_trends["all_locations"]["90_days"]["dates"][-7:]
        cached_trends["all_locations"]["7_days"]["bookings_ai"] = all_locations_ai_90[-7:]
        cached_trends["all_locations"]["7_days"]["bookings_web"] = all_locations_web_90[-7:]
        
        logger.info(f"[Tenant {tenant_id}] ✓ Incremental update complete")
        return cached_trends
        
    except Exception as e:
        logger.error(f"[Tenant {tenant_id}] Incremental update failed: {e}")
        raise
    finally:
        conn.close()


def full_trends_query(tenant_id, location_ids, location_names):
    """
    Perform full 90-day trends query (bootstrap or cache miss).
    
    Args:
        tenant_id (int): Tenant ID
        location_ids (list): List of location IDs
        location_names (dict): Map of location_id to location name
        
    Returns:
        dict: Complete trends data structure
    """
    conn = get_db_connection()
    
    try:
        # Generate 90-day date range (today back to day -89)
        today = datetime.now().date()
        date_range_90 = [(today - timedelta(days=i)) for i in range(89, -1, -1)]
        date_strings_90 = [d.strftime("%Y-%m-%d") for d in date_range_90]
        
        # Initialize data structure for all locations
        location_data = {}
        for loc_id in location_ids:
            location_data[loc_id] = {
                "dates": date_strings_90.copy(),
                "bookings_ai": [0] * 90,
                "bookings_web": [0] * 90
            }
        
        # Query: Get 90 days of booking data by source (using location timezone for date extraction)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    b.location_id,
                    DATE(b.start_time) as booking_date,
                    COUNT(*) FILTER (WHERE b.source = 'voice-ai') as ai_count,
                    COUNT(*) FILTER (WHERE b.source IN ('web', 'dashboard', 'onboarding')) as web_count
                FROM bookings b
                WHERE b.tenant_id = %s
                  AND b.location_id = ANY(%s)
                  AND b.start_time >= CURRENT_DATE - INTERVAL '89 days'
                  AND b.status = 'confirmed'
                GROUP BY b.location_id, DATE(b.start_time)
                ORDER BY booking_date
            """, (tenant_id, location_ids))
            
            # Fill in the data from query results
            for row in cur.fetchall():
                loc_id = row[0]
                booking_date = row[1]
                ai_count = row[2] or 0
                web_count = row[3] or 0
                
                # Find index in date array
                date_str = booking_date.strftime("%Y-%m-%d")
                if date_str in date_strings_90:
                    idx = date_strings_90.index(date_str)
                    location_data[loc_id]["bookings_ai"][idx] = ai_count
                    location_data[loc_id]["bookings_web"][idx] = web_count
        
        # Build trends structure with 7/30/90 day windows
        trends = {
            "all_locations": {},
            "by_location": {}
        }
        
        # Aggregate all_locations data
        all_locations_ai_90 = [0] * 90
        all_locations_web_90 = [0] * 90
        
        for loc_id in location_ids:
            for i in range(90):
                all_locations_ai_90[i] += location_data[loc_id]["bookings_ai"][i]
                all_locations_web_90[i] += location_data[loc_id]["bookings_web"][i]
        
        # Create time windows for all_locations
        trends["all_locations"]["7_days"] = {
            "dates": date_strings_90[-7:],
            "bookings_ai": all_locations_ai_90[-7:],
            "bookings_web": all_locations_web_90[-7:]
        }
        trends["all_locations"]["30_days"] = {
            "dates": date_strings_90[-30:],
            "bookings_ai": all_locations_ai_90[-30:],
            "bookings_web": all_locations_web_90[-30:]
        }
        trends["all_locations"]["90_days"] = {
            "dates": date_strings_90,
            "bookings_ai": all_locations_ai_90,
            "bookings_web": all_locations_web_90
        }
        
        # Create time windows for each location
        for loc_id in location_ids:
            loc_data = location_data[loc_id]
            
            trends["by_location"][str(loc_id)] = {
                "location_name": location_names.get(loc_id, f"Location {loc_id}"),
                "7_days": {
                    "dates": loc_data["dates"][-7:],
                    "bookings_ai": loc_data["bookings_ai"][-7:],
                    "bookings_web": loc_data["bookings_web"][-7:]
                },
                "30_days": {
                    "dates": loc_data["dates"][-30:],
                    "bookings_ai": loc_data["bookings_ai"][-30:],
                    "bookings_web": loc_data["bookings_web"][-30:]
                },
                "90_days": {
                    "dates": loc_data["dates"],
                    "bookings_ai": loc_data["bookings_ai"],
                    "bookings_web": loc_data["bookings_web"]
                }
            }
        
        logger.info(f"[Tenant {tenant_id}] ✓ Booking trends collected")
        return trends
        
    except Exception as e:
        logger.error(f"[Tenant {tenant_id}] Failed to collect booking trends: {e}")
        raise
    finally:
        conn.close()





# ============================================================================
# Metrics Storage
# ============================================================================

def save_metrics_to_database(tenant_id, metrics_json):
    """
    Save or update the aggregated metrics in the database.
    
    Uses INSERT ... ON CONFLICT to either insert new record or update existing.
    
    Args:
        tenant_id (int): Tenant ID
        metrics_json (dict): The complete metrics dictionary
    """
    logger.info(f"[Tenant {tenant_id}] Saving metrics to database...")
    
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tenant_aggregated_metrics 
                    (tenant_id, generated_at, metrics, metrics_version)
                VALUES 
                    (%s, %s, %s, %s)
                ON CONFLICT (tenant_id) 
                DO UPDATE SET
                    generated_at = EXCLUDED.generated_at,
                    metrics = EXCLUDED.metrics,
                    metrics_version = EXCLUDED.metrics_version
            """, (
                tenant_id,
                datetime.now(),
                json.dumps(metrics_json),
                'v1'
            ))
            
            conn.commit()
            logger.info(f"[Tenant {tenant_id}] ✓ Metrics saved successfully")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"[Tenant {tenant_id}] Failed to save metrics: {e}")
        raise
    finally:
        conn.close()


# ============================================================================
# Helper Functions
# ============================================================================

def get_active_locations_for_tenant(tenant_id):
    """
    Get all active location IDs for a tenant.
    
    Args:
        tenant_id (int): Tenant ID
        
    Returns:
        list: List of location IDs
    """
    conn = get_db_connection()
    location_ids = []
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT location_id
                FROM locations
                WHERE tenant_id = %s
                  AND is_active = true
                ORDER BY location_id
            """, (tenant_id,))
            
            location_ids = [row[0] for row in cur.fetchall()]
            
    except Exception as e:
        logger.error(f"[Tenant {tenant_id}] Failed to fetch locations: {e}")
        raise
    finally:
        conn.close()
    
    return location_ids


def calculate_growth_pct(current, previous):
    """
    Calculate percentage growth between two values.
    
    Args:
        current: Current period value
        previous: Previous period value
        
    Returns:
        float: Growth percentage rounded to 1 decimal place
               Returns 0.0 if previous is 0 or None
    """
    if not previous or previous == 0:
        return 0.0
    
    growth = ((current - previous) / previous) * 100
    return round(growth, 1)


def generate_month_labels(num_months=12):
    """
    Generate month labels for the last N months in YYYY-MM format.
    
    Args:
        num_months (int): Number of months to generate
        
    Returns:
        list: List of month strings like ["2023-09", "2023-10", ...]
    """
    today = datetime.now()
    months = []
    
    for i in range(num_months - 1, -1, -1):
        # Go back i months from today
        month_date = today.replace(day=1) - timedelta(days=i * 30)
        # Adjust to exact month
        if i > 0:
            year = today.year
            month = today.month - i
            if month <= 0:
                year -= 1
                month += 12
            month_date = month_date.replace(year=year, month=month, day=1)
        
        months.append(month_date.strftime("%Y-%m"))
    
    return months


# ============================================================================
# Main Celery Task
# ============================================================================

@app.task(bind=True)
def generate_dashboard_metrics_for_tenant(self, tenant_id):
    """
    Generate aggregated dashboard metrics for a single tenant.
    
    This task:
    1. Fetches all active locations for the tenant
    2. Collects summary metrics (bookings, customers, calls)
    3. Collects 12-month trends for bookings
    4. Collects call statistics
    5. Saves the complete JSON to tenant_aggregated_metrics table
    
    Args:
        tenant_id (int): The tenant ID to generate metrics for
        
    Returns:
        dict: Status information with tenant_id and generated_at timestamp
    """
    logger.info("=" * 80)
    logger.info(f"[TASK START] Generate Dashboard Metrics - Tenant {tenant_id}")
    logger.info(f"[TASK ID] {self.request.id}")
    logger.info("=" * 80)
    
    try:
        # Step 1: Get active locations for tenant
        location_ids = get_active_locations_for_tenant(tenant_id)
        
        if not location_ids:
            logger.warning(f"[Tenant {tenant_id}] No active locations found")
            return {
                "status": "skipped",
                "tenant_id": tenant_id,
                "reason": "no_active_locations"
            }
        
        logger.info(f"[Tenant {tenant_id}] Found {len(location_ids)} active locations: {location_ids}")
        
        # Get location names
        location_names = {}
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT location_id, name
                    FROM locations
                    WHERE tenant_id = %s
                      AND location_id = ANY(%s)
                """, (tenant_id, location_ids))
                
                for row in cur.fetchall():
                    location_names[row[0]] = row[1]
        finally:
            conn.close()
        
        # Step 2: Collect all metrics
        summary = collect_summary_metrics(tenant_id, location_ids)
        trends = collect_trends(tenant_id, location_ids, location_names)
        
        # Step 3: Assemble complete metrics JSON
        metrics = {
            "summary": summary,
            "trends": trends
        }
        
        logger.info(f"[Tenant {tenant_id}] Metrics collection complete")
        
        # Step 4: Save to database
        save_metrics_to_database(tenant_id, metrics)
        
        # Step 5: Return success
        result = {
            "status": "success",
            "tenant_id": tenant_id,
            "generated_at": datetime.now().isoformat(),
            "locations_processed": len(location_ids),
            "location_ids": location_ids
        }
        
        logger.info("=" * 80)
        logger.info(f"[TASK COMPLETE] Tenant {tenant_id} - Metrics generated successfully")
        logger.info("=" * 80)
        
        return result
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"[TASK FAILED] Tenant {tenant_id}: {e}")
        logger.error("=" * 80)
        
        # Re-raise so Celery marks task as failed
        raise
