from datetime import datetime, timezone
import pytz
import psycopg2
import os
from dotenv import load_dotenv
from tasks.availability import gen_availability, gen_availability_venue

# Load .env file if it exists (for local development)
load_dotenv()

def get_db_connection():
    try:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to database: {e}")
        raise

def fetch_jobs():
    jobs = []
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tenant_id, location_id, timezone, location_type
                FROM locations
                WHERE is_active = true
            """)
            for row in cur.fetchall():
                jobs.append({
                    "tenant_id": row[0],
                    "location_id": row[1],
                    "location_tz": row[2],
                    "location_type": row[3]
                })
        conn.close()
        return jobs
    except Exception as e:
        print(f"[ERROR] Failed to fetch jobs from database: {e}")
        return jobs

def run_all_jobs():
    jobs = fetch_jobs()
    
    # Print jobs to console
    print("[INFO] Retrieved jobs from database:")
    if jobs:
        for job in jobs:
            print(f"  - Tenant ID: {job['tenant_id']}, Location ID: {job['location_id']}, Timezone: {job['location_tz']}, Type: {job['location_type']}")
    else:
        print("  [WARN] No active jobs found in the database.")
    
    utc_now = datetime.now(timezone.utc)
    print(f"[INFO] UTC now: {utc_now.isoformat()}")

    for job in jobs:
        try:
            now_local = datetime.now(pytz.timezone(job["location_tz"]))
            if now_local.hour == 0 and now_local.minute == 0:
                print(f"[INFO] Generating availability for tenant {job['tenant_id']} location {job['location_id']} at {now_local}")
                if job["location_type"] == "rest":
                    gen_availability_venue.delay(job["tenant_id"], job["location_id"], job["location_tz"])
                else:
                    gen_availability.delay(job["tenant_id"], job["location_id"], job["location_tz"])
            else:
                print(f"[SKIP] It's not midnight in {job['location_tz']}. Local time is {now_local.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"[ERROR] Failed processing job for location {job.get('location_id')} (Tenant {job.get('tenant_id')}): {e}")

if __name__ == "__main__":
    run_all_jobs()