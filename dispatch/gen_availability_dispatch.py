from datetime import datetime, timezone
import pytz
from tasks.availability import gen_availability, gen_availability_venue

def run_all_jobs():
    jobs = [
        {"tenant_id": 1, "location_id": 1, "location_tz": "Australia/Sydney", "location_type": "nail"},
        {"tenant_id": 1, "location_id": 2, "location_tz": "Australia/Sydney", "location_type": "nail"},
        {"tenant_id": 2, "location_id": 34, "location_tz": "Australia/Sydney", "location_type": "rest"},
        {"tenant_id": 2, "location_id": 35, "location_tz": "Australia/Sydney", "location_type": "rest"},
        # Add more as needed
    ]

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
