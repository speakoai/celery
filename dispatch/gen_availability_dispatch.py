# dispatch/gen_availability_dispatch.py

from datetime import datetime
import pytz
from tasks.availability import gen_availability

def run_all_jobs():
    jobs = [
        {"tenant_id": 1, "location_id": 1, "location_tz": "Australia/Sydney"},
        {"tenant_id": 1, "location_id": 2, "location_tz": "Australia/Sydney"},
        # Add more as needed
    ]

    for job in jobs:
        now_local = datetime.now(pytz.timezone(job["location_tz"]))
        if now_local.hour == 0:
            print(f"[INFO] Generating availability for tenant {job['tenant_id']} location {job['location_id']} at {now_local}")
            gen_availability(job["tenant_id"], job["location_id"], job["location_tz"])
        else:
            print(f"[SKIP] It's not midnight in {job['location_tz']}")

if __name__ == "__main__":
    run_all_jobs()
