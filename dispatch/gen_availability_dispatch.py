# dispatch/gen_availability_dispatch.py

from tasks.availability import gen_availability

def run_all_jobs():
    jobs = [
        {"tenant_id": 1, "location_id": 1, "location_tz": "Australia/Sydney"},
        {"tenant_id": 1, "location_id": 2, "location_tz": "Australia/Sydney"},
    ]

    for job in jobs:
        print(f"[INFO] Generating availability for tenant {job['tenant_id']} location {job['location_id']}")
        gen_availability(job["tenant_id"], job["location_id"], job["location_tz"])

if __name__ == "__main__":
    run_all_jobs()
