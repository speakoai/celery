import redis
import json

# Redis connection (using external secure URL)
redis_url = "rediss://red-d09kn9ruibrs73fdup7g:kkaZuYXkGcPFwRwOsmipuyUw5cEsiLgt@virginia-keyvalue.render.com:6379"
r = redis.Redis.from_url(redis_url, decode_responses=True)

# Redis key
key = "availability:tenant_1:location_1"

# Target filters
target_date = "2025-05-10"
target_staff_id = 2
new_slots = [{"start": "09:00:00", "end": "18:00:00"}]  # Replace with your desired new slot(s)

# Load JSON from Redis
raw_json = r.get(key)
if raw_json is None:
    raise ValueError(f"No data found for key: {key}")

data = json.loads(raw_json)

# Locate matching date and staff
updated = False
for availability in data.get("availabilities", []):
    if availability.get("date") == target_date:
        for staff in availability.get("staff", []):
            if staff.get("id") == target_staff_id:
                print(f"[BEFORE] {staff['name']} on {target_date}: {staff['slots']}")
                staff["slots"] = new_slots
                print(f"[AFTER]  {staff['name']} on {target_date}: {staff['slots']}")
                updated = True

if updated:
    r.set(key, json.dumps(data))
    print("✅ Redis data updated successfully.")
else:
    print(f"❌ No staff found with id={target_staff_id} on date={target_date}")
