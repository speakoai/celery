import redis
import json

# Redis configuration
redis_url = "rediss://red-d09kn9ruibrs73fdup7g:kkaZuYXkGcPFwRwOsmipuyUw5cEsiLgt@virginia-keyvalue.render.com:6379"
key = "availability:tenant_1:location_1"
target_date = "2025-05-10"
target_staff_id = 2
new_slots = [{"start": "11:00:00", "end": "18:00:00"}]

try:
    # Connect to Redis
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    print("✅ Connected to Redis")

    # Fetch data from Redis
    raw_json = r.get(key)
    if raw_json is None:
        print(f"❌ No data found for key: {key}")
    else:
        data = json.loads(raw_json)

        # Modify slots for the specific date + staff ID
        updated = False
        for availability in data.get("availabilities", []):
            if availability.get("date") == target_date:
                for staff in availability.get("staff", []):
                    if staff.get("id") == target_staff_id:
                        print(f"[BEFORE] {staff['name']} slots: {staff['slots']}")
                        staff["slots"] = new_slots
                        print(f"[AFTER]  {staff['name']} slots: {staff['slots']}")
                        updated = True

        if updated:
            r.set(key, json.dumps(data))
            print("✅ Redis data updated successfully.")
        else:
            print(f"⚠️ No matching staff with id={target_staff_id} on {target_date}")

except redis.ConnectionError as e:
    print(f"❌ Redis connection error: {e}")
except redis.RedisError as e:
    print(f"❌ Redis command error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
