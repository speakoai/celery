from tasks.availability import gen_availability, gen_availability_venue

if __name__ == "__main__":
    result = gen_availability_venue(tenant_id=36 , location_id=42 )
    if result:
        print("✅ Task completed with result:")
    else:
        print("❌ No result returned")
