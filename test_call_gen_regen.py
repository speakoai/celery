from tasks.availability_gen_regen import gen_availability, gen_availability_venue

if __name__ == "__main__":
    result = gen_availability_venue(tenant_id=2 , location_id=35, location_tz='Australia/Sydney')
    
    if result:
        print("✅ Task completed with result:")
    else:
        print("❌ No result returned")
