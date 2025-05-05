from tasks.availability import gen_availability

if __name__ == "__main__":
    result = gen_availability(tenant_id=1, location_id=2)
    if result:
        print("✅ Task completed with result:")
    else:
        print("❌ No result returned")
