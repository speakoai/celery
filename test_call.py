from tasks.availability import fetch_sample_data

result = fetch_sample_data.delay()
print("Task queued. Task ID:", result.id)
