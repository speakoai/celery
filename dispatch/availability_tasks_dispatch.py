from tasks.availability import fetch_sample_data

def main():
    print("[DISPATCH] Triggering fetch_sample_data Celery task...")
    fetch_sample_data.delay()

if __name__ == "__main__":
    main()
