from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

app = Celery('myapp', 
            broker=os.getenv('CELERY_BROKER_URL'),
            backend=os.getenv('CELERY_RESULT_BACKEND', os.getenv('CELERY_BROKER_URL')))

# Configure Celery for better task persistence and monitoring
app.conf.update(
    result_expires=86400,  # Results expire after 24 hours
    task_track_started=True,  # Track when tasks start
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    # Store task results in Redis with proper settings
    result_backend_transport_options={
        'retry_policy': {
            'timeout': 5.0
        }
    },
    # Enable task events for monitoring
    worker_send_task_events=True,
    task_send_sent_event=True,
)

app.autodiscover_tasks(['tasks'])

# 👇 Add this line
import tasks.availability
import tasks.sms
