from celery import Celery
import os

app = Celery('myapp', broker=os.getenv('CELERY_BROKER_URL'))
app.autodiscover_tasks(['tasks'])  # Auto-import tasks.* modules
