from tasks.celery_app import app
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

@app.task
def add(x, y):
    logger.info(f'Adding {x} + {y}')
    return x + y
