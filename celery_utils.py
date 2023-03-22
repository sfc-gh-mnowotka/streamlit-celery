import celery
from pathlib import Path
from constants import CELERY_MAIN_PREFIX, CELERY_BACKEND_URL, CELERY_BROKER_URL, CELERY_TIMEZONE, CELERY_SCHEDULE_FILE


def get_celery_app():
    return celery.Celery(
        CELERY_MAIN_PREFIX,
        include=['tasks'],
        backend=CELERY_BACKEND_URL,
        broker=CELERY_BROKER_URL
    )


app = get_celery_app()


def start_celery_beat():
    beat = app.Beat(
        include=['tasks']
    )
    beat.run()


def start_celery_worker():
    worker = app.Worker(
        include=['tasks']
    )
    worker.start()


def schedule_task() -> bool:
    schedule_file = Path(CELERY_SCHEDULE_FILE)
    schedule_exists = schedule_file.is_file()
    if not schedule_exists:
        app.conf.beat_schedule = {
            'save-every-10-seconds': {
                'task': 'tasks.save_stats',
                'schedule': 10.0,
                'args': []
            },
        }
        app.conf.timezone = CELERY_TIMEZONE
    return schedule_exists


