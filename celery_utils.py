import celery
from pathlib import Path
import subprocess
from constants import CELERY_MAIN_PREFIX
from constants import CELERY_BACKEND_URL
from constants import CELERY_BROKER_URL
from constants import CELERY_SCHEDULE_FILE
from constants import CELERY_RUN_PROCESSES
from constants import CELERY_WORKER_COMMAND
from constants import CELERY_BEAT_COMMAND


def get_celery_app():
    return celery.Celery(
        CELERY_MAIN_PREFIX,
        include=['tasks'],
        backend=CELERY_BACKEND_URL,
        broker=CELERY_BROKER_URL
    )


app = get_celery_app()


def start_celery_worker():
    if CELERY_RUN_PROCESSES:
        subprocess.call(CELERY_WORKER_COMMAND.split())
    else:
        worker = app.Worker(
            include=['tasks']
        )
        worker.start()


def start_celery_beat():
    if CELERY_RUN_PROCESSES:
        subprocess.call(CELERY_BEAT_COMMAND.split())
    else:
        beat = app.Beat(
            include=['tasks']
        )
        beat.run()


def is_celery_beat_working():
    if CELERY_RUN_PROCESSES:
        p1 = subprocess.Popen(["ps"], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(["grep"] + [f"{CELERY_BEAT_COMMAND}"], stdin=p1.stdout, stdout=subprocess.PIPE)
        p3 = subprocess.Popen(["wc", "-l"], stdin=p2.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        p2.stdout.close()
        return int(p3.communicate()[0].strip()) > 1
    else:
        schedule_file = Path(CELERY_SCHEDULE_FILE)
        return schedule_file.is_file()


def are_workers_running():
    if CELERY_RUN_PROCESSES:
        p1 = subprocess.Popen(["ps"], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(["grep"] + [f"{CELERY_WORKER_COMMAND}"], stdin=p1.stdout, stdout=subprocess.PIPE)
        p3 = subprocess.Popen(["wc", "-l"], stdin=p2.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        p2.stdout.close()
        return int(p3.communicate()[0].strip()) > 1
    else:
        broker_file = Path(CELERY_BROKER_URL.split(":///")[1])
        return broker_file.is_file()

