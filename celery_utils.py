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
        return _is_process_running(CELERY_BEAT_COMMAND)
    else:
        schedule_file = Path(CELERY_SCHEDULE_FILE)
        return schedule_file.is_file()


def are_workers_running():
    if CELERY_RUN_PROCESSES:
        return _is_process_running(CELERY_WORKER_COMMAND)
    else:
        broker_file = Path(CELERY_BROKER_URL.split(":///")[1])
        return broker_file.is_file()


def stop_celery():
    _stop_process(CELERY_BEAT_COMMAND)
    _stop_process(CELERY_WORKER_COMMAND)
    schedule_file = Path(CELERY_SCHEDULE_FILE)
    schedule_file.unlink(missing_ok=True)
    broker_file = Path(CELERY_BROKER_URL.split(":///")[1])
    broker_file.unlink(missing_ok=True)


def _stop_process(command: str) -> None:
    subprocess.call(["pkill", "-9", "-f"] + [f"{command}"])


def _is_process_running(command: str) -> bool:
    print(f"Checking if {command} is running")
    p1 = subprocess.Popen(["pgrep", "-f"] + [f"{command}"], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["wc", "-l"], stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    print(f'pgrep -f "{command}" | wc -l')
    output = p2.communicate()[0]
    return int(output.strip()) > 0

