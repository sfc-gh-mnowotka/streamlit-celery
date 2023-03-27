from typing import List

import celery
from pathlib import Path
import subprocess
import psutil
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


def _find_process_pids(command: str) -> List[str]:
    ret = []
    for proc in psutil.process_iter(['pid', 'cmdline']):
        if proc.info['cmdline'] and command in ' '.join(proc.info['cmdline']):
            ret.append(str(proc.info['pid']))
    return ret


def _stop_process(command: str) -> None:
    pids = _find_process_pids(command)
    subprocess.call(["kill", "-9"] + pids)


def _is_process_running(command: str) -> bool:
    pids = _find_process_pids(command)
    return len(pids) > 0


