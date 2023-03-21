import streamlit as st
import celery
import threading
from persistence import fetch_results

app = celery.Celery(
    'project',
    include=['tasks'],
    backend='rpc://',
    broker='sqla+sqlite:///celerydb.sqlite'
)


app.conf.beat_schedule = {
    'save-every-10-seconds': {
        'task': 'tasks.save_stats',
        'schedule': 10.0,
        'args': []
    },
}

app.conf.timezone = 'UTC'


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


st.title("🥬Using with Celery")

if st.button("Start Celery Beat!"):
    thread = threading.Thread(target=start_celery_beat, name="Celery control thread")
    thread.daemon = True
    thread.start()

if st.button("Start Celery Worker!"):
    thread = threading.Thread(target=start_celery_worker, name="Celery control thread")
    thread.daemon = True
    thread.start()

if st.button("Fetch results"):
    results = fetch_results()
    st.write(results)

