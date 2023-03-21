import streamlit as st
import celery
from celery.result import allow_join_result
import threading
import time
from tasks import add, save_stats
from persistence import fetch_results

app = celery.Celery(
    'project',
    include=['tasks'],
    backend='rpc://',
    broker='sqla+sqlite:///celerydb.sqlite'
)


def start_celery():
    worker = app.Worker(
        include=['tasks']
    )
    worker.start()

    beat = app.Beat(
        include=['tasks']
    )
    beat.start()


st.title("🥬Using with Celery")

if st.button("Start Celery!"):
    thread = threading.Thread(target=start_celery, name="Celery control thread")
    thread.daemon = True
    thread.start()

if st.button("Schedule tasks"):
    @app.on_after_fork.connect
    def setup_periodic_tasks(sender, **kwargs):
        sender.add_periodic_task(10.0, save_stats.s(), name='save every 10 seconds')

if st.button("Add!"):
    result = add.delay(4, 4)
    with allow_join_result():
        with st.spinner('Wait for it...'):
            while not result.ready():
                time.sleep(1)
                continue
        st.write(result.get())

if st.button("Fetch results"):
    results = fetch_results()
    st.write(results)

