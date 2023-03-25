import streamlit as st
import threading
import time
from persistence import fetch_results
from celery_utils import start_celery_worker
from celery_utils import start_celery_beat
from celery_utils import is_celery_beat_working
from celery_utils import are_workers_running


st.title("🥬Using with Celery")

if st.button("Start Celery Beat!", disabled=is_celery_beat_working()):
    thread = threading.Thread(target=start_celery_beat, name="Celery beat control thread")
    thread.daemon = True
    thread.start()
    with st.spinner("Starting Celery Beat..."):
        time.sleep(1)
    st.experimental_rerun()

if st.button("Start Celery Workers!", disabled=are_workers_running()):
    thread = threading.Thread(target=start_celery_worker, name="Celery worker control thread")
    thread.daemon = True
    thread.start()
    with st.spinner("Starting Celery Workers..."):
        time.sleep(1)
    st.experimental_rerun()

if st.button("Fetch results"):
    results = fetch_results()
    st.write(results)

