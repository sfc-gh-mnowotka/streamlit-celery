import streamlit as st
import threading
import time
from persistence import fetch_results
from celery_utils import start_celery_worker, start_celery_beat, schedule_task


schedule_exists = schedule_task()

st.title("🥬Using with Celery")

if st.button("Start Celery Beat!", disabled=schedule_exists):
    if not schedule_exists:
        thread = threading.Thread(target=start_celery_beat, name="Celery beat control thread")
        thread.daemon = True
        thread.start()
        with st.spinner("Starting Celery Beat..."):
            time.sleep(1)
        st.experimental_rerun()

if st.button("Start Celery Worker!"):
    thread = threading.Thread(target=start_celery_worker, name="Celery worker control thread")
    thread.daemon = True
    thread.start()
    st.experimental_rerun()

if st.button("Fetch results"):
    results = fetch_results()
    st.write(results)

