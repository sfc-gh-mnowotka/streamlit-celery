import streamlit as st
import threading
import time
from persistence import fetch_results
from celery_utils import start_celery_worker
from celery_utils import start_celery_beat
from celery_utils import is_celery_beat_working
from celery_utils import are_workers_running
from celery_utils import stop_celery


st.title("🥬Using with Celery")

if st.button("Fetch results"):
    results = fetch_results()
    st.write(results)

with st.expander("⚙️ Celery controls"):
    col1, col2, col3 = st.columns(3)
    with col1:
        is_beat_working = is_celery_beat_working()
        label = "💓 Start Celery Beat!" if not is_beat_working else "💓 Celery Beat Started!"
        if st.button(label, disabled=is_beat_working):
            thread = threading.Thread(target=start_celery_beat, name="Celery beat control thread")
            thread.daemon = True
            thread.start()
            with st.spinner("Starting Celery Beat..."):
                time.sleep(1)
            st.experimental_rerun()

    with col2:
        are_workers_started = are_workers_running()
        label = "🔨Start Celery Workers!" if not are_workers_started else "🔨 Celery Workers Started!"
        if st.button(label, disabled=are_workers_started):
            thread = threading.Thread(target=start_celery_worker, name="Celery worker control thread")
            thread.daemon = True
            thread.start()
            with st.spinner("Starting Celery Workers..."):
                time.sleep(1)
            st.experimental_rerun()

    with col3:
        if st.button("Stop celery"):
            stop_celery()

