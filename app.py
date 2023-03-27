import streamlit as st
import threading
import time
import os
from persistence import get_latest_leaderboard
from celery_utils import start_celery_worker
from celery_utils import start_celery_beat
from celery_utils import is_celery_beat_working
from celery_utils import are_workers_running
from celery_utils import stop_celery
from formatting import render_leaderboard
from streamlit_js_eval import get_page_location


st.title("Github Issues Leaderboard")

st.write(os.system("ps"))
st.write(get_page_location())

results = get_latest_leaderboard()
if not results:
    st.warning("No results yet...")
else:
    render_leaderboard(results)

with st.sidebar:
    with st.expander("⚙️ Celery controls"):

        is_beat_working = is_celery_beat_working()
        label = "💓 Start Celery Beat!" if not is_beat_working else "💓 Celery Beat Started!"
        if st.button(label, disabled=is_beat_working):
            thread = threading.Thread(target=start_celery_beat, name="Celery beat control thread")
            thread.daemon = True
            thread.start()
            with st.spinner("Starting Celery Beat..."):
                time.sleep(1)
            st.experimental_rerun()

        are_workers_started = are_workers_running()
        label = "🔨Start Celery Workers!" if not are_workers_started else "🔨 Celery Workers Started!"
        if st.button(label, disabled=are_workers_started):
            thread = threading.Thread(target=start_celery_worker, name="Celery worker control thread")
            thread.daemon = True
            thread.start()
            with st.spinner("Starting Celery Workers..."):
                time.sleep(1)
            st.experimental_rerun()

        if st.button("🛑 Stop celery", disabled=not(is_beat_working or are_workers_started)):
            stop_celery()
            with st.spinner("Stopping Celery..."):
                time.sleep(1)
            st.experimental_rerun()

