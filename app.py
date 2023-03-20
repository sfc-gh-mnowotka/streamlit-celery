import streamlit as st
import celery
from celery.result import allow_join_result
import threading
import time
from tasks import add

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


st.title("🥬Using with Celery")

if st.button("Start Celery!"):
    thread = threading.Thread(target=start_celery, name="Celery control thread")
    thread.daemon = True
    thread.start()

if st.button("Add!"):
    result = add.delay(4, 4)
    with allow_join_result():
        with st.spinner('Wait for it...'):
            while not result.ready():
                time.sleep(1)
                continue
        st.write(result.get())

