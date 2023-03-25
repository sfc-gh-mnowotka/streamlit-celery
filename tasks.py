import pandas as pd
import numpy as np

from celery_utils import app
from persistence import persist_dataframe
from constants import CELERY_TIMEZONE

app.conf.beat_schedule = {
    'save-every-10-seconds': {
        'task': 'tasks.save_stats',
        'schedule': 10.0,
        'args': []
    },
}
app.conf.timezone = CELERY_TIMEZONE


@app.task
def add(x, y):
    return x + y


@app.task
def save_stats():
    df = pd.DataFrame(np.random.randint(0, 100, size=(15, 4)), columns=list('ABCD'))
    persist_dataframe(df)
