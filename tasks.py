import pandas as pd
import numpy as np

from celery_utils import get_celery_app
from persistence import persist_dataframe

app = get_celery_app()


@app.task
def add(x, y):
    return x + y


@app.task
def save_stats():
    df = pd.DataFrame(np.random.randint(0, 100, size=(15, 4)), columns=list('ABCD'))
    persist_dataframe(df)
