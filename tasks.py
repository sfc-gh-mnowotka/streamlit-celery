from celery import Celery

app = Celery('tasks', backend='rpc://', broker='sqla+sqlite:///celerydb.sqlite')


@app.task
def add(x, y):
    return x + y
