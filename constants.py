CELERY_MAIN_PREFIX = "project"
CELERY_BACKEND_URL = 'rpc://'
CELERY_BROKER_URL = 'sqla+sqlite:///celerydb.sqlite'
CELERY_TIMEZONE = 'UTC'
CELERY_SCHEDULE_FILE = 'celerybeat-schedule'
CELERY_RUN_PROCESSES = True
CELERY_WORKER_COMMAND = 'celery -A tasks worker --loglevel=info'
CELERY_BEAT_COMMAND = 'celery -A tasks beat --loglevel=info'
STATS_DB_URL = 'sqlite:///stats_database.sqlite'
STATS_TABLE_NAME = 'github_stats'
