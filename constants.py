import datetime

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

TODAY = datetime.date.today()
A_WEEK_AGO = TODAY - datetime.timedelta(days=7)
A_MONTH_AGO = TODAY - datetime.timedelta(days=30)
REPO_CREATION = datetime.date(2019, 1, 1)
TWENTY_FOUR_HOURS = 60 * 60 * 24

DATETIME_PERIODS = {
    "Last week": (A_WEEK_AGO, TODAY),
    "Last month": (A_MONTH_AGO, TODAY),
    "All time": (REPO_CREATION, TODAY),
}

GITHUB_REACTIONS = [
    "🫶",
    "👍",
    "👎",
    "😂",
    "🥳",
    "😕",
    "❤️",
    "🚀",
    "👁️",
]

LABEL_TO_COLUMN = dict(
    zip(
        GITHUB_REACTIONS,
        (
            "reactions_total_count",
            "reactions_plus1",
            "reactions_minus1",
            "reactions_laugh",
            "reactions_hooray",
            "reactions_confused",
            "reactions_heart",
            "reactions_rocket",
            "reactions_eyes",
        ),
    )
)