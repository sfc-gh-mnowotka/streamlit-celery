import logging
import pandas as pd
import streamlit as st
from ghapi.all import GhApi, paged

from celery_utils import app
from celery.schedules import crontab
from persistence import save_leaderboard
from constants import CELERY_TIMEZONE
from constants import PAGE_LOCATION_FILE
import json


logging.basicConfig()
logger = logging.getLogger("tasks")


def get_page_location():
    with open(PAGE_LOCATION_FILE) as json_file:
        return json.load(json_file)


app.conf.beat_schedule = {
    'generate-leaderboard-every-day': {
        'task': 'tasks.compute_leaderboard',
        'schedule': crontab(hour=4, minute=0),
        'args': []
    },
}
app.conf.timezone = CELERY_TIMEZONE

api = GhApi(token=st.secrets.github.token)


@app.task
def compute_leaderboard():
    all_issues = get_overall_issues()

    # Only query issues which had at least 1 reaction
    issue_numbers = all_issues.query("reactions_total_count > 0").number.unique().tolist()
    reactions_df = get_overall_reactions(issue_numbers)
    save_leaderboard(all_issues, reactions_df)


def get_overall_issues() -> pd.DataFrame:
    # Get raw data
    raw_issues = list()

    pages = paged(
        api.issues.list_for_repo,
        owner="streamlit",
        repo="streamlit",
        per_page=100,
        sort="created",
        direction="desc",
    )

    for page in pages:
        raw_issues += page

    # Parse into a dataframe
    df = pd.json_normalize(raw_issues)

    # Make sure types are properly understood
    df.created_at = pd.to_datetime(df.created_at)
    df.updated_at = pd.to_datetime(df.updated_at)

    # Replace special chars in columns to facilitate access in namedtuples
    df.columns = [
        col.replace(".", "_").replace("+1", "plus1").replace("-1", "minus1")
        for col in df.columns
    ]

    return df


def _get_overall_reactions(issue_number: int):
    # Get raw data
    raw_reactions = list()

    pages = paged(
        api.reactions.list_for_issue,
        owner="streamlit",
        repo="streamlit",
        issue_number=issue_number,
        per_page=100,
    )

    for page in pages:
        raw_reactions += page

    # Parse into a dataframe
    reactions_df = pd.json_normalize(raw_reactions)
    return reactions_df


def get_overall_reactions(issue_numbers: list):
    reactions_dfs = list()
    for issue_number in issue_numbers:
        reactions_df = _get_overall_reactions(issue_number)
        if not reactions_df.empty:
            reactions_df = reactions_df[
                ["created_at", "content", "user.login", "user.id", "user.avatar_url"]
            ]
            reactions_df["issue_number"] = issue_number
            reactions_dfs.append(reactions_df)

    return pd.concat(reactions_dfs)

