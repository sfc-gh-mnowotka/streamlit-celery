import asyncio
import logging
import websockets
from websockets.exceptions import InvalidStatusCode
import pandas as pd
import streamlit as st
from ghapi.all import GhApi, paged

from celery_utils import app
from celery.schedules import crontab
from persistence import save_leaderboard
from constants import CELERY_TIMEZONE
from constants import IFRAME_PATH
from constants import WS_SUFFIX
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
    'keep-alive': {
        'task': 'tasks.keep_alive',
        'schedule': 60 * 60 * 24,
        'args': [get_page_location()]
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


async def connect(url, origin, host):
    try:
        async with websockets.connect(
                url,
                user_agent_header="",
                extra_headers={
                    "Host": host,
                    "Origin": origin,
                }
        ) as websocket:
            await websocket.recv()
    except InvalidStatusCode as e:
        logger.info(e)
        logger.info(e.status_code)
        logger.info(e.headers)


@app.task
def keep_alive(location):
    if not location:
        return
    ws_protocol = "ws://" if location["protocol"] == "http:" else "wss://"
    iframe = IFRAME_PATH if IFRAME_PATH in location["pathname"] else ""
    url = f'{ws_protocol}{location["host"]}{iframe}{WS_SUFFIX}'
    logger.info(f"URL: {url}")
    asyncio.run(connect(url, location["origin"], location["host"]))


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

