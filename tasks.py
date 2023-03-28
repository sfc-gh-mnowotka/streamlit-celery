import asyncio
import logging
import websockets
from websockets.exceptions import InvalidStatusCode
import pandas as pd
import requests
import streamlit as st
from ghapi.all import GhApi, paged

from celery_utils import app
from celery.schedules import crontab
from persistence import save_leaderboard
from constants import CELERY_TIMEZONE
from constants import WS_SUFFIX
from constants import IFRAME_PATH
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
        'schedule': 10.0,
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
    session = requests.Session()
    response = session.get(origin,
                           headers={
                               'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
                           })
    cookies = "; ".join([f"{key}={value}" for key, value in response.cookies])
    try:
        async with websockets.connect(
                url,
                user_agent_header="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
                extra_headers={
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                    "Pragma": "no-cache",
                    "Cache-Control": "no-cache",
                    "Host": host,
                    "Origin": origin,
                    "Cookie": "_ga=GA1.2.261696798.1670256797; GCP_IAP_UID=101202930590561361811; _gid=GA1.2.1742699608.1679925473; streamlit_session=MTY3OTkzNDAzMHx4RTZVSElWd2NmOWt1dlM3RTFEU2FfRU1BbEZOYXhickhpV1p0Y0hlcXRaR3VPMkUyemRqTVEwX3ZWRjA1aG1QOGZzdy1wV3psOFd6TVY2TFJiRGdEaEtyTWdpUVNJTUJnNjJ0alR1NzJoaTZXZnZERWM3Y0lMelpFV0ZFem52OXU4OHU0WUctcDRaaTVGTXhiTVAxZThMRjU1N2xJSmxUcktVLVc5dGNTRW5XbmNTVk8tclZwWlByMGFtejNLNVEwMmxXc0J2R1NoRWMwd1V2LWtmRFo5VFlqS0tXT0NNPXzY3xkgCOFch3a_V6ZpJs73LveX1NGbcCxserzBsLA8Mg==; hubspotutk=a2cbc5a39e9fc8f5b18483d0f6f69d6b; __hssrc=1; _streamlit_csrf=MTY3OTk5MTE2OHxJa1ZtUzB4amEwODJjRFZhVGpoSFZGbGlhREpUVkdkSVNqUXhTRGt5YVZOT01GbGhPSFp4YUhOTE9GazlJZ289fIXmugk65XCnbVFkVzkH5XtqfsP8kmwRl49FgXmL9fMM; ajs_user_id=c4991f18-f9ba-5ee0-93b0-656631c7b9a6; ajs_anonymous_id=1b1d879a-6e2d-47bf-9659-716c709d4fed; _dd_s=logs=1&id=d13cce1e-ebb5-4f40-81d9-364dc086ac83&created=1680007632707&expire=1680008532707; _dc_gtm_UA-122023594-8=1; __hstc=225580997.a2cbc5a39e9fc8f5b18483d0f6f69d6b.1679934032241.1679991673630.1680007632990.5; __hssc=225580997.1.1680007632990"
                }
        ) as websocket:
            await websocket.send("Hello world!")
            await websocket.recv()
    except InvalidStatusCode as e:
        logger.info(e)
        logger.info(e.status_code)
        logger.info(e.headers)
        logger.info(cookies)

@app.task
def keep_alive(location):
    if not location:
        return
    ws_protocol = "ws://" if location["protocol"] == "http:" else "wss://"
    iframe = IFRAME_PATH if IFRAME_PATH in  location["pathname"] else ""
    # url = f'{ws_protocol}{location["host"]}{iframe}{WS_SUFFIX}'
    url = "ws://localhost:8501/_stcore/stream"
    logger.info("URL")
    logger.info(url)
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

