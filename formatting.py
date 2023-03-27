from typing import Iterable

import pandas as pd
import streamlit as st
from htbuilder import a, span

from constants import GITHUB_REACTIONS
from constants import LABEL_TO_COLUMN
from constants import DATETIME_PERIODS


def inline_space(num: int) -> str:
    return "&nbsp;" * num


def format_reaction_in_widget(reaction: str) -> str:
    if reaction == "🫶":
        return "🫶  (reactions overall)"
    elif reaction == "👄":
        return "👄  (comments)"
    else:
        return reaction


def _reactions_formatter(
    reactions: Iterable[str],
    counts: Iterable[int],
    grayscale_mask: Iterable[bool],
) -> str:

    reactions = "&nbsp;&nbsp;  ·  &nbsp;&nbsp;".join(
        [
            f"{reaction} {count}"
            if not grayscale
            else f'<span style="-webkit-filter: grayscale(100%); filter: grayscale(100%);">{reaction}</span> {count}'
            for (reaction, count, grayscale) in zip(reactions, counts, grayscale_mask)
        ]
    )

    return reactions


def _get_issue_html(issue, reactions_date_range_label: str, rank: int, sort_by: str) -> str:

    separator = f"{inline_space(2)}·{inline_space(2)}"
    import html

    title = html.escape(issue.title)
    title = title[:50] + "..." if len(title) > 50 else title
    url = issue.html_url
    issue_html = str(
        a(
            contenteditable=False,
            href=url,
            rel="noopener noreferrer",
            style="color:inherit;text-decoration:inherit; height:auto!important",
            target="_blank",
        )(
            span(
                style=(
                    "border-bottom:0.05em solid"
                    " rgba(55,53,47,0.25);font-weight:500;flex-shrink:0"
                )
            )(title),
            span(),
        )
    )
    reactions = GITHUB_REACTIONS
    reaction_counts = (
        issue.reactions_total_count,
        issue.reactions_plus1,
        issue.reactions_minus1,
        issue.reactions_laugh,
        issue.reactions_hooray,
        issue.reactions_confused,
        issue.reactions_heart,
        issue.reactions_rocket,
        issue.reactions_eyes,
        issue.comments,
    )
    creation_date = str(issue.created_at)[:10]
    grayscale_mask = [reaction != sort_by for reaction in GITHUB_REACTIONS]
    formatted_reactions = _reactions_formatter(
        reactions, reaction_counts, grayscale_mask
    )

    reactions_date_range_label = str(reactions_date_range_label)

    return f"""**{rank}**{inline_space(4)}{issue_html}{separator}{creation_date}{separator} :green[**+ {
    issue.num_reactions}** {issue.reaction} {
    reactions_date_range_label.lower()}]<br>
{inline_space(7)}<small data-testid="stCaptionContainer">{formatted_reactions}</small>
"""


def display_issue(issue, reactions_date_range_label: str, rank: int, sort_by: str) -> None:
    st.write(
        _get_issue_html(
            issue,
            reactions_date_range_label,
            rank=rank,
            sort_by=sort_by,
        ),
        unsafe_allow_html=True,
    )


def display_issues(df: pd.DataFrame, reactions_date_range_label: str, sort_by: str) -> None:
    for rank, row in enumerate((df.itertuples())):
        display_issue(row, reactions_date_range_label, rank=rank + 1, sort_by=sort_by)


def render_leaderboard(results):
    all_issues, reactions_df = results
    one, two, three, four = st.columns((2, 2, 3, 2))
    num_issues = one.selectbox(
        label="Show me at most...",
        options=(5, 10, 15),
        format_func=lambda x: f"{x} issues",
    )

    issues_date_range_label = two.selectbox(
        label="Created...",
        options=DATETIME_PERIODS.keys(),
        index=2,
    )

    issues_date_range_label = str(issues_date_range_label)

    sort_by_label = three.selectbox(
        label="Who received most...",
        options=LABEL_TO_COLUMN.keys(),
        format_func=format_reaction_in_widget,
    )

    sort_by_label = str(sort_by_label)

    reactions_date_range_label = four.selectbox(
        label="During...",
        options=DATETIME_PERIODS.keys(),
    )

    reactions_date_range_label = str(reactions_date_range_label)
    issues_date_range = DATETIME_PERIODS[issues_date_range_label]
    reactions_date_range = DATETIME_PERIODS[reactions_date_range_label]
    reactions_df.created_at = pd.to_datetime(reactions_df.created_at)

    # Reaction date filter
    numbers_from_reaction_date_filter = reactions_df[
        reactions_df.created_at.dt.date.between(*reactions_date_range)
    ].issue_number.unique()

    emoji_to_label_mapper = {
        "🫶": "heart",
        "❤️": "heart",
        "👍": "+1",
        "👎": "-1",
        "🚀": "rocket",
        "😕": "confused",
        "👁️": "eyes",
        "😂": "laugh",
        "🥳": "hooray",
    }

    result = (
        reactions_df[
            (
                reactions_df.content.eq(emoji_to_label_mapper[sort_by_label])
                if sort_by_label != "🫶"
                else True
            )
            & reactions_df.created_at.dt.date.between(*reactions_date_range)
            & reactions_df.issue_number.isin(
                all_issues[
                    all_issues.created_at.dt.date.between(*issues_date_range)
                ].number.unique()
            )
            ]
        .groupby("issue_number")
        .size()
        .sort_values(ascending=False)
        .to_frame("num_reactions")
        .assign(reaction=sort_by_label)
        .reset_index()
        .merge(all_issues, left_on="issue_number", right_on="number")
        .head(num_issues)
    )

    st.write("### Results")
    if result.empty:
        st.caption(
            f"Empty results. No issue created {issues_date_range_label.lower()} was found with {sort_by_label} reactions happening {reactions_date_range_label.lower()}."
        )
    display_issues(result, reactions_date_range_label, sort_by=sort_by_label)