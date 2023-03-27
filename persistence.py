import pandas as pd
import pickle
from sqlalchemy import create_engine, Column, Date, Integer, LargeBinary, UniqueConstraint, desc, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError

from constants import STATS_TABLE_NAME, STATS_DB_URL

# Set up the database model
Base = declarative_base()


class DailyLeaderboard(Base):
    __tablename__ = "daily_leaderboard"
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True, nullable=False, server_default=func.current_date())
    issues = Column(LargeBinary)
    reactions = Column(LargeBinary)
    __table_args__ = (UniqueConstraint("date"),)


class MyTable(Base):
    __tablename__ = STATS_TABLE_NAME
    date_key = Column(Integer, primary_key=True, default=text("(strftime('%s', 'now'))"))
    dataframe = Column(LargeBinary)


def create_table(engine) -> None:
    try:
        Base.metadata.create_all(engine)
    except OperationalError:
        pass


def insert_dataframe(session, df: pd.DataFrame) -> None:
    serialized_df = pickle.dumps(df)
    new_entry = MyTable(dataframe=serialized_df)
    try:
        session.add(new_entry)
        session.commit()
    except SQLAlchemyError:
        pass


def _get_session():
    engine = create_engine(STATS_DB_URL, echo=False)
    create_table(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def save_leaderboard(issues_df: pd.DataFrame, reactions_df: pd.DataFrame) -> None:
    session = _get_session()
    new_leaderboard = DailyLeaderboard(issues=pickle.dumps(issues_df), reactions=pickle.dumps(reactions_df))
    try:
        session.add(new_leaderboard)
        session.commit()
    except SQLAlchemyError:
        pass


def get_latest_leaderboard() -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    session = _get_session()
    latest_result = session.query(DailyLeaderboard).order_by(desc(DailyLeaderboard.date)).first()
    if not latest_result:
        return
    issues_df = pickle.loads(latest_result.issues)
    reactions_df = pickle.loads(latest_result.reactions)
    return issues_df, reactions_df


def fetch_dataframes(session, minutes: int = 15) -> List[pd.DataFrame]:
    time_threshold = datetime.utcnow() - timedelta(minutes=minutes)
    unix_time_threshold = int(time_threshold.timestamp())
    try:
        result = session.query(MyTable).filter(MyTable.date_key >= unix_time_threshold).all()
    except OperationalError:
        return []
    dataframes = []
    for row in result:
        dataframes.append(pickle.loads(row.dataframe))
    return dataframes


def persist_dataframe(df: pd.DataFrame) -> None:
    session = _get_session()
    insert_dataframe(session, df)


def fetch_results(minutes: int = 15) -> List[pd.DataFrame]:
    session = _get_session()
    restored_dataframes = fetch_dataframes(session, minutes)
    return restored_dataframes
