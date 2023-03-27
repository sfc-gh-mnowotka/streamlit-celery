import pandas as pd
import pickle
from sqlalchemy import create_engine, Column, Date, Integer, LargeBinary, UniqueConstraint, desc, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Tuple, Optional
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError

from constants import STATS_DB_URL

# Set up the database model
Base = declarative_base()


class DailyLeaderboard(Base):
    __tablename__ = "daily_leaderboard"
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True, nullable=False, server_default=func.current_date())
    issues = Column(LargeBinary)
    reactions = Column(LargeBinary)
    __table_args__ = (UniqueConstraint("date"),)


def create_table(engine) -> None:
    try:
        Base.metadata.create_all(engine)
    except OperationalError:
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

