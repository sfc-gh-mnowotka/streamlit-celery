import pandas as pd
import pickle
from sqlalchemy import create_engine, Column, Integer, LargeBinary, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from typing import List

from constants import STATS_TABLE_NAME, STATS_DB_URL

# Set up the database model
Base = declarative_base()


class MyTable(Base):
    __tablename__ = STATS_TABLE_NAME
    date_key = Column(Integer, primary_key=True, default=text("(strftime('%s', 'now'))"))
    dataframe = Column(LargeBinary)


def create_table(engine) -> None:
    Base.metadata.create_all(engine)


def insert_dataframe(session, df: pd.DataFrame) -> None:
    serialized_df = pickle.dumps(df)
    new_entry = MyTable(dataframe=serialized_df)
    session.add(new_entry)
    session.commit()


def fetch_dataframes(session, minutes: int = 15) -> List[pd.DataFrame]:
    time_threshold = datetime.utcnow() - timedelta(minutes=minutes)
    unix_time_threshold = int(time_threshold.timestamp())
    result = session.query(MyTable).filter(MyTable.date_key >= unix_time_threshold).all()

    dataframes = []
    for row in result:
        dataframes.append(pickle.loads(row.dataframe))
    return dataframes


def persist_dataframe(df: pd.DataFrame) -> None:
    engine = create_engine(STATS_DB_URL, echo=False)
    create_table(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    insert_dataframe(session, df)


def fetch_results(minutes: int = 15) -> List[pd.DataFrame]:
    engine = create_engine(STATS_DB_URL, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    restored_dataframes = fetch_dataframes(session, minutes)
    return restored_dataframes