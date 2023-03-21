import pandas as pd
import pickle
from sqlalchemy import create_engine, Column, String, LargeBinary, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from typing import List

# Set up the database model
Base = declarative_base()


class MyTable(Base):
    __tablename__ = 'github_stats'
    date_key = Column(String, primary_key=True, default=text('CURRENT_TIMESTAMP'))
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
    result = session.query(MyTable).filter(MyTable.date_key >= time_threshold).all()

    dataframes = []
    for row in result:
        dataframes.append(pickle.loads(row.dataframe))
    return dataframes


def persist_dataframe(df: pd.DataFrame) -> None:
    engine = create_engine('sqlite:///my_database.sqlite', echo=False)
    create_table(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    insert_dataframe(session, df)


def fetch_results(minutes: int = 15) -> List[pd.DataFrame]:
    engine = create_engine('sqlite:///my_database.sqlite', echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    restored_dataframes = fetch_dataframes(session, minutes)
    return restored_dataframes


def demo() -> None:
    # Create a sample pandas DataFrame
    data = {'A': [1, 2, 3],
            'B': [4, 5, 6]}
    df = pd.DataFrame(data)

    # Create an SQLite database and connect to it
    engine = create_engine('sqlite:///my_database.sqlite', echo=False)
    create_table(engine)

    # Insert the DataFrame into the table
    Session = sessionmaker(bind=engine)
    session = Session()
    insert_dataframe(session, df)

    # Deserialize the DataFrame from the database and print dataframes inserted in the last 15 minutes
    restored_dataframes = fetch_dataframes(session)
    print("DataFrames inserted in the last 15 minutes:")
    for restored_df in restored_dataframes:
        print(restored_df)

    session.close()