import json

import libsql_experimental as libsql
import pandas as pd
import os
from dotenv import load_dotenv
from pandas import read_sql_query
from sqlalchemy import create_engine, select, text, inspect
from sqlalchemy.orm import Session, base, DeclarativeBase, sessionmaker
import sqlalchemy_libsql
import sqlalchemy.dialects.sqlite
from datetime import timedelta, time, datetime
import argparse


import logging

from models import MarketStats, MarketOrders, Base, MarketHistory, Doctrines

logging.basicConfig(filename='logs/libsqltest.log', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.filehandler = logging.FileHandler('logs/libsqltest.log')
logger.filehandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

path_begin = "/mnt/c/Users/User/PycharmProjects"

new_orders = f"{path_begin}/eveESO/output/brazil/new_orders.json"
new_history = f"{path_begin}//eveESO/output/brazil/new_history.csv"
new_stats = f"{path_begin}/eveESO/output/brazil/new_stats.csv"
watchlist = f"{path_begin}/eveESO/output/brazil/watchlist.csv"
new_orderscsv = f"{path_begin}/eveESO/output/brazil/new_orders.csv"
new_doctrines = f"{path_begin}/eveESO/output/brazil/new_doctrines.csv"

load_dotenv()

url = os.getenv("TURSO_DATABASE_URL")
auth_token = os.getenv("TURSO_AUTH_TOKEN")

turso = f"sqlite+{url}/?authToken={auth_token}&secure=true"

sde_url = os.getenv("SDE_URL")
sde_auth_token = os.getenv("SDE_TOKEN")

sde = f"sqlite+{sde_url}/?authToken={sde_auth_token}&secure=true"

def example(df):

    conn = libsql.connect("wcmkt.db", sync_url=url, auth_token=auth_token)
    conn.sync()

    # noinspection SqlDialectInspection
    conn.execute("""CREATE TABLE IF NOT EXISTS marketstats (
    type_id INTEGER,
    total_volume_remain INTEGER,
    min_price FLOAT,
    price_5th_percentile FLOAT,
    avg_of_avg_price FLOAT,
    avg_daily_volume FLOAT,
    group_id INTEGER,
    type_name TEXT,
    group_name TEXT,
    category_id INTEGER,
    category_name TEXT,
    days_remaining FLOAT,
    last_update DATETIME
    
    );""")


def get_type_names(df):
    df.issued = pd.to_datetime(df.issued)
    df.drop(columns='type_name', inplace=True)
    type_ids = df.type_id.unique()
    typestr = ','.join(str(i) for i in type_ids)

    query = f"SELECT typeID, typeName FROM invTypes WHERE typeID IN ({typestr})"

    engine = create_engine(sde, echo=False)
    with engine.connect() as conn:
        df2 = pd.read_sql_query(query, conn)
        df2.reset_index(drop=True, inplace=True)

    rn_map = {'typeID':'type_id', 'typeName':'type_name'}
    df2.rename(columns=rn_map, inplace=True)
    named_df = pd.merge(df, df2, on='type_id', how='left')
    return named_df


def fetch_from_brazil(selected_items: list):
    engine = create_engine(turso, echo=False)
    items = selected_items
    items_str = ','.join(str(i) for i in items)

    # noinspection SqlDialectInspection
    stmt = text(f"""
            SELECT * FROM marketstats 
            WHERE type_id IN ({items_str})
        """)

    with Session(engine) as session:
        result = session.execute(stmt)
        rows = result.fetchall()

    # noinspection PyTypeChecker
    df = pd.DataFrame(rows, columns=result.keys())
    return df


def update_history():
    df = pd.read_csv(new_history)
    df.date = pd.to_datetime(df.date)
    data = df.to_dict(orient='records')

    engine = create_engine(turso, echo=False)
    start = datetime.now()

    with Session(engine) as session:
        try:
            mapper = inspect(MarketHistory)
            session.bulk_insert_mappings(mapper, data)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.info(f'error: {e}')
    session.close()
    finish = datetime.now()
    history_time = finish - start
    logger.info(f"history time: {history_time}, rows: {len(data)}")


def update_orders():
    df = pd.read_csv(new_orderscsv)
    df.issued = pd.to_datetime(df.issued)
    df = get_type_names(df)
    df.infer_objects()

    data = df.to_dict(orient='records')

    engine = create_engine(turso, echo=False)
    start = datetime.now()

    with Session(engine) as session:
        try:
            mapper = inspect(MarketOrders)
            session.bulk_insert_mappings(mapper, data)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f'error: {e}')

    session.close()
    finish = datetime.now()
    orders_time = finish - start
    logger.info(f"orders time: {orders_time}, rows: {len(data)}")

def update_stats():
    df = pd.read_csv(new_stats)
    df.last_update = pd.to_datetime(df.last_update)
    data = df.to_dict(orient='records')

    start = datetime.now()

    engine = create_engine(turso, echo=False)
    with Session(engine) as session:
        try:
            mapper = inspect(MarketStats)
            session.bulk_insert_mappings(mapper, data)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f'error: {e}')
    session.close()
    finish = datetime.now()
    stats_time = finish - start
    logger.info(f"stats time: {stats_time}, rows: {len(data)}")

def update_doctrines():
    df = pd.read_csv(new_doctrines)
    df.timestamp = df.timestamp.sort_values(ascending=False)
    ts = df.loc[0, "timestamp"]
    df.timestamp = df.timestamp.apply(lambda x: ts if x == 0 else x)
    df.infer_objects()
    data = df.to_dict(orient='records')

    engine = create_engine(turso, echo=False)
    start = datetime.now()

    with Session(engine) as session:
        try:
            mapper = inspect(Doctrines)
            session.bulk_insert_mappings(mapper, data)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f'error: {e}')
 
    finish = datetime.now()
    doctrines_time = finish - start
    logger.info(f"doctrines time: {doctrines_time}, rows: {len(data)}")

def main():
    parser = argparse.ArgumentParser(description="options for market ESI calls")

    # Add arguments
    parser.add_argument("--hist",
                        action="store_true",
                        help="Refresh history data"
                        )

    args = parser.parse_args()

    if args.hist:
        logger.info("Running Brazil update with full history refresh.")

    else:
        logger.info("Running market update in quick mode, saved history data will be used.")

    logger.info(f"connecting to database: {turso}")
    engine = create_engine(turso, echo=False)
    conn = engine.connect()
    logger.info('removing and recreating tables...')
    with conn.begin():
        conn.execute(text("DROP TABLE IF EXISTS marketstats"))
        conn.execute(text("DROP TABLE IF EXISTS marketorders"))
        conn.execute(text("DROP TABLE IF EXISTS doctrines"))
        conn.commit()
        Base.metadata.create_all(engine)

    logger.info("starting df update...")
    logger.info('updating table: orders')
    logger.info('-'*100)
    update_orders()
    logger.info('updating table: stats')
    logger.info('-'*100)
    update_stats()
    logger.info('updating table: doctrines')
    logger.info('-'*100)
    update_doctrines()
    logger.info('-'*100)

    if args.hist:
        logger.info('updating table: history')
        logger.info('-'*100)
        update_history()
    else:
        logger.info("df update complete")

    logger.info("syncing embedded replica and closing database connection...")


    conn = libsql.connect('wcmkt.db', sync_url=url, auth_token=auth_token)
    conn.sync()

    logger.info("sync complete. closing database connection and exiting program.")


if __name__ == "__main__":
    Start = datetime.now()
    main()
    print("="*100)
    Finish = datetime.now()
    Total_time = Finish - Start
    logger.info(f"total time: {Total_time}")
    print(f"""
          total time: {Total_time}
  
          """)
    print("="*100)