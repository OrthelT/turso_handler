import json

import libsql_experimental as libsql
import pandas as pd
import os
from dotenv import load_dotenv
from pandas import read_sql_query
from sqlalchemy import create_engine, select, text, inspect, Table
from sqlalchemy.orm import Session, base, DeclarativeBase, sessionmaker
import sqlalchemy_libsql
import sqlalchemy.dialects.sqlite
from datetime import timedelta, time, datetime
import argparse
import sqlite3

import logging
from logging.handlers import RotatingFileHandler

from models import MarketStats, MarketOrders, Base, MarketHistory, Doctrines, ShipTargets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Setup rotating file handler (10MB max size, keep 5 backup files)
logger.filehandler = RotatingFileHandler(
    'logs/turso_handler.log', 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
logger.filehandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(logger.filehandler)

logger.streamhandler = logging.StreamHandler()
logger.streamhandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.streamhandler.setLevel(logging.ERROR)
logger.addHandler(logger.streamhandler)

path_begin = "/mnt/c/Users/User/PycharmProjects"

new_orders = f"{path_begin}/eveESO/output/brazil/new_orders.json"
new_history = f"{path_begin}//eveESO/output/brazil/new_history.csv"
new_stats = f"{path_begin}/eveESO/output/brazil/new_stats.csv"
watchlist = f"{path_begin}/eveESO/output/brazil/watchlist.csv"
new_orderscsv = f"{path_begin}/eveESO/output/brazil/new_orders.csv"
new_doctrines = f"{path_begin}/eveESO/output/brazil/new_doctrines.csv"
ship_targets = f"{path_begin}/eveESO/data/ship_targets2.csv"

load_dotenv()

fly_mkt_url = os.getenv("FLY_WCMKT_URL")
fly_mkt_token = os.getenv("FLY_WCMKT_TOKEN")
mkt_url = f"sqlite+{fly_mkt_url}/?authToken={fly_mkt_token}&secure=true"

fly_sde_url = os.getenv("FLY_SDE_URL")
fly_sde_token = os.getenv("FLY_SDE_TOKEN")
sde_url = f"sqlite+{fly_sde_url}/?authToken={fly_sde_token}&secure=true"

fly_mkt_local = "wcmkt.db"
fly_sde_local = "sde.db"


# def example():

#     conn = libsql.connect("wcmkt.db", sync_url=fly_mkt_url, auth_token=fly_mkt_token)
#     conn.sync()

#     # noinspection SqlDialectInspection
#     conn.execute("""CREATE TABLE IF NOT EXISTS test_stats (
#     type_id INTEGER,
#     total_volume_remain INTEGER,
#     min_price FLOAT,
#     price_5th_percentile FLOAT,
#     avg_of_avg_price FLOAT,
#     avg_daily_volume FLOAT,
#     group_id INTEGER,
#     type_name TEXT,
#     group_name TEXT,
#     category_id INTEGER,
#     category_name TEXT,
#     days_remaining FLOAT,
#     last_update DATETIME
    
#     );""")

#     conn.close()
#     print("done")

def get_type_names(df):
    logger.info(f"getting type names for {len(df)} rows")
    logger.info(f"date: {datetime.now()}")
    logger.info(f"df: {df.head()}")
    logger.info(f"df columns: {df.columns}")
    logger.info("="*100)
    df.issued = pd.to_datetime(df.issued)
    df.drop(columns='type_name', inplace=True)
    type_ids = df.type_id.unique()
    typestr = ','.join(str(i) for i in type_ids)

    query = f"SELECT typeID, typeName FROM invTypes WHERE typeID IN ({typestr})"
    logger.info(f"query: {query}")
    try:
        engine = create_engine(sde_url, echo=True)
        with engine.connect() as conn:
            df2 = pd.read_sql_query(query, conn)
            df2.reset_index(drop=True, inplace=True)
            rn_map = {'typeID':'type_id', 'typeName':'type_name'}
            df2.rename(columns=rn_map, inplace=True)
            named_df = pd.merge(df, df2, on='type_id', how='left')
    except Exception as e:
        logger.info("*"*100)
        logger.error(f"error in get_type_names: {e}")
    
        logger.info("*"*100)
    logger.info("="*100)

    return named_df


def fetch_from_brazil(selected_items: list):
    engine = create_engine(mkt_url, echo=False)
    items = selected_items
    items_str = ','.join(str(i) for i in items)
    params = {'items': items_str}
    # noinspection SqlDialectInspection
    stmt = text(f"""
            SELECT * FROM marketstats 
            WHERE type_id IN (:items)
        """)

    # with Session(engine) as session:
    #     result = session.execute(stmt, params)
    #     rows = result.fetchall()

    with Session(engine) as session:
        result = session.query(MarketStats).filter(MarketStats.type_id.in_(items)).all()

    result_dict = [row.__dict__ for row in result]

    # noinspection PyTypeChecker
    df = pd.DataFrame(result_dict, columns=result_dict[0].keys())
    df.drop(columns=['_sa_instance_state'], inplace=True)
    old_cols = ['avg_price', 'group_id', 'group_name', 'category_name', 'last_update',
       'min_price', 'price', 'type_id', 'total_volume_remain', 'avg_volume',
       'type_name', 'category_id', 'days_remaining']
    new_cols = ['type_id', 'type_name', 'price', 'total_volume_remain', 'days_remaining','avg_volume','avg_price', 'group_name', 'category_name', 'group_id', 
        'category_id', 'last_update']
    
    rename_map = dict(zip(old_cols, new_cols))
    df.rename(columns=rename_map, inplace=True)

    return df


def update_history():
    logger.info(f"updating history")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    df = pd.read_csv(new_history)
    df.date = pd.to_datetime(df.date)
    data = df.to_dict(orient='records')

    engine = create_engine(mkt_url, echo=False)
    start = datetime.now()
    
    with Session(engine) as session:
        try:
            mapper = inspect(MarketHistory)
            session.bulk_insert_mappings(mapper, data)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.info("*"*100)
            logger.error(f'error: {e}')
            logger.info("*"*100)
    session.close()
    finish = datetime.now()
    history_time = finish - start
    logger.info(f"history time: {history_time}, rows: {len(data)}")
    logger.info("="*100)


def update_orders():
    logger.info(f"updating orders")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)

    df = pd.read_csv(new_orderscsv)
    df.issued = pd.to_datetime(df.issued)
    df.infer_objects()
    data = df.to_dict(orient='records')

    engine = create_engine(mkt_url, echo=False)
    start = datetime.now()

    with Session(engine) as session:
        try:
            # Get existing order IDs
            existing_orders = session.query(MarketOrders.order_id).all()
            existing_ids = {order[0] for order in existing_orders}
            
            # Split data into updates and inserts
            updates = []
            inserts = []
            for record in data:
                if record['order_id'] in existing_ids:
                    updates.append(record)
                else:
                    inserts.append(record)
            
            # Perform updates
            if updates:
                logger.info(f"Updating {len(updates)} existing orders")
                for record in updates:
                    session.query(MarketOrders).filter(
                        MarketOrders.order_id == record['order_id']
                    ).update(record)
            
            # Perform inserts
            if inserts:
                logger.info(f"Inserting {len(inserts)} new orders")
                mapper = inspect(MarketOrders)
                session.bulk_insert_mappings(mapper, inserts)
            
            session.commit()
        except Exception as e:
            session.rollback()
            logger.info("*"*100)
            logger.error(f'error: {e} in update_orders')
            logger.info("*"*100)

    finish = datetime.now()
    orders_time = finish - start
    logger.info(f"orders time: {orders_time}, rows: {len(data)}")
    logger.info("="*100)

def update_stats():
    logger.info(f"updating stats")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    df = pd.read_csv(new_stats)
    df.last_update = pd.to_datetime(df.last_update)

    # Rename avg_vol column to avg_volume to match the database model
    logger.info("Column names before renaming: %s", df.columns.tolist())
    df = df.rename(columns={'avg_vol': 'avg_volume'})
    logger.info("Column names after renaming: %s", df.columns.tolist())
    data = df.to_dict(orient='records')

    start = datetime.now()

    engine = create_engine(mkt_url, echo=False)
    with Session(engine) as session:
        try:
            mapper = inspect(MarketStats)
            session.bulk_insert_mappings(mapper, data)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.info("*"*100)
            logger.error(f'error: {e} in update_stats')
            logger.info("*"*100)

    finish = datetime.now()
    stats_time = finish - start
    logger.info(f"stats time: {stats_time}, rows: {len(data)}")
    logger.info("="*100)

def update_doctrines():
    start = datetime.now()
    logger.info(f"""
                {'='*100}
                updating doctrines table
                {'-'*100}
                """)
    df = pd.read_csv(new_doctrines)
    idrange = range(1,len(df)+1)
    df['id'] = idrange

    df = df.sort_values(by='timestamp', ascending=False)
    df.reset_index(drop=True, inplace=True)
    ts = df.timestamp[0]
    df.timestamp = df.timestamp.apply(lambda x: ts if x == str(0) else x)
    df.timestamp = pd.to_datetime(df.timestamp)
    df.rename(columns={'4H_price':'price'}, inplace=True)

    data = df.to_dict(orient='records')
    try:
        engine = create_engine(mkt_url,echo=False)    
        with Session(engine) as session:
            mapper = inspect(Doctrines)
            session.bulk_insert_mappings(mapper, data)
            session.commit()
    except Exception as e:
        session.rollback()
        logger.info("*"*100)
        logger.error(f'error: {e} in update_doctrines')
        logger.info("*"*100)

    finish = datetime.now()
    doctrines_time = finish - start

    logger.info(f"""
                {'-'*100}
                doctrines time: {doctrines_time}, rows: {len(data)}
                {'='*100}
                """)

def update_ship_targets():
    logger.info(f"updating ship targets")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    df = pd.read_csv(ship_targets)
    df.created_at = pd.to_datetime(df.created_at)

    data = df.to_dict(orient='records')

    engine = create_engine(mkt_url, echo=False)
    with Session(engine) as session:
        mapper = inspect(ShipTargets)
        session.bulk_insert_mappings(mapper, data)
        session.commit()

def main():
    logger.info(f"starting main")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    parser = argparse.ArgumentParser(description="options for market ESI calls")

    # Add arguments
    parser.add_argument("--hist",
                        action="store_true",
                        help="--hist to refresh history data"
                        )

    args = parser.parse_args()

    if args.hist:
        engine = create_engine(mkt_url, echo=False)
        conn = engine.connect()
        logger.info("Running Brazil update with full history refresh.")
        conn.execute(text("DROP TABLE IF EXISTS market_history"))
        conn.commit()

    else:
        logger.info("Running market update in quick mode, saved history data will be used.")

    logger.info(f"connecting to database: {mkt_url}")
    try:
        engine = create_engine(mkt_url, echo=False)
        conn = engine.connect()
        logger.info('removing and recreating tables...')
        conn.execute(text("DROP TABLE IF EXISTS marketstats"))
        conn.execute(text("DROP TABLE IF EXISTS doctrines"))
        conn.execute(text("DROP TABLE IF EXISTS ship_targets"))
        conn.commit()
        conn.close()
        logger.info('tables removed')

    except Exception as e:
        logger.error(f'error: {e}')
    
    try:
        logger.info('creating tables...')
        Base.metadata.create_all(engine)
        logger.info('tables created')
    except Exception as e:
        logger.info("*"*100)
        logger.error(f'error: {e} in create_tables')
        logger.info("*"*100)

    logger.info("starting db update...")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)

    #update history
    if args.hist:
        try:
            logger.info('updating table: history')
            logger.info('-'*100)
            update_history()
        except Exception as e:
            logger.info("*"*100)
            logger.error(f'error: {e} in update_history, returning to main')
            logger.info("*"*100)

    #update orders
    try:
        logger.info('updating table: orders')
        logger.info('-'*100)
        update_orders()
    except Exception as e:
        logger.info("*"*100)
        logger.error(f'error: {e} in update_orders, returning to main')
        logger.info("*"*100)

    #update stats
    try:
        logger.info('updating table: stats')
        logger.info('-'*100)
        update_stats()
    except Exception as e:
        logger.info("*"*100)
        logger.error(f'error: {e} in update_stats, returning to main')
        logger.info("*"*100)

    #update doctrines
    try:
        logger.info('updating table: doctrines')
        logger.info('-'*100)
        update_doctrines()
    except Exception as e:
        logger.info("*"*100)
        logger.error(f'error: {e} in update_doctrines, returning to main')
        logger.info("*"*100)

    logger.info('-'*100)

    #update ship targets
    try:
        logger.info('updating table: ship targets')
        logger.info('-'*100)
        update_ship_targets()
    except Exception as e:
        logger.info("*"*100)
        logger.error(f'error: {e} in update_ship_targets, returning to main')
        logger.info("*"*100)

if __name__ == "__main__":
    main()
