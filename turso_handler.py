import json
import libsql_experimental as libsql
import pandas as pd
import os
import shutil
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, select, text, inspect, Table
from sqlalchemy.orm import Session, base, DeclarativeBase, sessionmaker
import sqlalchemy_libsql
import sqlalchemy.dialects.sqlite
from datetime import timedelta, time, datetime
import argparse
import sqlite3
import numpy as np

from logging.handlers import RotatingFileHandler
from logging_config import setup_logging
from models import MarketStats, MarketOrders, Base, MarketHistory, Doctrines, ShipTargets

logger = setup_logging()

path_begin = "/mnt/c/Users/User/PycharmProjects"

new_orders = f"{path_begin}/eveESO/output/brazil/new_orders.json"
new_history = f"{path_begin}//eveESO/output/brazil/new_history.csv"
new_stats = f"{path_begin}/eveESO/output/brazil/new_stats.csv"
watchlist = f"{path_begin}/eveESO/output/brazil/watchlist.csv"
new_orderscsv = f"{path_begin}/eveESO/output/brazil/new_orders.csv"
new_doctrines = f"{path_begin}/eveESO/output/brazil/new_doctrines.csv"
ship_targets = f"{path_begin}/eveESO/data/ship_targets2.csv"
path = f"{path_begin}/eveESO/output/brazil/"
ship_targets_path = f"{path_begin}/eveESO/data/ship_targets.csv"

# Backup directory
backup_dir = f"{path_begin}/eveESO/backup"
os.makedirs(backup_dir, exist_ok=True)

load_dotenv()

fly_mkt_url = os.getenv("FLY_WCMKT_URL")
fly_mkt_token = os.getenv("FLY_WCMKT_TOKEN")
mkt_url = f"sqlite+{fly_mkt_url}/?authToken={fly_mkt_token}&secure=true"

fly_sde_url = os.getenv("FLY_SDE_URL")
fly_sde_token = os.getenv("FLY_SDE_TOKEN")
sde_url = f"sqlite+{fly_sde_url}/?authToken={fly_sde_token}&secure=true"

fly_mkt_local = "wcmkt.db"
fly_sde_local = "sde.db"

# Max retry attempts for database operations
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

def retry_operation(operation_func, *args, **kwargs):
    """Retry an operation with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Operation failed: {e}. Retrying in {wait_time} seconds... (Attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                logger.error(f"Operation failed after {MAX_RETRIES} attempts: {e}")
                raise

def backup_database():
    """Create a backup of the current database state."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{backup_dir}/wcmkt_backup_{timestamp}.db"
    
    try:
        # For local SQLite database
        if os.path.exists(fly_mkt_local):
            shutil.copy2(fly_mkt_local, backup_filename)
            logger.info(f"Local database backed up to {backup_filename}")
            return backup_filename
        
        # For remote Turso database - download a snapshot first
        engine = create_engine(mkt_url, echo=False)
        with engine.connect() as conn:
            # Get all tables and their data
            tables = {}
            for table_name in inspect(engine).get_table_names():
                result = conn.execute(text(f"SELECT * FROM {table_name}"))
                data = [dict(row) for row in result]
                tables[table_name] = data
            
            # Save the data to a JSON file as backup
            json_backup = f"{backup_dir}/wcmkt_backup_{timestamp}.json"
            with open(json_backup, 'w') as f:
                json.dump(tables, f)
            
            logger.info(f"Remote database backed up to {json_backup}")
            return json_backup
    except Exception as e:
        logger.error(f"Failed to create database backup: {e}")
        return None

def restore_database(backup_file):
    """Restore the database from a backup."""
    if not backup_file or not os.path.exists(backup_file):
        logger.error(f"Backup file not found: {backup_file}")
        return False
    
    try:
        if backup_file.endswith('.db'):
            # For local SQLite database
            shutil.copy2(backup_file, fly_mkt_local)
            logger.info(f"Database restored from {backup_file}")
        elif backup_file.endswith('.json'):
            # For remote Turso database - restore from JSON
            engine = create_engine(mkt_url, echo=False)
            with open(backup_file, 'r') as f:
                tables = json.load(f)
            
            with engine.connect() as conn:
                # Drop all existing tables and recreate from backup
                for table_name, data in tables.items():
                    if data:  # Only if there's data to restore
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        # Generate create table statements (simplified)
                        # In practice, you might need to determine the schema from the data
                        # For now, we'll rely on Base.metadata.create_all to recreate the schema
                
                # Recreate tables
                Base.metadata.create_all(engine)
                
                # Insert data
                for table_name, data in tables.items():
                    if data:
                        for row in data:
                            stmt = text(f"INSERT INTO {table_name} VALUES ({', '.join([':'+k for k in row.keys()])})")
                            conn.execute(stmt, row)
                conn.commit()
            
            logger.info(f"Database restored from {backup_file}")
        else:
            logger.error(f"Unknown backup file format: {backup_file}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Failed to restore database: {e}")
        return False

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
    df.timestamp = pd.to_datetime(df.timestamp)
    data = df.to_dict(orient='records')

    engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
    start = datetime.now()
    
    with Session(engine) as session:
        try:
            # Clear existing data
            logger.info(f"Clearing existing history data")
            session.execute(text("DELETE FROM market_history"))
            session.commit()
            
            # Insert new data in chunks
            logger.info(f"Inserting {len(data)} history records")
            if data:
                # Split into chunks
                chunk_size = 1000
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i+chunk_size]
                    logger.info(f"Processing chunk {i//chunk_size + 1}/{len(data)//chunk_size + 1} ({len(chunk)} records)")
                    mapper = inspect(MarketHistory)
                    session.bulk_insert_mappings(mapper, chunk)
                    session.commit()
                    
            logger.info(f"History update completed")
        except Exception as e:
            session.rollback()
            logger.info("*"*100)
            logger.error(f'error: {e} in update_history')
            logger.info("*"*100)
            raise
    
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
    
    # Replace inf and NaN values with None/NULL
    df = df.replace([np.inf, -np.inf], None)
    df = df.replace(np.nan, None)
    
    df.infer_objects()
    
    # Get only the columns that exist in the MarketOrders model
    valid_columns = [column.key for column in inspect(MarketOrders).columns]
    df = df[df.columns.intersection(valid_columns)]
    
    data = df.to_dict(orient='records')

    engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
    start = datetime.now()

    with Session(engine) as session:
        try:
            # First clear the existing data - more efficient for large updates
            logger.info(f"Clearing existing orders data")
            session.execute(text("DELETE FROM marketorders"))
            session.commit()
            
            # Then bulk insert all the new data at once
            logger.info(f"Inserting {len(data)} orders")
            if data:
                # Split into chunks to avoid overwhelming the database
                chunk_size = 1000
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i+chunk_size]
                    logger.info(f"Processing chunk {i//chunk_size + 1}/{len(data)//chunk_size + 1} ({len(chunk)} records)")
                    mapper = inspect(MarketOrders)
                    session.bulk_insert_mappings(mapper, chunk)
                    session.commit()
                    
            logger.info(f"Orders update completed")
        except Exception as e:
            session.rollback()
            logger.info("*"*100)
            logger.error(f'error: {e} in update_orders')
            logger.info("*"*100)
            raise

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

    engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
    with Session(engine) as session:
        try:
            # Clear existing data
            logger.info(f"Clearing existing stats data")
            session.execute(text("DELETE FROM marketstats"))
            session.commit()
            
            # Insert new data in chunks
            logger.info(f"Inserting {len(data)} stats records")
            if data:
                # Split into chunks
                chunk_size = 1000
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i+chunk_size]
                    logger.info(f"Processing chunk {i//chunk_size + 1}/{len(data)//chunk_size + 1} ({len(chunk)} records)")
                    mapper = inspect(MarketStats)
                    session.bulk_insert_mappings(mapper, chunk)
                    session.commit()
                    
            logger.info(f"Stats update completed")
        except Exception as e:
            session.rollback()
            logger.info("*"*100)
            logger.error(f'error: {e} in update_stats')
            logger.info("*"*100)
            raise

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
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})    
        with Session(engine) as session:
            # Clear existing data
            logger.info(f"Clearing existing doctrines data")
            session.execute(text("DELETE FROM doctrines"))
            session.commit()
            
            # Insert new data in chunks
            logger.info(f"Inserting {len(data)} doctrine records")
            if data:
                # Split into chunks
                chunk_size = 1000
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i+chunk_size]
                    logger.info(f"Processing chunk {i//chunk_size + 1}/{len(data)//chunk_size + 1} ({len(chunk)} records)")
                    mapper = inspect(Doctrines)
                    session.bulk_insert_mappings(mapper, chunk)
                    session.commit()
                    
            logger.info(f"Doctrines update completed")
    except Exception as e:
        session.rollback()
        logger.info("*"*100)
        logger.error(f'error: {e} in update_doctrines')
        logger.info("*"*100)
        raise

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

    start = datetime.now()
    engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
    with Session(engine) as session:
        try:
            # Clear existing data
            logger.info(f"Clearing existing ship targets data")
            session.execute(text("DELETE FROM ship_targets"))
            session.commit()
            
            # Insert new data in chunks
            logger.info(f"Inserting {len(data)} ship target records")
            if data:
                # Split into chunks
                chunk_size = 1000
                for i in range(0, len(data), chunk_size):
                    chunk = data[i:i+chunk_size]
                    logger.info(f"Processing chunk {i//chunk_size + 1}/{len(data)//chunk_size + 1} ({len(chunk)} records)")
                    mapper = inspect(ShipTargets)
                    session.bulk_insert_mappings(mapper, chunk)
                    session.commit()
                    
            logger.info(f"Ship targets update completed")
        except Exception as e:
            session.rollback()
            logger.info("*"*100)
            logger.error(f'error: {e} in update_ship_targets')
            logger.info("*"*100)
            raise
    
    finish = datetime.now()
    ship_time = finish - start
    logger.info(f"ship targets time: {ship_time}, rows: {len(data)}")
    logger.info("="*100)

def check_tables():
    logger.info(f"checking tables")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)

    table_dict = {}

    tables = os.listdir(path)

    for table in tables:
        logger.info("="*100)
        if table.endswith(".csv"):
            df = pd.read_csv(path + table)
            table_name = table.split(".")[0]
            table_dict[table_name] = len(df)
            logger.info(f"{table_name}: {len(df)}")
    df = pd.read_csv(ship_targets_path)
    table_dict['ship_targets'] = len(df)
    logger.info("="*100)
    return table_dict

def safe_update_operation(operation_func, description, table_dict, table_name, min_rows=1):
    """Safely perform an update operation with backup and restore on failure."""
    logger.info(f'Beginning {description}...')
    
    if table_dict[table_name] <= min_rows:
        logger.error(f"{table_name} table has insufficient data ({table_dict[table_name]} rows), skipping update.")
        return False

    # Create backup before performing the operation
    backup_file = backup_database()
    
    try:
        # Try to perform the operation with retries
        retry_operation(operation_func)
        logger.info(f'{description} completed successfully.')
        return True
    except Exception as e:
        logger.error(f'Error in {description}: {e}')
        if backup_file:
            logger.info(f'Attempting to restore database from backup: {backup_file}')
            if restore_database(backup_file):
                logger.info(f'Database successfully restored from backup.')
            else:
                logger.error(f'Failed to restore database from backup!')
        else:
            logger.error(f'No backup available for restore!')
        return False

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
    table_dict = check_tables()

    logger.info(f"connecting to database: {mkt_url}")
    
    # Make sure tables exist
    try:
        engine = create_engine(mkt_url, echo=False)
        Base.metadata.create_all(engine)
        logger.info('Ensured all tables exist')
    except Exception as e:
        logger.error(f'Error ensuring tables exist: {e}')
        return

    logger.info("starting db update...")
    logger.info(f"date: {datetime.now()}\n\n")
    logger.info("="*100)

    # Update history if requested
    if args.hist and table_dict.get('new_history', 0) > 1:
        safe_update_operation(
            update_history,
            'updating table: history',
            table_dict,
            'new_history'
        )

    # Update orders
    safe_update_operation(
        update_orders,
        'updating table: orders',
        table_dict,
        'new_orders'
    )

    # Update stats
    safe_update_operation(
        update_stats,
        'updating table: stats',
        table_dict,
        'new_stats'
    )

    # Update doctrines
    safe_update_operation(
        update_doctrines,
        'updating table: doctrines',
        table_dict,
        'new_doctrines'
    )

    # Update ship targets
    safe_update_operation(
        update_ship_targets,
        'updating table: ship targets',
        table_dict,
        'ship_targets'
    )

    logger.info("Database update process completed.")

if __name__ == "__main__":
    main()