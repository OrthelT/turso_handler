import json
import libsql_experimental as libsql
import pandas as pd
import os
import shutil
import time as time_module  # Rename to avoid conflict with datetime.time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import Session
from datetime import datetime
import argparse
import numpy as np

from logging_config import setup_logging
from models import MarketStats, MarketOrders, Base, MarketHistory, Doctrines, ShipTargets, DoctrineMap
from doctrine_data import preprocess_doctrine_fits

logger = setup_logging(__name__)

path_begin = "/mnt/c/Users/User/PycharmProjects"

new_orders = f"{path_begin}/eveESO/output/brazil/new_orders.json"
new_history = f"{path_begin}//eveESO/output/brazil/new_history.csv"
new_stats = f"{path_begin}/eveESO/output/brazil/new_stats.csv"
watchlist = f"{path_begin}/eveESO/output/brazil/watchlist.csv"
new_orderscsv = f"{path_begin}/eveESO/output/brazil/new_orders.csv"
new_doctrines = f"{path_begin}/eveESO/output/brazil/new_doctrines.csv"
doctrine_map = f"{path_begin}/eveESO/output/brazil/doctrine_map.csv"
lead_ships = f"{path_begin}/eveESO/output/brazil/lead_ships.csv"

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

# Updated chunk size to avoid message size limit errors
CHUNK_SIZE = 2000  # increased to 2000 to improve performance
reporting_ok = []
reporting_failed = []

restore_map = {'doctrine_fits': 'doctrine_fits', 
                   'doctrine_map': 'doctrine_map', 
                   'lead_ships': 'lead_ships', 
                   'new_doctrines': 'doctrines', 
                   'new_history': 'market_history', 
                   'new_orders': 'marketorders', 
                   'new_stats': 'marketstats', 
                   'ship_targets': 'ship_targets'}

def handle_null_columns(df, model_class):
    """
    Detects and handles completely null columns in a DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to check
        model_class: The SQLAlchemy model class to check against
        
    Returns:
        pd.DataFrame: DataFrame with null columns handled appropriately
    """
    logger.info(f"Checking for null columns in dataframe with {len(df)} rows")
    
    # Get column info from model
    model_columns = {}
    for column in inspect(model_class).columns:
        model_columns[column.key] = {
            'nullable': column.nullable,
            'type': str(column.type),
            'default': column.default,
        }
    
    # Check for completely null columns
    null_columns = []
    for col in df.columns:
        if col in model_columns and df[col].isna().all():
            null_columns.append(col)
    
    if null_columns:
        logger.warning(f"Found completely null columns: {null_columns}")
        
        # Handle each null column based on its SQLAlchemy type
        for col in null_columns:
            col_info = model_columns.get(col, {})
            col_type = col_info.get('type', '')
            
            # If column is not nullable in the model, we need to provide a default value
            if not col_info.get('nullable', True):
                logger.warning(f"Column {col} is not nullable, must provide default values")
                
                # Handle different data types
                if 'INT' in col_type.upper():
                    df[col] = 0
                    logger.info(f"Set default value 0 for integer column: {col}")
                elif 'FLOAT' in col_type.upper() or 'DOUBLE' in col_type.upper() or 'REAL' in col_type.upper():
                    df[col] = 0.0
                    logger.info(f"Set default value 0.0 for float column: {col}")
                elif 'VARCHAR' in col_type.upper() or 'TEXT' in col_type.upper() or 'CHAR' in col_type.upper():
                    df[col] = ''
                    logger.info(f"Set default value '' for string column: {col}")
                elif 'DATETIME' in col_type.upper() or 'TIMESTAMP' in col_type.upper():
                    df[col] = datetime.now()
                    logger.info(f"Set default value current timestamp for datetime column: {col}")
                elif 'BOOLEAN' in col_type.upper():
                    df[col] = False
                    logger.info(f"Set default value False for boolean column: {col}")
                else:
                    logger.warning(f"Unknown column type {col_type} for {col}, setting to None")
            else:
                logger.info(f"Column {col} is nullable, keeping as NULL/None")
    
    # Check for required columns that are missing
    required_columns = [col for col, info in model_columns.items() 
                        if not info.get('nullable', True) and col not in df.columns]
    
    if required_columns:
        logger.warning(f"Missing required columns: {required_columns}")
        
        # Add missing required columns with default values
        for col in required_columns:
            col_info = model_columns.get(col, {})
            col_type = col_info.get('type', '')
            
            # Set defaults based on column type
            if 'INT' in col_type.upper():
                df[col] = 0
            elif 'FLOAT' in col_type.upper() or 'DOUBLE' in col_type.upper() or 'REAL' in col_type.upper():
                df[col] = 0.0
            elif 'VARCHAR' in col_type.upper() or 'TEXT' in col_type.upper() or 'CHAR' in col_type.upper():
                df[col] = ''
            elif 'DATETIME' in col_type.upper() or 'TIMESTAMP' in col_type.upper():
                df[col] = datetime.now()
            elif 'BOOLEAN' in col_type.upper():
                df[col] = False
            else:
                df[col] = None
                
            logger.info(f"Added missing required column {col} with default value")
    
    return df

def retry_operation(operation_func, *args, **kwargs):
    """Retry an operation with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Operation failed: {e}. Retrying in {wait_time} seconds... (Attempt {attempt+1}/{MAX_RETRIES})")
                time_module.sleep(wait_time)
            else:
                logger.error(f"Operation failed after {MAX_RETRIES} attempts: {e}")
                raise

def backup_database():
    """Create a backup of the current database state."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{backup_dir}/wcmkt_backup_{timestamp}.db"
    
    # First, we will sync the remote database to the local database using libsql
    try:
        conn = libsql.connect(fly_mkt_local, sync_url=fly_mkt_url, auth_token=fly_mkt_token)
        conn.sync()
    except Exception as e:
        logger.error(f"Failed to sync database: {e}")

    # Then, we will copy the local database to the backup directory
    shutil.copy2(fly_mkt_local, backup_filename)
    logger.info(f"Database backed up to {backup_filename}")
    return backup_filename

def remove_old_db():
    """Remove old database backups."""
    db_path = "/mnt/c/Users/User/PycharmProjects/eveESO/backup/"
    db_dict = {}
    db_list = [file for file in os.listdir(db_path) if file.endswith(".db")]
    removed_dbs = []
    if len(db_list) > 3:
        logger.info(f"found {len(db_list)} db files, removing old ones")

        for file in db_list:
            ctime = os.path.getctime(os.path.join(db_path, file))
            db_dict[file] = ctime

        sorted_db_dict = sorted(db_dict.items(), key=lambda x: x[1], reverse=True)
        old_db = sorted_db_dict[2:]
        for file, ctime in old_db:
            db = os.path.join(db_path, file)
            os.remove(db)
            logger.info(f"removed {file}")
            removed_dbs.append(file)
        logger.info(f"found {len(removed_dbs)} old db files, removed {removed_dbs}")
        return removed_dbs

    else:
        logger.info(f"found {len(db_list)} db files, no need to remove")
        return None

def get_most_recent_backup(backup_dir):
    """Get the most recent backup file."""
    db_path = backup_dir
    db_list = [file for file in os.listdir(db_path) if file.endswith(".db")]
    db_list.sort(key=lambda x: os.path.getctime(os.path.join(db_path, x)), reverse=True)
    return os.path.join(db_path, db_list[0])

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

def get_backup_table(table_name)->pd.DataFrame:
    table = table_name
    most_recent_backup = get_most_recent_backup(backup_dir)
    engine = create_engine(f"sqlite:///{most_recent_backup}")
    with engine.connect() as conn:
        df = pd.read_sql_table(table, conn)
        return df

def update_history():
    logger.info("updating history")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    try:
        df = pd.read_csv(new_history)
        df.date = pd.to_datetime(df.date)
        df.timestamp = pd.to_datetime(df.timestamp)
        
        # Ensure id column is unique and sequential
        if 'id' in df.columns:
            # Generate new sequential IDs to avoid conflicts
            df['id'] = range(1, len(df) + 1)
        else:
            # Add an id column if it doesn't exist
            df.insert(0, 'id', range(1, len(df) + 1))
        
        # Handle null columns
        df = handle_null_columns(df, MarketHistory)
        
        # Validate that we have data before proceeding
        if df.empty:
            logger.error("History dataframe is empty. Aborting update to prevent data loss.")
            raise ValueError("Empty dataframe - aborting to prevent data loss")
        
        data = df.to_dict(orient='records')
        if not data:
            logger.error("No records found in history data. Aborting update to prevent data loss.")
            raise ValueError("No records in data - aborting to prevent data loss")
            
        logger.info(f"Prepared {len(data)} history records for insertion")
    
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
        start = datetime.now()
        
        # Only store record count, not full data (to avoid memory issues)
        record_count = 0
        with Session(engine) as session:
            try:
                result = session.execute(text("SELECT COUNT(*) FROM market_history")).scalar()
                record_count = result or 0
                logger.info(f"Found {record_count} existing history records")
            except Exception as e:
                logger.warning(f"Could not count existing records: {e}")
        
        with Session(engine) as session:
            try:
                # Only delete if we have new data
                logger.info("Clearing existing history data")
                try:
                    session.execute(text("DELETE FROM market_history"))
                    session.commit()
                except Exception as e:
                    logger.warning(f"Could not clear market_history table: {e}")
                    session.rollback()
                
                # Insert new data in chunks
                logger.info(f"Inserting {len(data)} history records")
                if data:
                    # Split into chunks
                    for i in range(0, len(data), CHUNK_SIZE):
                        chunk = data[i:i+CHUNK_SIZE]
                        logger.info(f"Processing chunk {i//CHUNK_SIZE + 1}/{(len(data)-1)//CHUNK_SIZE + 1} ({len(chunk)} records)")
                        mapper = inspect(MarketHistory)
                        session.bulk_insert_mappings(mapper, chunk)
                        session.commit()
                        
                logger.info("History update completed")
            except Exception as e:
                session.rollback()
                logger.info("*"*100)
                logger.error(f'error: {e} in update_history')
                logger.info("*"*100)
                
                # Log that we can't restore because we didn't keep the full backup
                if record_count > 0:
                    logger.error(f"Cannot rollback {record_count} history records - they were not backed up in memory")
                raise
        
        finish = datetime.now()
        history_time = finish - start
        logger.info(f"history time: {history_time}, rows: {len(data)}")
        logger.info("="*100)
        
    except Exception as e:
        logger.error(f"Failed to update history: {e}")
        raise

def update_orders():
    logger.info("updating orders")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)

    try:
        df = pd.read_csv(new_orderscsv)
        df.issued = pd.to_datetime(df.issued)
        
        # Replace inf and NaN values with None/NULL
        df = df.replace([np.inf, -np.inf], None)
        df = df.replace(np.nan, None)
        
        df.infer_objects()
        
        # Check if we need to add an ID column
        if 'id' in df.columns:
            # Generate new sequential IDs to avoid conflicts
            df['id'] = range(1, len(df) + 1)
        elif 'order_id' not in df.columns:
            # Only add an id if there's no order_id (which might be the primary key)
            df.insert(0, 'id', range(1, len(df) + 1))
        
        # Get only the columns that exist in the MarketOrders model
        valid_columns = [column.key for column in inspect(MarketOrders).columns]
        df = df[df.columns.intersection(valid_columns)]
        
        # Handle null columns
        df = handle_null_columns(df, MarketOrders)
        
        # Validate data
        if df.empty:
            logger.error("Orders dataframe is empty. Aborting update to prevent data loss.")
            raise ValueError("Empty dataframe - aborting to prevent data loss")
        
        data = df.to_dict(orient='records')
        if not data:
            logger.error("No records found in orders data. Aborting update to prevent data loss.")
            raise ValueError("No records in data - aborting to prevent data loss")
            
        logger.info(f"Prepared {len(data)} order records for insertion")
    
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
        start = datetime.now()
        
        # Only store record count, not full data (to avoid memory issues)
        record_count = 0
        with Session(engine) as session:
            try:
                result = session.execute(text("SELECT COUNT(*) FROM marketorders")).scalar()
                record_count = result or 0
                logger.info(f"Found {record_count} existing order records")
            except Exception as e:
                logger.warning(f"Could not count existing records: {e}")
    
        with Session(engine) as session:
            try:
                # First clear the existing data - more efficient for large updates
                logger.info("Clearing existing orders data")
                try:
                    session.execute(text("DELETE FROM marketorders"))
                    session.commit()
                except Exception as e:
                    logger.warning(f"Could not clear marketorders table: {e}")
                    session.rollback()
                
                # Then bulk insert all the new data at once
                logger.info(f"Inserting {len(data)} orders")
                if data:
                    # Split into smaller chunks to avoid exceeding message size limits
                    for i in range(0, len(data), CHUNK_SIZE):
                        chunk = data[i:i+CHUNK_SIZE]
                        logger.info(f"Processing chunk {i//CHUNK_SIZE + 1}/{(len(data)-1)//CHUNK_SIZE + 1} ({len(chunk)} records)")
                        mapper = inspect(MarketOrders)
                        session.bulk_insert_mappings(mapper, chunk)
                        session.commit()
                        
                logger.info("Orders update completed")
            except Exception as e:
                session.rollback()
                logger.info("*"*100)
                logger.error(f'error: {e} in update_orders')
                logger.info("*"*100)
                
                # Log that we can't restore because we didn't keep the full backup
                if record_count > 0:
                    logger.error(f"Cannot rollback {record_count} order records - they were not backed up in memory")
                    logger.info("restoring from backup instead")
                raise
    
        finish = datetime.now()
        orders_time = finish - start
        logger.info(f"orders time: {orders_time}, rows: {len(data)}")
        logger.info("="*100)
        
    except Exception as e:
        logger.error(f"Failed to update orders: {e}")
        raise

def update_stats():
    logger.info("updating stats")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    try:
        df = pd.read_csv(new_stats)
        df.last_update = pd.to_datetime(df.last_update)
    
        # Rename avg_vol column to avg_volume to match the database model
        df = df.rename(columns={'avg_vol': 'avg_volume'})
        
        # Ensure id column is unique and sequential
        if 'id' in df.columns:
            # Generate new sequential IDs to avoid conflicts
            df['id'] = range(1, len(df) + 1)
        else:
            # Add an id column if it doesn't exist (and type_id isn't the primary key)
            if 'type_id' not in df.columns or not inspect(MarketStats).primary_key[0].name == 'type_id':
                df.insert(0, 'id', range(1, len(df) + 1))
        
        # Handle null columns
        df = handle_null_columns(df, MarketStats)
        
        # Validate data
        if df.empty:
            logger.error("Stats dataframe is empty. Aborting update to prevent data loss.")
            raise ValueError("Empty dataframe - aborting to prevent data loss")
        
        data = df.to_dict(orient='records')
        if not data:
            logger.error("No records found in stats data. Aborting update to prevent data loss.")
            raise ValueError("No records in data - aborting to prevent data loss")
        
        logger.info(f"Prepared {len(data)} stats records for insertion")
    
        start = datetime.now()
    
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
        
        # Only store record count, not full data (to avoid memory issues)
        record_count = 0
        with Session(engine) as session:
            try:
                result = session.execute(text("SELECT COUNT(*) FROM marketstats")).scalar()
                record_count = result or 0
                logger.info(f"Found {record_count} existing stat records")
            except Exception as e:
                logger.warning(f"Could not count existing records: {e}")
                
        with Session(engine) as session:
            try:
                # Clear existing data
                logger.info("Clearing existing stats data")
                try:
                    session.execute(text("DELETE FROM marketstats"))
                    session.commit()
                except Exception as e:
                    logger.warning(f"Could not clear marketstats table: {e}")
                    session.rollback()
                
                # Insert new data in chunks
                logger.info(f"Inserting {len(data)} stats records")
                if data:
                    # Split into smaller chunks
                    for i in range(0, len(data), CHUNK_SIZE):
                        chunk = data[i:i+CHUNK_SIZE]
                        logger.info(f"Processing chunk {i//CHUNK_SIZE + 1}/{(len(data)-1)//CHUNK_SIZE + 1} ({len(chunk)} records)")
                        mapper = inspect(MarketStats)
                        session.bulk_insert_mappings(mapper, chunk)
                        session.commit()
                        
                logger.info("Stats update completed")
            except Exception as e:
                session.rollback()
                logger.info("*"*100)
                logger.error(f'error: {e} in update_stats')
                logger.info("*"*100)
                
                # Log that we can't restore because we didn't keep the full backup
                if record_count > 0:
                    logger.error(f"Cannot restore {record_count} stat records - they were not backed up in memory")
                
                raise
    
        finish = datetime.now()
        stats_time = finish - start
        logger.info(f"stats time: {stats_time}, rows: {len(data)}")
        logger.info("="*100)
        
    except Exception as e:
        logger.error(f"Failed to update stats: {e}")
        raise

def update_doctrines():
    logger.info(f"""
                {'='*100}
                updating doctrines table
                {'-'*100}
                """)
                
    try:
        start = datetime.now()
        df = pd.read_csv(new_doctrines)
        
        # Already has a good ID strategy, but let's ensure it's 1-based
        idrange = range(1, len(df) + 1)
        df['id'] = idrange
    
        df = df.sort_values(by='timestamp', ascending=False)
        df.reset_index(drop=True, inplace=True)
        ts = df.timestamp[0]
        df.timestamp = df.timestamp.apply(lambda x: ts if x == str(0) else x)
        df.timestamp = pd.to_datetime(df.timestamp)
        df.rename(columns={'4H_price':'price'}, inplace=True)
        
        # Handle null columns
        df = handle_null_columns(df, Doctrines)
        
        # Validate data
        if df.empty:
            logger.error("Doctrines dataframe is empty. Aborting update to prevent data loss.")
            raise ValueError("Empty dataframe - aborting to prevent data loss")
        
        data = df.to_dict(orient='records')
        if not data:
            logger.error("No records found in doctrines data. Aborting update to prevent data loss.")
            raise ValueError("No records in data - aborting to prevent data loss")
            
        logger.info(f"Prepared {len(data)} doctrine records for insertion")
        
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
        
        # Only store record count, not full data
        record_count = 0
        with Session(engine) as session:
            try:
                result = session.execute(text("SELECT COUNT(*) FROM doctrines")).scalar()
                record_count = result or 0
                logger.info(f"Found {record_count} existing doctrine records")
            except Exception as e:
                logger.warning(f"Could not count existing records: {e}")
                
        with Session(engine) as session:
            try:
                # Clear existing data
                logger.info("Clearing existing doctrines data")
                try:
                    session.execute(text("DELETE FROM doctrines"))
                    session.commit()
                except Exception as e:
                    logger.warning(f"Could not clear doctrines table: {e}")
                    session.rollback()
                
                # Insert new data in chunks
                logger.info(f"Inserting {len(data)} doctrine records")
                if data:
                    # Split into smaller chunks
                    for i in range(0, len(data), CHUNK_SIZE):
                        chunk = data[i:i+CHUNK_SIZE]
                        logger.info(f"Processing chunk {i//CHUNK_SIZE + 1}/{(len(data)-1)//CHUNK_SIZE + 1} ({len(chunk)} records)")
                        mapper = inspect(Doctrines)
                        session.bulk_insert_mappings(mapper, chunk)
                        session.commit()
                        
                logger.info("Doctrines update completed")
            except Exception as e:
                session.rollback()
                logger.info("*"*100)
                logger.error(f'error: {e} in update_doctrines')
                logger.info("*"*100)
                
                # Log that we can't restore because we didn't keep the full backup
                if record_count > 0:
                    logger.error(f"Cannot restore {record_count} doctrine records - they were not backed up in memory")
                
                raise
    
        finish = datetime.now()
        doctrines_time = finish - start
    
        logger.info(f"""
                    {'-'*100}
                    doctrines time: {doctrines_time}, rows: {len(data)}
                    {'='*100}
                    """)
    except Exception as e:
        logger.error(f"Failed to update doctrines: {e}")
        raise

def update_ship_targets():
    logger.info("updating ship targets")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    try:
        df = pd.read_csv(ship_targets)
        df.created_at = pd.to_datetime(df.created_at)
        
        # Ensure id column is unique and sequential
        # First check if id is in the DataFrame
        if 'id' in df.columns:
            # Generate new sequential IDs to avoid conflicts
            df['id'] = range(1, len(df) + 1)
        else:
            # Add an id column if it doesn't exist
            df.insert(0, 'id', range(1, len(df) + 1))
        
        # Handle null columns
        df = handle_null_columns(df, ShipTargets)
        
        # Validate that we have data before proceeding
        if df.empty:
            logger.error("Ship targets dataframe is empty. Aborting update to prevent data loss.")
            raise ValueError("Empty dataframe - aborting to prevent data loss")
        
        data = df.to_dict(orient='records')
        if not data:
            logger.error("No records found in ship targets data. Aborting update to prevent data loss.")
            raise ValueError("No records in data - aborting to prevent data loss")
            
        logger.info(f"Prepared {len(data)} ship target records for insertion")
    
        start = datetime.now()
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
        
        # Only store record count, not full data
        record_count = 0
        with Session(engine) as session:
            try:
                result = session.execute(text("SELECT COUNT(*) FROM ship_targets")).scalar()
                record_count = result or 0
                logger.info(f"Found {record_count} existing ship target records")
            except Exception as e:
                logger.warning(f"Could not count existing records: {e}")
        
        with Session(engine) as session:
            try:
                # Clear existing data only after we've validated new data
                logger.info("Clearing existing ship targets data")
                try:
                    session.execute(text("DELETE FROM ship_targets"))
                    session.commit()
                except Exception as e:
                    logger.warning(f"Could not clear ship_targets table: {e}")
                    session.rollback()
                
                # Insert new data in chunks
                logger.info(f"Inserting {len(data)} ship target records")
                if data:
                    # Split into smaller chunks
                    for i in range(0, len(data), CHUNK_SIZE):
                        chunk = data[i:i+CHUNK_SIZE]
                        logger.info(f"Processing chunk {i//CHUNK_SIZE + 1}/{(len(data)-1)//CHUNK_SIZE + 1} ({len(chunk)} records)")
                        mapper = inspect(ShipTargets)
                        session.bulk_insert_mappings(mapper, chunk)
                        session.commit()
                        
                logger.info("Ship targets update completed")
            except Exception as e:
                session.rollback()
                logger.info("*"*100)
                logger.error(f'error: {e} in update_ship_targets')
                logger.info("*"*100)
                
                # Log that we can't restore because we didn't keep the full backup
                if record_count > 0:
                    logger.error(f"Cannot restore {record_count} ship target records - they were not backed up in memory")
                
                raise
        
        finish = datetime.now()
        ship_time = finish - start
        logger.info(f"ship targets time: {ship_time}, rows: {len(data)}")
        logger.info("="*100)
        
    except Exception as e:
        logger.error(f"Failed to update ship targets: {e}")
        raise

def update_doctrine_map():
    logger.info("updating doctrine map")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    try:
        df = pd.read_csv(doctrine_map)
        
        # Ensure id column is unique and sequential
        if 'id' in df.columns:
            # Generate new sequential IDs to avoid conflicts
            df['id'] = range(1, len(df) + 1)
        else:
            # Add an id column if it doesn't exist
            df.insert(0, 'id', range(1, len(df) + 1))
        
        # Handle null columns
        df = handle_null_columns(df, DoctrineMap)
        
        # Validate that we have data before proceeding
        if df.empty:
            logger.error("Doctrine map dataframe is empty. Aborting update to prevent data loss.")
            raise ValueError("Empty dataframe - aborting to prevent data loss")
        
        data = df.to_dict(orient='records')
        if not data:
            logger.error("No records found in doctrine map data. Aborting update to prevent data loss.")
            raise ValueError("No records in data - aborting to prevent data loss")
            
        logger.info(f"Prepared {len(data)} doctrine map records for insertion")
    
        start = datetime.now()
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
        
        # Only store record count, not full data
        record_count = 0
        with Session(engine) as session:
            try:
                result = session.execute(text("SELECT COUNT(*) FROM doctrine_map")).scalar()
                record_count = result or 0
                logger.info(f"Found {record_count} existing doctrine map records")
            except Exception as e:
                logger.warning(f"Could not count existing records: {e}")
        
        with Session(engine) as session:
            try:
                # Clear existing data only after we've validated new data
                logger.info("Clearing existing doctrine map data")
                try:
                    session.execute(text("DELETE FROM doctrine_map"))
                    session.commit()
                except Exception as e:
                    logger.warning(f"Could not clear doctrine_map table: {e}")
                    session.rollback()
                
                # Insert new data in chunks
                logger.info(f"Inserting {len(data)} doctrine map records")
                if data:
                    # Doctrine map is small, no need to chunk
                    logger.info(f"Processing all {len(data)} records at once")
                    mapper = inspect(DoctrineMap)
                    session.bulk_insert_mappings(mapper, data)
                    session.commit()
                        
                logger.info("Doctrine map update completed")
            except Exception as e:
                session.rollback()
                logger.info("*"*100)
                logger.error(f'error: {e} in update_doctrine_map')
                logger.info("*"*100)
                
                # Log that we can't restore because we didn't keep the full backup
                if record_count > 0:
                    logger.error(f"Cannot restore {record_count} doctrine map records - they were not backed up in memory")
                
                raise
        
        finish = datetime.now()
        doctrine_map_time = finish - start
        logger.info(f"doctrine map time: {doctrine_map_time}, rows: {len(data)}")
        logger.info("="*100)
        
    except Exception as e:
        logger.error(f"Failed to update doctrine map: {e}")
        raise

def update_doctrine_fits():
    logger.info("updating doctrine fits")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    try:
        df = preprocess_doctrine_fits()

    except Exception as e:
        logger.error(f"Failed to update doctrine fits: {e}")
        reporting_failed.append('doctrine_fits')
        return False
    
    turso_url = f"sqlite+{fly_mkt_url}/?authToken={fly_mkt_token}&secure=true"
    engine = create_engine(turso_url)

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS doctrine_fits"))
            conn.execute(text("CREATE TABLE doctrine_fits (id INTEGER PRIMARY KEY AUTOINCREMENT, doctrine_name TEXT, fit_name TEXT, ship_type_id INTEGER, doctrine_id INTEGER, fit_id INTEGER, ship_name TEXT, target INTEGER)"))
            conn.execute(text("INSERT INTO doctrine_fits (doctrine_name, fit_name, ship_type_id, doctrine_id, fit_id, ship_name, target) VALUES (:doctrine_name, :fit_name, :ship_type_id, :doctrine_id, :fit_id, :ship_name, :target)"), df.to_dict(orient='records'))
            conn.commit()

    except Exception as e:
        logger.error(f"Failed to update doctrine fits: {e}")
        reporting_failed.append('doctrine_fits')
        return False

    return True

def update_lead_ships():
    logger.info("updating lead ships")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    try:
        df = pd.read_csv(lead_ships)
        engine = create_engine(mkt_url)
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS lead_ships"))
            conn.execute(text("CREATE TABLE lead_ships (id INTEGER PRIMARY KEY AUTOINCREMENT, doctrine_name TEXT, doctrine_id INTEGER, lead_ship INTEGER, fit_id INTEGER)"))
            conn.execute(text("INSERT INTO lead_ships (doctrine_name, doctrine_id, lead_ship, fit_id) VALUES (:doctrine_name, :doctrine_id, :lead_ship, :fit_id)"), df.to_dict(orient='records'))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to update lead ships: {e}")
        reporting_failed.append('lead_ships')
        return False

    return True

def check_tables():
    logger.info("checking tables")
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
    
    # Add doctrine_map to table_dict
    try:
        df = pd.read_csv(doctrine_map)
        table_dict['doctrine_map'] = len(df)
        logger.info(f"doctrine_map: {len(df)}")
    except Exception as e:
        logger.warning(f"Could not read doctrine_map: {e}")
        table_dict['doctrine_map'] = 0
        
    logger.info("="*100)
    return table_dict

def safe_update_operation(operation_func, description, table_dict, table_name, min_rows=1):
    """Safely perform an update operation with backup and restore on failure."""
    logger.info(f'Beginning {description}...')
    
    if table_dict[table_name] <= min_rows:
        logger.error(f"{table_name} table has insufficient data ({table_dict[table_name]} rows), skipping update.")
        reporting_failed.append(table_name)
        return False
    
    try:
        # Try to perform the operation with retries
        retry_operation(operation_func)
        logger.info(f'{description} completed successfully.')
        reporting_ok.append(table_name)
        print(f'{description} completed successfully.')
        return True
    except Exception as e:
        logger.error(f'Error in {description}: {e}')
        reporting_failed.append(table_name)
        return False

def restore_tables(table, db_url, backup_dir, table_map):
    restored_table = table_map[table]
    logger.info(f"restoring {table} to {db_url} as {restored_table}")

    backup_db = get_most_recent_backup(backup_dir)
    backup_engine = create_engine(f"sqlite:///{backup_db}")
    with backup_engine.connect() as conn:
        df = pd.read_sql_table(restored_table, conn)
    engine = create_engine(db_url)
    with engine.connect() as conn:
        try:
            df.to_sql(restored_table, conn, if_exists='replace', index=False)
            logger.info(f"table: {restored_table} restored successfully")
            return True
        except Exception as e:
            logger.error(f"Error restoring {restored_table}: {e}")
            return False




def main():
    logger.info("starting db update main function")
    print(f"date: {datetime.now()}")
    print("starting db update main function")
    print("="*100)
    print("performing data integrity checks")
 
    print("="*100)
    parser = argparse.ArgumentParser(description="options for market ESI calls")

    # Add arguments
    parser.add_argument("--hist",
                        action="store_true",
                        help="--hist to refresh history data"
                        )
    parser.add_argument("--update_fits",
                        action="store_true",
                        help="--update_fits to refresh doctrine fits data"
                        )
    parser.add_argument("--update_lead_ships",
                        action="store_true",
                        help="--update_lead_ships to refresh lead ships data"
                        )

    args = parser.parse_args()
    table_dict = check_tables()

    logger.info(f"connecting to database: {mkt_url}")
    print(f"connecting to database: {mkt_url}")
    print("backing up database")
    db_file = backup_database()
    logger.info(f"database backed up to: {db_file}")
    print(f"database backed up to: {db_file}")
    logger.info("removing old db files")
    print("removing old db files")
    removed_dbs = remove_old_db()
    logger.info(f"removed {removed_dbs} old db files")
    print(f"removed {removed_dbs} old db files")
    
    print("="*100)

    # Make sure tables exist
    try:
        engine = create_engine(mkt_url, echo=False)
        Base.metadata.create_all(engine)
        logger.info('Ensured all tables exist')
    except Exception as e:
        logger.error(f'Error ensuring tables exist: {e}')
        return

    logger.info("starting db update...")
    print("starting db update...")
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

    # Update doctrine map
    if args.update_fits:
        safe_update_operation(
            update_doctrine_map,
            'updating table: doctrine map',
            table_dict,
            'doctrine_map'
        )

    # Update doctrine fits
    if args.update_fits:
        safe_update_operation(
            update_doctrine_fits,
            'updating table: doctrine fits',
            table_dict,
            'doctrine_fits'
        )
    
    # Update lead ships
    if args.update_lead_ships:
        safe_update_operation(
            update_lead_ships,
            'updating table: lead ships',
            table_dict,
            'lead_ships'
        )
    

    logger.info("Database update process completed.")

    logger.info(f"reporting_ok: {reporting_ok}")
    logger.info(f"reporting_ok_count: {len(reporting_ok)}")
    if reporting_failed:
        logger.error(f"reporting_failed: {reporting_failed}")
        logger.error(f"reporting_failed_count: {len(reporting_failed)}")
    else:
        logger.info("all data updates reported success")
    logger.info("="*80)

    reporting_dict = {'reporting_ok': reporting_ok, 'reporting_failed': reporting_failed}
    logger.info(f"reporting_dict: {reporting_dict}")

    # Write reporting to file
    with open('reporting.json', 'w') as f:
        json.dump(reporting_dict, f)

    print("="*80)
    print("update complete")
    print("-"*80)
    print(f"reporting_ok: {reporting_ok}")
    print(f"reporting_failed: {reporting_failed}")
    ok = len(reporting_ok)
    failed = len(reporting_failed)
    print(f"ok: {ok}")
    print(f"failed: {failed}")
    print(f"success rate: {(ok / (ok + failed)):.2f}")
    print("="*80)

    if failed > 0:
        print("failed to restore some tables, attempting to restore from backup")
        for table in reporting_failed:
            status = restore_tables(table = table, db_url = mkt_url, backup_dir = backup_dir, table_map = restore_map)
            if status:
                print(f"table: {table} restored successfully")
            else:
                print(f"table: {table} restoration failed")

if __name__ == "__main__":
    pass