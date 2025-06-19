import json
import libsql
import pandas as pd
import os
import shutil
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert, select, text, inspect
from sqlalchemy.orm import Session
from datetime import datetime
import argparse
import numpy as np

from logging_config import setup_logging
from doctrine_data import preprocess_doctrine_fits
from db_utils import get_wcmkt_remote_engine, get_wcmkt_libsql_connection
from models import MarketStats, MarketOrders, MarketHistory, Doctrines, DoctrineMap, DoctrineFits, LeadShips, ShipTargets, Base

logger = setup_logging(__name__)

datadir = "data"
update_dir = "data/latest"

new_orders = os.path.join(update_dir, "new_orders.csv")
new_history = os.path.join(update_dir, "new_history.csv")
new_stats = os.path.join(update_dir, "new_stats.csv")
watchlist = os.path.join(update_dir, "watchlist.csv")
new_orderscsv = os.path.join(update_dir, "new_orders.csv")
new_doctrines = os.path.join(update_dir, "new_doctrines.csv")
doctrine_map = os.path.join(update_dir, "doctrine_map.csv")
lead_ships = os.path.join(update_dir, "lead_ships.csv")
ship_targets = os.path.join(datadir, "ship_targets2.csv")

# Backup directory
backup_dir = "backup"



load_dotenv()

turso_url = os.getenv("TURSO_URL")
turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")

mkt_db = "wcmkt.db"
sde_db = "sde.db"

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

restore_class_map = {'doctrine_fits': DoctrineFits, 
                   'doctrine_map': DoctrineMap, 
                   'lead_ships': LeadShips, 
                   'new_doctrines': Doctrines, 
                   'new_history': MarketHistory, 
                   'new_orders': MarketOrders, 
                   'new_stats': MarketStats, 
                   'ship_targets': ShipTargets,
                   }

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

def backup_database():
    """Create a backup of the current database state."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{backup_dir}/wcmkt_backup_{timestamp}.db"
    
    # First, we will sync the remote database to the local database using libsql
    try:
        conn = get_wcmkt_libsql_connection()
        conn.sync()
    except Exception as e:
        logger.error(f"Failed to sync database: {e}")

    # Then, we will copy the local database to the backup directory
    shutil.copy2(mkt_db, backup_filename)
    logger.info(f"Database backed up to {backup_filename}")
    return backup_filename

def remove_old_db():
    """Remove old database backups."""
    db_path = backup_dir  # Changed from hardcoded path to use the backup_dir variable
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

def restore_database(table):
    failed_class = restore_class_map[table]
    backup_file = get_most_recent_backup(backup_dir)
    
    engine = create_engine(f"sqlite+libsql:///{backup_file}")
    with engine.connect() as conn:
        df = pd.read_sql_table(failed_class.__tablename__, conn)
    conn.close()
    engine.dispose()
    data = clean_data(df, failed_class)
    print(data)
    
    engine = get_wcmkt_remote_engine()
    Base.metadata.create_all(engine)
    session = Session(engine)
    bulk_insert_in_chunks(session, failed_class, data)
    session.commit()
    session.close()
    engine.dispose()
    logger.info(f"restored {table} from backup")

def update_history():
    logger.info("updating history")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    df = pd.read_csv(new_history)
    df.date = pd.to_datetime(df.date)
    df.timestamp = pd.to_datetime(df.timestamp)

    data = clean_data(df, MarketHistory)

    logger.info(f"Prepared {len(data)} history records for insertion")

    return update_table_with_error_handling('new_history', data, MarketHistory)


def update_orders():
    logger.info("updating orders")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)

    df = pd.read_csv(new_orderscsv)
    df.issued = pd.to_datetime(df.issued)
    data = clean_data(df, MarketOrders)

    return update_table_with_error_handling('new_orders', data, MarketOrders)

def update_stats():
    logger.info("updating stats")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)

    df = pd.read_csv(new_stats)
    df.last_update = pd.to_datetime(df.last_update)

    # Rename avg_vol column to avg_volume to match the database model
    df = df.rename(columns={'avg_vol': 'avg_volume'})
    data = clean_data(df, MarketStats)

    return update_table_with_error_handling('new_stats', data, MarketStats)


def update_doctrines():
    logger.info(f"""
                {'='*100}
                updating doctrines table
                {'-'*100}
                """)
                

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
    
    data = clean_data(df, Doctrines)
    
    return update_table_with_error_handling('new_doctrines', data, Doctrines)

def update_ship_targets():
    logger.info("updating ship targets")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    df = pd.read_csv(ship_targets)
    df.created_at = pd.to_datetime(df.created_at)
    data = clean_data(df, ShipTargets)
    
    logger.info(f"Prepared {len(data)} ship target records for insertion")
    
    return update_table_with_error_handling('ship_targets', data, ShipTargets)

def update_doctrine_map():
    logger.info("updating doctrine map")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    

    df = pd.read_csv(doctrine_map)
    
    data = clean_data(df, DoctrineMap)
    
    return update_table_with_error_handling('doctrine_map', data, DoctrineMap)

def update_doctrine_fits():
    logger.info("updating doctrine fits")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    df = preprocess_doctrine_fits()
    data = clean_data(df, DoctrineFits)
    
    return update_table_with_error_handling('doctrine_fits', data, DoctrineFits)

def update_lead_ships():
    logger.info("updating lead ships")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    df = pd.read_csv(lead_ships)
    data = clean_data(df, LeadShips)
    
    return update_table_with_error_handling('lead_ships', data, LeadShips)

def check_tables():
    logger.info("checking tables")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)

    table_dict = {}

    tables = os.listdir(update_dir)

    for table in tables:
        logger.info("="*100)
        if table.endswith(".csv"):
            df = pd.read_csv(os.path.join(update_dir, table))
            table_name = table.split(".")[0]
            table_dict[table_name] = len(df)
            logger.info(f"{table_name}: {len(df)}")

    df = pd.read_csv(ship_targets)
    table_dict['ship_targets'] = len(df)
    logger.info(f"ship_targets: {len(df)}")
        
    logger.info("="*100)
    return table_dict

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

    logger.info(f"connecting to database: {turso_url}")
    print(f"connecting to database: {turso_url}")
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
        engine = get_wcmkt_remote_engine()
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
        if not update_history():
            return False
        print('market_history update completed successfully.')
        

    # Update orders
    if not update_orders():
        return False
    print('market_orders update completed successfully.')

    # Update stats
    if not update_stats():
        return False
    print('market_stats update completed successfully.')

    # Update doctrines
    if not update_doctrines():
        return False
    print('new_doctrines update completed successfully.')

    # Update ship targets
    if not update_ship_targets():
        return False
    print('ship_targets update completed successfully.')

    # Update doctrine map
    if args.update_fits:
        if not update_doctrine_map():
            return False
        print('doctrine_map update completed successfully.')

    # Update doctrine fits
    if args.update_fits:
        if not update_doctrine_fits():
            return False
        print('doctrine_fits update completed successfully.')
    
    # Update lead ships
    if args.update_lead_ships:
        if not update_lead_ships():
            return False
        print('lead_ships update completed successfully.')
    

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

def clean_data(df: pd.DataFrame, model_class) -> list[dict]:
        
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
    valid_columns = [column.key for column in inspect(model_class).columns]
    df = df[df.columns.intersection(valid_columns)]


    df = prepare_data_for_insertion(df, model_class)

    data = df.to_dict(orient='records')
    return data

def bulk_insert_in_chunks(session: Session, table, data, chunk_size=1000, disable_sync=False, table_name=None):
    """
    Insert `data` (a list of dicts) into `table` in chunks, using a single transaction.
    Centralized error handling for all table operations.

    :param session: an active SQLAlchemy Session
    :param table: SQLAlchemy table object (e.g., MarketOrders)
    :param data: list of dictionaries
    :param chunk_size: number of rows per chunk
    :param disable_sync: if True, set PRAGMA synchronous = OFF (SQLite only)
    :param table_name: name of the table for error reporting and restoration
    :return: True if successful, False if failed
    """

    try:
        with session.begin():
            for i in range(0, len(data), chunk_size):
                chunk = data[i : i + chunk_size]
                logger.info(f"inserting chunk {i//chunk_size + 1} of {len(data)//chunk_size + 1}")
                stmt = insert(table).values(chunk)
                session.execute(stmt)
                logger.info("--------------------------------")
        
        # If we get here, the operation was successful
        if table_name:
            reporting_ok.append(table_name)
            logger.info(f"{table_name} update completed successfully")
        return True
            
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        if table_name:
            reporting_failed.append(table_name)
            # Restore from backup if available
            try:
                restore_database(table_name)
                logger.info(f"Restored {table_name} from backup after error")
            except Exception as restore_error:
                logger.error(f"Failed to restore {table_name} from backup: {restore_error}")
        return False

def update_table_with_error_handling(table_name, data, model_class, clear_table=True):
    """
    Centralized function to update a table with error handling.
    
    :param table_name: name of the table (used for reporting and restoration)
    :param data: cleaned data to insert
    :param model_class: SQLAlchemy model class
    :param clear_table: whether to clear the table before inserting
    :return: True if successful, False if failed
    """
    logger.info(f"updating {table_name}")
    logger.info(f"date: {datetime.now()}")
    logger.info("="*100)
    
    engine = get_wcmkt_remote_engine()
    
    try:
        with Session(engine) as session:
            # Count existing records
            try:
                result = session.execute(text(f"SELECT COUNT(*) FROM {model_class.__tablename__}")).scalar()
                record_count = result or 0
                logger.info(f"Found {record_count} existing {table_name} records")
            except Exception as e:
                logger.warning(f"Could not count existing records: {e}")
            
            # Clear existing data if requested
            if clear_table:
                logger.info(f"Clearing existing {table_name} data")
                try:
                    session.execute(text(f"DELETE FROM {model_class.__tablename__}"))
                    session.commit()
                except Exception as e:
                    logger.warning(f"Could not clear {model_class.__tablename__} table: {e}")
                    session.rollback()
            
            # Insert new data
            logger.info(f"Inserting {len(data)} {table_name} records")
            if data:
                if not bulk_insert_in_chunks(session, model_class, data, table_name=table_name):
                    return False
            
            logger.info(f"{table_name} update completed")
            return True
            
    except Exception as e:
        logger.error(f"Error in {table_name} update: {e}")
        return False

def prepare_data_for_insertion(df, model_class):
    """Convert datetime strings to datetime objects for a model DataFrame"""
    inspector = inspect(model_class)
    
    for column in inspector.columns:
        column_name = column.key
        if column_name in df.columns:
            # Check if it's a DateTime column
            if hasattr(column.type, '__class__') and 'DateTime' in str(column.type.__class__):
                try:
                    # Convert the entire Series to datetime
                    df[column_name] = pd.to_datetime(df[column_name])
                    # Convert to Python datetime objects for SQLAlchemy
                    df[column_name] = df[column_name].dt.to_pydatetime()
                except Exception as e:
                    print(f"Error converting {column_name}: {e}")
                    # Set to current datetime as fallback for the entire column
                    df[column_name] = datetime.now()
    
    return df

if __name__ == "__main__":
    main()
