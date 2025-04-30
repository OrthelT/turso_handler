#!/usr/bin/env python
"""
Debugging script to understand message size issues with database updates
"""
import os
import pandas as pd
import json
import sys
from sqlalchemy import inspect, create_engine, text
from sqlalchemy.orm import Session
import numpy as np
from datetime import datetime
import logging

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import from the main script
from turso_handler import (
    new_history, new_orderscsv, new_stats, new_doctrines, ship_targets,
    mkt_url, MarketHistory, MarketOrders, MarketStats, Doctrines, ShipTargets,
    handle_null_columns
)

def calculate_data_size(data):
    """Calculate the size of data when serialized to JSON"""
    json_data = json.dumps(data)
    size_bytes = len(json_data.encode('utf-8'))
    return size_bytes

def print_size_info(name, data):
    """Print information about the size of data"""
    size_bytes = calculate_data_size(data)
    logger.info(f"{name} data size: {size_bytes} bytes ({size_bytes/1024/1024:.2f} MB)")
    
    if size_bytes > 4 * 1024 * 1024:
        logger.warning(f"WARNING: {name} exceeds 4MB message size limit!")
    
    if data and isinstance(data, list) and len(data) > 0:
        sample_row_size = calculate_data_size(data[0])
        logger.info(f"Sample row size: {sample_row_size} bytes")
        logger.info(f"Row count: {len(data)}")
        logger.info(f"Average row size: {size_bytes/len(data):.2f} bytes")
        
        # Check if any individual fields are particularly large
        if isinstance(data[0], dict):
            for key, value in data[0].items():
                field_size = calculate_data_size(value)
                if field_size > 1000:  # Arbitrary threshold for "large" fields
                    logger.warning(f"Large field: '{key}' = {field_size} bytes")

def inspect_csv_file(file_path, name):
    """Inspect a CSV file for size information"""
    logger.info(f"\n{'='*80}\nInspecting {name} from {file_path}\n{'-'*80}")
    
    try:
        # Read the file
        df = pd.read_csv(file_path)
        logger.info(f"CSV dimensions: {df.shape}")
        
        # Check for very large columns
        for col in df.columns:
            col_size = df[col].memory_usage(deep=True)
            logger.info(f"Column '{col}' uses {col_size/1024/1024:.2f} MB")
        
        # Calculate the total memory usage
        memory_usage = df.memory_usage(deep=True).sum()
        logger.info(f"Total memory usage: {memory_usage/1024/1024:.2f} MB")
        
        # Convert to records and check size
        records = df.to_dict(orient='records')
        print_size_info(name, records)
        
        return df, records
    except Exception as e:
        logger.error(f"Error inspecting {file_path}: {e}")
        return None, None

def inspect_database_connection():
    """Test the database connection and check query limits"""
    logger.info(f"\n{'='*80}\nTesting database connection\n{'-'*80}")
    
    try:
        # Connect to the database
        engine = create_engine(mkt_url, echo=False, connect_args={"timeout": 30})
        
        with Session(engine) as session:
            # Check if tables exist
            insp = inspect(engine)
            tables = insp.get_table_names()
            logger.info(f"Available tables: {tables}")
            
            # Try a simple query
            result = session.execute(text("SELECT 1")).fetchone()
            logger.info(f"Simple query result: {result}")
            
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return False

def run_diagnostic():
    """Run diagnostic tests to understand message size issues"""
    logger.info(f"\n{'='*80}\nStarting diagnostics for message size issues\n{'='*80}")
    
    # First check database connection
    if not inspect_database_connection():
        logger.error("Database connection failed, cannot continue diagnostics")
        return
    
    # Check the size of each data file
    data_files = [
        (new_history, "history", MarketHistory),
        (new_orderscsv, "orders", MarketOrders),
        (new_stats, "stats", MarketStats),
        (new_doctrines, "doctrines", Doctrines),
        (ship_targets, "ship_targets", ShipTargets)
    ]
    
    for file_path, name, model in data_files:
        logger.info(f"\nAnalyzing {name} data:")
        
        if not os.path.exists(file_path):
            logger.warning(f"File {file_path} does not exist!")
            continue
            
        # Get file size
        file_size = os.path.getsize(file_path)
        logger.info(f"File size: {file_size} bytes ({file_size/1024/1024:.2f} MB)")
        
        # Read and analyze the file
        df, records = inspect_csv_file(file_path, name)
        
        if df is not None and records is not None:
            # Apply ID column if needed (like in the main script)
            if 'id' in df.columns:
                df['id'] = range(1, len(df) + 1)
            else:
                df.insert(0, 'id', range(1, len(df) + 1))
            
            # Apply null column handling
            df = handle_null_columns(df, model)
            
            # Convert to records after ID and null handling
            processed_records = df.to_dict(orient='records')
            logger.info(f"\nAfter ID and null handling:")
            print_size_info(f"{name} (processed)", processed_records)
            
            # Test different chunk sizes
            for chunk_size in [100, 50, 10]:
                chunk = processed_records[:chunk_size]
                logger.info(f"\nChunk size {chunk_size}:")
                print_size_info(f"{name} chunk", chunk)
        
        logger.info(f"{'='*80}")

if __name__ == "__main__":
    run_diagnostic() 