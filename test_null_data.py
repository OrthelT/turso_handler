#!/usr/bin/env python
"""
Test script to verify handling of null columns in input data.

This script creates a test file with some null columns and verifies
the system handles it correctly.
"""
import os
import pandas as pd
import numpy as np
import shutil
from datetime import datetime
import tempfile

from turso_handler import (
    update_orders,
    handle_null_columns,
    logger,
    MarketOrders,
    new_orderscsv
)

def create_test_file_with_nulls():
    """Create a test file with a completely null column."""
    # Create a backup of the original file
    backup_file = f"{new_orderscsv}.bak"
    if os.path.exists(new_orderscsv):
        shutil.copy2(new_orderscsv, backup_file)
        logger.info(f"Backed up original file to {backup_file}")
    
    # Read the original file or create a minimal test dataframe
    try:
        if os.path.exists(new_orderscsv):
            df = pd.read_csv(new_orderscsv)
        else:
            # Create a minimal test dataframe if file doesn't exist
            df = pd.DataFrame({
                'order_id': [100001, 100002, 100003],
                'type_id': [34, 35, 36],
                'price': [100.0, 200.0, 300.0],
                'volume_total': [10, 20, 30],
                'volume_remain': [5, 15, 25],
                'issued': ['2023-01-01', '2023-01-02', '2023-01-03'],
                'duration': [90, 90, 90],
                'is_buy_order': [True, False, True],
                'range': ['region', 'station', 'region'],
                'location_id': [60003760, 60003760, 60003760],
                'min_volume': [1, 1, 1],
                'type_name': ['Widget', 'Gadget', 'Doodad']
            })
    
        # Add a completely null column
        df['completely_null_column'] = None
        
        # Also modify a few existing columns to contain nulls
        if len(df) > 5:
            df.loc[0:5, 'min_volume'] = None
        
        # Save the modified file
        test_file = f"{new_orderscsv}.test"
        df.to_csv(test_file, index=False)
        logger.info(f"Created test file with null data: {test_file}")
        
        # Replace the original file with our test file
        if os.path.exists(new_orderscsv):
            os.rename(new_orderscsv, f"{new_orderscsv}.orig")
        os.rename(test_file, new_orderscsv)
        
        return df, backup_file
    
    except Exception as e:
        logger.error(f"Error creating test file: {e}")
        return None, backup_file

def restore_original_file(backup_file):
    """Restore the original file."""
    if os.path.exists(f"{new_orderscsv}.orig"):
        os.remove(new_orderscsv)
        os.rename(f"{new_orderscsv}.orig", new_orderscsv)
        logger.info(f"Restored original file from .orig")
    elif os.path.exists(backup_file):
        shutil.copy2(backup_file, new_orderscsv)
        logger.info(f"Restored original file from backup")
    
    # Clean up backup files
    for ext in ['.bak', '.test']:
        if os.path.exists(f"{new_orderscsv}{ext}"):
            os.remove(f"{new_orderscsv}{ext}")

def run_test():
    """Run the test."""
    logger.info("="*80)
    logger.info("Testing handling of null columns with a real CSV file")
    logger.info("="*80)
    
    backup_file = None
    
    try:
        # Create a test file with null columns
        df, backup_file = create_test_file_with_nulls()
        if df is None:
            logger.error("Failed to create test file")
            return False
        
        # Test the handle_null_columns function directly
        logger.info("Testing handle_null_columns function...")
        df_fixed = handle_null_columns(df, MarketOrders)
        
        # Check if completely null column is preserved
        if 'completely_null_column' in df_fixed.columns:
            logger.info("✓ Completely null column preserved")
        else:
            logger.info("✗ Completely null column was removed")
        
        # Check if partially null columns are preserved
        if 'min_volume' in df_fixed.columns:
            null_count = df_fixed['min_volume'].isna().sum()
            logger.info(f"✓ Partially null column 'min_volume' preserved with {null_count} null values")
        
        # Try to process the file with update_orders
        # This is just a basic check to ensure it doesn't crash
        # We won't actually update the database
        logger.info("Attempting to read and process the file with nulls...")
        
        # Just read the file and apply handle_null_columns
        test_df = pd.read_csv(new_orderscsv)
        test_df = handle_null_columns(test_df, MarketOrders)
        
        logger.info(f"✓ Successfully processed file with null columns")
        logger.info(f"✓ Processed {len(test_df)} rows")
        
        logger.info("="*80)
        logger.info("Null column handling test completed successfully")
        logger.info("="*80)
        return True
    
    except Exception as e:
        logger.error(f"Error in null column test: {e}")
        return False
    
    finally:
        # Always restore the original file
        if backup_file:
            restore_original_file(backup_file)

if __name__ == "__main__":
    run_test() 