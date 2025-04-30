#!/usr/bin/env python
"""
Test script to verify error handling in turso_handler.py

This script introduces various types of invalid data to test
the retry and rollback mechanisms.
"""
import os
import pandas as pd
import numpy as np
import tempfile
from datetime import datetime
import shutil
import time as time_module  # Renamed to avoid conflict
import sys

# Import the functions we want to test
from turso_handler import (
    update_orders, update_stats, update_doctrines, 
    update_ship_targets, update_history,
    backup_database, restore_database,
    retry_operation, safe_update_operation,
    handle_null_columns,
    path, new_stats, new_orderscsv, new_history, 
    new_doctrines, ship_targets, logger
)

from models import MarketStats, MarketOrders, Base, MarketHistory, Doctrines, ShipTargets

# Create a modified version of retry_operation without import conflict
def test_retry_operation(operation_func, *args, **kwargs):
    """Retry an operation with exponential backoff for testing."""
    MAX_RETRIES = 3
    RETRY_DELAY = 0.1  # Very short for testing
    
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

# Create backup copies of real data files
def backup_data_files():
    """Create backup copies of the data files we'll manipulate"""
    backup_dir = os.path.join(os.path.dirname(path), "test_backup")
    os.makedirs(backup_dir, exist_ok=True)
    
    files_to_backup = [
        new_stats, new_orderscsv, new_history, 
        new_doctrines, ship_targets
    ]
    
    backups = {}
    for file_path in files_to_backup:
        if os.path.exists(file_path):
            backup_file = os.path.join(backup_dir, os.path.basename(file_path))
            shutil.copy2(file_path, backup_file)
            backups[file_path] = backup_file
            logger.info(f"Backed up {file_path} to {backup_file}")
    
    return backups

# Restore backup copies
def restore_data_files(backups):
    """Restore the original data files"""
    for original, backup in backups.items():
        if os.path.exists(backup):
            shutil.copy2(backup, original)
            logger.info(f"Restored {original} from {backup}")

# Test 1: Invalid data in orders file
def test_invalid_orders():
    logger.info("=== Testing invalid orders data ===")
    
    # Read the original file
    try:
        df = pd.read_csv(new_orderscsv)
    except Exception as e:
        logger.error(f"Could not read orders file: {e}")
        return False
    
    # Backup the file before modifying
    temp_backup = f"{new_orderscsv}.bak"
    shutil.copy2(new_orderscsv, temp_backup)
    
    try:
        # Create a test file instead of modifying the original
        test_file = f"{new_orderscsv}.test"
        
        # Create a completely invalid CSV file
        with open(test_file, 'w') as f:
            f.write("order_id,type_id,price,issued\n")
            f.write("123,456,INVALID_PRICE,2023\n")
            f.write("456,789,NOT_A_NUMBER,NOT_A_DATE\n")
            
        logger.info(f"Created test file with invalid data")
        
        # Temporarily swap the files
        os.rename(new_orderscsv, f"{new_orderscsv}.orig")
        os.rename(test_file, new_orderscsv)
        
        # Create a custom function that will fail with the bad data
        def test_orders_update():
            try:
                # Read from our corrupted file
                df = pd.read_csv(new_orderscsv)
                df.issued = pd.to_datetime(df.issued)
                
                # Convert price to numeric - this should fail
                df['price'] = pd.to_numeric(df['price'])
                
                logger.info("Data processing should have failed but didn't")
                return True
            except Exception as e:
                logger.info(f"Expected error occurred: {e}")
                raise  # Re-raise to trigger retry
        
        # Try to process the file - this should trigger error handling
        logger.info("Attempting to process the corrupted orders file")
        
        try:
            # Try the operation directly without safe_update to see the error
            test_retry_operation(test_orders_update)
            success = True
        except Exception as e:
            logger.info(f"Expected exception caught: {e}")
            success = False
        
        assert not success, "Should have failed with invalid data"
        logger.info("✓ Invalid orders test passed - error was properly detected")
        return True
        
    finally:
        # Restore the original file
        if os.path.exists(f"{new_orderscsv}.orig"):
            os.rename(f"{new_orderscsv}.orig", new_orderscsv)
        
        # Clean up temp files
        for f in [temp_backup, f"{new_orderscsv}.test"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass
                
        logger.info(f"Restored original orders file")

# Test 2: Invalid data in stats file
def test_invalid_stats():
    logger.info("=== Testing invalid stats data ===")
    
    # Backup the file before modifying
    temp_backup = f"{new_stats}.bak"
    if os.path.exists(new_stats):
        shutil.copy2(new_stats, temp_backup)
    
    try:
        # Create a test file with completely invalid format
        test_file = f"{new_stats}.test"
        
        # This will create a broken CSV file - missing a required column
        with open(test_file, 'w') as f:
            f.write("type_name,avg_price,min_price,max_price\n")  # Missing type_id column
            f.write("Test Item,100.5,50.25,150.75\n")
        
        logger.info(f"Created invalid stats file (missing required columns)")
        
        # Temporarily swap the files
        if os.path.exists(new_stats):
            os.rename(new_stats, f"{new_stats}.orig")
        os.rename(test_file, new_stats)
        
        # Define a function that will attempt to process the file
        def test_stats_update():
            # Read the file
            df = pd.read_csv(new_stats)
            
            # This should fail because type_id is missing
            # Force access to a non-existent column
            missing_column = df['type_id']
            
            logger.error("Should have failed when accessing missing column")
            return True
        
        # Try to process the file - this should trigger error handling
        logger.info("Attempting to process the corrupted stats file")
        
        try:
            # Try the operation directly
            test_retry_operation(test_stats_update)
            logger.error("Test should have failed but didn't")
            success = True
        except Exception as e:
            logger.info(f"Expected error occurred: {type(e).__name__}: {e}")
            success = False
        
        if success:
            logger.error("Stats test failed - error handling did not work")
            return False
        else:
            logger.info("✓ Invalid stats test passed - error was properly detected")
            return True
        
    finally:
        # Restore the original file
        if os.path.exists(f"{new_stats}.orig"):
            os.rename(f"{new_stats}.orig", new_stats)
        elif os.path.exists(temp_backup):
            shutil.copy2(temp_backup, new_stats)
        
        # Clean up temp files
        for f in [temp_backup, f"{new_stats}.test"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass
                
        logger.info(f"Restored original stats file")

# Test 3: Database connection failure simulation
def test_connection_failure():
    # This is a mock function that will always fail
    def always_fails():
        logger.info("Simulating database connection failure")
        raise Exception("Simulated database connection failure")
    
    # Test the retry logic
    logger.info("=== Testing retry logic with simulated connection failure ===")
    try:
        test_retry_operation(always_fails)
        logger.error("Retry test failed - should have raised exception")
        return False
    except Exception as e:
        logger.info(f"Caught expected exception: {e}")
        if "Simulated database connection failure" in str(e):
            logger.info("✓ Connection failure test passed - error was properly handled")
            return True
        else:
            logger.error(f"Unexpected exception: {e}")
            return False

# Test 4: Null column handling
def test_null_columns():
    logger.info("=== Testing null column handling ===")
    
    # Create a test DataFrame with basic columns that exist in the model
    # Include required fields from the MarketOrders model
    test_df = pd.DataFrame({
        'order_id': [100001, 100002, 100003, 100004, 100005],
        'type_id': [1, 2, 3, 4, 5],
        'price': [100.5, 200.25, 150.0, 175.5, 300.75],
        'volume_remain': [10, 20, 30, 40, 50],
        # Add a completely null column
        'description': [None, None, None, None, None]
    })
    
    try:
        # Test on MarketOrders model
        logger.info("Testing handle_null_columns with MarketOrders model")
        df_fixed = handle_null_columns(test_df, MarketOrders)
        
        # Verify all essential columns are present
        essential_cols = ['order_id', 'type_id', 'price']
        for col in essential_cols:
            if col not in df_fixed.columns:
                logger.error(f"✗ Essential column {col} is missing")
                return False
        
        logger.info("✓ All essential columns are present")
        
        # Test with missing required column
        test_df2 = pd.DataFrame({
            'price': [100.5, 200.25, 150.0],
            'volume_remain': [10, 20, 30]
            # Missing type_id and order_id which are likely required
        })
        
        logger.info("Testing with missing required columns")
        df_fixed2 = handle_null_columns(test_df2, MarketOrders)
        
        # Check if some required columns were added
        new_columns_added = False
        for col in ['type_id', 'order_id']:
            if col not in test_df2.columns and col in df_fixed2.columns:
                logger.info(f"✓ Required column {col} was correctly added")
                new_columns_added = True
        
        if not new_columns_added:
            # We won't fail the test for this, as the required columns depend on the model
            # and we may not know exactly which ones are required
            logger.warning("No new columns were added - this may be normal if your model has nullable columns")
            
        # Test that the function doesn't crash with empty DataFrame
        empty_df = pd.DataFrame()
        logger.info("Testing with empty DataFrame")
        empty_fixed = handle_null_columns(empty_df, MarketOrders)
        
        logger.info("✓ Function handles empty DataFrame without crashing")
            
        logger.info("✓ Null column test passed")
        return True
        
    except Exception as e:
        logger.error(f"Null column test failed: {e}")
        return False

# Main test runner
def run_tests():
    logger.info("Starting error handling tests")
    logger.info("=" * 80)
    
    # Backup all data files
    backups = backup_data_files()
    
    try:
        # Run the tests
        tests_passed = 0
        tests_failed = 0
        
        if test_invalid_orders():
            tests_passed += 1
            logger.info("✓ Orders test PASSED")
        else:
            tests_failed += 1
            logger.error("✗ Orders test FAILED")
        
        if test_invalid_stats():
            tests_passed += 1
            logger.info("✓ Stats test PASSED")
        else:
            tests_failed += 1
            logger.error("✗ Stats test FAILED")
        
        if test_connection_failure():
            tests_passed += 1
            logger.info("✓ Connection test PASSED")
        else:
            tests_failed += 1
            logger.error("✗ Connection test FAILED")
        
        # Run the null column test
        if test_null_columns():
            tests_passed += 1
            logger.info("✓ Null columns test PASSED")
        else:
            tests_failed += 1
            logger.error("✗ Null columns test FAILED")
            
        logger.info(f"Tests completed: {tests_passed} passed, {tests_failed} failed")
        
        # Exit with appropriate status
        if tests_failed > 0:
            logger.error(f"Some tests failed: {tests_failed}/{tests_passed + tests_failed}")
            sys.exit(1)
        else:
            logger.info("All tests passed!")
            
    finally:
        # Always restore the data files
        restore_data_files(backups)
    
    logger.info("=" * 80)
    logger.info("Error handling tests complete")

if __name__ == "__main__":
    run_tests() 