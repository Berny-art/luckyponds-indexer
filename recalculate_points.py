#!/usr/bin/env python3
import os
import time
import logging
import sqlite3
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv

# Import our database access layers and components
from data_access import EventsDatabase, ApplicationDatabase
from utils import (
    get_events_db_path, 
    get_app_db_path, 
    get_toss_points_multiplier,
    get_win_points,
    get_referral_bonus_points,
    get_current_timestamp,
    setup_logger
)

# Configure logging
logger = setup_logger('points_recalculator')

# Load environment variables
load_dotenv()

# Configuration
EVENTS_DB_PATH = get_events_db_path()
APP_DB_PATH = get_app_db_path()
TOSS_POINTS_MULTIPLIER = get_toss_points_multiplier()
WIN_POINTS = get_win_points()
REFERRAL_BONUS_POINTS = get_referral_bonus_points()

def reset_points_data():
    """Reset all points-related data in the application database."""
    logger.info("Resetting points data in application database...")
    
    app_db = ApplicationDatabase(APP_DB_PATH)
    conn = app_db.get_connection()
    
    try:
        cursor = conn.cursor()
        
        # Begin transaction
        conn.execute('BEGIN TRANSACTION')
        
        # Delete all user points data
        cursor.execute('DELETE FROM user_points')
        logger.info("Deleted all records from user_points table")
        
        # Delete all user point events
        cursor.execute('DELETE FROM user_point_events')
        logger.info("Deleted all records from user_point_events table")
        
        # Reset calculator state
        cursor.execute('''
        UPDATE calculator_state 
        SET last_processed_toss_id = 0,
            last_processed_winner_id = 0,
            last_processed_timestamp = ?,
            last_run_timestamp = ?
        WHERE id = 1
        ''', (get_current_timestamp(), get_current_timestamp()))
        logger.info("Reset calculator state")
        
        # Commit the transaction
        conn.commit()
        logger.info("Points data reset successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error resetting points data: {e}")
        raise
    finally:
        conn.close()

def process_toss_events(batch_size=1000):
    """Process all coin toss events from the beginning with minimum 1 point rule."""
    logger.info("Processing all coin toss events...")
    
    events_db = EventsDatabase(EVENTS_DB_PATH)
    app_db = ApplicationDatabase(APP_DB_PATH)
    
    # Get all toss events
    conn = events_db.get_connection()
    cursor = conn.cursor()
    
    # Get total count for progress reporting
    cursor.execute('SELECT COUNT(*) FROM coin_tossed_events')
    total_events = cursor.fetchone()[0]
    logger.info(f"Found {total_events} coin toss events to process")
    
    # Process in batches
    offset = 0
    processed_count = 0
    last_processed_id = 0
    
    app_conn = app_db.get_connection()
    
    try:
        while True:
            cursor.execute('''
            SELECT id, tx_hash, block_timestamp, pond_type, frog_address, amount
            FROM coin_tossed_events
            ORDER BY id ASC
            LIMIT ? OFFSET ?
            ''', (batch_size, offset))
            
            events = cursor.fetchall()
            if not events:
                break
            
            app_conn.execute('BEGIN TRANSACTION')
            app_cursor = app_conn.cursor()
            
            for event in events:
                event_id = event['id']
                tx_hash = event['tx_hash']
                block_timestamp = event['block_timestamp']
                pond_type = event['pond_type']
                address = event['frog_address'].lower()
                amount = event['amount']
                
                # Calculate points with minimum 1 point
                amount_in_eth = float(amount) / 10**18
                calculated_points = amount_in_eth * TOSS_POINTS_MULTIPLIER
                toss_points = max(1, int(calculated_points))
                
                # Ensure user exists in points table
                app_cursor.execute('''
                INSERT OR IGNORE INTO user_points
                (address, total_points, toss_points, winner_points, referral_points, last_updated)
                VALUES (?, 0, 0, 0, 0, ?)
                ''', (address, block_timestamp))
                
                # Update points
                app_cursor.execute('''
                UPDATE user_points
                SET total_points = total_points + ?,
                    toss_points = toss_points + ?,
                    last_updated = ?
                WHERE address = ?
                ''', (toss_points, toss_points, block_timestamp, address))
                
                # Record the event
                app_cursor.execute('''
                INSERT INTO user_point_events
                (address, event_type, points, tx_hash, pond_type, timestamp)
                VALUES (?, 'toss', ?, ?, ?, ?)
                ''', (address, toss_points, tx_hash, pond_type, block_timestamp))
                
                processed_count += 1
                last_processed_id = max(last_processed_id, event_id)
                
                # Log progress periodically
                if processed_count % 1000 == 0:
                    logger.info(f"Processed {processed_count}/{total_events} toss events ({processed_count/total_events*100:.1f}%)")
            
            app_conn.commit()
            offset += batch_size
    
    except Exception as e:
        if app_conn:
            app_conn.rollback()
        logger.error(f"Error processing toss events: {e}")
        raise
    finally:
        if app_conn:
            app_conn.close()
        conn.close()
    
    logger.info(f"Completed processing all {processed_count} toss events")
    return last_processed_id

def process_winner_events(batch_size=1000):
    """Process all winner events from the beginning."""
    logger.info("Processing all winner events...")
    
    events_db = EventsDatabase(EVENTS_DB_PATH)
    app_db = ApplicationDatabase(APP_DB_PATH)
    
    # Get all winner events
    conn = events_db.get_connection()
    cursor = conn.cursor()
    
    # Get total count for progress reporting
    cursor.execute('SELECT COUNT(*) FROM lucky_winner_selected_events')
    total_events = cursor.fetchone()[0]
    logger.info(f"Found {total_events} winner events to process")
    
    # Process in batches
    offset = 0
    processed_count = 0
    last_processed_id = 0
    
    app_conn = app_db.get_connection()
    
    try:
        while True:
            cursor.execute('''
            SELECT id, tx_hash, block_timestamp, pond_type, winner_address, prize
            FROM lucky_winner_selected_events
            ORDER BY id ASC
            LIMIT ? OFFSET ?
            ''', (batch_size, offset))
            
            events = cursor.fetchall()
            if not events:
                break
            
            app_conn.execute('BEGIN TRANSACTION')
            app_cursor = app_conn.cursor()
            
            for event in events:
                event_id = event['id']
                tx_hash = event['tx_hash']
                block_timestamp = event['block_timestamp']
                pond_type = event['pond_type']
                address = event['winner_address'].lower()
                
                # Ensure user exists in points table
                app_cursor.execute('''
                INSERT OR IGNORE INTO user_points
                (address, total_points, toss_points, winner_points, referral_points, last_updated)
                VALUES (?, 0, 0, 0, 0, ?)
                ''', (address, block_timestamp))
                
                # Update points
                app_cursor.execute('''
                UPDATE user_points
                SET total_points = total_points + ?,
                    winner_points = winner_points + ?,
                    last_updated = ?
                WHERE address = ?
                ''', (WIN_POINTS, WIN_POINTS, block_timestamp, address))
                
                # Record the event
                app_cursor.execute('''
                INSERT INTO user_point_events
                (address, event_type, points, tx_hash, pond_type, timestamp)
                VALUES (?, 'winner', ?, ?, ?, ?)
                ''', (address, WIN_POINTS, tx_hash, pond_type, block_timestamp))
                
                processed_count += 1
                last_processed_id = max(last_processed_id, event_id)
                
                # Log progress periodically
                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count}/{total_events} winner events ({processed_count/total_events*100:.1f}%)")
            
            app_conn.commit()
            offset += batch_size
    
    except Exception as e:
        if app_conn:
            app_conn.rollback()
        logger.error(f"Error processing winner events: {e}")
        raise
    finally:
        if app_conn:
            app_conn.close()
        conn.close()
    
    logger.info(f"Completed processing all {processed_count} winner events")
    return last_processed_id

def process_referrals():
    """Process all referrals and activate them if needed."""
    logger.info("Processing referrals...")
    
    app_db = ApplicationDatabase(APP_DB_PATH)
    events_db = EventsDatabase(EVENTS_DB_PATH)
    
    app_conn = app_db.get_connection()
    app_cursor = app_conn.cursor()
    
    try:
        # Find all users with referrers that are not activated
        app_cursor.execute('''
        SELECT ur.address, ur.referrer_address 
        FROM user_referrals ur
        WHERE ur.referrer_address IS NOT NULL AND ur.is_activated = 0
        ''')
        
        pending_referrals = app_cursor.fetchall()
        logger.info(f"Found {len(pending_referrals)} pending referrals")
        
        if not pending_referrals:
            app_conn.close()
            return 0
        
        # Check which users have made tosses
        events_conn = events_db.get_connection()
        events_cursor = events_conn.cursor()
        
        activated_count = 0
        
        for referral in pending_referrals:
            user_address = referral['address']
            referrer_address = referral['referrer_address']
            
            # Check if user has made any tosses
            events_cursor.execute('''
            SELECT COUNT(*) FROM coin_tossed_events 
            WHERE frog_address = ?
            ''', (user_address,))
            
            toss_count = events_cursor.fetchone()[0]
            
            if toss_count > 0:
                # Begin transaction
                app_conn.execute('BEGIN TRANSACTION')
                
                # Activate the referral
                current_time = get_current_timestamp()
                app_cursor.execute('''
                UPDATE user_referrals 
                SET is_activated = 1, activated_at = ? 
                WHERE address = ?
                ''', (current_time, user_address))
                
                # Ensure referrer exists in user_points
                app_cursor.execute('''
                INSERT OR IGNORE INTO user_points 
                (address, total_points, toss_points, winner_points, referral_points, last_updated) 
                VALUES (?, ?, 0, 0, ?, ?)
                ''', (referrer_address, REFERRAL_BONUS_POINTS, REFERRAL_BONUS_POINTS, current_time))
                
                # Update existing record
                app_cursor.execute('''
                UPDATE user_points 
                SET total_points = total_points + ?,
                    referral_points = referral_points + ?,
                    last_updated = ? 
                WHERE address = ?
                ''', (REFERRAL_BONUS_POINTS, REFERRAL_BONUS_POINTS, current_time, referrer_address))
                
                # Log the referral bonus in the points events table
                tx_hash = f"referral_{user_address}_{current_time}"  # Create a unique identifier
                app_cursor.execute('''
                INSERT INTO user_point_events 
                (address, event_type, points, tx_hash, pond_type, timestamp) 
                VALUES (?, 'referral', ?, ?, 'referral', ?)
                ''', (referrer_address, REFERRAL_BONUS_POINTS, tx_hash, current_time))
                
                app_conn.commit()
                activated_count += 1
                
                logger.info(f"Activated referral: {user_address} referred by {referrer_address}")
        
        events_conn.close()
        app_conn.close()
        
        logger.info(f"Activated {activated_count} referrals")
        return activated_count
        
    except Exception as e:
        if app_conn:
            app_conn.rollback()
        logger.error(f"Error processing referrals: {e}")
        if 'events_conn' in locals() and events_conn:
            events_conn.close()
        if app_conn:
            app_conn.close()
        raise

def update_calculator_state(toss_id, winner_id):
    """Update the calculator state with the latest processed IDs."""
    logger.info("Updating calculator state...")
    
    app_db = ApplicationDatabase(APP_DB_PATH)
    current_time = get_current_timestamp()
    
    app_db.update_calculator_state(toss_id, winner_id, current_time)
    logger.info(f"Updated calculator state: toss_id={toss_id}, winner_id={winner_id}")

def recalculate_all_points():
    """Recalculate all points from scratch with the minimum 1 point rule."""
    logger.info("Starting full points recalculation")
    start_time = time.time()
    
    try:
        # Step 1: Reset all points data
        reset_points_data()
        
        # Step 2: Process all toss events with minimum 1 point
        last_toss_id = process_toss_events()
        
        # Step 3: Process all winner events
        last_winner_id = process_winner_events()
        
        # Step 4: Process referrals
        process_referrals()
        
        # Step 5: Update calculator state
        update_calculator_state(last_toss_id, last_winner_id)
        
        elapsed = time.time() - start_time
        logger.info(f"Points recalculation completed successfully in {elapsed:.2f} seconds")
        
        return True
    except Exception as e:
        logger.error(f"Error during points recalculation: {e}")
        return False

# For Jupyter/IPython environment, create these simple function calls
def reset_only():
    """Just reset the points data without recalculating."""
    reset_points_data()
    return "Points data reset successfully"

def recalculate():
    """Recalculate all points from scratch."""
    success = recalculate_all_points()
    if success:
        return "Points recalculation completed successfully"
    else:
        return "Error during points recalculation"

# To use in Jupyter, just call either:
# reset_only()
# or
# recalculate()