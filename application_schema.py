# application_schema.py

import os
import time
import sqlite3
import logging

logger = logging.getLogger(__name__)

def setup_application_database(db_path: str):
    """Set up the application database schema from scratch."""
    logger.info(f"Setting up application database at {db_path}")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Create user_points table
        cursor.execute('''
        CREATE TABLE user_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            total_points INTEGER NOT NULL DEFAULT 0,
            toss_points INTEGER NOT NULL DEFAULT 0,
            winner_points INTEGER NOT NULL DEFAULT 0,
            referral_points INTEGER NOT NULL DEFAULT 0,
            last_updated INTEGER NOT NULL,
            UNIQUE(address)
        )
        ''')
        
        # Create user_point_events table
        cursor.execute('''
        CREATE TABLE user_point_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            event_type TEXT NOT NULL,
            points INTEGER NOT NULL,
            tx_hash TEXT NOT NULL,
            pond_type TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
        ''')
        
        # Create user_referrals table
        cursor.execute('''
        CREATE TABLE user_referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            referral_code TEXT NOT NULL,
            referrer_address TEXT,
            is_activated INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            activated_at INTEGER,
            UNIQUE(address),
            UNIQUE(referral_code)
        )
        ''')
        
        # Create calculator_state table
        cursor.execute('''
        CREATE TABLE calculator_state (
            id INTEGER PRIMARY KEY,
            last_processed_toss_id INTEGER NOT NULL DEFAULT 0,
            last_processed_winner_id INTEGER NOT NULL DEFAULT 0,
            last_processed_timestamp INTEGER NOT NULL,
            last_run_timestamp INTEGER NOT NULL
        )
        ''')
        
        # Initialize calculator state
        current_time = int(time.time())
        cursor.execute('''
        INSERT INTO calculator_state 
        (id, last_processed_toss_id, last_processed_winner_id, last_processed_timestamp, last_run_timestamp) 
        VALUES (1, 0, 0, ?, ?)
        ''', (current_time, current_time))
        
        # Create indices for better query performance
        cursor.execute('CREATE INDEX idx_user_points_address ON user_points (address)')
        cursor.execute('CREATE INDEX idx_user_points_total ON user_points (total_points)')
        
        cursor.execute('CREATE INDEX idx_user_point_events_address ON user_point_events (address)')
        cursor.execute('CREATE INDEX idx_user_point_events_type ON user_point_events (event_type)')
        
        cursor.execute('CREATE INDEX idx_referral_code ON user_referrals (referral_code)')
        cursor.execute('CREATE INDEX idx_referrer_address ON user_referrals (referrer_address)')
        
        conn.commit()
        logger.info("Application database setup completed successfully")
        
    except Exception as e:
        logger.error(f"Error setting up application database: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()