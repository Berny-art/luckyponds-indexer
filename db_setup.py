#!/usr/bin/env python3
import os
import time
import sqlite3
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = os.getenv("DB_PATH", "./data/lucky_ponds.db")
START_BLOCK = int(os.getenv("START_BLOCK", "0"))

def setup_database():
    """Set up the database schema."""
    logger.info(f"Setting up database at {DB_PATH}")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Create indexer_state table to track progress
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS indexer_state (
            id INTEGER PRIMARY KEY,
            last_block INTEGER NOT NULL,
            last_updated_timestamp INTEGER NOT NULL
        )
        ''')
        
        # Check if we need to initialize the indexer_state
        cursor.execute('SELECT COUNT(*) FROM indexer_state')
        if cursor.fetchone()[0] == 0:
            current_time = int(time.time())
            cursor.execute('INSERT INTO indexer_state (id, last_block, last_updated_timestamp) VALUES (1, ?, ?)', 
                          (START_BLOCK, current_time))
            logger.info(f"Initialized indexer state with block {START_BLOCK}")
        else:
            # Update existing record to add last_updated_timestamp if needed
            cursor.execute('SELECT * FROM indexer_state WHERE id = 1')
            columns = [description[0] for description in cursor.description]
            if 'last_updated_timestamp' not in columns:
                logger.info("Updating indexer_state table to add last_updated_timestamp")
                cursor.execute('ALTER TABLE indexer_state ADD COLUMN last_updated_timestamp INTEGER')
                current_time = int(time.time())
                cursor.execute('UPDATE indexer_state SET last_updated_timestamp = ? WHERE id = 1', (current_time,))
        
        # Create coin_tossed_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS coin_tossed_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            block_timestamp INTEGER NOT NULL,
            pond_type TEXT NOT NULL,
            frog_address TEXT NOT NULL,
            amount TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            total_pond_tosses INTEGER NOT NULL,
            total_pond_value TEXT NOT NULL,
            points_processed INTEGER DEFAULT 0,
            UNIQUE(tx_hash, pond_type, frog_address)
        )
        ''')
        logger.info("Created or verified coin_tossed_events table")
        
        # Check if points_processed column exists in coin_tossed_events
        cursor.execute('PRAGMA table_info(coin_tossed_events)')
        columns = [info[1] for info in cursor.fetchall()]
        if 'points_processed' not in columns:
            logger.info("Adding points_processed column to coin_tossed_events table")
            cursor.execute('ALTER TABLE coin_tossed_events ADD COLUMN points_processed INTEGER DEFAULT 0')
        
        # Create lucky_winner_selected_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS lucky_winner_selected_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            block_timestamp INTEGER NOT NULL,
            pond_type TEXT NOT NULL,
            winner_address TEXT NOT NULL,
            prize TEXT NOT NULL,
            selector TEXT NOT NULL,
            points_processed INTEGER DEFAULT 0,
            UNIQUE(tx_hash, pond_type)
        )
        ''')
        logger.info("Created or verified lucky_winner_selected_events table")
        
        # Check if points_processed column exists in lucky_winner_selected_events
        cursor.execute('PRAGMA table_info(lucky_winner_selected_events)')
        columns = [info[1] for info in cursor.fetchall()]
        if 'points_processed' not in columns:
            logger.info("Adding points_processed column to lucky_winner_selected_events table")
            cursor.execute('ALTER TABLE lucky_winner_selected_events ADD COLUMN points_processed INTEGER DEFAULT 0')
        
        # Create pond_action_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS pond_action_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            block_timestamp INTEGER NOT NULL,
            pond_type TEXT NOT NULL,
            name TEXT NOT NULL,
            start_time INTEGER NOT NULL,
            end_time INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            UNIQUE(tx_hash, pond_type)
        )
        ''')
        logger.info("Created or verified pond_action_events table")
        
        # Create config_changed_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS config_changed_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            block_timestamp INTEGER NOT NULL,
            config_type TEXT NOT NULL,
            pond_type TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            old_address TEXT,
            new_address TEXT,
            UNIQUE(tx_hash, config_type, pond_type)
        )
        ''')
        logger.info("Created or verified config_changed_events table")
        
        # Create emergency_action_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS emergency_action_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            block_timestamp INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            recipient TEXT NOT NULL,
            token TEXT NOT NULL,
            amount TEXT NOT NULL,
            pond_type TEXT NOT NULL,
            UNIQUE(tx_hash, pond_type, recipient)
        )
        ''')
        logger.info("Created or verified emergency_action_events table")
        
        # Create user_points table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            total_points INTEGER NOT NULL DEFAULT 0,
            toss_points INTEGER NOT NULL DEFAULT 0,
            max_toss_points INTEGER NOT NULL DEFAULT 0,
            winner_points INTEGER NOT NULL DEFAULT 0,
            last_updated INTEGER NOT NULL,
            UNIQUE(address)
        )
        ''')
        logger.info("Created or verified user_points table")
        
        # Create user_point_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_point_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            event_type TEXT NOT NULL,
            points INTEGER NOT NULL,
            tx_hash TEXT NOT NULL,
            pond_type TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
        ''')
        logger.info("Created or verified user_point_events table")
        
        # Create user_referrals table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            referral_code TEXT NOT NULL,
            referrer_address TEXT,
            is_activated INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            activated_at INTEGER,
            referral_points_earned INTEGER NOT NULL DEFAULT 0,
            UNIQUE(address),
            UNIQUE(referral_code)
        )
        ''')
        logger.info("Created or verified user_referrals table")
        
        # Create calculator_state table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS calculator_state (
            id INTEGER PRIMARY KEY,
            last_processed_toss_id INTEGER NOT NULL DEFAULT 0,
            last_processed_winner_id INTEGER NOT NULL DEFAULT 0,
            last_processed_timestamp INTEGER NOT NULL,
            last_run_timestamp INTEGER NOT NULL
        )
        ''')
        logger.info("Created or verified calculator_state table")
        
        # Initialize calculator state if it doesn't exist
        cursor.execute('SELECT COUNT(*) FROM calculator_state')
        if cursor.fetchone()[0] == 0:
            current_time = int(time.time())
            cursor.execute('''
            INSERT INTO calculator_state 
            (id, last_processed_toss_id, last_processed_winner_id, last_processed_timestamp, last_run_timestamp) 
            VALUES (1, 0, 0, ?, ?)
            ''', (current_time, current_time))
            logger.info("Initialized calculator state")
        
        # Indices for user_referrals
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_referral_code ON user_referrals (referral_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_referrer_address ON user_referrals (referrer_address)')
        
        # Add indices for faster querying
        logger.info("Creating database indices for performance optimization...")
        
        # Indices for coin_tossed_events
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coin_tossed_address ON coin_tossed_events (frog_address)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coin_tossed_pond ON coin_tossed_events (pond_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coin_tossed_timestamp ON coin_tossed_events (block_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coin_tossed_block ON coin_tossed_events (block_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_coin_tossed_processed ON coin_tossed_events (points_processed)')
        
        # Indices for lucky_winner_selected_events
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_winner_address ON lucky_winner_selected_events (winner_address)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_winner_pond ON lucky_winner_selected_events (pond_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_winner_timestamp ON lucky_winner_selected_events (block_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_winner_block ON lucky_winner_selected_events (block_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_winner_processed ON lucky_winner_selected_events (points_processed)')
        
        # Indices for pond_action_events
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pond_action_pond ON pond_action_events (pond_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pond_action_timestamp ON pond_action_events (block_timestamp)')
        
        # Indices for user_points
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_points_total ON user_points (total_points)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_points_address ON user_points (address)')
        
        # Indices for user_point_events
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_point_events_address ON user_point_events (address)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_point_events_type ON user_point_events (event_type)')
        
        conn.commit()
        logger.info("Database setup completed successfully")
        
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    setup_database()
    logger.info("Database setup is complete. You can now run the indexer.")