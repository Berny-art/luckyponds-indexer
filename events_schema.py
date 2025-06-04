import os
import sqlite3
import logging

logger = logging.getLogger(__name__)

def setup_events_database(db_path: str, start_block: int = 0):
    """Set up the events database schema from scratch."""
    logger.info(f"Setting up events database at {db_path}")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Create indexer_state table to track progress
        cursor.execute('''
        CREATE TABLE indexer_state (
            id INTEGER PRIMARY KEY,
            last_block INTEGER NOT NULL,
            last_updated_timestamp INTEGER NOT NULL
        )
        ''')
        
        # Initialize the indexer_state with start_block
        current_time = int(os.path.getmtime(db_path) if os.path.exists(db_path) else 0)
        cursor.execute('INSERT INTO indexer_state (id, last_block, last_updated_timestamp) VALUES (1, ?, ?)', 
                      (start_block, current_time))
        
        # Create coin_tossed_events table
        cursor.execute('''
        CREATE TABLE coin_tossed_events (
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
            token_address TEXT NOT NULL,
            UNIQUE(tx_hash, pond_type, frog_address)
        )
        ''')
        
        # Create lucky_winner_selected_events table
        cursor.execute('''
        CREATE TABLE lucky_winner_selected_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            block_timestamp INTEGER NOT NULL,
            pond_type TEXT NOT NULL,
            winner_address TEXT NOT NULL,
            prize TEXT NOT NULL,
            selector TEXT NOT NULL,
            token_address TEXT NOT NULL,
            UNIQUE(tx_hash, pond_type)
        )
        ''')
        
        # Create pond_action_events table
        cursor.execute('''
        CREATE TABLE pond_action_events (
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
        
        # Create config_changed_events table
        cursor.execute('''
        CREATE TABLE config_changed_events (
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
        
        # Create emergency_action_events table
        cursor.execute('''
        CREATE TABLE emergency_action_events (
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
        
        # Create indices for better query performance
        cursor.execute('CREATE INDEX idx_coin_tossed_block ON coin_tossed_events (block_number)')
        cursor.execute('CREATE INDEX idx_coin_tossed_address ON coin_tossed_events (frog_address)')
        cursor.execute('CREATE INDEX idx_coin_tossed_timestamp ON coin_tossed_events (block_timestamp)')
        cursor.execute('CREATE INDEX idx_coin_tossed_token ON coin_tossed_events (token_address)')
        
        cursor.execute('CREATE INDEX idx_winner_block ON lucky_winner_selected_events (block_number)')
        cursor.execute('CREATE INDEX idx_winner_address ON lucky_winner_selected_events (winner_address)')
        cursor.execute('CREATE INDEX idx_winner_timestamp ON lucky_winner_selected_events (block_timestamp)')
        cursor.execute('CREATE INDEX idx_winner_token ON lucky_winner_selected_events (token_address)')
        
        cursor.execute('CREATE INDEX idx_pond_action_pond ON pond_action_events (pond_type)')
        cursor.execute('CREATE INDEX idx_pond_action_timestamp ON pond_action_events (block_timestamp)')
        
        conn.commit()
        logger.info("Events database setup completed successfully")
        
    except Exception as e:
        logger.error(f"Error setting up events database: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()