#!/usr/bin/env python3
import os
import time
import logging
import sqlite3
from typing import Dict, Any, Optional, List, Tuple
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
TOSS_POINTS_MULTIPLIER = int(os.getenv("TOSS_POINTS_MULTIPLIER", "10"))  # 10 points per amount tossed (in ETH)
WIN_POINTS = int(os.getenv("WIN_POINTS", "100"))  # Fixed 100 points for a win
REFERRAL_BONUS_POINTS = int(os.getenv("REFERRAL_BONUS_POINTS", "20"))  # Points for referral activation

class PointsCalculator:
    def __init__(self, db_path: str):
        """Initialize the points calculator with the database path."""
        self.db_path = db_path
        self.setup_database()
        
    def setup_database(self):
        """Set up the additional tables needed for points calculation if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Create user_points table if it doesn't exist
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
            
            # Create user_point_events table if it doesn't exist
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
            
            # Create user_referrals table if it doesn't exist
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
            
            # Create indices for faster querying
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_points_address ON user_points (address)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_points_total ON user_points (total_points)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_point_events_address ON user_point_events (address)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_point_events_type ON user_point_events (event_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_referral_code ON user_referrals (referral_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_referrer_address ON user_referrals (referrer_address)')
            
            # Create a calculator state table to track the last processed event
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS calculator_state (
                id INTEGER PRIMARY KEY,
                last_processed_toss_id INTEGER NOT NULL DEFAULT 0,
                last_processed_winner_id INTEGER NOT NULL DEFAULT 0,
                last_processed_timestamp INTEGER NOT NULL,
                last_run_timestamp INTEGER NOT NULL
            )
            ''')
            
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
            
            conn.commit()
            logger.info("Points calculator database setup completed successfully")
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_calculator_state(self) -> Dict[str, int]:
        """Get the current state of the points calculator."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            SELECT last_processed_toss_id, last_processed_winner_id, last_processed_timestamp 
            FROM calculator_state 
            WHERE id = 1
            ''')
            result = cursor.fetchone()
            
            if result:
                return {
                    "last_processed_toss_id": result[0],
                    "last_processed_winner_id": result[1],
                    "last_processed_timestamp": result[2]
                }
            else:
                # Initialize if no record exists
                current_time = int(time.time())
                cursor.execute('''
                INSERT INTO calculator_state 
                (id, last_processed_toss_id, last_processed_winner_id, last_processed_timestamp, last_run_timestamp) 
                VALUES (1, 0, 0, ?, ?)
                ''', (current_time, current_time))
                conn.commit()
                
                return {
                    "last_processed_toss_id": 0,
                    "last_processed_winner_id": 0,
                    "last_processed_timestamp": current_time
                }
                
        except Exception as e:
            logger.error(f"Error getting calculator state: {e}")
            return {
                "last_processed_toss_id": 0,
                "last_processed_winner_id": 0,
                "last_processed_timestamp": 0
            }
        finally:
            conn.close()
    
    def update_calculator_state(self, toss_id: int, winner_id: int, timestamp: int):
        """Update the state of the points calculator."""
        current_time = int(time.time())
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            UPDATE calculator_state 
            SET last_processed_toss_id = ?, 
                last_processed_winner_id = ?, 
                last_processed_timestamp = ?,
                last_run_timestamp = ?
            WHERE id = 1
            ''', (toss_id, winner_id, timestamp, current_time))
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating calculator state: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def add_user_points(self, address: str, event_type: str, points: int, tx_hash: str, pond_type: str, timestamp: int):
        """Add points to a user's total and record the specific event."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # First, ensure the user exists in the user_points table
            cursor.execute('''
            INSERT OR IGNORE INTO user_points 
            (address, total_points, toss_points, max_toss_points, winner_points, last_updated) 
            VALUES (?, 0, 0, 0, 0, ?)
            ''', (address.lower(), timestamp))
            
            # Update the specific point category and total points
            if event_type == 'toss':
                cursor.execute('''
                UPDATE user_points 
                SET toss_points = toss_points + ?, 
                    total_points = total_points + ?,
                    last_updated = ?
                WHERE address = ?
                ''', (points, points, timestamp, address.lower()))
            elif event_type == 'winner':
                cursor.execute('''
                UPDATE user_points 
                SET winner_points = winner_points + ?, 
                    total_points = total_points + ?,
                    last_updated = ?
                WHERE address = ?
                ''', (points, points, timestamp, address.lower()))
            elif event_type == 'referral':
                cursor.execute('''
                UPDATE user_points 
                SET total_points = total_points + ?,
                    last_updated = ?
                WHERE address = ?
                ''', (points, timestamp, address.lower()))
            
            # Record the specific point-earning event
            cursor.execute('''
            INSERT INTO user_point_events 
            (address, event_type, points, tx_hash, pond_type, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (address.lower(), event_type, points, tx_hash, pond_type, timestamp))
            
            conn.commit()
            logger.debug(f"Added {points} {event_type} points to {address}")
            
        except Exception as e:
            logger.error(f"Error adding points for {address}: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def process_coin_toss_events(self):
        """Process unprocessed coin toss events and award points."""
        state = self.get_calculator_state()
        last_id = state["last_processed_toss_id"]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get unprocessed coin toss events
            cursor.execute('''
            SELECT id, tx_hash, block_timestamp, pond_type, frog_address, amount
            FROM coin_tossed_events
            WHERE id > ? AND points_processed = 0
            ORDER BY id ASC
            LIMIT 1000  -- Process in chunks to avoid memory issues
            ''', (last_id,))
            
            events = cursor.fetchall()
            
            if not events:
                logger.info("No new coin toss events to process")
                return 0
            
            processed_count = 0
            max_id = last_id
            conn.isolation_level = None  # Use autocommit mode
            cursor.execute('BEGIN TRANSACTION')
            
            for event in events:
                event_id, tx_hash, block_timestamp, pond_type, address, amount = event
                
                # Calculate points - amount in ETH * multiplier
                amount_in_eth = float(amount) / 10**18  # Convert from wei to ETH
                toss_points = int(amount_in_eth * TOSS_POINTS_MULTIPLIER)
                
                # Award toss points
                self._add_user_points_transaction(
                    cursor,
                    address,
                    'toss',
                    toss_points,
                    tx_hash,
                    pond_type,
                    block_timestamp
                )
                
                # Mark as processed
                cursor.execute('''
                UPDATE coin_tossed_events SET points_processed = 1 WHERE id = ?
                ''', (event_id,))
                
                # Check and activate referrals
                self._check_and_activate_referral_transaction(cursor, address, block_timestamp)
                
                processed_count += 1
                max_id = max(max_id, event_id)
            
            cursor.execute('COMMIT')
            
            # Update the calculator state with the last processed ID
            if max_id > last_id:
                self.update_calculator_state(
                    max_id,
                    state["last_processed_winner_id"],
                    int(time.time())
                )
            
            logger.info(f"Processed {processed_count} coin toss events")
            return processed_count
            
        except Exception as e:
            logger.error(f"Error processing coin toss events: {e}")
            if conn.isolation_level is None:
                cursor.execute('ROLLBACK')
            return 0
        finally:
            conn.close()
    
    def process_winner_events(self):
        """Process unprocessed winner events and award points."""
        state = self.get_calculator_state()
        last_id = state["last_processed_winner_id"]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get unprocessed winner events
            cursor.execute('''
            SELECT id, tx_hash, block_timestamp, pond_type, winner_address
            FROM lucky_winner_selected_events
            WHERE id > ? AND points_processed = 0
            ORDER BY id ASC
            LIMIT 1000  -- Process in chunks to avoid memory issues
            ''', (last_id,))
            
            events = cursor.fetchall()
            
            if not events:
                logger.info("No new winner events to process")
                return 0
            
            processed_count = 0
            max_id = last_id
            
            conn.isolation_level = None  # Use autocommit mode
            cursor.execute('BEGIN TRANSACTION')
            
            for event in events:
                event_id, tx_hash, block_timestamp, pond_type, address = event
                
                # Award fixed points for winning
                self._add_user_points_transaction(
                    cursor,
                    address,
                    'winner',
                    WIN_POINTS,
                    tx_hash,
                    pond_type,
                    block_timestamp
                )
                
                # Mark as processed
                cursor.execute('''
                UPDATE lucky_winner_selected_events SET points_processed = 1 WHERE id = ?
                ''', (event_id,))
                
                processed_count += 1
                max_id = max(max_id, event_id)
            
            cursor.execute('COMMIT')
            
            # Update the calculator state with the last processed ID
            if max_id > last_id:
                self.update_calculator_state(
                    state["last_processed_toss_id"],
                    max_id,
                    int(time.time())
                )
            
            logger.info(f"Processed {processed_count} winner events")
            return processed_count
            
        except Exception as e:
            logger.error(f"Error processing winner events: {e}")
            if conn.isolation_level is None:
                cursor.execute('ROLLBACK')
            return 0
        finally:
            conn.close()
    
    def _add_user_points_transaction(self, cursor, address: str, event_type: str, points: int, 
                                   tx_hash: str, pond_type: str, timestamp: int):
        """Add points to a user as part of a transaction (without commit)."""
        # First, ensure the user exists in the user_points table
        cursor.execute('''
        INSERT OR IGNORE INTO user_points 
        (address, total_points, toss_points, max_toss_points, winner_points, last_updated) 
        VALUES (?, 0, 0, 0, 0, ?)
        ''', (address.lower(), timestamp))
        
        # Update the specific point category and total points
        if event_type == 'toss':
            cursor.execute('''
            UPDATE user_points 
            SET toss_points = toss_points + ?, 
                total_points = total_points + ?,
                last_updated = ?
            WHERE address = ?
            ''', (points, points, timestamp, address.lower()))
        elif event_type == 'winner':
            cursor.execute('''
            UPDATE user_points 
            SET winner_points = winner_points + ?, 
                total_points = total_points + ?,
                last_updated = ?
            WHERE address = ?
            ''', (points, points, timestamp, address.lower()))
        elif event_type == 'referral':
            cursor.execute('''
            UPDATE user_points 
            SET total_points = total_points + ?,
                last_updated = ?
            WHERE address = ?
            ''', (points, timestamp, address.lower()))
        
        # Record the specific point-earning event
        cursor.execute('''
        INSERT INTO user_point_events 
        (address, event_type, points, tx_hash, pond_type, timestamp) 
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (address.lower(), event_type, points, tx_hash, pond_type, timestamp))
    
    def _check_and_activate_referral_transaction(self, cursor, user_address: str, timestamp: int):
        """Check and activate referrals as part of a transaction."""
        user_address = user_address.lower()
        
        # Check if user has a referrer and hasn't been activated yet
        cursor.execute('''
        SELECT ur.referrer_address, ur.is_activated 
        FROM user_referrals ur
        WHERE ur.address = ? AND ur.referrer_address IS NOT NULL
        ''', (user_address,))
        
        result = cursor.fetchone()
        
        # If no referral or already activated, nothing to do
        if not result or result[1] == 1:
            return False
        
        referrer_address = result[0]
        
        # Activate the referral
        cursor.execute('''
        UPDATE user_referrals 
        SET is_activated = 1, activated_at = ? 
        WHERE address = ?
        ''', (timestamp, user_address))
        
        # Award points to the referrer
        cursor.execute('''
        UPDATE user_referrals 
        SET referral_points_earned = referral_points_earned + ? 
        WHERE address = ?
        ''', (REFERRAL_BONUS_POINTS, referrer_address))
        
        # Also update the user_points table
        cursor.execute('''
        INSERT OR IGNORE INTO user_points 
        (address, total_points, toss_points, max_toss_points, winner_points, last_updated) 
        VALUES (?, ?, 0, 0, 0, ?)
        ''', (referrer_address, REFERRAL_BONUS_POINTS, timestamp))
        
        cursor.execute('''
        UPDATE user_points 
        SET total_points = total_points + ?, 
            last_updated = ? 
        WHERE address = ?
        ''', (REFERRAL_BONUS_POINTS, timestamp, referrer_address))
        
        # Create a unique identifier for this referral event
        tx_hash = f"referral_{user_address}_{timestamp}"
        
        # Log the referral bonus in the points events table
        cursor.execute('''
        INSERT INTO user_point_events 
        (address, event_type, points, tx_hash, pond_type, timestamp) 
        VALUES (?, 'referral', ?, ?, 'referral', ?)
        ''', (referrer_address, REFERRAL_BONUS_POINTS, tx_hash, timestamp))
        
        logger.info(f"Activated referral: {user_address} referred by {referrer_address}, awarded {REFERRAL_BONUS_POINTS} points")
        return True
    
    def generate_referral_code(self, length: int = 8) -> str:
        """Generate a unique referral code."""
        import random
        import string
        
        characters = string.ascii_uppercase + string.digits
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        while True:
            # Generate a random code
            code = ''.join(random.choices(characters, k=length))
            
            # Check if the code already exists
            cursor.execute('SELECT COUNT(*) FROM user_referrals WHERE referral_code = ?', (code,))
            count = cursor.fetchone()[0]
            
            # If code doesn't exist, return it
            if count == 0:
                conn.close()
                return code
    
    def create_user_referral(self, address: str) -> Dict[str, Any]:
        """Create a referral code for a user if they don't have one."""
        address = address.lower()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if user already has a referral code
        cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
        user_referral = cursor.fetchone()
        
        if user_referral:
            result = dict(zip([column[0] for column in cursor.description], user_referral))
            conn.close()
            return result
        
        # Create a new referral code
        referral_code = self.generate_referral_code()
        current_time = int(time.time())
        
        # Insert new record
        cursor.execute('''
        INSERT INTO user_referrals 
        (address, referral_code, created_at, is_activated, referral_points_earned) 
        VALUES (?, ?, ?, 0, 0)
        ''', (address, referral_code, current_time))
        conn.commit()
        
        # Get the newly created record
        cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
        user_referral = cursor.fetchone()
        result = dict(zip([column[0] for column in cursor.description], user_referral))
        
        conn.close()
        return result
    
    def apply_referral_code(self, user_address: str, referral_code: str) -> Tuple[bool, str]:
        """Apply a referral code to a user account."""
        user_address = user_address.lower()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if user already has a referrer
            cursor.execute('SELECT referrer_address FROM user_referrals WHERE address = ?', (user_address,))
            result = cursor.fetchone()
            
            if result and result[0]:
                conn.close()
                return False, "User already has a referrer"
            
            # Find the referrer by code
            cursor.execute('SELECT address FROM user_referrals WHERE referral_code = ?', (referral_code,))
            referrer_result = cursor.fetchone()
            
            if not referrer_result:
                conn.close()
                return False, "Invalid referral code"
            
            referrer_address = referrer_result[0]
            
            # Make sure user isn't trying to refer themselves
            if user_address == referrer_address:
                conn.close()
                return False, "Cannot refer yourself"
            
            # Create user referral record if it doesn't exist
            cursor.execute('SELECT COUNT(*) FROM user_referrals WHERE address = ?', (user_address,))
            if cursor.fetchone()[0] == 0:
                self.create_user_referral(user_address)
            
            # Update user's referrer
            cursor.execute('''
            UPDATE user_referrals 
            SET referrer_address = ? 
            WHERE address = ?
            ''', (referrer_address, user_address))
            conn.commit()
            
            conn.close()
            return True, "Referral code applied successfully"
        
        except Exception as e:
            conn.rollback()
            conn.close()
            logger.error(f"Error applying referral code: {e}")
            return False, f"Error: {str(e)}"
    
    def run_points_calculation(self):
        """Run the full points calculation process."""
        start_time = time.time()
        logger.info("Starting points calculation")
        
        try:
            # Process coin toss events
            toss_count = self.process_coin_toss_events()
            
            # Process winner events
            winner_count = self.process_winner_events()
            
            elapsed = time.time() - start_time
            logger.info(f"Points calculation completed: processed {toss_count} toss events and {winner_count} winner events in {elapsed:.2f} seconds")
            
            return toss_count + winner_count
            
        except Exception as e:
            logger.error(f"Error in points calculation: {e}")
            return 0

if __name__ == "__main__":
    calculator = PointsCalculator(DB_PATH)
    
    # Run once
    calculator.run_points_calculation()
    
    # Or run periodically
    # while True:
    #     calculator.run_points_calculation()
    #     logger.info("Sleeping for 1 hour until next calculation")
    #     time.sleep(3600)  # Run every hour