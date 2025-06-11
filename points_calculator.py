#!/usr/bin/env python3
import time
from typing import Dict, Any, Tuple
from dotenv import load_dotenv

# Import our database access layers and utilities
from data_access import EventsDatabase, ApplicationDatabase
from token_config import TokenConfig
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
logger = setup_logger('points_calculator')

# Load environment variables
load_dotenv()

# Configuration constants from environment variables
TOSS_POINTS_MULTIPLIER = get_toss_points_multiplier()
WIN_POINTS = get_win_points()
REFERRAL_BONUS_POINTS = get_referral_bonus_points()

class PointsCalculator:
    def __init__(self, app_db_path: str, events_db_path: str):
        """
        Initialize the points calculator with database paths.
        
        Args:
            app_db_path: Path to the application database
            events_db_path: Path to the events database
        """
        self.app_db = ApplicationDatabase(app_db_path)
        self.events_db = EventsDatabase(events_db_path)
        self.token_config = TokenConfig()
        self.ensure_calculator_state()
        
    def ensure_calculator_state(self):
        """Ensure calculator state exists in the application database."""
        calculator_state = self.app_db.get_calculator_state()
        if not calculator_state or calculator_state.get("last_processed_timestamp", 0) == 0:
            current_time = get_current_timestamp()
            self.app_db.update_calculator_state(0, 0, current_time)
            logger.info("Initialized calculator state")
    
    def add_user_points(self, address: str, event_type: str, points: int, tx_hash: str, pond_type: str, timestamp: int):
        """
        Add points to a user's total and record the specific event.
        
        Args:
            address: User's blockchain address
            event_type: Type of event (toss, winner, referral)
            points: Number of points to award
            tx_hash: Transaction hash of the related blockchain event
            pond_type: Type of pond (e.g., "hourly", "daily")
            timestamp: Unix timestamp of the event
        """
        # Use the app database method
        self.app_db.add_user_points(address, event_type, points, tx_hash, pond_type, timestamp)
        logger.debug(f"Added {points} {event_type} points to {address}")
    
    def process_coin_toss_events(self, batch_size: int = 1000) -> int:
        """
        Process unprocessed coin toss events and award points.
        
        Args:
            batch_size: Number of events to process in one batch
            
        Returns:
            Number of events processed
        """
        # Get calculator state to know where we left off
        state = self.app_db.get_calculator_state()
        last_id = state.get("last_processed_toss_id", 0)
        
        # Get unprocessed events from the events database
        toss_events = self.events_db.get_unprocessed_toss_events(last_id, batch_size)
        
        if not toss_events:
            logger.info("No new coin toss events to process")
            return 0
        
        processed_count = 0
        max_id = last_id
        
        # Process each event
        for event in toss_events:
            event_id = event['id']
            tx_hash = event['tx_hash']
            block_timestamp = event['block_timestamp']
            pond_type = event['pond_type']
            address = event['frog_address']
            amount = event['amount']
            token_address = event.get('token_address', '0x0000000000000000000000000000000000000000')
            
            # Calculate points using token-aware calculation
            toss_points = self.token_config.calculate_points(
                amount=amount,
                token_address=token_address,
                pond_type=pond_type,
                multiplier=TOSS_POINTS_MULTIPLIER
            )
            
            # Award toss points
            self.add_user_points(address, 'toss', toss_points, tx_hash, pond_type, block_timestamp)
            
            # Check and activate referrals
            self.check_and_activate_referral(address, block_timestamp)
            
            processed_count += 1
            max_id = max(max_id, event_id)
        
        # Update the calculator state with the last processed ID
        if max_id > last_id:
            self.app_db.update_calculator_state(
                max_id,
                state.get("last_processed_winner_id", 0),
                get_current_timestamp()
            )
        
        logger.info(f"Processed {processed_count} coin toss events")
        return processed_count
    
    def process_winner_events(self, batch_size: int = 1000) -> int:
        """
        Process unprocessed winner events and award points.
        
        Args:
            batch_size: Number of events to process in one batch
            
        Returns:
            Number of events processed
        """
        # Get calculator state to know where we left off
        state = self.app_db.get_calculator_state()
        last_id = state.get("last_processed_winner_id", 0)
        
        # Get unprocessed events from the events database
        winner_events = self.events_db.get_unprocessed_winner_events(last_id, batch_size)
        
        if not winner_events:
            logger.info("No new winner events to process")
            return 0
        
        processed_count = 0
        max_id = last_id
        
        # Process each event
        for event in winner_events:
            event_id = event['id']
            tx_hash = event['tx_hash']
            block_timestamp = event['block_timestamp']
            pond_type = event['pond_type']
            address = event['winner_address']
            
            # Award fixed points for winning
            self.add_user_points(address, 'winner', WIN_POINTS, tx_hash, pond_type, block_timestamp)
            
            processed_count += 1
            max_id = max(max_id, event_id)
        
        # Update the calculator state with the last processed ID
        if max_id > last_id:
            self.app_db.update_calculator_state(
                state.get("last_processed_toss_id", 0),
                max_id,
                get_current_timestamp()
            )
        
        logger.info(f"Processed {processed_count} winner events")
        return processed_count
    
    def check_and_activate_referral(self, user_address: str, timestamp: int) -> bool:
        """
        Check if a user has a referrer and activate the referral if needed.
        
        Args:
            user_address: Address of the user to check
            timestamp: Timestamp to use for activation
        
        Returns:
            True if referral was activated, False otherwise
        """
        user_address = user_address.lower()
        
        # Get a direct database connection for this operation
        conn = self.app_db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Check if user has a referrer and hasn't been activated yet
            cursor.execute('''
            SELECT ur.referrer_address, ur.is_activated 
            FROM user_referrals ur
            WHERE ur.address = ? AND ur.referrer_address IS NOT NULL
            ''', (user_address,))
            
            result = cursor.fetchone()
            
            # If no referral or already activated, nothing to do
            if not result or result['is_activated'] == 1:
                conn.close()
                return False
            
            referrer_address = result['referrer_address']
            
            # Begin transaction
            conn.execute('BEGIN TRANSACTION')
            
            try:
                # Activate the referral
                cursor.execute('''
                UPDATE user_referrals 
                SET is_activated = 1, activated_at = ? 
                WHERE address = ?
                ''', (timestamp, user_address))
                
                # Award referral bonus points to the referrer
                self.add_user_points(
                    referrer_address, 
                    'referral', 
                    REFERRAL_BONUS_POINTS, 
                    'activation_' + user_address,  # Use a unique identifier
                    'referral',  # pond_type
                    timestamp
                )
                
                # Commit the transaction
                conn.commit()
                conn.close()
                
                logger.info(f"Activated referral: {user_address} referred by {referrer_address}")
                return True
                
            except Exception as e:
                conn.rollback()
                conn.close()
                logger.error(f"Error activating referral for {user_address}: {e}")
                return False
                
        except Exception as e:
            if conn:
                conn.close()
            logger.error(f"Error checking referral for {user_address}: {e}")
            return False
    
    def generate_referral_code(self, length: int = 8) -> str:
        """
        Generate a unique referral code.
        
        Args:
            length: Length of the referral code
            
        Returns:
            Unique referral code string
        """
        import random
        import string
        
        characters = string.ascii_uppercase + string.digits
        conn = self.app_db.get_connection()
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
        """
        Create a referral code for a user if they don't have one.
        
        Args:
            address: User blockchain address
            
        Returns:
            Dictionary with user referral information
        """
        address = address.lower()
        conn = self.app_db.get_connection()
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
        current_time = get_current_timestamp()
        
        # Insert new record
        cursor.execute('''
        INSERT INTO user_referrals 
        (address, referral_code, created_at, is_activated) 
        VALUES (?, ?, ?, 0)
        ''', (address, referral_code, current_time))
        conn.commit()
        
        # Get the newly created record
        cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
        user_referral = cursor.fetchone()
        result = dict(zip([column[0] for column in cursor.description], user_referral))
        
        conn.close()
        return result
    
    def apply_referral_code(self, user_address: str, referral_code: str) -> Tuple[bool, str]:
        """
        Apply a referral code to a user account.
        
        Args:
            user_address: Address of the user applying the code
            referral_code: Referral code to apply
            
        Returns:
            Tuple of (success, message)
        """
        user_address = user_address.lower()
        conn = self.app_db.get_connection()
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
    
    def run_points_calculation(self, batch_size: int = 1000) -> int:
        """
        Run the full points calculation process.
        
        Args:
            batch_size: Number of events to process in one batch
            
        Returns:
            Total number of events processed
        """
        start_time = time.time()
        logger.info("Starting points calculation")
        
        try:
            # Process coin toss events
            toss_count = self.process_coin_toss_events(batch_size)
            
            # Process winner events
            winner_count = self.process_winner_events(batch_size)
            
            elapsed = time.time() - start_time
            logger.info(f"Points calculation completed: processed {toss_count} toss events and {winner_count} winner events in {elapsed:.2f} seconds")
            
            return toss_count + winner_count
            
        except Exception as e:
            logger.error(f"Error in points calculation: {e}")
            return 0

# Main execution block
if __name__ == "__main__":
    # Get database paths from environment or defaults
    app_db_path = get_app_db_path()
    events_db_path = get_events_db_path()
    
    # Create calculator and run once
    calculator = PointsCalculator(app_db_path, events_db_path)
    calculator.run_points_calculation()