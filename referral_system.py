#!/usr/bin/env python3
import random
import string
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv

# Import our database access layer and utilities
from data_access import ApplicationDatabase
from utils import (
    get_app_db_path,
    get_referral_bonus_points,
    get_current_timestamp,
    setup_logger
)

# Configure logging
logger = setup_logger('referral_system')

# Load environment variables
load_dotenv()

# Configuration
REFERRAL_BONUS_POINTS = get_referral_bonus_points()

class ReferralSystem:
    """
    Manages the referral system functionality including:
    - Generating unique referral codes
    - Applying referral codes to users
    - Activating referrals and awarding points
    """
    
    def __init__(self, app_db_path: str):
        """
        Initialize the referral system with the application database.
        
        Args:
            app_db_path: Path to the application database
        """
        self.app_db = ApplicationDatabase(app_db_path)
    
    def generate_referral_code(self, length: int = 8) -> str:
        """
        Generate a unique referral code.
        
        Args:
            length: Length of the referral code
            
        Returns:
            Unique referral code string
        """
        # Use cryptographically strong random for better security
        secure_random = random.SystemRandom()
        characters = string.ascii_uppercase + string.digits
        
        conn = self.app_db.get_connection()
        cursor = conn.cursor()
        
        while True:
            # Generate a random code
            code = ''.join(secure_random.choice(characters) for _ in range(length))
            
            # Check if the code already exists
            cursor.execute('SELECT COUNT(*) FROM user_referrals WHERE referral_code = ?', (code,))
            count = cursor.fetchone()[0]
            
            # If code doesn't exist, return it
            if count == 0:
                conn.close()
                return code
    
    def get_or_create_user_referral(self, address: str) -> Dict[str, Any]:
        """
        Get or create a user's referral record.
        
        Args:
            address: Blockchain address of the user
            
        Returns:
            Dictionary containing referral information
        """
        address = address.lower()
        conn = self.app_db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Check if user already has a referral code
            cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
            user_referral = cursor.fetchone()
            
            if user_referral:
                # Convert row to dictionary
                result = dict(user_referral)
            else:
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
                result = dict(user_referral)
            
            conn.close()
            return result
            
        except Exception as e:
            if conn in locals():
                conn.rollback()
                conn.close()
            logger.error(f"Error in get_or_create_user_referral: {e}")
            raise
    
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
        referral_code = referral_code.upper()  # Normalize to uppercase
        
        conn = self.app_db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Begin transaction
            conn.execute('BEGIN TRANSACTION')
            
            # Check if user already has a referrer
            cursor.execute('SELECT referrer_address FROM user_referrals WHERE address = ?', (user_address,))
            result = cursor.fetchone()
            
            if result and result['referrer_address']:
                conn.rollback()
                conn.close()
                return False, "User already has a referrer"
            
            # Find the referrer by code
            cursor.execute('SELECT address FROM user_referrals WHERE referral_code = ?', (referral_code,))
            referrer_result = cursor.fetchone()
            
            if not referrer_result:
                conn.rollback()
                conn.close()
                return False, "Invalid referral code"
            
            referrer_address = referrer_result['address']
            
            # Make sure user isn't trying to refer themselves
            if user_address == referrer_address:
                conn.rollback()
                conn.close()
                return False, "Cannot refer yourself"
            
            # Create user referral record if it doesn't exist
            cursor.execute('SELECT COUNT(*) FROM user_referrals WHERE address = ?', (user_address,))
            if cursor.fetchone()[0] == 0:
                # Generate a referral code for the user
                new_code = self.generate_referral_code()
                current_time = get_current_timestamp()
                
                cursor.execute('''
                INSERT INTO user_referrals 
                (address, referral_code, created_at, is_activated) 
                VALUES (?, ?, ?, 0)
                ''', (user_address, new_code, current_time))
            
            # Update user's referrer
            cursor.execute('''
            UPDATE user_referrals 
            SET referrer_address = ? 
            WHERE address = ?
            ''', (referrer_address, user_address))
            
            # Commit the transaction
            conn.commit()
            conn.close()
            
            return True, "Referral code applied successfully"
        
        except Exception as e:
            if conn in locals():
                conn.rollback()
                conn.close()
            logger.error(f"Error applying referral code: {e}")
            return False, f"Error: {str(e)}"
    
    def check_and_activate_referral(self, user_address: str) -> bool:
        """
        Check if a user has made their first toss and activate their referral if needed.
        
        Args:
            user_address: Address of the user to check
        
        Returns:
            True if referral was activated, False otherwise
        """
        user_address = user_address.lower()
        
        # Get direct database connection for this operation
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
            
            # Connect to events database to check if user has made any tosses
            # Note: In a real implementation, we'd query the events database here
            # For now, we'll assume this is done elsewhere and just activate
            
            # Begin transaction
            conn.execute('BEGIN TRANSACTION')
            
            # Activate the referral
            current_time = get_current_timestamp()
            cursor.execute('''
            UPDATE user_referrals 
            SET is_activated = 1, activated_at = ? 
            WHERE address = ?
            ''', (current_time, user_address))
            
            # Award points to the referrer
            # Ensure referrer exists in user_points
            cursor.execute('''
            INSERT OR IGNORE INTO user_points 
            (address, total_points, toss_points, winner_points, referral_points, last_updated) 
            VALUES (?, ?, 0, 0, ?, ?)
            ''', (referrer_address, REFERRAL_BONUS_POINTS, REFERRAL_BONUS_POINTS, current_time))
            
            # Update existing record
            cursor.execute('''
            UPDATE user_points 
            SET total_points = total_points + ?,
                referral_points = referral_points + ?,
                last_updated = ? 
            WHERE address = ?
            ''', (REFERRAL_BONUS_POINTS, REFERRAL_BONUS_POINTS, current_time, referrer_address))
            
            # Log the referral bonus in the points events table
            tx_hash = f"referral_{user_address}_{current_time}"  # Create a unique identifier
            cursor.execute('''
            INSERT INTO user_point_events 
            (address, event_type, points, tx_hash, pond_type, timestamp) 
            VALUES (?, 'referral', ?, ?, 'referral', ?)
            ''', (referrer_address, REFERRAL_BONUS_POINTS, tx_hash, current_time))
            
            # Commit the transaction
            conn.commit()
            
            logger.info(f"Activated referral: {user_address} referred by {referrer_address}, awarded {REFERRAL_BONUS_POINTS} points")
            conn.close()
            return True
        
        except Exception as e:
            if conn in locals():
                conn.rollback()
                conn.close()
            logger.error(f"Error checking/activating referral: {e}")
            return False
    
    def process_pending_activations(self, batch_size: int = 100) -> int:
        """
        Process pending referral activations in batches.
        This method should be called periodically to activate referrals
        for users who have made tosses since their referral was recorded.
        
        Args:
            batch_size: Number of activations to process in one batch
            
        Returns:
            Number of referrals activated
        """
        # Note: This requires joining with the events database
        # In a complete implementation, we would add this method
        # For now, we'll just return 0 as a placeholder
        logger.info("Process pending activations not implemented for separated databases")
        return 0
    
    def get_user_stats(self, address: str) -> Dict[str, Any]:
        """
        Get comprehensive stats for a user including referral info.
        
        Args:
            address: User blockchain address
            
        Returns:
            Dictionary with user stats
        """
        address = address.lower()
        conn = self.app_db.get_connection()
        cursor = conn.cursor()
        
        # Initialize result structure
        result = {
            "address": address,
            "total_points": 0,
            "referral_code": None,
            "referrer_code_used": None,
            "referrals_count": 0,
            "referrals_activated": 0,
            "total_tosses": 0,
            "total_value_spent": "0",
            "total_wins": 0
        }
        
        try:
            # Get points info
            cursor.execute('SELECT * FROM user_points WHERE address = ?', (address,))
            points_data = cursor.fetchone()
            if points_data:
                result["total_points"] = points_data["total_points"]
            
            # Get referral info
            cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
            referral_data = cursor.fetchone()
            if referral_data:
                result["referral_code"] = referral_data["referral_code"]
                
                # If this user has a referrer, get that referrer's code
                if referral_data["referrer_address"]:
                    cursor.execute('SELECT referral_code FROM user_referrals WHERE address = ?', 
                                  (referral_data["referrer_address"],))
                    referrer_code = cursor.fetchone()
                    if referrer_code:
                        result["referrer_code_used"] = referrer_code["referral_code"]
            
            # Count referrals (total and activated)
            cursor.execute('''
            SELECT COUNT(*), SUM(is_activated) 
            FROM user_referrals 
            WHERE referrer_address = ?
            ''', (address,))
            
            referral_counts = cursor.fetchone()
            if referral_counts and referral_counts[0]:
                result["referrals_count"] = referral_counts[0]
                result["referrals_activated"] = referral_counts[1] or 0
            
            # Note: For complete implementation, we would get toss/win info
            # from the events database. In this revision we'll assume this data
            # is fetched elsewhere or by a separate method.
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            if conn in locals():
                conn.close()
            
        return result
    
    def get_leaderboard(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get the global leaderboard with all required information.
        
        Args:
            limit: Maximum number of entries to return
            offset: Pagination offset
            
        Returns:
            List of user dictionaries
        """
        conn = self.app_db.get_connection()
        cursor = conn.cursor()
        
        leaderboard = []
        
        try:
            # Query to get all the users with points, ordered by total points
            cursor.execute('''
            SELECT up.address, up.total_points, up.toss_points, up.winner_points, up.referral_points
            FROM user_points up
            ORDER BY up.total_points DESC
            LIMIT ? OFFSET ?
            ''', (limit, offset))
            
            users = cursor.fetchall()
            
            for user in users:
                address = user['address']
                
                # Get referral info for this user
                cursor.execute('''
                SELECT referral_code, is_activated,
                       (SELECT COUNT(*) FROM user_referrals WHERE referrer_address = ?) as referrals_count,
                       (SELECT COUNT(*) FROM user_referrals WHERE referrer_address = ? AND is_activated = 1) as activated_referrals
                FROM user_referrals
                WHERE address = ?
                ''', (address, address, address))
                
                referral_info = cursor.fetchone()
                
                # Build user stats dictionary
                user_stats = {
                    "address": address,
                    "total_points": user['total_points'],
                    "toss_points": user['toss_points'],
                    "winner_points": user['winner_points'],
                    "referral_points": user['referral_points'],
                    "referral_code": referral_info['referral_code'] if referral_info else None,
                    "referrals_count": referral_info['referrals_count'] if referral_info else 0,
                    "referrals_activated": referral_info['activated_referrals'] if referral_info else 0
                }
                
                leaderboard.append(user_stats)
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error getting leaderboard: {e}")
            if conn in locals():
                conn.close()
        
        return leaderboard

# Main execution function for testing
if __name__ == "__main__":
    # Get application database path
    app_db_path = get_app_db_path()
    
    # Create referral system
    referral_system = ReferralSystem(app_db_path)
    
    # Test code generation
    code = referral_system.generate_referral_code()
    print(f"Generated referral code: {code}")
    
    # Example of creating a user referral
    test_address = "0x1234567890abcdef1234567890abcdef12345678"
    user = referral_system.get_or_create_user_referral(test_address)
    print(f"User referral: {user}")