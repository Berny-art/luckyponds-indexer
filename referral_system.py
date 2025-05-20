#!/usr/bin/env python3
import os
import time
import random
import string
import sqlite3
import logging
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
DB_PATH = os.getenv("DB_PATH", "/app/data/lucky_ponds.db")
REFERRAL_BONUS_POINTS = 20  # Points awarded to referrer when a referee makes their first toss

def generate_referral_code(length: int = 8) -> str:
    """Generate a unique referral code."""
    characters = string.ascii_uppercase + string.digits
    while True:
        # Generate a random code
        code = ''.join(random.choices(characters, k=length))
        
        # Check if the code already exists
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM user_referrals WHERE referral_code = ?', (code,))
        count = cursor.fetchone()[0]
        conn.close()
        
        # If code doesn't exist, return it
        if count == 0:
            return code

def get_or_create_user_referral(address: str) -> Dict[str, Any]:
    """Get or create a user's referral record."""
    address = address.lower()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if user already has a referral code
    cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
    user_referral = cursor.fetchone()
    
    if user_referral:
        result = dict(zip([column[0] for column in cursor.description], user_referral))
    else:
        # Create a new referral code
        referral_code = generate_referral_code()
        current_time = int(time.time())
        
        # Insert new record
        cursor.execute('''
        INSERT INTO user_referrals 
        (address, referral_code, created_at) 
        VALUES (?, ?, ?)
        ''', (address, referral_code, current_time))
        conn.commit()
        
        # Get the newly created record
        cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
        user_referral = cursor.fetchone()
        result = dict(zip([column[0] for column in cursor.description], user_referral))
    
    conn.close()
    return result

def apply_referral_code(user_address: str, referral_code: str) -> Tuple[bool, str]:
    """Apply a referral code to a user account."""
    user_address = user_address.lower()
    conn = sqlite3.connect(DB_PATH)
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

def check_and_activate_referral(user_address: str) -> bool:
    """
    Check if a user has made their first toss and activate their referral if needed.
    Returns True if a referral was activated, False otherwise.
    """
    user_address = user_address.lower()
    conn = sqlite3.connect(DB_PATH)
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
        if not result or result[1] == 1:
            conn.close()
            return False
        
        referrer_address = result[0]
        
        # Check if user has made at least one toss
        cursor.execute('''
        SELECT COUNT(*) FROM coin_tossed_events 
        WHERE frog_address = ?
        ''', (user_address,))
        
        toss_count = cursor.fetchone()[0]
        
        if toss_count > 0:
            # Activate the referral
            current_time = int(time.time())
            cursor.execute('''
            UPDATE user_referrals 
            SET is_activated = 1, activated_at = ? 
            WHERE address = ?
            ''', (current_time, user_address))
            
            # Award points to the referrer
            cursor.execute('''
            UPDATE user_referrals 
            SET referral_points_earned = referral_points_earned + ? 
            WHERE address = ?
            ''', (REFERRAL_BONUS_POINTS, referrer_address))
            
            # Also update the user_points table if the referrer exists there
            cursor.execute('''
            INSERT OR IGNORE INTO user_points 
            (address, total_points, toss_points, max_toss_points, winner_points, last_updated) 
            VALUES (?, ?, 0, 0, 0, ?)
            ''', (referrer_address, REFERRAL_BONUS_POINTS, current_time))
            
            cursor.execute('''
            UPDATE user_points 
            SET total_points = total_points + ?, 
                last_updated = ? 
            WHERE address = ?
            ''', (REFERRAL_BONUS_POINTS, current_time, referrer_address))
            
            # Log the referral bonus in the points events table
            tx_hash = f"referral_{user_address}_{current_time}"  # Create a unique identifier
            cursor.execute('''
            INSERT INTO user_point_events 
            (address, event_type, points, tx_hash, pond_type, timestamp) 
            VALUES (?, 'referral', ?, ?, 'referral', ?)
            ''', (referrer_address, REFERRAL_BONUS_POINTS, tx_hash, current_time))
            
            conn.commit()
            logger.info(f"Activated referral: {user_address} referred by {referrer_address}, awarded {REFERRAL_BONUS_POINTS} points")
            conn.close()
            return True
        
        conn.close()
        return False
    
    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"Error checking/activating referral: {e}")
        return False

def get_user_stats(address: str) -> Dict[str, Any]:
    """Get comprehensive stats for a user including referral info."""
    address = address.lower()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Initialize result structure
    result = {
        "address": address,
        "total_points": 0,
        "toss_points": 0,
        "winner_points": 0,
        "referral_code": None,
        "referrer_code_used": None,
        "referrals_count": 0,
        "referrals_activated": 0,
        "referral_points_earned": 0,
        "total_tosses": 0,
        "total_value_spent": "0",
        "total_wins": 0
    }
    
    try:
        # Get points info
        cursor.execute('SELECT * FROM user_points WHERE address = ?', (address,))
        points_data = cursor.fetchone()
        if points_data:
            points_dict = dict(zip([column[0] for column in cursor.description], points_data))
            result["total_points"] = points_dict["total_points"]
            result["toss_points"] = points_dict["toss_points"]  # Add this line
            result["winner_points"] = points_dict["winner_points"]  # Add this line
        
        # Get referral info
        cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
        referral_data = cursor.fetchone()
        if referral_data:
            referral_dict = dict(zip([column[0] for column in cursor.description], referral_data))
            result["referral_code"] = referral_dict["referral_code"]
            result["referral_points_earned"] = referral_dict["referral_points_earned"]
            
            # If this user has a referrer, get that referrer's code
            if referral_dict["referrer_address"]:
                cursor.execute('SELECT referral_code FROM user_referrals WHERE address = ?', 
                               (referral_dict["referrer_address"],))
                referrer_code = cursor.fetchone()
                if referrer_code:
                    result["referrer_code_used"] = referrer_code[0]
        
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
        
        # Get toss info
        cursor.execute('''
        SELECT COUNT(*), COALESCE(SUM(CAST(amount as DECIMAL)), 0) 
        FROM coin_tossed_events 
        WHERE frog_address = ?
        ''', (address,))
        
        toss_data = cursor.fetchone()
        if toss_data:
            result["total_tosses"] = toss_data[0]
            result["total_value_spent"] = str(toss_data[1])
        
        # Get win info
        cursor.execute('''
        SELECT COUNT(*) 
        FROM lucky_winner_selected_events 
        WHERE winner_address = ?
        ''', (address,))
        
        win_count = cursor.fetchone()
        if win_count:
            result["total_wins"] = win_count[0]
    
    except Exception as e:
        logger.error(f"Error getting user stats: {e}")
    finally:
        conn.close()
    
    return result

def get_leaderboard(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """Get the global leaderboard with all required information."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    leaderboard = []
    
    try:
        # Query to get all the users with points, ordered by total points
        cursor.execute('''
        SELECT up.address, up.total_points
        FROM user_points up
        ORDER BY up.total_points DESC
        LIMIT ? OFFSET ?
        ''', (limit, offset))
        
        users = cursor.fetchall()
        
        for user in users:
            address = user[0]
            
            # Get detailed stats for each user
            user_stats = get_user_stats(address)
            leaderboard.append(user_stats)
    
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
    finally:
        conn.close()
    
    return leaderboard

# Testing function
if __name__ == "__main__":
    # Test code generation
    print(f"Generated code: {generate_referral_code()}")
    
    # Example of creating a user referral
    user = get_or_create_user_referral("0x1234567890abcdef1234567890abcdef12345678")
    print(f"User referral: {user}")