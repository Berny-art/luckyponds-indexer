import sqlite3
import logging
import os

logger = logging.getLogger(__name__)

def migrate_points_to_mainnet(testnet_db_path: str, mainnet_db_path: str):
    """Migrate user points from testnet to mainnet database."""
    
    # Ensure mainnet database directory exists
    os.makedirs(os.path.dirname(mainnet_db_path), exist_ok=True)
    
    # Connect to both databases
    testnet_conn = sqlite3.connect(testnet_db_path)
    mainnet_conn = sqlite3.connect(mainnet_db_path)
    
    try:
        testnet_cursor = testnet_conn.cursor()
        mainnet_cursor = mainnet_conn.cursor()
        
        # Migrate user_points table
        logger.info("Migrating user points...")
        testnet_cursor.execute("SELECT address, total_points, toss_points, winner_points, referral_points, last_updated FROM user_points")
        user_points = testnet_cursor.fetchall()
        
        for point_data in user_points:
            mainnet_cursor.execute('''
                INSERT OR REPLACE INTO user_points 
                (address, total_points, toss_points, winner_points, referral_points, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', point_data)
        
        # Migrate user_referrals table
        logger.info("Migrating user referrals...")
        testnet_cursor.execute("SELECT address, referral_code, referrer_address, is_activated, created_at, activated_at FROM user_referrals")
        referrals = testnet_cursor.fetchall()
        
        for referral_data in referrals:
            mainnet_cursor.execute('''
                INSERT OR REPLACE INTO user_referrals 
                (address, referral_code, referrer_address, is_activated, created_at, activated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', referral_data)
        
        # Migrate user_point_events table (optional, for audit trail)
        logger.info("Migrating user point events...")
        testnet_cursor.execute("SELECT address, event_type, points, tx_hash, pond_type, timestamp FROM user_point_events")
        point_events = testnet_cursor.fetchall()
        
        for event_data in point_events:
            mainnet_cursor.execute('''
                INSERT OR REPLACE INTO user_point_events 
                (address, event_type, points, tx_hash, pond_type, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', event_data)
        
        mainnet_conn.commit()
        logger.info(f"Successfully migrated {len(user_points)} users, {len(referrals)} referrals, and {len(point_events)} point events")
        
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        mainnet_conn.rollback()
        raise
    finally:
        testnet_conn.close()
        mainnet_conn.close()

if __name__ == "__main__":
    # Update these paths based on your configuration
    testnet_db = "/app/data/application.db"
    mainnet_db = "/app/data/mainnet_application.db"
    
    migrate_points_to_mainnet(testnet_db, mainnet_db)