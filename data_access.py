# data_access.py

import sqlite3
import logging
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class Database:
    """Base database access class"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_connection(self):
        """Get a database connection with row factory enabled"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def execute_query(self, query: str, params: Tuple = ()):
        """Execute a query and return all results"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            conn.close()
    
    def execute_scalar(self, query: str, params: Tuple = ()):
        """Execute a query and return a single value"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            conn.close()
    
    def execute_non_query(self, query: str, params: Tuple = ()):
        """Execute a non-query statement (insert, update, delete)"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()
    
    def execute_many(self, query: str, params_list: List[Tuple]):
        """Execute many statements at once"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()
    
    def execute_transaction(self, queries_with_params: List[Tuple[str, Tuple]]):
        """Execute multiple statements in a transaction"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            for query, params in queries_with_params:
                cursor.execute(query, params)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction failed: {e}")
            raise
        finally:
            conn.close()

class EventsDatabase(Database):
    """Handles access to raw blockchain events database"""
    
    def get_last_indexed_block(self) -> int:
        """Get the last indexed block number"""
        return self.execute_scalar(
            'SELECT last_block FROM indexer_state WHERE id = 1'
        ) or 0
    
    def update_last_indexed_block(self, block_number: int):
        """Update the last indexed block number"""
        import time
        current_time = int(time.time())
        return self.execute_non_query(
            'UPDATE indexer_state SET last_block = ?, last_updated_timestamp = ? WHERE id = 1',
            (block_number, current_time)
        )
    
    def get_unprocessed_toss_events(self, last_id: int, limit: int = 1000) -> List[Dict]:
        """Get unprocessed coin toss events"""
        rows = self.execute_query(
            '''
            SELECT id, tx_hash, block_timestamp, pond_type, frog_address, amount, token_address
            FROM coin_tossed_events
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            ''',
            (last_id, limit)
        )
        return [dict(row) for row in rows]
    
    def get_unprocessed_winner_events(self, last_id: int, limit: int = 1000) -> List[Dict]:
        """Get unprocessed winner events"""
        rows = self.execute_query(
            '''
            SELECT id, tx_hash, block_timestamp, pond_type, winner_address, prize
            FROM lucky_winner_selected_events
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            ''',
            (last_id, limit)
        )
        return [dict(row) for row in rows]

class ApplicationDatabase(Database):
    """Handles access to application database (points, referrals, etc.)"""
    
    def get_calculator_state(self) -> Dict[str, int]:
        """Get the current state of the points calculator"""
        row = self.execute_query(
            '''
            SELECT last_processed_toss_id, last_processed_winner_id, last_processed_timestamp 
            FROM calculator_state 
            WHERE id = 1
            '''
        )
        if row:
            return dict(row[0])
        return {
            "last_processed_toss_id": 0,
            "last_processed_winner_id": 0,
            "last_processed_timestamp": 0
        }
    
    def update_calculator_state(self, toss_id: int, winner_id: int, timestamp: int):
        """Update the state of the points calculator"""
        import time
        current_time = int(time.time())
        return self.execute_non_query(
            '''
            UPDATE calculator_state 
            SET last_processed_toss_id = ?, 
                last_processed_winner_id = ?, 
                last_processed_timestamp = ?,
                last_run_timestamp = ?
            WHERE id = 1
            ''', 
            (toss_id, winner_id, timestamp, current_time)
        )
    
    def add_user_points(self, address: str, event_type: str, points: int, 
                       tx_hash: str, pond_type: str, timestamp: int):
        """Add points to a user and record the event"""
        address = address.lower()
        
        # Execute as a transaction
        return self.execute_transaction([
            # Ensure user exists
            (
                '''
                INSERT OR IGNORE INTO user_points 
                (address, total_points, toss_points, winner_points, referral_points, last_updated) 
                VALUES (?, 0, 0, 0, 0, ?)
                ''',
                (address, timestamp)
            ),
            # Update points based on event type
            (
                '''
                UPDATE user_points 
                SET {0}_points = {0}_points + ?, 
                    total_points = total_points + ?,
                    last_updated = ?
                WHERE address = ?
                '''.format('toss' if event_type == 'toss' else 
                          'winner' if event_type == 'winner' else 
                          'referral'),
                (points, points, timestamp, address)
            ),
            # Record the event
            (
                '''
                INSERT INTO user_point_events 
                (address, event_type, points, tx_hash, pond_type, timestamp) 
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (address, event_type, points, tx_hash, pond_type, timestamp)
            )
        ])
    
    def get_user_referral(self, address: str) -> Optional[Dict]:
        """Get a user's referral information"""
        rows = self.execute_query(
            'SELECT * FROM user_referrals WHERE address = ?',
            (address.lower(),)
        )
        return dict(rows[0]) if rows else None