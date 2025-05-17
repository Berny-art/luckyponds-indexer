#!/usr/bin/env python3
import os
import json
import time
import logging
from typing import List, Dict, Any, Optional, Tuple
import sqlite3
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.types import LogReceipt
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
RPC_URL = os.getenv("RPC_URL", "https://rpc.hyperliquid-testnet.xyz/evm")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").lower()
DB_PATH = os.getenv("DB_PATH", "/app/data/lucky_ponds.db")
START_BLOCK = int(os.getenv("START_BLOCK", "0"))
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "30"))  # In seconds
INITIAL_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "100"))

# Load ABI
try:
    with open('contract_abi.json', 'r') as f:
        CONTRACT_ABI = json.load(f)
except FileNotFoundError:
    logger.error("contract_abi.json not found. Please create this file with the contract events ABI.")
    CONTRACT_ABI = []

# Connect to Web3 with extended timeout
w3 = Web3(Web3.HTTPProvider(
    RPC_URL,
    request_kwargs={'timeout': 60}
))

# Create contract instance if ABI is available
if CONTRACT_ABI:
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=CONTRACT_ABI)
    # Event signatures we're interested in
    EVENT_SIGNATURES = {
        'CoinTossed': contract.events.CoinTossed,
        'LuckyWinnerSelected': contract.events.LuckyWinnerSelected,  # Updated from LuckyFrogSelected
        'PondAction': contract.events.PondAction,
        'ConfigChanged': contract.events.ConfigChanged,  # Updated from ConfigUpdated
        'EmergencyAction': contract.events.EmergencyAction  # New event
    }
else:
    logger.warning("No ABI loaded. Please ensure contract_abi.json exists and is valid.")
    EVENT_SIGNATURES = {}

class AdaptiveBlockchainIndexer:
    def __init__(self, db_path: str):
        """Initialize the blockchain indexer with the database path."""
        self.db_path = db_path
        self.setup_database()
        self.last_indexed_block = self.get_last_indexed_block()
        self.current_batch_size = INITIAL_BATCH_SIZE
        self.min_batch_size = 5
        self.max_batch_size = 200
        self.backoff_factor = 0.5  # How much to reduce batch size on failure
        self.success_factor = 1.1  # How much to increase batch size on success (10%)
        
    def setup_database(self):
        """Create the database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create indexer_state table to track progress
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS indexer_state (
            id INTEGER PRIMARY KEY,
            last_block INTEGER NOT NULL
        )
        ''')
        
        # Check if we need to initialize the indexer_state
        cursor.execute('SELECT COUNT(*) FROM indexer_state')
        if cursor.fetchone()[0] == 0:
            cursor.execute('INSERT INTO indexer_state (id, last_block) VALUES (1, ?)', (START_BLOCK,))
        
        # Create coin_tossed_events table
        # Updated to match new event structure
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
            UNIQUE(tx_hash, pond_type, frog_address)
        )
        ''')
        
        # Update lucky_frog_selected_events to lucky_winner_selected_events
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
            UNIQUE(tx_hash, pond_type)
        )
        ''')
        
        # Migrate data from old table if it exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='lucky_frog_selected_events'")
        if cursor.fetchone():
            cursor.execute('''
            INSERT OR IGNORE INTO lucky_winner_selected_events 
            (tx_hash, block_number, block_timestamp, pond_type, winner_address, prize, selector)
            SELECT tx_hash, block_number, block_timestamp, pond_type, lucky_frog, prize, selector
            FROM lucky_frog_selected_events
            ''')
            logger.info("Migrated data from lucky_frog_selected_events to lucky_winner_selected_events")
        
        # Pond action events table - remains similar
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
        
        # Update config_updated_events to config_changed_events
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
        
        # Migrate data from old table if it exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='config_updated_events'")
        if cursor.fetchone():
            cursor.execute('''
            INSERT OR IGNORE INTO config_changed_events 
            (tx_hash, block_number, block_timestamp, config_type, pond_type, old_value, new_value, old_address, new_address)
            SELECT tx_hash, block_number, block_timestamp, config_type, pond_type, old_value, new_value, old_address, new_address
            FROM config_updated_events
            ''')
            logger.info("Migrated data from config_updated_events to config_changed_events")
        
        # Add new table for emergency_action_events
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
        
        # Add new table for user_points to track competition
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
        
        # Add table to track user points history for each event
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
        
        conn.commit()
        conn.close()
    
    def get_last_indexed_block(self) -> int:
        """Get the last indexed block from the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT last_block FROM indexer_state WHERE id = 1')
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else START_BLOCK
    
    def update_last_indexed_block(self, block_number: int):
        """Update the last indexed block in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE indexer_state SET last_block = ? WHERE id = 1', (block_number,))
        conn.commit()
        conn.close()
        self.last_indexed_block = block_number
    
    def process_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process an event based on its type."""
        event_name = event['event']
        
        try:
            if event_name == 'CoinTossed':
                self.process_coin_tossed_event(event, block_timestamp)
            elif event_name == 'LuckyWinnerSelected':  # Updated from LuckyFrogSelected
                self.process_lucky_winner_selected_event(event, block_timestamp)
            elif event_name == 'PondAction':
                self.process_pond_action_event(event, block_timestamp)
            elif event_name == 'ConfigChanged':  # Updated from ConfigUpdated
                self.process_config_changed_event(event, block_timestamp)
            elif event_name == 'EmergencyAction':  # New event
                self.process_emergency_action_event(event, block_timestamp)
        except Exception as e:
            logger.error(f"Error processing {event_name} event: {e}")
    
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
            elif event_type == 'max_toss':
                cursor.execute('''
                UPDATE user_points 
                SET max_toss_points = max_toss_points + ?, 
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
            
            # Record the specific point-earning event
            cursor.execute('''
            INSERT INTO user_point_events 
            (address, event_type, points, tx_hash, pond_type, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (address.lower(), event_type, points, tx_hash, pond_type, timestamp))
            
            conn.commit()
        except Exception as e:
            logger.error(f"Error adding points for {address}: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_pond_period_points(self, pond_type: str) -> int:
        """Get the points awarded for a specific pond period."""
        # Convert bytes to hex if needed
        if isinstance(pond_type, bytes):
            pond_type_hex = pond_type.hex()
        else:
            pond_type_hex = pond_type
        
        # Get the standard pond types from contract for comparison
        try:
            # These should be configured based on your contract's standard pond types
            FIVE_MIN_POND_TYPE = "0x4608d971a2c5e7a58fc11b6e24dfb34a5d5229ba79a246c8db8bff13c28585e3"
            HOURLY_POND_TYPE = "0x71436e6480b02d0a0d9d1b32f2605b5a8d5bf57edc5276dbae776a3205ff042a"
            DAILY_POND_TYPE = "0x84eebf87e6e26633aeb5b6fb33eabeeade8b46fb27ee88a8c28ef70231ebd6a8"
            WEEKLY_POND_TYPE = "0xe1f30d5367a00d703c7de2a91f675de0b1b59b1d7a662b30b1512a39d217148c"
            MONTHLY_POND_TYPE = "0xe0069269e2394a85569da74fd56114a3b0219c4ffecfaeb48a5e2a13ee8b4f97"
            
            if pond_type_hex == FIVE_MIN_POND_TYPE:
                return 1  # 5-minute pond: 1 point
            elif pond_type_hex == HOURLY_POND_TYPE:
                return 5  # Hourly pond: 5 points
            elif pond_type_hex == DAILY_POND_TYPE:
                return 10  # Daily pond: 10 points
            elif pond_type_hex == WEEKLY_POND_TYPE:
                return 20  # Weekly pond: 20 points
            elif pond_type_hex == MONTHLY_POND_TYPE:
                return 50  # Monthly pond: 50 points
            else:
                # For custom ponds, check the period from the pond configuration
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                SELECT period FROM pond_action_events 
                WHERE pond_type = ? 
                ORDER BY block_timestamp DESC 
                LIMIT 1
                ''', (pond_type,))
                
                result = cursor.fetchone()
                conn.close()
                
                if result:
                    period = result[0]
                    # Map period enum to points
                    period_points = {
                        0: 1,  # FiveMin
                        1: 5,  # Hourly
                        2: 10, # Daily
                        3: 20, # Weekly
                        4: 50  # Monthly
                    }
                    return period_points.get(period, 5)  # Default to 5 if period not found
                return 5  # Default to 5 points for unknown pond types
        except Exception as e:
            logger.error(f"Error determining pond period points: {e}")
            return 5  # Default to 5 points
    
    def check_max_toss(self, pond_type: str, amount: str, max_amount: str = None) -> bool:
        """Check if a toss is a max toss amount for the pond."""
        if max_amount is None:
            # Try to get the max toss amount from the config
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get the latest max toss amount from config_changed_events
            cursor.execute('''
            SELECT new_value FROM config_changed_events 
            WHERE pond_type = ? AND config_type = 'maxTotalTossAmount'
            ORDER BY block_timestamp DESC 
            LIMIT 1
            ''', (pond_type,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                max_amount = result[0]
            else:
                # Default max amount if not found
                max_amount = "1000000000000000000"  # 1 ETH in wei as default
        
        # Compare the toss amount with max amount
        return amount == max_amount
    
    def process_lucky_winner_selected_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a LuckyWinnerSelected event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO lucky_winner_selected_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                winner_address, prize, selector
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                args['pondType'].hex(),
                args['winner'].lower(),  # Updated from 'luckyFrog' to 'winner'
                str(args['prize']),
                args['selector'].lower()
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        finally:
            conn.close()
    
    def process_pond_action_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a PondAction event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO pond_action_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                name, start_time, end_time, action_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                args['pondType'].hex(),
                args['name'],
                args['startTime'],
                args['endTime'],
                args['actionType']
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        finally:
            conn.close()
    
    def process_config_changed_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a ConfigChanged event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO config_changed_events (
                tx_hash, block_number, block_timestamp, config_type, 
                pond_type, old_value, new_value, old_address, new_address
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                args['configType'],
                args['pondType'].hex(),
                str(args['oldValue']) if args['oldValue'] is not None else None,
                str(args['newValue']) if args['newValue'] is not None else None,
                args['oldAddress'].lower() if args['oldAddress'] != '0x0000000000000000000000000000000000000000' else None,
                args['newAddress'].lower() if args['newAddress'] != '0x0000000000000000000000000000000000000000' else None
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        finally:
            conn.close()
    
    def process_emergency_action_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save an EmergencyAction event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO emergency_action_events (
                tx_hash, block_number, block_timestamp, action_type, 
                recipient, token, amount, pond_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                args['actionType'],
                args['recipient'].lower(),
                args['token'].lower(),
                str(args['amount']),
                args['pondType'].hex()
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        finally:
            conn.close()
    
    def process_logs(self, logs: List[LogReceipt], block_timestamps: Dict[int, int]):
        """Process a list of event logs."""
        for log in logs:
            # Try to decode the log
            for event_name, event_abi in EVENT_SIGNATURES.items():
                try:
                    # Decode the log
                    decoded_log = event_abi().process_log(log)
                    block_timestamp = block_timestamps.get(log['blockNumber'], 0)
                    self.process_event(decoded_log, block_timestamp)
                    break  # Stop trying other event signatures if one matches
                except Exception:
                    # This log doesn't match this event signature, continue to the next
                    continue

    def try_get_logs(self, start_block: int, end_block: int, max_retries: int = 3) -> Tuple[bool, List[LogReceipt]]:
        """Try to get logs for a range of blocks with retries."""
        for attempt in range(max_retries):
            try:
                logs = w3.eth.get_logs({
                    'fromBlock': start_block,
                    'toBlock': end_block,
                    'address': Web3.to_checksum_address(CONTRACT_ADDRESS)
                })
                return True, logs
            except Exception as e:
                delay = 2 ** attempt
                logger.error(f"Error getting logs for blocks {start_block}-{end_block} (attempt {attempt+1}/{max_retries}): {e}")
                
                # Check for invalid block range error
                if "invalid block range" in str(e).lower():
                    logger.warning("Invalid block range detected")
                    return False, []
                
                # Check for rate limiting
                if "rate limited" in str(e).lower():
                    delay = delay * 2
                    logger.warning(f"Rate limit encountered. Waiting {delay} seconds before retry...")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("Max retries reached")
                    return False, []
        
        return False, []

    def try_get_block(self, block_number: int, max_retries: int = 3) -> Tuple[bool, Any]:
        """Try to get a block with retries."""
        for attempt in range(max_retries):
            try:
                block = w3.eth.get_block(block_number)
                return True, block
            except Exception as e:
                delay = 2 ** attempt
                logger.error(f"Error getting block {block_number} (attempt {attempt+1}/{max_retries}): {e}")
                
                # Check for rate limiting
                if "rate limited" in str(e).lower():
                    delay = delay * 2
                    logger.warning(f"Rate limit encountered. Waiting {delay} seconds before retry...")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("Max retries reached")
                    return False, None
        
        return False, None
    
    def process_block_range(self, start_block: int, end_block: int) -> bool:
        """Process a range of blocks, and adjust batch size based on success/failure."""
        logger.info(f"Processing blocks {start_block} to {end_block} (batch size: {self.current_batch_size})")
        
        # Try to get logs for the range
        success, logs = self.try_get_logs(start_block, end_block)
        
        if not success:
            # Reduce batch size for next time
            new_batch_size = max(self.min_batch_size, int(self.current_batch_size * self.backoff_factor))
            logger.warning(f"Reducing batch size from {self.current_batch_size} to {new_batch_size}")
            self.current_batch_size = new_batch_size
            
            # If range is too large, try processing a smaller range
            if end_block - start_block > self.min_batch_size:
                mid_block = start_block + self.min_batch_size
                logger.info(f"Falling back to smaller range: {start_block} to {mid_block}")
                result = self.process_block_range(start_block, mid_block)
                
                # If successful, try the next small batch
                if result and mid_block < end_block:
                    next_end = min(mid_block + self.min_batch_size, end_block)
                    return self.process_block_range(mid_block + 1, next_end)
                return result
            else:
                # Process blocks one at a time as last resort
                logger.info("Processing blocks one by one as fallback")
                for block_num in range(start_block, end_block + 1):
                    success = self.process_single_block(block_num)
                    if not success:
                        return False
                return True
        
        # We got logs successfully, now get block timestamps
        block_timestamps = {}
        if logs:
            unique_blocks = set(log['blockNumber'] for log in logs)
            for block_num in unique_blocks:
                success, block = self.try_get_block(block_num)
                if success:
                    block_timestamps[block_num] = block.timestamp
                else:
                    logger.warning(f"Could not get timestamp for block {block_num}")
        
        # Process the logs with their timestamps
        self.process_logs(logs, block_timestamps)
        
        # Update the last indexed block
        self.update_last_indexed_block(end_block)
        
        # Increase batch size for next time (success case)
        new_batch_size = min(self.max_batch_size, int(self.current_batch_size * self.success_factor))
        if new_batch_size > self.current_batch_size:
            logger.info(f"Increasing batch size from {self.current_batch_size} to {new_batch_size}")
            self.current_batch_size = new_batch_size
        
        return True
    
    def process_single_block(self, block_num: int) -> bool:
        """Process a single block as a fallback method."""
        logger.info(f"Processing single block {block_num}")
        success, logs = self.try_get_logs(block_num, block_num)
        
        if success and logs:
            success, block = self.try_get_block(block_num)
            if success:
                block_timestamps = {block_num: block.timestamp}
                self.process_logs(logs, block_timestamps)
        
        # Always update the last indexed block to avoid getting stuck
        self.update_last_indexed_block(block_num)
        return success
    
    def start_indexing(self):
        """Start the indexing process with adaptive batch sizes."""
        logger.info(f"Starting indexer from block {self.last_indexed_block} with initial batch size {self.current_batch_size}")
        
        while True:
            try:
                # Get the current block number
                current_block = w3.eth.block_number
                
                # Don't index up to the latest block to avoid reorgs
                safe_block = current_block - 5
                
                if safe_block <= self.last_indexed_block:
                    logger.info(f"No new blocks to index. Last indexed: {self.last_indexed_block}, Current safe block: {safe_block}")
                    time.sleep(POLLING_INTERVAL)
                    continue
                
                # Calculate the next batch within safe limit
                start_block = self.last_indexed_block + 1
                end_block = min(start_block + self.current_batch_size - 1, safe_block)
                
                # Process the batch
                self.process_block_range(start_block, end_block)
                
                # Small delay to avoid hammering the RPC
                time.sleep(1)
                
            except BlockNotFound:
                logger.error("Block not found, network might be syncing")
                time.sleep(POLLING_INTERVAL)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(POLLING_INTERVAL)

if __name__ == "__main__":
    indexer = AdaptiveBlockchainIndexer(DB_PATH)
    indexer.start_indexing()