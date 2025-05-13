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
        'LuckyFrogSelected': contract.events.LuckyFrogSelected,
        'PondAction': contract.events.PondAction,
        'ConfigUpdated': contract.events.ConfigUpdated,
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
        
        # Create lucky_frog_selected_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS lucky_frog_selected_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            block_timestamp INTEGER NOT NULL,
            pond_type TEXT NOT NULL,
            lucky_frog TEXT NOT NULL,
            prize TEXT NOT NULL,
            selector TEXT NOT NULL,
            UNIQUE(tx_hash, pond_type)
        )
        ''')
        
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
        
        # Create config_updated_events table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS config_updated_events (
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
            elif event_name == 'LuckyFrogSelected':
                self.process_lucky_frog_selected_event(event, block_timestamp)
            elif event_name == 'PondAction':
                self.process_pond_action_event(event, block_timestamp)
            elif event_name == 'ConfigUpdated':
                self.process_config_updated_event(event, block_timestamp)
        except Exception as e:
            logger.error(f"Error processing {event_name} event: {e}")
    
    def process_coin_tossed_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a CoinTossed event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO coin_tossed_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                frog_address, amount, timestamp, total_pond_tosses, total_pond_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                args['pondType'].hex(),
                args['frog'].lower(),
                str(args['amount']),
                args['timestamp'],
                args['totalPondTosses'],
                str(args['totalPondValue'])
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        finally:
            conn.close()
    
    def process_lucky_frog_selected_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a LuckyFrogSelected event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO lucky_frog_selected_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                lucky_frog, prize, selector
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                args['pondType'].hex(),
                args['luckyFrog'].lower(),
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
    
    def process_config_updated_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a ConfigUpdated event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO config_updated_events (
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