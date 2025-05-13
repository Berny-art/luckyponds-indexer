#!/usr/bin/env python3
import os
import json
import time
import logging
from typing import List, Dict, Any, Optional
import sqlite3
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.types import LogReceipt
import threading
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration - Set these in your .env file
RPC_URL = os.getenv("RPC_URL", "")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").lower()
DB_PATH = os.getenv("DB_PATH", "lucky_ponds.db")
START_BLOCK = int(os.getenv("START_BLOCK", "22169383"))
BLOCK_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "500"))
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "15"))  # In seconds

# Load ABI
with open('contract_abi.json', 'r') as f:
    CONTRACT_ABI = json.load(f)

# Connect to Web3
w3 = Web3(Web3.HTTPProvider(RPC_URL))
contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=CONTRACT_ABI)

# Event signatures we're interested in
EVENT_SIGNATURES = {
    'CoinTossed': contract.events.CoinTossed,
    'LuckyFrogSelected': contract.events.LuckyFrogSelected,
    'PondAction': contract.events.PondAction,
    'ConfigUpdated': contract.events.ConfigUpdated,
    # Add any other events you're interested in
}

class BlockchainIndexer:
    def __init__(self, db_path: str):
        """Initialize the blockchain indexer with the database path."""
        self.db_path = db_path
        self.setup_database()
        self.last_indexed_block = self.get_last_indexed_block()
        
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
    
    def process_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process an event based on its type."""
        event_name = event['event']
        
        if event_name == 'CoinTossed':
            self.process_coin_tossed_event(event, block_timestamp)
        elif event_name == 'LuckyFrogSelected':
            self.process_lucky_frog_selected_event(event, block_timestamp)
        elif event_name == 'PondAction':
            self.process_pond_action_event(event, block_timestamp)
        elif event_name == 'ConfigUpdated':
            self.process_config_updated_event(event, block_timestamp)
        # Add handlers for any other events you're interested in
    
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
                except Exception as e:
                    # This log doesn't match this event signature, continue to the next
                    continue
    
    def index_blocks(self, start_block: int, end_block: int):
        """Index events from a range of blocks."""
        logger.info(f"Indexing blocks from {start_block} to {end_block}")
        
        # We need to get block timestamps for each block
        block_timestamps = {}
        
        try:
            # Get event logs from all blocks in the range
            logs = w3.eth.get_logs({
                'fromBlock': start_block,
                'toBlock': end_block,
                'address': Web3.to_checksum_address(CONTRACT_ADDRESS)
            })
            
            # Get timestamps for each unique block
            unique_blocks = set(log['blockNumber'] for log in logs)
            for block_num in unique_blocks:
                block = w3.eth.get_block(block_num)
                block_timestamps[block_num] = block.timestamp
            
            # Process all logs with their timestamps
            self.process_logs(logs, block_timestamps)
            
            # Update the last indexed block
            self.update_last_indexed_block(end_block)
            logger.info(f"Indexed {len(logs)} events from blocks {start_block} to {end_block}")
        
        except Exception as e:
            logger.error(f"Error indexing blocks {start_block} to {end_block}: {e}")
            # Don't update the last indexed block on error
    
    def start_indexing(self):
        """Start the indexing process."""
        logger.info(f"Starting indexer from block {self.last_indexed_block}")
        
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
                
                # Index in batches to avoid timeout
                start_block = self.last_indexed_block + 1
                end_block = min(start_block + BLOCK_BATCH_SIZE - 1, safe_block)
                
                self.index_blocks(start_block, end_block)
                
                # If we've caught up to the safe block, wait before the next batch
                if end_block >= safe_block:
                    time.sleep(POLLING_INTERVAL)
            
            except BlockNotFound:
                logger.error("Block not found, network might be syncing")
                time.sleep(POLLING_INTERVAL)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(POLLING_INTERVAL)

if __name__ == "__main__":
    indexer = BlockchainIndexer(DB_PATH)
    indexer.start_indexing()