#!/usr/bin/env python3
import os
import json
import time
from typing import List, Dict, Any, Tuple
import sqlite3
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.types import LogReceipt
from dotenv import load_dotenv

# Import our new database and utility classes
from data_access import EventsDatabase
from utils import get_events_db_path, setup_logger, get_current_timestamp

# Configure logging
logger = setup_logger('indexer')

# Load environment variables
load_dotenv()

# Configuration
RPC_URL = os.getenv("RPC_URL", "https://rpc.hyperliquid-testnet.xyz/evm")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").lower()
START_BLOCK = int(os.getenv("START_BLOCK", "0"))
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "10"))  # In seconds
INITIAL_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "200"))

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
    request_kwargs={'timeout': 120}
))

# Create contract instance if ABI is available
if CONTRACT_ABI:
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=CONTRACT_ABI)
    # Event signatures we're interested in
    EVENT_SIGNATURES = {
        'CoinTossed': contract.events.CoinTossed,
        'LuckyWinnerSelected': contract.events.LuckyWinnerSelected,
        'PondAction': contract.events.PondAction,
        'ConfigChanged': contract.events.ConfigChanged,
        'EmergencyAction': contract.events.EmergencyAction
    }
else:
    logger.warning("No ABI loaded. Please ensure contract_abi.json exists and is valid.")
    EVENT_SIGNATURES = {}

class FastBlockchainIndexer:
    def __init__(self, db_path: str):
        """Initialize the blockchain indexer with the database path."""
        self.db = EventsDatabase(db_path)
        self.last_indexed_block = self.db.get_last_indexed_block() or START_BLOCK
        self.current_batch_size = INITIAL_BATCH_SIZE
        self.min_batch_size = 10
        self.max_batch_size = 400
        self.backoff_factor = 0.5  # How much to reduce batch size on failure
        self.success_factor = 1.2  # How much to increase batch size on success (20%)
    
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
                delay = 5 ** attempt
                logger.error(f"Error getting logs for blocks {start_block}-{end_block} (attempt {attempt+1}/{max_retries}): {e}")
                
                # Check for invalid block range error
                if "invalid block range" in str(e).lower():
                    logger.warning("Invalid block range detected")
                    return False, []
                
                # Check for rate limiting
                if "rate limited" in str(e).lower() or "429" in str(e):
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
                time.sleep(0.5)  # Small delay to avoid hammering the RPC
                return True, block
            except Exception as e:
                delay = 2 ** attempt
                logger.error(f"Error getting block {block_number} (attempt {attempt+1}/{max_retries}): {e}")
                
                # Check for rate limiting
                if "rate limited" in str(e).lower() or "429" in str(e):
                    delay = delay * 2
                    logger.warning(f"Rate limit encountered. Waiting {delay} seconds before retry...")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("Max retries reached")
                    return False, None
        
        return False, None
    
    def store_coin_tossed_event(self, conn, tx_hash: str, block_number: int, block_timestamp: int, 
                              pond_type: str, participant_address: str, amount: str, 
                              timestamp: int, total_pond_tosses: int, total_pond_value: str):
        """Store a CoinTossed event in the database."""
        cursor = conn.cursor()
        try:
            cursor.execute('''
            INSERT INTO coin_tossed_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                frog_address, amount, timestamp, total_pond_tosses, total_pond_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx_hash,
                block_number,
                block_timestamp,
                pond_type,
                participant_address,
                amount,
                timestamp,
                total_pond_tosses,
                total_pond_value
            ))
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
    
    def store_lucky_winner_event(self, conn, tx_hash: str, block_number: int, block_timestamp: int,
                               pond_type: str, winner_address: str, prize: str, selector: str):
        """Store a LuckyWinnerSelected event in the database."""
        cursor = conn.cursor()
        try:
            cursor.execute('''
            INSERT INTO lucky_winner_selected_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                winner_address, prize, selector
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx_hash,
                block_number,
                block_timestamp,
                pond_type,
                winner_address,
                prize,
                selector
            ))
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
    
    def store_pond_action_event(self, conn, tx_hash: str, block_number: int, block_timestamp: int,
                              pond_type: str, name: str, start_time: int, end_time: int, action_type: str):
        """Store a PondAction event in the database."""
        cursor = conn.cursor()
        try:
            cursor.execute('''
            INSERT INTO pond_action_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                name, start_time, end_time, action_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx_hash,
                block_number,
                block_timestamp,
                pond_type,
                name,
                start_time,
                end_time,
                action_type
            ))
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
    
    def store_config_changed_event(self, conn, tx_hash: str, block_number: int, block_timestamp: int,
                                 config_type: str, pond_type: str, old_value: str, new_value: str,
                                 old_address: str, new_address: str):
        """Store a ConfigChanged event in the database."""
        cursor = conn.cursor()
        try:
            cursor.execute('''
            INSERT INTO config_changed_events (
                tx_hash, block_number, block_timestamp, config_type, 
                pond_type, old_value, new_value, old_address, new_address
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx_hash,
                block_number,
                block_timestamp,
                config_type,
                pond_type,
                old_value,
                new_value,
                old_address,
                new_address
            ))
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
    
    def store_emergency_action_event(self, conn, tx_hash: str, block_number: int, block_timestamp: int,
                                   action_type: str, recipient: str, token: str, amount: str, pond_type: str):
        """Store an EmergencyAction event in the database."""
        cursor = conn.cursor()
        try:
            cursor.execute('''
            INSERT INTO emergency_action_events (
                tx_hash, block_number, block_timestamp, action_type, 
                recipient, token, amount, pond_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx_hash,
                block_number,
                block_timestamp,
                action_type,
                recipient,
                token,
                amount,
                pond_type
            ))
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
    
    def process_logs(self, logs: List[LogReceipt], block_timestamps: Dict[int, int]):
        """Process a list of event logs using a single database connection for efficiency."""
        if not logs:
            return
            
        # Get a direct connection to make batch operations more efficient
        conn = sqlite3.connect(self.db.db_path)
        conn.isolation_level = None  # Use autocommit mode
        
        try:
            cursor = conn.cursor()
            cursor.execute('BEGIN TRANSACTION')
            
            processed_count = 0
            for log in logs:
                # Try to decode the log
                for event_name, event_abi in EVENT_SIGNATURES.items():
                    try:
                        # Decode the log
                        decoded_log = event_abi().process_log(log)
                        block_timestamp = block_timestamps.get(log['blockNumber'], 0)
                        
                        # Extract common event parameters
                        args = decoded_log['args']
                        tx_hash = decoded_log['transactionHash'].hex()
                        block_number = decoded_log['blockNumber']
                        
                        # Process based on event type
                        event_name = decoded_log['event']
                        if event_name == 'CoinTossed':
                            pond_type = args['pondType'].hex()
                            
                            # Handle different parameter names based on contract
                            participant_address = args.get('participant', args.get('frog', None))
                            if participant_address is None:
                                logger.error(f"Could not find participant/frog address in event: {args}")
                                continue
                                
                            participant_address = participant_address.lower()
                            amount = str(args['amount'])
                            timestamp = args['timestamp']
                            total_pond_tosses = args['totalPondTosses']
                            total_pond_value = str(args['totalPondValue'])
                            
                            self.store_coin_tossed_event(
                                conn, tx_hash, block_number, block_timestamp, pond_type,
                                participant_address, amount, timestamp, total_pond_tosses, total_pond_value
                            )
                            
                        elif event_name == 'LuckyWinnerSelected':
                            pond_type = args['pondType'].hex()
                            
                            # Handle different parameter names based on contract
                            winner_address = args.get('winner', args.get('luckyFrog', None))
                            if winner_address is None:
                                logger.error(f"Could not find winner/luckyFrog address in event: {args}")
                                continue
                                
                            winner_address = winner_address.lower()
                            prize = str(args['prize'])
                            selector = args['selector'].lower()
                            
                            self.store_lucky_winner_event(
                                conn, tx_hash, block_number, block_timestamp, pond_type,
                                winner_address, prize, selector
                            )
                            
                        elif event_name == 'PondAction':
                            pond_type = args['pondType'].hex()
                            name = args['name']
                            start_time = args['startTime']
                            end_time = args['endTime']
                            action_type = args['actionType']
                            
                            self.store_pond_action_event(
                                conn, tx_hash, block_number, block_timestamp, pond_type,
                                name, start_time, end_time, action_type
                            )
                            
                        elif event_name == 'ConfigChanged':
                            # Get the right config type field name based on contract
                            config_type = args.get('configType', args.get('config', ''))
                            pond_type = args['pondType'].hex()
                            
                            old_value = str(args['oldValue']) if args.get('oldValue') is not None else None
                            new_value = str(args['newValue']) if args.get('newValue') is not None else None
                            
                            old_address = args['oldAddress'].lower() if args.get('oldAddress', '0x0000000000000000000000000000000000000000') != '0x0000000000000000000000000000000000000000' else None
                            new_address = args['newAddress'].lower() if args.get('newAddress', '0x0000000000000000000000000000000000000000') != '0x0000000000000000000000000000000000000000' else None
                            
                            self.store_config_changed_event(
                                conn, tx_hash, block_number, block_timestamp, config_type, pond_type,
                                old_value, new_value, old_address, new_address
                            )
                            
                        elif event_name == 'EmergencyAction':
                            action_type = args['actionType']
                            recipient = args['recipient'].lower()
                            token = args['token'].lower()
                            amount = str(args['amount'])
                            pond_type = args['pondType'].hex()
                            
                            self.store_emergency_action_event(
                                conn, tx_hash, block_number, block_timestamp, action_type,
                                recipient, token, amount, pond_type
                            )
                        
                        processed_count += 1
                        break  # Stop trying other event signatures if one matches
                    except Exception:
                        # This log doesn't match this event signature, continue to the next
                        continue
            
            cursor.execute('COMMIT')
            logger.info(f"Successfully processed {processed_count} events in batch transaction")
            
        except Exception as e:
            logger.error(f"Error in batch processing logs: {e}")
            if conn.isolation_level is None:
                cursor.execute('ROLLBACK')
        finally:
            conn.close()
    
    def process_block_range(self, start_block: int, end_block: int) -> bool:
        """Process a range of blocks, and adjust batch size based on success/failure."""
        start_time = time.time()
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
        event_count = len(logs)
        self.process_logs(logs, block_timestamps)
        
        # Update the last indexed block using our database object
        self.db.update_last_indexed_block(end_block)
        self.last_indexed_block = end_block
        
        # Increase batch size for next time (success case)
        new_batch_size = min(self.max_batch_size, int(self.current_batch_size * self.success_factor))
        if new_batch_size > self.current_batch_size:
            logger.info(f"Increasing batch size from {self.current_batch_size} to {new_batch_size}")
            self.current_batch_size = new_batch_size
        
        # Log performance metrics
        elapsed = time.time() - start_time
        blocks_processed = end_block - start_block + 1
        logger.info(f"Processed {blocks_processed} blocks with {event_count} events in {elapsed:.2f} seconds " +
                    f"({blocks_processed/elapsed:.2f} blocks/sec, {event_count/elapsed:.2f} events/sec)")
        
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
        self.db.update_last_indexed_block(block_num)
        self.last_indexed_block = block_num
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
    # Use the utility function to get the db path
    events_db_path = get_events_db_path()
    
    # Create the indexer and start it
    indexer = FastBlockchainIndexer(events_db_path)
    indexer.start_indexing()