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
DB_PATH = os.getenv("DB_PATH", "./app/data/lucky_ponds.db")
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
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.setup_database()
        self.last_indexed_block = self.get_last_indexed_block()
        self.current_batch_size = INITIAL_BATCH_SIZE
        self.min_batch_size = 10
        self.max_batch_size = 500
        self.backoff_factor = 0.5  # How much to reduce batch size on failure
        self.success_factor = 1.2  # How much to increase batch size on success (20%)
        
    def setup_database(self):
        """Set up the database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Create indexer_state table to track progress
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS indexer_state (
                id INTEGER PRIMARY KEY,
                last_block INTEGER NOT NULL,
                last_updated_timestamp INTEGER NOT NULL
            )
            ''')
            
            # Check if we need to initialize the indexer_state
            cursor.execute('SELECT COUNT(*) FROM indexer_state')
            if cursor.fetchone()[0] == 0:
                current_time = int(time.time())
                cursor.execute('INSERT INTO indexer_state (id, last_block, last_updated_timestamp) VALUES (1, ?, ?)', 
                              (START_BLOCK, current_time))
                logger.info(f"Initialized indexer state with block {START_BLOCK}")
            
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
                points_processed INTEGER DEFAULT 0,
                UNIQUE(tx_hash, pond_type, frog_address)
            )
            ''')
            logger.info("Created or verified coin_tossed_events table")
            
            # Create lucky_winner_selected_events table
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
                points_processed INTEGER DEFAULT 0,
                UNIQUE(tx_hash, pond_type)
            )
            ''')
            logger.info("Created or verified lucky_winner_selected_events table")
            
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
            logger.info("Created or verified pond_action_events table")
            
            # Create config_changed_events table
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
            logger.info("Created or verified config_changed_events table")
            
            # Create emergency_action_events table
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
            logger.info("Created or verified emergency_action_events table")
            
            # Create indices for better query performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_coin_tossed_block ON coin_tossed_events (block_number)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_lucky_winner_block ON lucky_winner_selected_events (block_number)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_coin_tossed_processed ON coin_tossed_events (points_processed)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_lucky_winner_processed ON lucky_winner_selected_events (points_processed)')
            
            conn.commit()
            logger.info("Database setup completed successfully")
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_last_indexed_block(self) -> int:
        """Get the last indexed block from the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT last_block FROM indexer_state WHERE id = 1')
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                # If no record exists, insert initial record
                current_time = int(time.time())
                cursor.execute('INSERT INTO indexer_state (id, last_block, last_updated_timestamp) VALUES (1, ?, ?)', 
                              (START_BLOCK, current_time))
                conn.commit()
                return START_BLOCK
        except sqlite3.OperationalError:
            logger.error("indexer_state table not found. Database setup might have failed.")
            return START_BLOCK
        finally:
            conn.close()
    
    def update_last_indexed_block(self, block_number: int):
        """Update the last indexed block in the database."""
        current_time = int(time.time())
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE indexer_state SET last_block = ?, last_updated_timestamp = ? WHERE id = 1', 
                      (block_number, current_time))
        conn.commit()
        conn.close()
        self.last_indexed_block = block_number
    
    def process_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process an event based on its type."""
        event_name = event['event']
        
        try:
            if event_name == 'CoinTossed':
                self.process_coin_tossed_event(event, block_timestamp)
            elif event_name == 'LuckyWinnerSelected':
                self.process_lucky_winner_selected_event(event, block_timestamp)
            elif event_name == 'PondAction':
                self.process_pond_action_event(event, block_timestamp)
            elif event_name == 'ConfigChanged':
                self.process_config_changed_event(event, block_timestamp)
            elif event_name == 'EmergencyAction':
                self.process_emergency_action_event(event, block_timestamp)
        except Exception as e:
            logger.error(f"Error processing {event_name} event: {e}")
    
    def process_coin_tossed_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a CoinTossed event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Extract data from event
            tx_hash = event['transactionHash'].hex()
            block_number = event['blockNumber']
            pond_type = args['pondType'].hex()
            
            # Handle different parameter names based on contract
            participant_address = args.get('participant', args.get('frog', None))
            if participant_address is None:
                logger.error(f"Could not find participant/frog address in event: {args}")
                return
                
            participant_address = participant_address.lower()
            amount = str(args['amount'])
            timestamp = args['timestamp']
            total_pond_tosses = args['totalPondTosses']
            total_pond_value = str(args['totalPondValue'])
            
            # Insert into coin_tossed_events table
            cursor.execute('''
            INSERT INTO coin_tossed_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                frog_address, amount, timestamp, total_pond_tosses, total_pond_value,
                points_processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
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
            conn.commit()
            
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        except Exception as e:
            logger.error(f"Error processing coin tossed event: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def process_lucky_winner_selected_event(self, event: Dict[str, Any], block_timestamp: int):
        """Process and save a LuckyWinnerSelected event."""
        args = event['args']
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Extract data from event
            tx_hash = event['transactionHash'].hex()
            block_number = event['blockNumber']
            pond_type = args['pondType'].hex()
            
            # Handle different parameter names based on contract
            winner_address = args.get('winner', args.get('luckyFrog', None))
            if winner_address is None:
                logger.error(f"Could not find winner/luckyFrog address in event: {args}")
                return
                
            winner_address = winner_address.lower()
            prize = str(args['prize'])
            selector = args['selector'].lower()
            
            # Insert into lucky_winner_selected_events table
            cursor.execute('''
            INSERT INTO lucky_winner_selected_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                winner_address, prize, selector, points_processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            ''', (
                tx_hash,
                block_number,
                block_timestamp,
                pond_type,
                winner_address,
                prize,
                selector
            ))
            conn.commit()
            
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        except Exception as e:
            logger.error(f"Error processing lucky winner event: {e}")
            conn.rollback()
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
            # Get the right config type field name based on contract
            config_type = args.get('configType', args.get('config', ''))
            
            cursor.execute('''
            INSERT INTO config_changed_events (
                tx_hash, block_number, block_timestamp, config_type, 
                pond_type, old_value, new_value, old_address, new_address
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                config_type,
                args['pondType'].hex(),
                str(args['oldValue']) if args.get('oldValue') is not None else None,
                str(args['newValue']) if args.get('newValue') is not None else None,
                args['oldAddress'].lower() if args.get('oldAddress', '0x0000000000000000000000000000000000000000') != '0x0000000000000000000000000000000000000000' else None,
                args['newAddress'].lower() if args.get('newAddress', '0x0000000000000000000000000000000000000000') != '0x0000000000000000000000000000000000000000' else None
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
        except Exception as e:
            logger.error(f"Error processing config changed event: {e}")
            conn.rollback()
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
        except Exception as e:
            logger.error(f"Error processing emergency action event: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def process_logs(self, logs: List[LogReceipt], block_timestamps: Dict[int, int]):
        """Process a list of event logs using a single database transaction for efficiency."""
        if not logs:
            return
            
        conn = sqlite3.connect(self.db_path)
        conn.isolation_level = None  # Use autocommit mode
        cursor = conn.cursor()
        
        try:
            cursor.execute('BEGIN TRANSACTION')
            
            processed_count = 0
            for log in logs:
                # Try to decode the log
                for event_name, event_abi in EVENT_SIGNATURES.items():
                    try:
                        # Decode the log
                        decoded_log = event_abi().process_log(log)
                        block_timestamp = block_timestamps.get(log['blockNumber'], 0)
                        
                        # Process based on event type
                        event_name = decoded_log['event']
                        if event_name == 'CoinTossed':
                            self._process_coin_tossed_transaction(cursor, decoded_log, block_timestamp)
                        elif event_name == 'LuckyWinnerSelected':
                            self._process_lucky_winner_transaction(cursor, decoded_log, block_timestamp)
                        elif event_name == 'PondAction':
                            self._process_pond_action_transaction(cursor, decoded_log, block_timestamp)
                        elif event_name == 'ConfigChanged':
                            self._process_config_changed_transaction(cursor, decoded_log, block_timestamp)
                        elif event_name == 'EmergencyAction':
                            self._process_emergency_action_transaction(cursor, decoded_log, block_timestamp)
                        
                        processed_count += 1
                        break  # Stop trying other event signatures if one matches
                    except Exception:
                        # This log doesn't match this event signature, continue to the next
                        continue
            
            cursor.execute('COMMIT')
            logger.info(f"Successfully processed {processed_count} events in batch transaction")
            
        except Exception as e:
            logger.error(f"Error in batch processing logs: {e}")
            cursor.execute('ROLLBACK')
        finally:
            conn.close()
    
    def _process_coin_tossed_transaction(self, cursor, event, block_timestamp):
        """Helper method to process CoinTossed events within a transaction."""
        args = event['args']
        
        # Extract data from event
        tx_hash = event['transactionHash'].hex()
        block_number = event['blockNumber']
        pond_type = args['pondType'].hex()
        
        # Handle different parameter names based on contract
        participant_address = args.get('participant', args.get('frog', None))
        if participant_address is None:
            logger.error(f"Could not find participant/frog address in event: {args}")
            return
            
        participant_address = participant_address.lower()
        amount = str(args['amount'])
        timestamp = args['timestamp']
        total_pond_tosses = args['totalPondTosses']
        total_pond_value = str(args['totalPondValue'])
        
        try:
            # Insert into coin_tossed_events table
            cursor.execute('''
            INSERT INTO coin_tossed_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                frog_address, amount, timestamp, total_pond_tosses, total_pond_value,
                points_processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
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
    
    def _process_lucky_winner_transaction(self, cursor, event, block_timestamp):
        """Helper method to process LuckyWinnerSelected events within a transaction."""
        args = event['args']
        
        # Extract data from event
        tx_hash = event['transactionHash'].hex()
        block_number = event['blockNumber']
        pond_type = args['pondType'].hex()
        
        # Handle different parameter names based on contract
        winner_address = args.get('winner', args.get('luckyFrog', None))
        if winner_address is None:
            logger.error(f"Could not find winner/luckyFrog address in event: {args}")
            return
            
        winner_address = winner_address.lower()
        prize = str(args['prize'])
        selector = args['selector'].lower()
        
        try:
            # Insert into lucky_winner_selected_events table
            cursor.execute('''
            INSERT INTO lucky_winner_selected_events (
                tx_hash, block_number, block_timestamp, pond_type, 
                winner_address, prize, selector, points_processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
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
    
    def _process_pond_action_transaction(self, cursor, event, block_timestamp):
        """Helper method to process PondAction events within a transaction."""
        args = event['args']
        
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
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
    
    def _process_config_changed_transaction(self, cursor, event, block_timestamp):
        """Helper method to process ConfigChanged events within a transaction."""
        args = event['args']
        
        try:
            # Get the right config type field name based on contract
            config_type = args.get('configType', args.get('config', ''))
            
            cursor.execute('''
            INSERT INTO config_changed_events (
                tx_hash, block_number, block_timestamp, config_type, 
                pond_type, old_value, new_value, old_address, new_address
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event['transactionHash'].hex(),
                event['blockNumber'],
                block_timestamp,
                config_type,
                args['pondType'].hex(),
                str(args['oldValue']) if args.get('oldValue') is not None else None,
                str(args['newValue']) if args.get('newValue') is not None else None,
                args['oldAddress'].lower() if args.get('oldAddress', '0x0000000000000000000000000000000000000000') != '0x0000000000000000000000000000000000000000' else None,
                args['newAddress'].lower() if args.get('newAddress', '0x0000000000000000000000000000000000000000') != '0x0000000000000000000000000000000000000000' else None
            ))
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass
    
    def _process_emergency_action_transaction(self, cursor, event, block_timestamp):
        """Helper method to process EmergencyAction events within a transaction."""
        args = event['args']
        
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
        except sqlite3.IntegrityError:
            # Skip duplicate events
            pass

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
        
        # Update the last indexed block
        self.update_last_indexed_block(end_block)
        
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
    indexer = FastBlockchainIndexer(DB_PATH)
    indexer.start_indexing()