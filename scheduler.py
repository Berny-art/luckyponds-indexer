#!/usr/bin/env python3
import os
import json
import time
import logging
import schedule
import argparse
from typing import Dict, Any
from dotenv import load_dotenv
from web3 import Web3
from datetime import datetime

# Import our database access layers and components
from data_access import EventsDatabase, ApplicationDatabase
from points_calculator import PointsCalculator
from utils import (
    get_events_db_path, 
    get_app_db_path, 
    get_points_calculation_interval,
    setup_logger
)

# Configure logging
logger = setup_logger('scheduler')

# Load environment variables
load_dotenv()

# Configuration
EVENTS_DB_PATH = get_events_db_path()
APP_DB_PATH = get_app_db_path()
POINTS_CALCULATION_INTERVAL = get_points_calculation_interval()
WINNER_SELECTION_INTERVAL = int(os.getenv("WINNER_SELECTION_INTERVAL", "300"))  # 5 minutes default

# Winner selection configuration
RPC_URL = os.getenv("RPC_URL", "https://rpc.hyperliquid-testnet.xyz/evm")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "300000"))
GAS_PRICE_GWEI = int(os.getenv("GAS_PRICE_GWEI", "20"))

# Initialize Web3 and contract for winner selection
w3 = None
contract = None
account = None

def initialize_web3():
    """Initialize Web3 connection and contract for winner selection."""
    global w3, contract, account
    
    if not PRIVATE_KEY or not CONTRACT_ADDRESS:
        logger.warning("PRIVATE_KEY or CONTRACT_ADDRESS not set. Winner selection will be disabled.")
        return False
    
    try:
        # Connect to Web3
        w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={'timeout': 120}))
        
        # Load account
        account = w3.eth.account.from_key(PRIVATE_KEY)
        logger.info(f"Keeper account initialized: {account.address}")
        
        # Load contract ABI and create contract instance
        with open('contract_abi.json', 'r') as f:
            contract_abi = json.load(f)
        
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS), 
            abi=contract_abi
        )
        
        # Check initial balance
        balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
        logger.info(f"Keeper initial balance: {balance:.6f} ETH")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize Web3/contract: {e}")
        return False

def run_points_calculation():
    """Execute the points calculation process."""
    logger.info("Starting scheduled points calculation")
    
    try:
        calculator = PointsCalculator(APP_DB_PATH, EVENTS_DB_PATH)
        events_processed = calculator.run_points_calculation()
        
        logger.info(f"Scheduled points calculation completed: {events_processed} events processed")
        return events_processed
    except Exception as e:
        logger.error(f"Error in scheduled points calculation: {e}")
        return 0

def run_winner_selection():
    """Execute the winner selection process."""
    if not w3 or not contract or not account:
        logger.warning("Winner selection not initialized. Skipping.")
        return 0
    
    logger.info("Starting scheduled winner selection")
    
    try:
        processed_count = 0
        max_iterations = 10  # Prevent infinite loops
        initial_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
        
        while processed_count < max_iterations:
            # Check if upkeep is needed
            upkeep_needed, perform_data = contract.functions.checkUpkeep(b"").call()
            
            if not upkeep_needed:
                if processed_count == 0:
                    logger.info("No winner selection upkeep needed")
                break
            
            processed_count += 1
            pond_type = perform_data[:32].hex() if len(perform_data) >= 32 else "unknown"
            logger.info(f"Processing winner selection {processed_count}: pond {pond_type[:10]}...")
            
            # Build and send transaction
            tx = contract.functions.performUpkeep(perform_data).build_transaction({
                'from': account.address,
                'gas': GAS_LIMIT,
                'gasPrice': w3.to_wei(GAS_PRICE_GWEI, 'gwei'),
                'nonce': w3.eth.get_transaction_count(account.address)
            })
            
            # Sign and send transaction
            signed_tx = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for receipt
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            
            if receipt.status == 1:
                cost = w3.from_wei(receipt.gasUsed * w3.to_wei(GAS_PRICE_GWEI, 'gwei'), 'ether')
                logger.info(f"✅ Winner selected! TX: {tx_hash.hex()}, Gas used: {receipt.gasUsed}, Cost: {cost:.6f} ETH")
            else:
                logger.error(f"❌ Winner selection transaction failed: {tx_hash.hex()}")
                break  # Stop on failure
            
            # Small delay between transactions to avoid nonce issues
            if processed_count < max_iterations:
                time.sleep(2)
        
        if processed_count > 0:
            # Show final balance and total cost
            final_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
            total_cost = initial_balance - final_balance
            logger.info(f"Winner selection completed: {processed_count} pond(s) processed")
            logger.info(f"Total cost: {total_cost:.6f} ETH (balance: {final_balance:.6f} ETH)")
        
        return processed_count
        
    except Exception as e:
        logger.error(f"Error in winner selection: {e}")
        return 0

def run_health_check():
    """Perform periodic health checks."""
    logger.info("Performing health check")
    
    try:
        # Check database connectivity
        events_db = EventsDatabase(EVENTS_DB_PATH)
        app_db = ApplicationDatabase(APP_DB_PATH)
        
        # Simple connectivity test
        events_conn = events_db.get_connection()
        events_conn.execute('SELECT 1')
        events_conn.close()
        
        app_conn = app_db.get_connection()
        app_conn.execute('SELECT 1')
        app_conn.close()
        
        # Check Web3 connectivity if initialized
        if w3 and account:
            current_block = w3.eth.block_number
            balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
            logger.info(f"Health check OK - Block: {current_block}, Balance: {balance:.6f} ETH")
        else:
            logger.info("Health check OK - Databases accessible")
            
    except Exception as e:
        logger.error(f"Health check failed: {e}")

def start_scheduler(
    points_interval: int = POINTS_CALCULATION_INTERVAL,
    winner_interval: int = WINNER_SELECTION_INTERVAL,
    enable_winner_selection: bool = True,
    health_check_interval: int = 3600  # 1 hour
):
    """Start the scheduler with all configured tasks."""
    
    logger.info(f"Starting scheduler with:")
    logger.info(f"  - Points calculation: every {points_interval} seconds")
    logger.info(f"  - Winner selection: every {winner_interval} seconds ({'enabled' if enable_winner_selection else 'disabled'})")
    logger.info(f"  - Health checks: every {health_check_interval} seconds")
    
    # Initialize Web3 for winner selection
    web3_initialized = initialize_web3() if enable_winner_selection else False
    
    if enable_winner_selection and not web3_initialized:
        logger.warning("Winner selection requested but initialization failed. Continuing without winner selection.")
        enable_winner_selection = False
    
    # Schedule jobs
    schedule.every(points_interval).seconds.do(run_points_calculation)
    
    if enable_winner_selection:
        schedule.every(winner_interval).seconds.do(run_winner_selection)
    
    schedule.every(health_check_interval).seconds.do(run_health_check)
    
    # Run initial tasks
    logger.info("Running initial tasks...")
    run_points_calculation()
    
    if enable_winner_selection:
        time.sleep(5)  # Small delay between initial runs
        run_winner_selection()
    
    run_health_check()
    
    # Keep the scheduler running
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lucky Ponds Scheduler")
    parser.add_argument(
        "--points-interval", 
        type=int, 
        default=POINTS_CALCULATION_INTERVAL,
        help="Interval in seconds between points calculations (default: 1 hour)"
    )
    parser.add_argument(
        "--winner-interval", 
        type=int, 
        default=WINNER_SELECTION_INTERVAL,
        help="Interval in seconds between winner selections (default: 5 minutes)"
    )
    parser.add_argument(
        "--disable-winner-selection", 
        action="store_true",
        help="Disable automatic winner selection"
    )
    parser.add_argument(
        "--health-interval", 
        type=int, 
        default=3600,
        help="Interval in seconds between health checks (default: 1 hour)"
    )
    parser.add_argument(
        "--run-once", 
        action="store_true",
        help="Run calculations once and exit instead of scheduling"
    )
    parser.add_argument(
        "--points-only", 
        action="store_true",
        help="Run only points calculation once and exit"
    )
    parser.add_argument(
        "--winner-only", 
        action="store_true",
        help="Run only winner selection once and exit"
    )
    
    args = parser.parse_args()
    
    if args.run_once:
        logger.info("Running all tasks once")
        run_points_calculation()
        if not args.disable_winner_selection:
            if initialize_web3():
                time.sleep(2)
                run_winner_selection()
        run_health_check()
    elif args.points_only:
        logger.info("Running points calculation once")
        run_points_calculation()
    elif args.winner_only:
        logger.info("Running winner selection once")
        if initialize_web3():
            run_winner_selection()
        else:
            logger.error("Failed to initialize Web3 for winner selection")
    else:
        start_scheduler(
            points_interval=args.points_interval,
            winner_interval=args.winner_interval,
            enable_winner_selection=not args.disable_winner_selection,
            health_check_interval=args.health_interval
        )