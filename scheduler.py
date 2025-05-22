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

# Import our components
from points_calculator import PointsCalculator
from utils import (
    get_events_db_path, 
    get_app_db_path, 
    setup_logger
)

# Configure logging
logger = setup_logger('scheduler')

# Load environment variables
load_dotenv()

# Configuration
EVENTS_DB_PATH = get_events_db_path()
APP_DB_PATH = get_app_db_path()
POINTS_INTERVAL = int(os.getenv("POINTS_CALCULATION_INTERVAL", "3600"))  # 1 hour
WINNER_INTERVAL = int(os.getenv("WINNER_SELECTION_INTERVAL", "300"))     # 5 minutes

# Winner selection configuration
RPC_URL = os.getenv("RPC_URL")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "300000"))
GAS_PRICE_GWEI = int(os.getenv("GAS_PRICE_GWEI", "20"))

# Global Web3 components
w3 = None
contract = None
account = None

def initialize_web3():
    """Initialize Web3 connection for winner selection."""
    global w3, contract, account
    
    if not PRIVATE_KEY or not CONTRACT_ADDRESS or PRIVATE_KEY == "0x..." or CONTRACT_ADDRESS == "0x...":
        logger.warning("Winner selection disabled - PRIVATE_KEY or CONTRACT_ADDRESS not configured")
        return False
    
    try:
        w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={'timeout': 120}))
        account = w3.eth.account.from_key(PRIVATE_KEY)
        
        with open('contract_abi.json', 'r') as f:
            contract_abi = json.load(f)
        
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS), 
            abi=contract_abi
        )
        
        balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
        logger.info(f"Winner selection initialized - Account: {account.address}, Balance: {balance:.6f} ETH")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize winner selection: {e}")
        return False

def run_points_calculation():
    """Run points calculation."""
    logger.info("Starting points calculation")
    try:
        calculator = PointsCalculator(APP_DB_PATH, EVENTS_DB_PATH)
        events_processed = calculator.run_points_calculation()
        logger.info(f"Points calculation completed: {events_processed} events processed")
        return events_processed
    except Exception as e:
        logger.error(f"Points calculation failed: {e}")
        return 0

def run_winner_selection():
    """Run winner selection."""
    if not w3 or not contract or not account:
        logger.debug("Winner selection not initialized, skipping")
        return 0
    
    logger.info("Starting winner selection")
    try:
        processed_count = 0
        max_iterations = 10
        initial_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
        
        while processed_count < max_iterations:
            upkeep_needed, perform_data = contract.functions.checkUpkeep(b"").call()
            
            if not upkeep_needed:
                if processed_count == 0:
                    logger.info("No winner selection needed")
                break
            
            processed_count += 1
            pond_type = perform_data[:32].hex() if len(perform_data) >= 32 else "unknown"
            logger.info(f"Processing winner selection {processed_count}: pond {pond_type[:10]}...")
            
            tx = contract.functions.performUpkeep(perform_data).build_transaction({
                'from': account.address,
                'gas': GAS_LIMIT,
                'gasPrice': w3.to_wei(GAS_PRICE_GWEI, 'gwei'),
                'nonce': w3.eth.get_transaction_count(account.address)
            })
            
            signed_tx = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            
            if receipt.status == 1:
                cost = w3.from_wei(receipt.gasUsed * w3.to_wei(GAS_PRICE_GWEI, 'gwei'), 'ether')
                logger.info(f"✅ Winner selected! TX: {tx_hash.hex()}, Cost: {cost:.6f} ETH")
            else:
                logger.error(f"❌ Transaction failed: {tx_hash.hex()}")
                break
            
            time.sleep(2)  # Small delay between transactions
        
        if processed_count > 0:
            final_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
            total_cost = initial_balance - final_balance
            logger.info(f"Winner selection completed: {processed_count} selection(s), Cost: {total_cost:.6f} ETH")
        
        return processed_count
        
    except Exception as e:
        logger.error(f"Winner selection failed: {e}")
        return 0

def main():
    """Main scheduler function."""
    parser = argparse.ArgumentParser(description="Lucky Ponds Scheduler")
    parser.add_argument("--points-interval", type=int, default=POINTS_INTERVAL,
                       help=f"Points calculation interval in seconds (default: {POINTS_INTERVAL})")
    parser.add_argument("--winner-interval", type=int, default=WINNER_INTERVAL,
                       help=f"Winner selection interval in seconds (default: {WINNER_INTERVAL})")
    parser.add_argument("--disable-winner-selection", action="store_true",
                       help="Disable winner selection")
    parser.add_argument("--use-utc-timing", action="store_true",
                       help="Use UTC-based cron-style timing instead of intervals")
    parser.add_argument("--run-once", action="store_true",
                       help="Run both tasks once and exit")
    parser.add_argument("--points-only", action="store_true",
                       help="Run only points calculation once")
    parser.add_argument("--winner-only", action="store_true",
                       help="Run only winner selection once")
    
    args = parser.parse_args()
    
    # Handle one-time runs
    if args.run_once:
        logger.info("Running all tasks once")
        run_points_calculation()
        if not args.disable_winner_selection and initialize_web3():
            time.sleep(2)
            run_winner_selection()
        return
    
    if args.points_only:
        logger.info("Running points calculation once")
        run_points_calculation()
        return
    
    if args.winner_only:
        logger.info("Running winner selection once")
        if initialize_web3():
            run_winner_selection()
        return
    
    # Initialize winner selection if enabled
    winner_enabled = not args.disable_winner_selection and initialize_web3()
    
    # Configure scheduling based on timing mode
    if args.use_utc_timing:
        logger.info("Starting UTC-based scheduler:")
        logger.info("  - Points calculation: every hour at :30")
        logger.info("  - Winner selection: 5-min ponds at XX:00:21/XX:05:21/etc, hourly at XX:01:30, daily/weekly/monthly at 00:01:30")
        
        # Points calculation: every hour at 30 minutes past
        schedule.every().hour.at(":30").do(run_points_calculation)
        
        if winner_enabled:
            # 5-minute ponds: 21 seconds after each 5-minute interval
            # Run at :21 seconds of every minute, but only when minute % 5 == 0
            schedule.every().minute.at(":21").do(lambda: run_winner_selection() if datetime.now().minute % 5 == 0 else None)
            
            # Hourly ponds: 1 minute 30 seconds after each hour (61s timelock + 29s buffer)  
            schedule.every().hour.at("01:05").do(run_winner_selection)
            
            # Daily ponds: 1 minute 30 seconds after midnight UTC
            schedule.every().day.at("00:01:05").do(run_winner_selection)
            
            # Weekly ponds: 1 minute 30 seconds after Saturday midnight UTC
            schedule.every().saturday.at("00:01:05").do(run_winner_selection)
            
            # Monthly ponds: 1 minute 30 seconds after first day of month
            schedule.every().day.at("00:01:30").do(lambda: run_winner_selection() if datetime.now().day == 1 else None)
    else:
        logger.info(f"Starting interval-based scheduler:")
        logger.info(f"  - Points calculation: every {args.points_interval} seconds")
        logger.info(f"  - Winner selection: every {args.winner_interval} seconds")
        
        # Traditional interval-based scheduling
        schedule.every(args.points_interval).seconds.do(run_points_calculation)
        if winner_enabled:
            schedule.every(args.winner_interval).seconds.do(run_winner_selection)
    
    # Run initial tasks (only if not using UTC timing to avoid immediate execution)
    if not args.use_utc_timing:
        logger.info("Running initial tasks...")
        run_points_calculation()
        if winner_enabled:
            time.sleep(5)
            run_winner_selection()
    else:
        logger.info("UTC timing mode - tasks will run at scheduled times")
        # Show next execution times
        current_time = datetime.now()
        logger.info(f"Current UTC time: {current_time.strftime('%H:%M:%S')}")
        next_points = f"{current_time.hour}:30" if current_time.minute < 30 else f"{(current_time.hour + 1) % 24:02d}:30"
        logger.info(f"Next points calculation: {next_points}")
        
        # Calculate next 5-minute interval with 21s offset
        current_minute = current_time.minute
        next_5min_base = ((current_minute // 5) + 1) * 5
        if next_5min_base >= 60:
            next_5min_base = 0
            next_winner_hour = (current_time.hour + 1) % 24
        else:
            next_winner_hour = current_time.hour
        logger.info(f"Next winner selection: {next_winner_hour:02d}:{next_5min_base:02d}:21")
    
    # Main scheduler loop
    logger.info("Scheduler running. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")

if __name__ == "__main__":
    main()