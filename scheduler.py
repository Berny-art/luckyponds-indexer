#!/usr/bin/env python3
import os
import json
import time
import logging
import schedule
from dotenv import load_dotenv
from web3 import Web3
from datetime import datetime

# Import our components
from points_calculator import PointsCalculator
from recalculate_points import process_referrals
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

def run_referral_processing():
    """Run referral processing to activate pending referrals."""
    logger.info("Starting referral processing")
    try:
        activated_count = process_referrals()
        logger.info(f"Referral processing completed: {activated_count} referrals activated")
        return activated_count
    except Exception as e:
        logger.error(f"Referral processing failed: {e}")
        return 0
    
def main():
    """Main scheduler function."""
    logger.info("Starting UTC-based scheduler")
    
    # Initialize winner selection
    winner_enabled = initialize_web3()
    
    # Schedule points calculation every 15 minutes
    schedule.every(15).minutes.do(run_points_calculation)

    # Schedule referral processing every 15 minutes
    schedule.every(15).minutes.do(run_referral_processing)
    
    if winner_enabled:
        # Winner selection only runs on specific schedules
        # Hourly ponds: 1 minute 10 seconds after each hour
        schedule.every().hour.at("01:10").do(run_winner_selection)
        
        # Daily ponds: 1 minute 15 seconds after midnight UTC
        schedule.every().day.at("00:01:15").do(run_winner_selection)
        
        # Weekly ponds: 1 minute 20 seconds after Saturday midnight UTC
        schedule.every().saturday.at("00:01:20").do(run_winner_selection)
        
        # Monthly ponds: 1 minute 25 seconds after first day of month
        schedule.every().day.at("00:01:25").do(lambda: run_winner_selection() if datetime.now().day == 1 else None)
        
        logger.info("Winner selection scheduled at specific UTC times")
    else:
        logger.info("Winner selection disabled")
    
    logger.info("Scheduler running - tasks will run at scheduled times")
        
    # Main scheduler loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")

if __name__ == "__main__":
    main()