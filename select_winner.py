#!/usr/bin/env python3
import os
import json
import time
import logging
import argparse
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("winner_selector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
RPC_URL = os.getenv("RPC_URL", "https://rpc.hyperliquid-testnet.xyz/evm")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").lower()
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")  # Private key for the wallet that will trigger selections
GAS_PRICE_BUFFER = 1.2  # Multiply estimated gas price by this factor
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "300000"))  # Gas limit for transactions
CHAIN_ID = int(os.getenv("CHAIN_ID", "1338"))  # Default to 1338 for Hyperliquid testnet

def setup_web3():
    """Initialize Web3 connection and contract."""
    # Connect to the blockchain
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    
    # Add middleware for POA chains if needed (like Polygon)
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    
    # Check connection
    if not w3.is_connected():
        logger.error(f"Failed to connect to {RPC_URL}")
        raise Exception("Failed to connect to blockchain")
    
    # Get network chain ID if not specified in env
    if CHAIN_ID == 0:
        chain_id = w3.eth.chain_id
        logger.info(f"Using auto-detected chain ID: {chain_id}")
    else:
        chain_id = CHAIN_ID
        logger.info(f"Using configured chain ID: {chain_id}")
    
    # Load contract ABI
    try:
        with open('contract_abi.json', 'r') as f:
            contract_abi = json.load(f)
    except FileNotFoundError:
        logger.error("contract_abi.json not found")
        raise Exception("Contract ABI file not found")
    
    # Create contract instance
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(CONTRACT_ADDRESS), 
        abi=contract_abi
    )
    
    # Check if account is valid
    if not PRIVATE_KEY:
        logger.error("No private key provided")
        raise Exception("Private key not configured")
    
    account = Account.from_key(PRIVATE_KEY)
    logger.info(f"Using account: {account.address}")
    
    return w3, contract, account, chain_id

def get_all_pond_types(contract):
    """Query the contract to get all available pond types."""
    try:
        # First try to get standard pond types
        standard_ponds = contract.functions.getStandardPondTypes().call()
        # Convert result to list of hex strings
        pond_types = []
        for pond in standard_ponds:
            if isinstance(pond, bytes):
                pond_types.append('0x' + pond.hex())
            else:
                pond_types.append(pond)
        
        # Try getting all pond types if available (including custom ones)
        try:
            all_ponds = contract.functions.getAllPondTypes().call()
            for pond in all_ponds:
                if isinstance(pond, bytes):
                    pond_hex = '0x' + pond.hex()
                else:
                    pond_hex = pond
                if pond_hex not in pond_types:
                    pond_types.append(pond_hex)
        except Exception as e:
            logger.warning(f"Could not get all pond types, using standard ones: {e}")
        
        logger.info(f"Found {len(pond_types)} pond types: {pond_types}")
        return pond_types
    except Exception as e:
        logger.error(f"Error getting pond types: {e}")
        # Return empty list as fallback
        return []

def get_eligible_ponds(contract):
    """Get ponds that are eligible for winner selection (ended but no winner selected yet)."""
    eligible_ponds = []
    all_pond_types = get_all_pond_types(contract)
    
    current_time = int(time.time())
    logger.info(f"Current time: {current_time}")
    
    # Define selection delays (in seconds)
    FIVE_MIN_DELAY = 20  # 20 seconds delay for 5-minute ponds
    STANDARD_DELAY = 60  # 60 seconds (1 minute) delay for all other pond types
    
    for pond_type in all_pond_types:
        try:
            # Convert hex string to bytes32 if needed
            if isinstance(pond_type, str) and pond_type.startswith('0x'):
                pond_type_bytes = bytes.fromhex(pond_type[2:])
            else:
                pond_type_bytes = pond_type
            
            # Get pond status
            pond_status = contract.functions.getPondStatus(pond_type_bytes).call()
            pond_name = pond_status[0]
            end_time = pond_status[2]
            total_tosses = pond_status[3]
            prize_distributed = pond_status[6]
            
            # Get the pond period to determine the appropriate delay
            try:
                pond_period = pond_status[12]  # Assuming period is at index 12 in the returned tuple
                # Use shorter delay only for 5-minute ponds (period = 0)
                selection_delay = FIVE_MIN_DELAY if pond_period == 0 else STANDARD_DELAY
            except (IndexError, TypeError):
                selection_delay = STANDARD_DELAY
                logger.warning(f"Could not determine period for pond '{pond_name}', using standard delay of {STANDARD_DELAY}s")
            
            # The pond is eligible if:
            # 1. Prize not distributed yet
            # 2. Pond has ended (plus appropriate delay)
            # 3. There are tosses to select from
            if not prize_distributed and current_time >= (end_time + selection_delay) and total_tosses > 0:
                logger.info(f"Pond '{pond_name}' is eligible for winner selection. End time: {end_time}, Current time: {current_time}, Delay: {selection_delay}s")
                eligible_ponds.append({
                    'type': pond_type,
                    'name': pond_name,
                    'type_bytes': pond_type_bytes,
                    'end_time': end_time,
                    'period': pond_period if 'pond_period' in locals() else None
                })
            else:
                if prize_distributed:
                    logger.debug(f"Pond '{pond_name}' already has prize distributed")
                elif current_time < end_time:
                    logger.debug(f"Pond '{pond_name}' has not ended yet. End time: {end_time}")
                elif current_time < (end_time + selection_delay):
                    time_remaining = (end_time + selection_delay) - current_time
                    logger.debug(f"Pond '{pond_name}' ended but waiting for delay period. {time_remaining}s remaining.")
                elif total_tosses == 0:
                    logger.debug(f"Pond '{pond_name}' has no tosses")
        except Exception as e:
            logger.error(f"Error checking pond {pond_type}: {e}")
    
    return eligible_ponds

def select_lucky_winner(w3, contract, account, pond_info, chain_id):
    """Trigger the selectLuckyWinner function for a specific pond."""
    pond_type = pond_info['type']
    pond_type_bytes = pond_info['type_bytes']
    pond_name = pond_info['name']
    
    logger.info(f"Selecting winner for pond '{pond_name}' (type: {pond_type})")
    
    try:
        # Build transaction
        nonce = w3.eth.get_transaction_count(account.address)
        
        # Get gas price (with buffer for faster confirmation)
        gas_price = int(w3.eth.gas_price * GAS_PRICE_BUFFER)
        
        # Build the transaction
        tx = contract.functions.selectLuckyWinner(pond_type_bytes).build_transaction({
            'from': account.address,
            'gas': GAS_LIMIT,
            'gasPrice': gas_price,
            'nonce': nonce,
            'chainId': chain_id
        })
        
        # Sign the transaction
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)
        
        # Send the transaction
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logger.info(f"Transaction sent: {tx_hash.hex()}")
        
        # Wait for the transaction to be mined
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        logger.info(f"Transaction confirmed in block: {receipt.blockNumber}")
        
        if receipt.status == 1:
            logger.info(f"Successfully selected winner for pond '{pond_name}'")
            return True
        else:
            logger.error(f"Transaction failed: {receipt}")
            return False
            
    except Exception as e:
        logger.error(f"Error selecting winner: {e}", exc_info=True)
        return False

def main():
    """Main function to process command-line arguments and select winners."""
    parser = argparse.ArgumentParser(description="Select lucky winners for ponds")
    parser.add_argument('--pond-type', type=str, default='auto', 
                        help='Pond type to select winner for. Use "auto" to find all eligible ponds.')
    parser.add_argument('--period', type=str, choices=['five_min', 'hourly', 'daily', 'weekly', 'monthly'], 
                        help='Filter ponds by period (used with --pond-type=auto)')
    args = parser.parse_args()
    
    try:
        # Setup Web3 connection
        w3, contract, account, chain_id = setup_web3()
        
        if args.pond_type == 'auto':
            # Get eligible ponds and select winners
            eligible_ponds = get_eligible_ponds(contract)
            
            if not eligible_ponds:
                logger.info("No eligible ponds found for winner selection")
                return
            
            # Filter by period if specified
            if args.period:
                period_map = {
                    'five_min': 0,
                    'hourly': 1,
                    'daily': 2,
                    'weekly': 3,
                    'monthly': 4
                }
                period_value = period_map.get(args.period)
                
                filtered_ponds = []
                for pond in eligible_ponds:
                    try:
                        # Get pond period
                        pond_status = contract.functions.getPondStatus(pond['type_bytes']).call()
                        pond_period = pond_status[12]  # Assuming period is at index 12 in the returned tuple
                        
                        if pond_period == period_value:
                            filtered_ponds.append(pond)
                    except Exception as e:
                        logger.error(f"Error checking pond period: {e}")
                
                eligible_ponds = filtered_ponds
                logger.info(f"Filtered to {len(eligible_ponds)} ponds of period {args.period}")
            
            # Process eligible ponds
            results = {}
            for pond in eligible_ponds:
                pond_name = pond['name']
                success = select_lucky_winner(w3, contract, account, pond, chain_id)
                results[pond_name] = success
                # Short delay between transactions to avoid nonce issues
                time.sleep(2)
            
            # Log summary
            for name, success in results.items():
                logger.info(f"{name}: {'Success' if success else 'Failed'}")
                
        else:
            # Process specific pond type
            pond_type = args.pond_type
            if pond_type.startswith('0x'):
                # It's a hex string
                pond_type_bytes = bytes.fromhex(pond_type[2:])
            else:
                # It's a pond name, try to get the pond type
                logger.error(f"Please provide a valid hex-encoded pond type. '{pond_type}' is not valid.")
                return
                
            # Check if the pond exists and is eligible
            try:
                pond_status = contract.functions.getPondStatus(pond_type_bytes).call()
                pond_name = pond_status[0]
                end_time = pond_status[2]
                total_tosses = pond_status[3]
                prize_distributed = pond_status[6]
                
                current_time = int(time.time())
                
                if prize_distributed:
                    logger.warning(f"Prize already distributed for pond '{pond_name}'")
                    return
                
                if current_time < end_time:
                    logger.warning(f"Pond '{pond_name}' has not ended yet. End time: {end_time}")
                    return
                
                if total_tosses == 0:
                    logger.warning(f"No tosses in pond '{pond_name}', nothing to select")
                    return
                    
                # Pond is eligible, select winner
                pond_info = {
                    'type': pond_type,
                    'name': pond_name,
                    'type_bytes': pond_type_bytes,
                    'end_time': end_time
                }
                success = select_lucky_winner(w3, contract, account, pond_info, chain_id)
                logger.info(f"{pond_name}: {'Success' if success else 'Failed'}")
                
            except Exception as e:
                logger.error(f"Error checking pond {pond_type}: {e}")
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()