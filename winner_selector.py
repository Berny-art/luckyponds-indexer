#!/usr/bin/env python3
"""
Simple keeper-based winner selector for Lucky Ponds
Uses contract's checkUpkeep/performUpkeep functions
"""

import os
import logging
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()

# Configuration
RPC_URL = os.getenv("RPC_URL", "https://rpc.hyperliquid-testnet.xyz/evm")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "350000"))
GAS_PRICE = int(os.getenv("GAS_PRICE", "20000000000"))

def main():
    """Run keeper check and perform upkeep if needed."""
    
    # Validate environment variables
    if not CONTRACT_ADDRESS or not PRIVATE_KEY:
        logger.error("CONTRACT_ADDRESS and PRIVATE_KEY must be set")
        return
    
    # Load ABI
    try:
        with open('contract_abi.json', 'r') as f:
            CONTRACT_ABI = json.load(f)
    except FileNotFoundError:
        logger.error("contract_abi.json not found")
        return
    
    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={'timeout': 60}))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except:
        pass
    
    if not w3.is_connected():
        logger.error("Failed to connect to Web3")
        return
    
    # Set up account and contract
    account = w3.eth.account.from_key(PRIVATE_KEY)
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=CONTRACT_ABI)
    
    logger.info(f"Connected to chain {w3.eth.chain_id}")
    logger.info(f"Checking upkeep for contract {CONTRACT_ADDRESS}")
    
    try:
        # First verify contract exists
        code = w3.eth.get_code(Web3.to_checksum_address(CONTRACT_ADDRESS))
        if code == b'':
            logger.error("No contract found at the specified address")
            return
        
        # Check if upkeep is needed
        logger.debug("Calling checkUpkeep...")
        upkeep_needed, perform_data = contract.functions.checkUpkeep(b"").call()
        
        if not upkeep_needed:
            logger.info("No upkeep needed")
            return
        
        # Decode pond type for logging
        pond_type = perform_data[:32].hex() if len(perform_data) >= 32 else "unknown"
        logger.info(f"Upkeep needed for pond {pond_type[:10]}...")
        
        # Check balance
        balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
        if balance < 0.01:
            logger.error(f"Insufficient balance: {balance:.6f} ETH")
            return
        
        # Estimate gas
        try:
            base_tx = contract.functions.performUpkeep(perform_data).build_transaction({
                'from': account.address,
                'gasPrice': GAS_PRICE,
                'nonce': w3.eth.get_transaction_count(account.address)
            })
            estimated_gas = w3.eth.estimate_gas(base_tx)
            gas_limit = min(int(estimated_gas * 1.2), GAS_LIMIT)
            logger.debug(f"Estimated gas: {estimated_gas}, using: {gas_limit}")
        except Exception as e:
            logger.warning(f"Gas estimation failed: {e}, using default limit")
            gas_limit = GAS_LIMIT
        
        # Build and send transaction
        transaction = contract.functions.performUpkeep(perform_data).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'gasPrice': GAS_PRICE,
            'nonce': w3.eth.get_transaction_count(account.address)
        })
        
        signed_tx = account.sign_transaction(transaction)
        raw_tx = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction'))
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        
        logger.info(f"Transaction sent: {tx_hash.hex()}")
        
        # Wait for confirmation
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        
        if receipt.status == 1:
            cost = w3.from_wei(receipt.gasUsed * GAS_PRICE, 'ether')
            logger.info(f"✅ Winner selected! Gas used: {receipt.gasUsed}, Cost: {cost:.6f} ETH")
        else:
            logger.error(f"❌ Transaction failed: {tx_hash.hex()}")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        
        # Try some debugging
        try:
            all_ponds = contract.functions.getAllPondTypes().call()
            logger.info(f"Contract is accessible, found {len(all_ponds)} ponds")
        except Exception as debug_error:
            logger.error(f"Contract call test failed: {debug_error}")

if __name__ == "__main__":
    main()#!/usr/bin/env python3
"""
Simple keeper-based winner selector for Lucky Ponds
Uses contract's checkUpkeep/performUpkeep functions
"""

import os
import logging
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()

# Configuration
RPC_URL = os.getenv("RPC_URL", "https://rpc.hyperliquid-testnet.xyz/evm")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "350000"))
GAS_PRICE = int(os.getenv("GAS_PRICE", "20000000000"))

# Load ABI
with open('contract_abi.json', 'r') as f:
    CONTRACT_ABI = json.load(f)

def main():
    """Run keeper check and perform upkeep if needed."""
    
    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={'timeout': 60}))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except:
        pass
    
    if not w3.is_connected():
        logger.error("Failed to connect to Web3")
        return
    
    # Set up account and contract
    account = w3.eth.account.from_key(PRIVATE_KEY)
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=CONTRACT_ABI)
    
    logger.info(f"Connected to chain {w3.eth.chain_id}, using account {account.address}")
    logger.info(f"Checking upkeep for contract {CONTRACT_ADDRESS}")
    
    try:
        # Check if upkeep is needed
        logger.info("Calling checkUpkeep...")
        upkeep_needed, perform_data = contract.functions.checkUpkeep(b"").call()
        
        if not upkeep_needed:
            logger.info("No upkeep needed")
            return
        
        # Decode pond type for logging
        pond_type = perform_data[:32].hex() if len(perform_data) >= 32 else "unknown"
        logger.info(f"Upkeep needed for pond {pond_type[:10]}...")
        
        # Check account balance
        balance = w3.eth.get_balance(account.address)
        balance_eth = w3.from_wei(balance, 'ether')
        logger.info(f"Account balance: {balance_eth:.6f} ETH")
        
        if balance_eth < 0.01:  # Less than 0.01 ETH
            logger.error("Insufficient balance for transaction")
            return
        
        # Try to estimate gas first
        logger.info("Estimating gas...")
        try:
            base_tx = contract.functions.performUpkeep(perform_data).build_transaction({
                'from': account.address,
                'gasPrice': GAS_PRICE,
                'nonce': w3.eth.get_transaction_count(account.address)
            })
            
            estimated_gas = w3.eth.estimate_gas(base_tx)
            gas_limit = min(int(estimated_gas * 1.2), GAS_LIMIT)
            logger.info(f"Estimated gas: {estimated_gas}, using: {gas_limit}")
        except Exception as gas_error:
            logger.error(f"Gas estimation failed: {gas_error}")
            # Try with a smaller gas limit
            gas_limit = 200000
            logger.info(f"Using fallback gas limit: {gas_limit}")
        
        # Build and send transaction
        logger.info("Building transaction...")
        transaction = contract.functions.performUpkeep(perform_data).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'gasPrice': GAS_PRICE,
            'nonce': w3.eth.get_transaction_count(account.address)
        })
        
        logger.info("Signing and sending transaction...")
        signed_tx = account.sign_transaction(transaction)
        raw_tx = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction'))
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        
        logger.info(f"Transaction sent: {tx_hash.hex()}")
        
        # Wait for confirmation
        logger.info("Waiting for confirmation...")
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        
        if receipt.status == 1:
            cost = w3.from_wei(receipt.gasUsed * GAS_PRICE, 'ether')
            logger.info(f"✅ Winner selected! TX: {tx_hash.hex()}, Gas used: {receipt.gasUsed}, Cost: {cost:.6f} ETH")
        else:
            logger.error(f"❌ Transaction failed: {tx_hash.hex()}")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        
        # Additional debugging for contract calls
        try:
            # Check if contract exists at address
            code = w3.eth.get_code(Web3.to_checksum_address(CONTRACT_ADDRESS))
            if code == b'':
                logger.error("No contract code found at the specified address!")
            else:
                logger.info(f"Contract code found ({len(code)} bytes)")
                
            # Try to call a simple view function to test connectivity
            try:
                all_ponds = contract.functions.getAllPondTypes().call()
                logger.info(f"Found {len(all_ponds)} ponds in contract")
            except Exception as view_error:
                logger.error(f"Failed to call getAllPondTypes: {view_error}")
                
        except Exception as debug_error:
            logger.error(f"Debug check failed: {debug_error}")

if __name__ == "__main__":
    main()#!/usr/bin/env python3
"""
Simple keeper-based winner selector for Lucky Ponds
Uses contract's checkUpkeep/performUpkeep functions
"""

import os
import logging
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()

# Configuration
RPC_URL = os.getenv("RPC_URL", "https://rpc.hyperliquid-testnet.xyz/evm")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "350000"))
GAS_PRICE = int(os.getenv("GAS_PRICE", "20000000000"))

# Load ABI
with open('contract_abi.json', 'r') as f:
    CONTRACT_ABI = json.load(f)

def main():
    """Run keeper check and perform upkeep if needed."""
    
    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={'timeout': 60}))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except:
        pass
    
    # Set up account and contract
    account = w3.eth.account.from_key(PRIVATE_KEY)
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=CONTRACT_ABI)
    
    logger.info(f"Checking upkeep for contract {CONTRACT_ADDRESS}")
    
    try:
        # Check if upkeep is needed
        upkeep_needed, perform_data = contract.functions.checkUpkeep(b"").call()
        
        if not upkeep_needed:
            logger.info("No upkeep needed")
            return
        
        # Decode pond type for logging
        pond_type = perform_data[:32].hex() if len(perform_data) >= 32 else "unknown"
        logger.info(f"Upkeep needed for pond {pond_type[:10]}...")
        
        # Estimate gas
        base_tx = contract.functions.performUpkeep(perform_data).build_transaction({
            'from': account.address,
            'gasPrice': GAS_PRICE,
            'nonce': w3.eth.get_transaction_count(account.address)
        })
        
        try:
            estimated_gas = w3.eth.estimate_gas(base_tx)
            gas_limit = min(int(estimated_gas * 1.2), GAS_LIMIT)
        except:
            gas_limit = GAS_LIMIT
        
        # Build and send transaction
        transaction = contract.functions.performUpkeep(perform_data).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'gasPrice': GAS_PRICE,
            'nonce': w3.eth.get_transaction_count(account.address)
        })
        
        signed_tx = account.sign_transaction(transaction)
        raw_tx = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction'))
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        
        # Wait for confirmation
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        
        if receipt.status == 1:
            cost = w3.from_wei(receipt.gasUsed * GAS_PRICE, 'ether')
            logger.info(f"✅ Winner selected! TX: {tx_hash.hex()}, Cost: {cost:.6f} ETH")
        else:
            logger.error(f"❌ Transaction failed: {tx_hash.hex()}")
            
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    main()