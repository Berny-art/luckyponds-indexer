#!/usr/bin/env python3
import os, json
from web3 import Web3
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "300000"))

w3 = Web3(Web3.HTTPProvider(os.getenv("RPC_URL")))
account = w3.eth.account.from_key(os.getenv("PRIVATE_KEY"))
contract = w3.eth.contract(address=os.getenv("CONTRACT_ADDRESS"), abi=json.load(open('contract_abi.json')))

# Show initial balance
initial_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
print(f"{timestamp} - INFO - Keeper account: {account.address}")
print(f"{timestamp} - INFO - Initial balance: {initial_balance:.6f} ETH")

processed_count = 0
max_iterations = 10  # Prevent infinite loops

while processed_count < max_iterations:
    upkeep_needed, perform_data = contract.functions.checkUpkeep(b"").call()
    
    if not upkeep_needed:
        if processed_count == 0:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            print(f"{timestamp} - INFO - No upkeep needed")
        break
    
    processed_count += 1
    pond_type = perform_data[:32].hex() if len(perform_data) >= 32 else "unknown"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    print(f"{timestamp} - INFO - Processing pond {processed_count}: {pond_type[:10]}...")
    
    tx = contract.functions.performUpkeep(perform_data).build_transaction({
        'from': account.address, 'gas': GAS_LIMIT, 'gasPrice': w3.to_wei(20, 'gwei'), 
        'nonce': w3.eth.get_transaction_count(account.address)
    })
    tx_hash = w3.eth.send_raw_transaction(account.sign_transaction(tx).rawTransaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    if receipt.status == 1:
        cost = w3.from_wei(receipt.gasUsed * w3.to_wei(20, 'gwei'), 'ether')
        print(f"{timestamp} - INFO - ✅ Winner selected! TX: {tx_hash.hex()}, Gas used: {receipt.gasUsed}, Cost: {cost:.6f} ETH")
    else:
        print(f"{timestamp} - ERROR - ❌ Transaction failed: {tx_hash.hex()}")
        break  # Stop on failure
    
    # Small delay between transactions to avoid nonce issues
    if processed_count < max_iterations:
        time.sleep(2)

if processed_count > 0:
    # Show final balance and total cost
    final_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
    total_cost = initial_balance - final_balance
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    print(f"{timestamp} - INFO - Processed {processed_count} pond(s) total")
    print(f"{timestamp} - INFO - Final balance: {final_balance:.6f} ETH (spent: {total_cost:.6f} ETH)")