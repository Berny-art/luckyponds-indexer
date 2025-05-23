#!/usr/bin/env python3
import os
import json
import time
from web3 import Web3
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Connect to Ethereum node
w3 = Web3(Web3.HTTPProvider(os.getenv("RPC_URL")))
account = w3.eth.account.from_key(os.getenv("PRIVATE_KEY"))

# Load contract
with open("contract_abi.json") as abi_file:
    abi = json.load(abi_file)

contract = w3.eth.contract(address=os.getenv("CONTRACT_ADDRESS"), abi=abi)

# Logging utility
def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    print(f"{timestamp} - {msg}")

# Display balance info
initial_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
log(f"Keeper account: {account.address}")
log(f"Initial balance: {initial_balance:.6f} ETH")

processed_count = 0
max_iterations = 10

while processed_count < max_iterations:
    upkeep_needed, perform_data = contract.functions.checkUpkeep(b"").call()
    if not upkeep_needed:
        if processed_count == 0:
            log("No upkeep needed")
        break

    processed_count += 1
    pond_type = Web3.toHex(perform_data[:32]) if len(perform_data) >= 32 else "unknown"
    log(f"Processing pond {processed_count}: {pond_type[:10]}...")

    try:
        # Prepare EIP-1559 transaction
        base_fee = w3.eth.get_block("latest").get("baseFeePerGas", w3.to_wei(20, "gwei"))
        priority_fee = w3.to_wei(2, "gwei")
        max_fee = base_fee + priority_fee

        # Decode pondType from performData
        decoded_pond = w3.codec.decode_single("bytes32", perform_data)

        # Estimate gas with buffer
        estimated_gas = contract.functions.selectLuckyWinner(decoded_pond).estimate_gas({"from": account.address})
        buffered_gas = int(estimated_gas * 1.2)

        # Build the transaction
        tx = contract.functions.selectLuckyWinner(decoded_pond).build_transaction({
            "from": account.address,
            "gas": buffered_gas,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "nonce": w3.eth.get_transaction_count(account.address)
        })

        # Sign and send
        signed_tx = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

        if receipt.status == 1:
            cost = w3.from_wei(receipt.gasUsed * max_fee, "ether")
            log(f"✅ Winner selected! TX: {tx_hash.hex()}, Gas used: {receipt.gasUsed}, Cost: {cost:.6f} ETH")
        else:
            log(f"❌ Transaction failed: {tx_hash.hex()}")
            break

    except Exception as e:
        log(f"❌ Error: {str(e)}")
        break

    if processed_count < max_iterations:
        time.sleep(2)

if processed_count > 0:
    final_balance = w3.from_wei(w3.eth.get_balance(account.address), 'ether')
    total_cost = initial_balance - final_balance
    log(f"Processed {processed_count} pond(s) total")
    log(f"Final balance: {final_balance:.6f} ETH (spent: {total_cost:.6f} ETH)")
