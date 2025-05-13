#!/usr/bin/env python3
"""
Script to create or save the contract ABI to a file.
This is used by the indexer to decode events.
"""

import json
import sys

# This is your contract ABI extracted from the provided file
CONTRACT_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "pondType",
                "type": "bytes32"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "frog",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "amount",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "timestamp",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "totalPondTosses",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "totalPondValue",
                "type": "uint256"
            }
        ],
        "name": "CoinTossed",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "string",
                "name": "configType",
                "type": "string"
            },
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "pondType",
                "type": "bytes32"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "oldValue",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "newValue",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "address",
                "name": "oldAddress",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "address",
                "name": "newAddress",
                "type": "address"
            }
        ],
        "name": "ConfigUpdated",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "string",
                "name": "actionType",
                "type": "string"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "recipient",
                "type": "address"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "token",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "amount",
                "type": "uint256"
            },
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "pondType",
                "type": "bytes32"
            }
        ],
        "name": "EmergencyAction",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "pondType",
                "type": "bytes32"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "luckyFrog",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "prize",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "address",
                "name": "selector",
                "type": "address"
            }
        ],
        "name": "LuckyFrogSelected",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "address",
                "name": "account",
                "type": "address"
            }
        ],
        "name": "Paused",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "pondType",
                "type": "bytes32"
            },
            {
                "indexed": False,
                "internalType": "string",
                "name": "name",
                "type": "string"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "startTime",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "endTime",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "string",
                "name": "actionType",
                "type": "string"
            }
        ],
        "name": "PondAction",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "role",
                "type": "bytes32"
            },
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "previousAdminRole",
                "type": "bytes32"
            },
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "newAdminRole",
                "type": "bytes32"
            }
        ],
        "name": "RoleAdminChanged",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "role",
                "type": "bytes32"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "account",
                "type": "address"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "sender",
                "type": "address"
            }
        ],
        "name": "RoleGranted",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "bytes32",
                "name": "role",
                "type": "bytes32"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "account",
                "type": "address"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "sender",
                "type": "address"
            }
        ],
        "name": "RoleRevoked",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "address",
                "name": "account",
                "type": "address"
            }
        ],
        "name": "Unpaused",
        "type": "event"
    }
]

def save_abi():
    """Save the contract ABI to a file"""
    with open('contract_abi.json', 'w') as f:
        json.dump(CONTRACT_ABI, f, indent=2)
    print("Contract ABI saved to contract_abi.json")

if __name__ == "__main__":
    save_abi()