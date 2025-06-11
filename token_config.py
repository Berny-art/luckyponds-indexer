#!/usr/bin/env python3
"""
Token configuration utility for handling token-specific points calculation.
"""
import os
import json
import time
from typing import Dict, Tuple, Optional
from web3 import Web3
from dotenv import load_dotenv
from utils import setup_logger

# Configure logging
logger = setup_logger('token_config')

# Load environment variables
load_dotenv()

# Token configuration from frontend
DEFAULT_TOKENS = {
    'HYPE': {
        'symbol': 'HYPE',
        'address': '0x0000000000000000000000000000000000000000',
        'name': 'Hyperliquid',
        'decimals': 18,
        'isNative': True,
    },
    'BUDDY': {
        'symbol': 'BUDDY',
        'address': '0x47bb061C0204Af921F43DC73C7D7768d2672DdEE',
        'name': 'Alright Buddy',
        'decimals': 6,
        'isNative': False,
    },
    'RUB': {
        'symbol': 'RUB',
        'address': '0x7DCfFCb06B40344eecED2d1Cbf096B299fE4b405',
        'name': 'RUB',
        'decimals': 18,
        'isNative': False,
    },
}

class TokenConfig:
    """Handles token configuration and pond-specific min/max values"""
    
    def __init__(self):
        """Initialize token configuration"""
        self.w3 = None
        self.contract = None
        self.pond_cache = {}  # Cache for pond configurations
        self.cache_ttl = 300  # 5 minutes cache TTL
        self.last_cache_update = {}
        
        # Initialize Web3 if available
        self._init_web3()
    
    def _init_web3(self):
        """Initialize Web3 connection"""
        try:
            rpc_url = os.getenv("RPC_URL")
            contract_address = os.getenv("CONTRACT_ADDRESS")
            
            if not rpc_url or not contract_address:
                logger.warning("RPC_URL or CONTRACT_ADDRESS not configured, contract queries disabled")
                return
            
            self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))
            
            with open('contract_abi.json', 'r') as f:
                contract_abi = json.load(f)
            
            self.contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(contract_address),
                abi=contract_abi
            )
            
            logger.info("TokenConfig initialized with Web3 connection")
            
        except Exception as e:
            logger.warning(f"Failed to initialize Web3 connection: {e}")
    
    def get_token_info(self, token_address: str) -> Optional[Dict]:
        """Get token information by address"""
        token_address = token_address.lower()
        
        # Check for native token (zero address)
        if token_address == '0x0000000000000000000000000000000000000000':
            return DEFAULT_TOKENS['HYPE']
        
        # Check configured tokens
        for token in DEFAULT_TOKENS.values():
            if token['address'].lower() == token_address:
                return token
        
        # Unknown token
        logger.warning(f"Unknown token address: {token_address}")
        return None
    
    def get_pond_config(self, pond_type: str, token_address: str) -> Tuple[int, int]:
        """
        Get min and max toss amounts for a specific pond type and token.
        
        Args:
            pond_type: Pond type as hex string
            token_address: Token contract address
            
        Returns:
            Tuple of (min_amount, max_amount) in token's smallest unit
        """
        # For native tokens, use the zero address for caching
        normalized_token_address = token_address.lower()
        token_info = self.get_token_info(normalized_token_address)
        
        # Use a simplified cache key for native tokens
        if token_info and token_info.get('isNative', False):
            cache_key = f"{pond_type}_native"
        else:
            cache_key = f"{pond_type}_{normalized_token_address}"
        
        current_time = time.time()
        
        # Check cache first
        if (cache_key in self.pond_cache and 
            cache_key in self.last_cache_update and
            current_time - self.last_cache_update[cache_key] < self.cache_ttl):
            return self.pond_cache[cache_key]
        
        # Try to get from contract
        if self.contract:
            try:
                # Convert pond_type to bytes32 if it's a hex string
                if isinstance(pond_type, str) and pond_type.startswith('0x'):
                    pond_type_bytes = bytes.fromhex(pond_type[2:])
                    if len(pond_type_bytes) < 32:
                        # Pad to 32 bytes
                        pond_type_bytes = pond_type_bytes + b'\x00' * (32 - len(pond_type_bytes))
                elif isinstance(pond_type, str):
                    # If it's a hex string without 0x prefix
                    pond_type_bytes = bytes.fromhex(pond_type)
                    if len(pond_type_bytes) < 32:
                        pond_type_bytes = pond_type_bytes + b'\x00' * (32 - len(pond_type_bytes))
                else:
                    # Assume it's already in the right format
                    pond_type_bytes = pond_type
                
                # Get pond status
                result = self.contract.functions.getPondStatus(pond_type_bytes).call()
                min_toss_price = result[8]  # minTossPrice is at index 8
                max_total_toss_amount = result[9]  # maxTotalTossAmount is at index 9
                pond_token_address = result[11]  # tokenAddress is at index 11
                
                # Verify this pond is for the correct token type
                # For native tokens, pond_token_address should be zero address
                if token_info and token_info.get('isNative', False):
                    if pond_token_address != '0x0000000000000000000000000000000000000000':
                        logger.warning(f"Pond {pond_type} is not for native token (found token: {pond_token_address})")
                        raise Exception("Token mismatch")
                else:
                    # For ERC20 tokens, verify addresses match
                    if pond_token_address.lower() != normalized_token_address:
                        logger.warning(f"Pond {pond_type} is for different token (expected: {normalized_token_address}, found: {pond_token_address})")
                        raise Exception("Token mismatch")
                
                # Cache the result
                self.pond_cache[cache_key] = (min_toss_price, max_total_toss_amount)
                self.last_cache_update[cache_key] = current_time
                
                logger.debug(f"Got pond config for {cache_key}: min={min_toss_price}, max={max_total_toss_amount}, token={pond_token_address}")
                return (min_toss_price, max_total_toss_amount)
                
            except Exception as e:
                logger.warning(f"Failed to get pond config from contract for {cache_key}: {e}")
        
        # Fallback to default values based on token
        if token_info:
            return self._get_default_limits(token_info)
        
        # Ultimate fallback - assume 18 decimals
        logger.warning(f"Using fallback limits for unknown token {normalized_token_address}")
        return (10**17, 10**19)  # 0.1 to 10 ETH equivalent
    
    def _get_default_limits(self, token_info: Dict) -> Tuple[int, int]:
        """Get default min/max limits based on token info"""
        decimals = token_info['decimals']
        symbol = token_info['symbol']
        
        if symbol == 'HYPE':
            # HYPE: 0.1 to 10 (18 decimals)
            min_amount = int(0.1 * 10**decimals)  # 0.1 HYPE
            max_amount = int(10 * 10**decimals)   # 10 HYPE
        elif symbol == 'BUDDY':
            # BUDDY: 100 to 10000 (6 decimals)
            min_amount = int(100 * 10**decimals)   # 100 BUDDY
            max_amount = int(10000 * 10**decimals) # 10000 BUDDY
        elif symbol == 'RUB':
            # RUB: similar to HYPE (18 decimals)
            min_amount = int(0.1 * 10**decimals)  # 0.1 RUB
            max_amount = int(10 * 10**decimals)   # 10 RUB
        else:
            # Default to ETH-like values
            min_amount = int(0.1 * 10**decimals)
            max_amount = int(10 * 10**decimals)
        
        return (min_amount, max_amount)
    
    def calculate_points(self, amount: str, token_address: str, pond_type: str, multiplier: int) -> int:
        """
        Calculate points for a toss based on token-specific ranges.
        
        Args:
            amount: Toss amount in token's smallest unit (as string)
            token_address: Token contract address
            pond_type: Pond type identifier
            multiplier: Base points multiplier
            
        Returns:
            Calculated points (minimum 1)
        """
        try:
            amount_int = int(amount)
            token_info = self.get_token_info(token_address)
            
            if not token_info:
                # Fallback to ETH-like calculation
                amount_in_eth = float(amount_int) / 10**18
                calculated_points = amount_in_eth * multiplier
                return max(1, int(calculated_points))
            
            # Get pond-specific min/max values
            min_amount, max_amount = self.get_pond_config(pond_type, token_address)
            
            # Normalize the amount to a 0-100 scale based on min/max
            if min_amount >= max_amount:
                # Invalid range, fallback to 1 point
                logger.warning(f"Invalid min/max range for {token_address}: min={min_amount}, max={max_amount}")
                return 1
            
            # Clamp amount to valid range
            clamped_amount = max(min_amount, min(amount_int, max_amount))
            
            # Calculate normalized position (0-100)
            range_size = max_amount - min_amount
            position_in_range = clamped_amount - min_amount
            normalized_position = position_in_range / range_size  # 0.0 to 1.0
            
            # Calculate points: 1 point at minimum, 100 points at maximum, scaled by multiplier
            base_points = 1 + (normalized_position * 99)  # 1 to 100 range
            calculated_points = base_points * (multiplier / 10)  # Scale by multiplier (default 10)
            
            result = max(1, int(calculated_points))
            
            logger.debug(f"Points calculation: amount={amount_int}, min={min_amount}, max={max_amount}, "
                        f"normalized={normalized_position:.3f}, base_points={base_points:.1f}, "
                        f"final_points={result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating points for amount {amount}, token {token_address}: {e}")
            # Fallback to minimum points
            return 1
    
    def clear_cache(self):
        """Clear the pond configuration cache"""
        self.pond_cache.clear()
        self.last_cache_update.clear()
        logger.info("Pond configuration cache cleared")
