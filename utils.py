# utils.py

import os
import time
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment variable getters with defaults
def get_events_db_path() -> str:
    return os.getenv("EVENTS_DB_PATH", "./data/events.db")

def get_app_db_path() -> str:
    return os.getenv("APP_DB_PATH", "./data/application.db")

def get_start_block() -> int:
    return int(os.getenv("START_BLOCK", "0"))

def get_toss_points_multiplier() -> int:
    return int(os.getenv("TOSS_POINTS_MULTIPLIER", "10"))

def get_win_points() -> int:
    return int(os.getenv("WIN_POINTS", "100"))

def get_referral_bonus_points() -> int:
    return int(os.getenv("REFERRAL_BONUS_POINTS", "20"))

def get_points_calculation_interval() -> int:
    return int(os.getenv("POINTS_CALCULATION_INTERVAL", "3600"))

def get_current_timestamp() -> int:
    """Get the current Unix timestamp"""
    return int(time.time())

def setup_logger(name: str) -> logging.Logger:
    """Set up a logger with standard configuration"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger