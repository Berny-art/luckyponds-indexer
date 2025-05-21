#!/usr/bin/env python3
import os
import time
import logging
import schedule
import argparse
from typing import Dict, Any
from dotenv import load_dotenv

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

def start_scheduler(interval_seconds: int = POINTS_CALCULATION_INTERVAL):
    """Start the scheduler to run points calculation at specified intervals."""
    logger.info(f"Starting points calculation scheduler with {interval_seconds} second interval")
    
    # Schedule the job to run at the specified interval
    schedule.every(interval_seconds).seconds.do(run_points_calculation)
    
    # Run once immediately on startup
    run_points_calculation()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lucky Ponds Points Calculator Scheduler")
    parser.add_argument(
        "--interval", 
        type=int, 
        default=POINTS_CALCULATION_INTERVAL,
        help="Interval in seconds between points calculations (default: 1 hour)"
    )
    parser.add_argument(
        "--run-once", 
        action="store_true",
        help="Run the calculation once and exit instead of scheduling"
    )
    args = parser.parse_args()
    
    if args.run_once:
        logger.info("Running points calculation once")
        run_points_calculation()
    else:
        start_scheduler(args.interval)