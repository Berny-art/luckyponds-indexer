#!/usr/bin/env python3
import os
import time
import logging
import schedule
import argparse
from typing import Dict, Any
from dotenv import load_dotenv
from points_calculator import PointsCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = os.getenv("DB_PATH", "./data/lucky_ponds.db")
POINTS_CALCULATION_INTERVAL = int(os.getenv("POINTS_CALCULATION_INTERVAL", "3600"))  # Default: every hour

def run_points_calculation():
    """Execute the points calculation process."""
    logger.info("Starting scheduled points calculation")
    
    try:
        calculator = PointsCalculator(DB_PATH)
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