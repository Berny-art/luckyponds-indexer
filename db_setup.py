# db_setup.py

import os
import sys
import logging
from dotenv import load_dotenv
from events_schema import setup_events_database
from application_schema import setup_application_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
EVENTS_DB_PATH = os.getenv("EVENTS_DB_PATH", "./data/events.db")
APP_DB_PATH = os.getenv("APP_DB_PATH", "./data/application.db")
START_BLOCK = int(os.getenv("START_BLOCK", "0"))

def setup_databases():
    """Set up both databases from scratch."""
    try:
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(EVENTS_DB_PATH), exist_ok=True)
        os.makedirs(os.path.dirname(APP_DB_PATH), exist_ok=True)
        
        # Set up events database
        setup_events_database(EVENTS_DB_PATH, START_BLOCK)
        
        # Set up application database
        setup_application_database(APP_DB_PATH)
        
        logger.info("Database setup completed successfully!")
        return True
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False

if __name__ == "__main__":
    success = setup_databases()
    sys.exit(0 if success else 1)