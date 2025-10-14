#!/usr/bin/env python3
"""
Background scheduler for running sync tasks every 3 hours
Runs alongside the FastAPI dashboard in the same process
"""

import asyncio
import schedule
import time
from datetime import datetime
from loguru import logger

# Import sync functions
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from scripts.sync.sync_pabau_to_db import sync_pabau
from scripts.sync.fetch_mailchimp_unsubscribes import fetch_unsubscribes
from scripts.sync.sync_db_to_mailchimp import sync_to_mailchimp


async def run_full_sync():
    """Run the complete sync cycle"""
    try:
        logger.info("=" * 80)
        logger.info(f"Starting sync cycle at {datetime.now()}")
        logger.info("=" * 80)
        
        # Step 1: Sync Pabau to Database
        logger.info("Step 1/3: Syncing Pabau to Database...")
        await sync_pabau()
        
        # Step 2: Fetch Mailchimp unsubscribes
        logger.info("Step 2/3: Fetching Mailchimp unsubscribes...")
        await fetch_unsubscribes()
        
        # Step 3: Sync Database to Mailchimp
        logger.info("Step 3/3: Syncing Database to Mailchimp...")
        await sync_to_mailchimp()
        
        logger.info("=" * 80)
        logger.info(f"Sync cycle completed at {datetime.now()}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error in sync cycle: {e}")
        import traceback
        traceback.print_exc()


def job():
    """Wrapper to run async function in sync context"""
    asyncio.run(run_full_sync())


def start_scheduler():
    """Start the background scheduler"""
    logger.info("Starting background scheduler...")
    logger.info("Sync will run at :20 past every 3rd hour (00:20, 03:20, 06:20, 09:20, 12:20, 15:20, 18:20, 21:20)")
    
    # Schedule sync at :20 past specific hours (every 3 hours)
    schedule.every().day.at("00:20").do(job)
    schedule.every().day.at("03:20").do(job)
    schedule.every().day.at("06:20").do(job)
    schedule.every().day.at("09:20").do(job)
    schedule.every().day.at("12:20").do(job)
    schedule.every().day.at("15:20").do(job)
    schedule.every().day.at("18:20").do(job)
    schedule.every().day.at("21:20").do(job)
    
    # Skip initial sync on startup to avoid memory issues on Render free tier
    # To trigger manual sync immediately: set env var RUN_INITIAL_SYNC=true
    import os
    if os.getenv('RUN_INITIAL_SYNC', 'false').lower() == 'true':
        logger.info("Running initial sync...")
        job()
    else:
        logger.info("Skipping initial sync. Next sync will run at the next scheduled time.")
        logger.info("To enable initial sync, set RUN_INITIAL_SYNC=true")
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    start_scheduler()

