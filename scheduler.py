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
    start_time = datetime.now()
    try:
        logger.info("=" * 80)
        logger.info(f"Starting sync cycle at {start_time}")
        logger.info("=" * 80)
        
        # Step 1: Sync Pabau to Database (HEAVY - ~30 min)
        logger.info("Step 1/3: Syncing Pabau to Database...")
        logger.info("Note: This step processes ~28K clients and takes ~30 minutes")
        try:
            await sync_pabau()
            logger.info("✅ Pabau sync completed")
        except Exception as e:
            logger.error(f"❌ Pabau sync failed: {e}")
            # Continue with other syncs even if Pabau fails
        
        # Step 2: Fetch Mailchimp unsubscribes (FAST - ~1 min)
        logger.info("Step 2/3: Fetching Mailchimp unsubscribes...")
        try:
            await fetch_unsubscribes()
            logger.info("✅ Mailchimp unsubscribes sync completed")
        except Exception as e:
            logger.error(f"❌ Mailchimp unsubscribes sync failed: {e}")
        
        # Step 3: Sync Database to Mailchimp (FAST - ~5 min)
        logger.info("Step 3/3: Syncing Database to Mailchimp...")
        try:
            await sync_to_mailchimp()
            logger.info("✅ Database to Mailchimp sync completed")
        except Exception as e:
            logger.error(f"❌ Database to Mailchimp sync failed: {e}")
        
        elapsed = (datetime.now() - start_time).total_seconds() / 60
        logger.info("=" * 80)
        logger.info(f"Sync cycle completed at {datetime.now()}")
        logger.info(f"Total duration: {elapsed:.1f} minutes")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Fatal error in sync cycle: {e}")
        import traceback
        traceback.print_exc()


def job():
    """Wrapper to run async function in sync context"""
    asyncio.run(run_full_sync())


def start_scheduler():
    """Start the background scheduler"""
    logger.info("Starting background scheduler...")
    logger.info("Sync will run at :55 past every 3rd hour (00:55, 03:55, 06:55, 09:55, 12:55, 15:55, 18:55, 21:55)")
    
    # Schedule sync at :55 past specific hours (every 3 hours)
    schedule.every().day.at("00:55").do(job)
    schedule.every().day.at("03:55").do(job)
    schedule.every().day.at("06:55").do(job)
    schedule.every().day.at("09:55").do(job)
    schedule.every().day.at("12:55").do(job)
    schedule.every().day.at("15:55").do(job)
    schedule.every().day.at("18:55").do(job)
    schedule.every().day.at("21:55").do(job)
    
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

