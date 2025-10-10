#!/usr/bin/env python3
"""
Script to run initial backfill
Run this once when setting up the sync for the first time
"""
import sys
import asyncio
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from database import SessionLocal, init_db
from services import SyncService


async def main(limit: int = None):
    """Run initial backfill
    
    Args:
        limit: Optional limit on number of records to sync (for testing)
    """
    logger.info("=" * 80)
    logger.info("Starting initial backfill process")
    if limit:
        logger.info(f"TEST MODE: Limited to {limit} records")
    logger.info("=" * 80)
    
    # Initialize database
    init_db()
    logger.info("Database initialized")
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Create sync service
        sync_service = SyncService(db)
        
        # Confirm before proceeding
        if limit:
            print(f"\nTEST MODE: This will sync up to {limit} contacts/leads from Pabau to Mailchimp.")
        else:
            print("\nThis will sync ALL contacts and leads from Pabau to Mailchimp.")
        print("This may take a while depending on the number of records.")
        response = input("\nDo you want to continue? (yes/no): ")
        
        if response.lower() not in ["yes", "y"]:
            logger.info("Backfill cancelled by user")
            return
        
        # Run backfill
        stats = await sync_service.initial_backfill()
        
        # Print results
        logger.info("=" * 80)
        logger.info("Backfill completed!")
        logger.info("=" * 80)
        logger.info(f"Contacts processed: {stats['contacts_processed']}")
        logger.info(f"Contacts succeeded: {stats['contacts_succeeded']}")
        logger.info(f"Contacts failed: {stats['contacts_failed']}")
        logger.info(f"Leads processed: {stats['leads_processed']}")
        logger.info(f"Leads succeeded: {stats['leads_succeeded']}")
        logger.info(f"Leads failed: {stats['leads_failed']}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Backfill failed: {str(e)}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run initial backfill from Pabau to Mailchimp")
    parser.add_argument("--limit", type=int, help="Limit number of records to sync (for testing)")
    args = parser.parse_args()
    
    asyncio.run(main(limit=args.limit))

