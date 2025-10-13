#!/usr/bin/env python3
"""
INCREMENTAL SYNC: Fetch unsubscribed members from Mailchimp
Run every 30 minutes to capture unsubscribes

This script:
1. Fetches all unsubscribed members from Mailchimp
2. Checks if they exist in database
3. Updates opt_in_email to 0 if currently 1
4. Logs all changes
"""

import sys
import os
import asyncio
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.mailchimp_client import MailchimpClient
from db.database import get_db


async def fetch_unsubscribes():
    """Fetch and process unsubscribed members"""
    
    print(f"[{datetime.now()}] Fetching Mailchimp unsubscribes...")
    
    db = get_db()
    mailchimp = MailchimpClient()
    
    try:
        # Fetch all unsubscribed members
        members = await mailchimp.get_all_members(status="unsubscribed")
        
        print(f"  Found {len(members)} unsubscribed members in Mailchimp")
        
        updated_count = 0
        skipped_count = 0
        
        for member in members:
            email = member['email_address']
            
            # Update in database
            result = db.update_opt_in_from_mailchimp(email, 0)  # 0 = opted out
            
            if result:
                updated_count += 1
                db.log_sync(
                    entity_type=result,
                    entity_id=None,
                    pabau_id=None,
                    email=email,
                    action='mailchimp_unsubscribe',
                    status='success',
                    message=f'Updated {result} opt_in_email to 0'
                )
            else:
                skipped_count += 1
        
        print(f"  Updated: {updated_count}, Skipped: {skipped_count}")
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        db.log_sync(
            entity_type=None,
            entity_id=None,
            pabau_id=None,
            email='',
            action='mailchimp_unsubscribe',
            status='error',
            error_details=str(e)
        )
    finally:
        db.close()


if __name__ == '__main__':
    asyncio.run(fetch_unsubscribes())

