#!/usr/bin/env python3
"""
BACKFILL: Fetch Mailchimp data for existing database contacts
Run after loading Pabau data to enrich with Mailchimp status

This script:
1. Gets all contacts from database (clients + leads)
2. For each contact, checks if they exist in Mailchimp
3. Updates database with Mailchimp status, ID, and tags
4. Logs all operations
"""

import sys
import os
import asyncio
import hashlib
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.mailchimp_client import MailchimpClient
from db.database import get_db


def get_mailchimp_member_id(email: str) -> str:
    """
    Generate Mailchimp member ID (MD5 hash of lowercase email)
    
    Args:
        email: Email address
    
    Returns:
        MD5 hash string
    """
    return hashlib.md5(email.lower().encode()).hexdigest()


async def fetch_mailchimp_member(mailchimp: MailchimpClient, email: str) -> dict:
    """
    Fetch member from Mailchimp by email
    
    Returns:
        Member data dict or None if not found
    """
    try:
        member_id = get_mailchimp_member_id(email)
        member = await mailchimp.get_list_member(member_id)
        return member
    except Exception as e:
        # Member not found (404) is expected
        if '404' in str(e):
            return None
        raise


async def backfill_mailchimp_data():
    """Main backfill function"""
    
    print("=" * 80)
    print("BACKFILL: MAILCHIMP DATA → DATABASE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("")
    
    # Initialize
    db = get_db()
    mailchimp = MailchimpClient()
    
    try:
        # Get all contacts from database
        print("📊 Getting contacts from database...")
        
        with db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM v_all_contacts")
            all_contacts = cursor.fetchall()
        
        print(f"✅ Found {len(all_contacts)} contacts in database")
        print("")
        
        if not all_contacts:
            print("⚠️  No contacts in database")
            print("   Run backfill scripts 01 and 02 first")
            return
        
        # Fetch Mailchimp data for each contact
        print("📥 Fetching Mailchimp data...")
        print("")
        
        found_count = 0
        not_found_count = 0
        error_count = 0
        subscribed_count = 0
        unsubscribed_count = 0
        
        for i, contact in enumerate(all_contacts, 1):
            try:
                email = contact['email']
                contact_type = contact['contact_type']
                
                # Fetch from Mailchimp
                member = await fetch_mailchimp_member(mailchimp, email)
                
                if member:
                    # Found in Mailchimp - update database
                    found_count += 1
                    
                    member_id = get_mailchimp_member_id(email)
                    status = member.get('status')
                    tags = [tag['name'] for tag in member.get('tags', [])]
                    
                    if status == 'subscribed':
                        subscribed_count += 1
                    elif status == 'unsubscribed':
                        unsubscribed_count += 1
                    
                    # Update database
                    if contact_type == 'client':
                        db.update_client_mailchimp_status(email, member_id, status, tags)
                    else:
                        db.update_lead_mailchimp_status(email, member_id, status, tags)
                    
                    # Log
                    db.log_sync(
                        entity_type=contact_type,
                        entity_id=contact['entity_id'],
                        pabau_id=contact['pabau_id'],
                        email=email,
                        action='backfill_mailchimp',
                        status='success',
                        message=f"Found in Mailchimp: {status}, tags: {tags}"
                    )
                else:
                    # Not found in Mailchimp
                    not_found_count += 1
                    
                    # Log
                    db.log_sync(
                        entity_type=contact_type,
                        entity_id=contact['entity_id'],
                        pabau_id=contact['pabau_id'],
                        email=email,
                        action='backfill_mailchimp',
                        status='skipped',
                        message='Not found in Mailchimp'
                    )
                
                # Progress
                if i % 10 == 0:
                    print(f"  Processed {i}/{len(all_contacts)} contacts...", end='\r')
                
                # Rate limiting - Mailchimp has limits
                if i % 100 == 0:
                    await asyncio.sleep(1)
            
            except Exception as e:
                error_count += 1
                print(f"❌ Error processing {contact['email']}: {e}")
                
                db.log_sync(
                    entity_type=contact['contact_type'],
                    entity_id=contact['entity_id'],
                    pabau_id=contact['pabau_id'],
                    email=contact['email'],
                    action='backfill_mailchimp',
                    status='error',
                    error_details=str(e)
                )
        
        print("")
        print("")
        print("=" * 80)
        print("BACKFILL COMPLETE!")
        print("=" * 80)
        print(f"📊 Total contacts checked: {len(all_contacts)}")
        print(f"✅ Found in Mailchimp: {found_count}")
        print(f"  - Subscribed: {subscribed_count}")
        print(f"  - Unsubscribed: {unsubscribed_count}")
        print(f"⚠️  Not in Mailchimp: {not_found_count}")
        print(f"❌ Errors: {error_count}")
        print("")
        
        # Show summary
        summary = db.get_summary()
        print("Database Summary:")
        for row in summary:
            print(f"  {row['category']}: {row['in_mailchimp']} in Mailchimp, {row['needs_sync']} need sync")
        
        print("")
        print(f"Completed at: {datetime.now()}")
        print("")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise
    finally:
        db.close()


if __name__ == '__main__':
    try:
        asyncio.run(backfill_mailchimp_data())
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

