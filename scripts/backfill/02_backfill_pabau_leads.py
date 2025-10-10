#!/usr/bin/env python3
"""
BACKFILL: Load ALL leads from Pabau to database
Run once to populate initial data

This script:
1. Fetches ALL leads from Pabau API (paginated)
2. Extracts custom field "Mailchimp Subscription Status"
3. Transforms data to match database schema
4. Inserts/updates into database
5. Logs all operations
"""

import sys
import os
import asyncio
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.pabau_client import PabauClient
from db.database import get_db
from utils.transforms import transform_lead_for_db


async def backfill_leads():
    """Main backfill function"""
    
    print("=" * 80)
    print("BACKFILL: PABAU LEADS → DATABASE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("")
    
    print("⚠️  IMPORTANT: Before running this script, ensure you have:")
    print("   1. Created custom field in Pabau for Leads:")
    print("      Name: 'opt_in_email_lead'")
    print("      Type: Integer or Number")
    print("      Values: 0 (opted out) or 1 (opted in)")
    print("      Note: Same format as client opt_in_email field")
    print("")
    
    confirm = input("Have you created the custom field? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Please create the custom field in Pabau first, then run this script again.")
        print("   Pabau → Settings → Custom Fields → Add Field (for Leads)")
        print("   Field name: opt_in_email_lead | Type: Integer | Values: 0 or 1")
        return
    
    print("")
    
    # Initialize
    db = get_db()
    pabau = PabauClient()
    
    try:
        # Fetch ALL leads from Pabau (excluding last 7 days for testing)
        print("📥 Fetching leads from Pabau...")
        print("  ⚠️  EXCLUDING last 7 days of data for incremental sync testing")
        print("  This may take a few minutes for large datasets...")
        print("")
        
        all_leads = await pabau.get_all_leads_paginated()
        
        print(f"✅ Fetched {len(all_leads)} leads from Pabau")
        print("")
        
        if not all_leads:
            print("⚠️  No leads found in Pabau")
            return
        
        # Get existing lead IDs from database to make it resumable
        print("🔍 Checking existing records in database...")
        existing_ids = db.execute_query(
            "SELECT pabau_id FROM leads WHERE pabau_id IS NOT NULL"
        )
        existing_pabau_ids = {row['pabau_id'] for row in existing_ids}
        print(f"  Found {len(existing_pabau_ids)} existing leads in database")
        print("")
        
        # Transform and insert
        print("💾 Processing leads...")
        print("")
        
        success_count = 0
        error_count = 0
        opted_in_count = 0
        no_consent_field_count = 0
        skipped_recent_count = 0
        skipped_existing_count = 0
        
        # Calculate cutoff date (7 days ago)
        cutoff_date = datetime.now() - timedelta(days=7)
        
        for i, lead_raw in enumerate(all_leads, 1):
            try:
                # Transform
                lead_data = transform_lead_for_db(lead_raw)
                
                # Skip if already in database (for resumability)
                if lead_data['pabau_id'] in existing_pabau_ids:
                    skipped_existing_count += 1
                    if i % 100 == 0:
                        print(f"  Progress: {i}/{len(all_leads)} - {success_count} new, {skipped_existing_count} already in DB", end='\r')
                    continue
                
                # Skip leads created or updated in last 7 days (for testing incremental sync)
                dates = lead_raw.get('dates', {})
                updated_date_str = dates.get('updated_date') if dates else None
                if updated_date_str:
                    try:
                        updated_date = datetime.fromisoformat(updated_date_str.replace('Z', '+00:00'))
                        if updated_date > cutoff_date:
                            skipped_recent_count += 1
                            continue
                    except:
                        pass  # If date parsing fails, include the record
                
                if not lead_data['email']:
                    print(f"⚠️  Skipping lead {lead_data['pabau_id']} - no email")
                    db.log_sync(
                        entity_type='lead',
                        entity_id=None,
                        pabau_id=lead_data['pabau_id'],
                        email='',
                        action='backfill_lead',
                        status='skipped',
                        message='No email address'
                    )
                    continue
                
                # Track consent status
                if lead_data['custom_field_mailchimp_consent'] == 'Opted In':
                    opted_in_count += 1
                elif lead_data['custom_field_mailchimp_consent'] == 'Not Set':
                    no_consent_field_count += 1
                
                # Insert/update
                db_id = db.upsert_lead(lead_data)
                
                # Log success
                db.log_sync(
                    entity_type='lead',
                    entity_id=db_id,
                    pabau_id=lead_data['pabau_id'],
                    email=lead_data['email'],
                    action='backfill_lead',
                    status='success',
                    message=f"Lead {lead_data['first_name']} {lead_data['last_name']} loaded (consent: {lead_data['custom_field_mailchimp_consent']})"
                )
                
                success_count += 1
                if lead_data['custom_field_mailchimp_consent'] == 1:
                    opted_in_count += 1
                
                # Progress - show every 100 for large datasets
                if i % 100 == 0:
                    print(f"  Progress: {i}/{len(all_leads)} - {success_count} new, {skipped_existing_count} already in DB, {skipped_recent_count} recent", end='\r')
            
            except Exception as e:
                error_count += 1
                print(f"❌ Error processing lead {lead_raw.get('id')}: {e}")
                
                db.log_sync(
                    entity_type='lead',
                    entity_id=None,
                    pabau_id=lead_raw.get('id'),
                    email=lead_raw.get('email', ''),
                    action='backfill_lead',
                    status='error',
                    error_details=str(e)
                )
        
        print("")
        print("")
        print("=" * 80)
        print("BACKFILL COMPLETE!")
        print("=" * 80)
        print(f"✅ Success (new):                {success_count}")
        print(f"⏭️  Skipped (existing):          {skipped_existing_count}")
        print(f"⏭️  Skipped (last 7 days):       {skipped_recent_count}")
        print(f"⚠️  No consent field set:        {no_consent_field_count}")
        print(f"❌ Errors:                       {error_count}")
        print(f"📧 Opted in (custom field = 1):  {opted_in_count}")
        print("")
        print("⚠️  NOTE: Last 7 days excluded - use sync script to catch up")
        print("")
        
        if no_consent_field_count > 0:
            print("⚠️  WARNING: Some leads don't have 'Mailchimp Subscription Status' set")
            print("   These leads will NOT be synced to Mailchimp")
            print("   Action required: Set custom field in Pabau for these leads")
            print("")
        
        # Show summary
        summary = db.get_summary()
        print("Database Summary:")
        for row in summary:
            print(f"  {row['category']}: {row['total']} total, {row['opted_in_email']} opted in")
        
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
        asyncio.run(backfill_leads())
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

