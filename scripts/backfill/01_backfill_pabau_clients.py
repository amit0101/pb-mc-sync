#!/usr/bin/env python3
"""
BACKFILL: Load ALL clients from Pabau to database
Run once to populate initial data

This script:
1. Fetches ALL clients from Pabau API (paginated)
2. Transforms data to match database schema
3. Inserts/updates into database
4. Logs all operations
"""

import sys
import os
import asyncio
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.pabau_client import PabauClient
from db.database import get_db
from utils.transforms import transform_client_for_db


async def backfill_clients():
    """Main backfill function - fetches and inserts in batches"""
    
    print("=" * 80)
    print("BACKFILL: PABAU CLIENTS → DATABASE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("")
    
    # Initialize
    db = get_db()
    pabau = PabauClient()
    
    try:
        # Get existing client IDs from database to make it resumable
        print("🔍 Checking existing records in database...")
        existing_ids = db.execute_query(
            "SELECT pabau_id FROM clients WHERE pabau_id IS NOT NULL"
        )
        existing_pabau_ids = {row['pabau_id'] for row in existing_ids}
        print(f"  Found {len(existing_pabau_ids)} existing clients in database")
        print("")
        
        # Fetch and process in batches
        print("📥 Fetching and processing clients from Pabau...")
        print("  ⚠️  EXCLUDING last 7 days of data for incremental sync testing")
        print("  Processing in batches of 5000 records for safety")
        print("")
        
        success_count = 0
        error_count = 0
        opted_in_count = 0
        skipped_recent_count = 0
        skipped_existing_count = 0
        total_fetched = 0
        
        # Calculate cutoff date (7 days ago)
        cutoff_date = datetime.now() - timedelta(days=7)
        
        # Paginate through all records
        page = 1
        batch_count = 0
        
        while True:
            # Fetch one page
            response = await pabau.get_contacts(page=page, page_size=50)
            clients = response.get("clients", [])
            
            if not clients:
                print(f"\n📄 Page {page} returned no clients - pagination complete")
                break
            
            total_fetched += len(clients)
            print(f"📄 Page {page}: Processing {len(clients)} clients (total fetched: {total_fetched})...")
            
            # Process this page
            for client_raw in clients:
                try:
                    # Transform
                    client_data = transform_client_for_db(client_raw)
                    
                    # Skip if already in database (for resumability)
                    if client_data['pabau_id'] in existing_pabau_ids:
                        skipped_existing_count += 1
                        continue
                    
                    # Skip clients created or updated in last 7 days (for testing incremental sync)
                    created = client_raw.get('created', {})
                    created_date_str = created.get('created_date')
                    if created_date_str:
                        try:
                            created_date = datetime.fromisoformat(created_date_str.replace('Z', '+00:00'))
                            if created_date > cutoff_date:
                                skipped_recent_count += 1
                                continue
                        except:
                            pass  # If date parsing fails, include the record
                    
                    if not client_data['email']:
                        db.log_sync(
                            entity_type='client',
                            entity_id=None,
                            pabau_id=client_data['pabau_id'],
                            email='',
                            action='backfill_client',
                            status='skipped',
                            message='No email address'
                        )
                        continue
                    
                    # Insert/update
                    db_id = db.upsert_client(client_data)
                    
                    # Log success
                    db.log_sync(
                        entity_type='client',
                        entity_id=db_id,
                        pabau_id=client_data['pabau_id'],
                        email=client_data['email'],
                        action='backfill_client',
                        status='success',
                        message=f"Client {client_data['first_name']} {client_data['last_name']} loaded"
                    )
                    
                    success_count += 1
                    if client_data['opt_in_email'] == 1:
                        opted_in_count += 1
                
                except Exception as e:
                    error_count += 1
                    print(f"      ❌ Error: {e}")
                    db.log_sync(
                        entity_type='client',
                        entity_id=None,
                        pabau_id=client_raw.get('details', {}).get('id'),
                        email=client_raw.get('communications', {}).get('email', ''),
                        action='backfill_client',
                        status='error',
                        error_details=str(e)
                    )
            
            # Show progress summary every 100 pages (5000 records)
            batch_count += 1
            if batch_count % 100 == 0:
                print(f"   ✅ Batch checkpoint: {success_count} new, {skipped_existing_count} existing, {skipped_recent_count} recent")
            
            # Check if this is the last page
            if len(clients) < 50:
                print(f"\n📄 Page {page} returned < 50 clients - this is the last page")
                break
            
            page += 1
        
        print("")
        print(f"✅ Pagination complete: Fetched {total_fetched} total contacts across {page} pages")
        print("")
        
        # Show final summary
        print("=" * 80)
        print("BACKFILL COMPLETE!")
        print("=" * 80)
        print(f"✅ Success (new):          {success_count}")
        print(f"⏭️  Skipped (existing):    {skipped_existing_count}")
        print(f"⏭️  Skipped (last 7 days): {skipped_recent_count}")
        print(f"❌ Errors:                 {error_count}")
        print(f"📧 Opted in for email:     {opted_in_count}")
        print("")
        print("⚠️  NOTE: Last 7 days excluded - use sync script to catch up")
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
        asyncio.run(backfill_clients())
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

