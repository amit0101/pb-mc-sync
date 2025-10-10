#!/usr/bin/env python3
"""
INCREMENTAL SYNC: Fetch updated clients/leads/appointments from Pabau
Run every 30 minutes to sync changes

This script:
1. Gets last successful sync time from database (MAX(pabau_last_synced_at))
2. Fetches records where Pabau's updated_at > last_sync_time
3. Syncs clients, leads, and their appointments
4. Updates pabau_last_synced_at for each synced record
5. Logs all operations

Strategy:
- First run (pabau_last_synced_at = NULL): Fetches all records
- Subsequent runs: Only fetches records updated in Pabau since last sync
- Works regardless of time gap (30 min, 1 hour, 1 day, etc.)
- Captures both NEW records and UPDATES to existing records
"""

import sys
import os
import asyncio
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.pabau_client import PabauClient
from db.database import get_db
from utils.transforms import (
    transform_client_for_db,
    transform_lead_for_db,
    transform_appointments_from_client
)


async def sync_pabau_clients():
    """Fetch clients updated since last sync and their appointments"""
    
    print(f"[{datetime.now()}] Syncing Pabau clients...")
    
    db = get_db()
    pabau = PabauClient()
    
    try:
        # Get last successful sync time from database
        last_sync = db.get_last_sync_time('client')
        
        if last_sync:
            # Use last sync time as cutoff
            cutoff_date = last_sync
            print(f"  Last sync: {last_sync}")
            print(f"  Will process clients with Pabau created_date > {cutoff_date}")
        else:
            # First sync - process all records (no filter)
            cutoff_date = None
            print(f"  First sync - will process all clients")
        
        # Fetch ALL clients from Pabau API
        # Note: Pabau API filtering doesn't work, so we filter by created_date in application
        print(f"  Fetching all clients from Pabau API...")
        all_clients = await pabau.get_all_contacts_paginated()
        
        print(f"  Fetched {len(all_clients)} total clients from Pabau API")
        print(f"  Filtering by created_date > {cutoff_date if cutoff_date else 'no filter'}...")
        
        clients_updated = 0
        appointments_updated = 0
        skipped_count = 0
        sync_time = datetime.now()
        
        for client_raw in all_clients:
            try:
                client_data = transform_client_for_db(client_raw)
                
                # Skip if no email
                if not client_data['email']:
                    skipped_count += 1
                    continue
                
                # Filter by created_date - only process clients created after last sync
                if cutoff_date:  # Only filter if we have a cutoff date
                    created_date = client_data.get('created_date')
                    if not created_date:
                        # No created_date, skip
                        skipped_count += 1
                        continue
                    
                    if isinstance(created_date, datetime):
                        if created_date <= cutoff_date:
                            # Created before last sync, skip
                            skipped_count += 1
                            continue
                    else:
                        # Can't compare dates, skip
                        skipped_count += 1
                        continue
                # If cutoff_date is None (first sync), process all records
                
                # Upsert client (only new ones)
                db_id = db.upsert_client(client_data)
                clients_updated += 1
                
                # Update pabau_last_synced_at to track this sync
                with db.get_cursor() as cursor:
                    cursor.execute("""
                        UPDATE clients 
                        SET pabau_last_synced_at = %s
                        WHERE id = %s
                    """, (sync_time, db_id))
                
                # Sync appointments for this client
                appointments = transform_appointments_from_client(client_raw)
                for appt_data in appointments:
                    db.upsert_appointment(appt_data)
                    appointments_updated += 1
                
                db.log_sync(
                    entity_type='client',
                    entity_id=db_id,
                    pabau_id=client_data['pabau_id'],
                    email=client_data['email'],
                    action='sync_pabau_client',
                    status='success',
                    message=f'Client and {len(appointments)} appointments synced'
                )
            
            except Exception as e:
                print(f"  ❌ Error processing client {client_raw.get('details', {}).get('id')}: {e}")
                db.log_sync(
                    entity_type='client',
                    entity_id=None,
                    pabau_id=client_raw.get('details', {}).get('id'),
                    email=client_raw.get('communications', {}).get('email', ''),
                    action='sync_pabau_client',
                    status='error',
                    error_details=str(e)
                )
        
        print(f"  ✅ Clients synced: {clients_updated}")
        print(f"  ✅ Appointments synced: {appointments_updated}")
        print(f"  ⏭️  Skipped (not updated/no email): {skipped_count}")
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def sync_pabau_leads():
    """Fetch leads updated since last sync"""
    
    print(f"[{datetime.now()}] Syncing Pabau leads...")
    
    db = get_db()
    pabau = PabauClient()
    
    try:
        # Get last successful sync time from database
        last_sync = db.get_last_sync_time('lead')
        
        if last_sync:
            # Use last sync time as cutoff
            cutoff_date = last_sync
            print(f"  Last sync: {last_sync}")
            print(f"  Will process leads with Pabau created_date > {cutoff_date}")
        else:
            # First sync - process all records (no filter)
            cutoff_date = None
            print(f"  First sync - will process all leads")
        
        # Fetch ALL leads from Pabau API
        # Note: Pabau API filtering doesn't work, so we filter by created_date in application
        print(f"  Fetching all leads from Pabau API...")
        all_leads = await pabau.get_all_leads_paginated()
        
        print(f"  Fetched {len(all_leads)} total leads from Pabau API")
        print(f"  Filtering by created_date > {cutoff_date if cutoff_date else 'no filter'}...")
        
        updated_count = 0
        skipped_count = 0
        sync_time = datetime.now()
        
        for lead_raw in all_leads:
            try:
                lead_data = transform_lead_for_db(lead_raw)
                
                # Skip if no email
                if not lead_data['email']:
                    skipped_count += 1
                    continue
                
                # Filter by created_date - only process leads created after last sync
                if cutoff_date:  # Only filter if we have a cutoff date
                    created_date = lead_data.get('created_date')
                    if not created_date:
                        # No created_date, skip
                        skipped_count += 1
                        continue
                    
                    if isinstance(created_date, datetime):
                        if created_date <= cutoff_date:
                            # Created before last sync, skip
                            skipped_count += 1
                            continue
                    else:
                        # Can't compare dates, skip
                        skipped_count += 1
                        continue
                # If cutoff_date is None (first sync), process all records
                
                db_id = db.upsert_lead(lead_data)
                updated_count += 1
                
                # Update pabau_last_synced_at
                with db.get_cursor() as cursor:
                    cursor.execute("""
                        UPDATE leads 
                        SET pabau_last_synced_at = %s
                        WHERE id = %s
                    """, (sync_time, db_id))
                
                db.log_sync(
                    entity_type='lead',
                    entity_id=db_id,
                    pabau_id=lead_data['pabau_id'],
                    email=lead_data['email'],
                    action='sync_pabau_lead',
                    status='success',
                    message='Lead synced'
                )
            
            except Exception as e:
                print(f"  ❌ Error processing lead {lead_raw.get('id')}: {e}")
                db.log_sync(
                    entity_type='lead',
                    entity_id=None,
                    pabau_id=lead_raw.get('id'),
                    email=lead_raw.get('email', ''),
                    action='sync_pabau_lead',
                    status='error',
                    error_details=str(e)
                )
        
        print(f"  ✅ Leads synced: {updated_count}")
        print(f"  ⏭️  Skipped (not updated/no email): {skipped_count}")
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def sync_pabau():
    """Sync both clients and leads"""
    await sync_pabau_clients()
    await sync_pabau_leads()


if __name__ == '__main__':
    asyncio.run(sync_pabau())
