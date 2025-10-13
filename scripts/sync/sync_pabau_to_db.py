#!/usr/bin/env python3
"""
INCREMENTAL SYNC: Fetch clients/leads/appointments from Pabau and sync to database

This script:
1. Gets last successful sync time from database (MAX(pabau_last_synced_at))
2. Fetches ALL pages from Pabau API (no filtering available)
3. Filters by created_date in application (only process new records)
4. Syncs clients, leads, and their appointments
5. Updates pabau_last_synced_at for each synced record
6. Logs all operations

Strategy:
- Pabau API has NO working date filters or ordering
- Must fetch all ~860 pages (~42K clients, takes ~30 min)
- Filter by created_date > last_sync in application
- Run infrequently (every 4-6 hours)
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
    """Fetch ALL clients from Pabau and sync new ones to database
    
    Strategy: Fetch all pages, filter by created_date in application
    """
    
    start_time = datetime.now()
    print(f"[{start_time}] Syncing Pabau clients...")
    
    db = get_db()
    pabau = PabauClient()
    
    try:
        # Get last successful sync time from database
        last_sync = db.get_last_sync_time('client')
        
        if last_sync:
            cutoff_date = last_sync
            print(f"  Last sync: {last_sync}")
            print(f"  Will process clients created after {cutoff_date}")
        else:
            cutoff_date = None
            print(f"  First sync - will process all clients")
        
        # Fetch ALL pages from Pabau (no filtering works in API)
        print(f"  Fetching all clients from Pabau API...")
        
        clients_updated = 0
        appointments_updated = 0
        skipped_old = 0
        skipped_no_email = 0
        sync_time = datetime.now()
        page = 1
        total_fetched = 0
        
        while True:
            # Fetch one page at a time
            response = await pabau.get_contacts(page=page, page_size=50)
            clients_on_page = response.get("clients", [])
            
            if not clients_on_page:
                print(f"  Page {page}: Empty - stopping")
                break
            
            total_fetched += len(clients_on_page)
            
            # Progress update every 50 pages
            if page % 50 == 0:
                elapsed = (datetime.now() - start_time).total_seconds() / 60
                print(f"  Page {page}: Fetched {total_fetched} clients so far ({elapsed:.1f} min, "
                      f"{clients_updated} new, {skipped_old} old, {skipped_no_email} no email)")
            
            # Process clients on this page
            for client_raw in clients_on_page:
                try:
                    client_data = transform_client_for_db(client_raw)
                    
                    # Skip if no email
                    if not client_data['email']:
                        skipped_no_email += 1
                        continue
                    
                    # Filter by created_date - only process clients created after last sync
                    if cutoff_date:
                        created_date_str = client_data.get('created_date')
                        if not created_date_str:
                            skipped_old += 1
                            continue
                        
                        # Parse string date to datetime
                        try:
                            if isinstance(created_date_str, str):
                                created_date = datetime.strptime(created_date_str, '%Y-%m-%d %H:%M:%S')
                            elif isinstance(created_date_str, datetime):
                                created_date = created_date_str
                            else:
                                skipped_old += 1
                                continue
                            
                            # Skip if created before last sync
                            if created_date <= cutoff_date:
                                skipped_old += 1
                                continue
                        except:
                            skipped_old += 1
                            continue
                    
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
            
            # Move to next page
            page += 1
        
        elapsed_total = (datetime.now() - start_time).total_seconds() / 60
        print()
        print(f"  ✅ Completed in {elapsed_total:.1f} minutes")
        print(f"  📊 Total fetched: {total_fetched} clients")
        print(f"  ✅ New clients synced: {clients_updated}")
        print(f"  ✅ Appointments synced: {appointments_updated}")
        print(f"  ⏭️  Skipped (old): {skipped_old}")
        print(f"  ⏭️  Skipped (no email): {skipped_no_email}")
        
        # Log sync completion (even if 0 new records)
        db.log_sync(
            entity_type='sync_run',
            entity_id=None,
            pabau_id=None,
            email=None,
            action='sync_pabau_clients_completed',
            status='success',
            message=f'Synced {clients_updated} clients, {appointments_updated} appointments from {total_fetched} fetched'
        )
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def sync_pabau_leads():
    """Fetch ALL leads from Pabau and sync new ones to database
    
    Strategy: Fetch all pages, filter by created_date in application
    """
    
    start_time = datetime.now()
    print(f"[{start_time}] Syncing Pabau leads...")
    
    db = get_db()
    pabau = PabauClient()
    
    try:
        # Get last successful sync time from database
        last_sync = db.get_last_sync_time('lead')
        
        if last_sync:
            cutoff_date = last_sync
            print(f"  Last sync: {last_sync}")
            print(f"  Will process leads created after {cutoff_date}")
        else:
            cutoff_date = None
            print(f"  First sync - will process all leads")
        
        # Fetch ALL pages from Pabau (no filtering works in API)
        print(f"  Fetching all leads from Pabau API...")
        
        updated_count = 0
        skipped_old = 0
        skipped_no_email = 0
        sync_time = datetime.now()
        page = 1
        total_fetched = 0
        
        while True:
            # Fetch one page at a time
            response = await pabau.get_leads(page=page, page_size=50)
            leads_on_page = response.get("leads", [])
            
            if not leads_on_page:
                print(f"  Page {page}: Empty - stopping")
                break
            
            total_fetched += len(leads_on_page)
            
            # Progress update every 50 pages
            if page % 50 == 0:
                elapsed = (datetime.now() - start_time).total_seconds() / 60
                print(f"  Page {page}: Fetched {total_fetched} leads so far ({elapsed:.1f} min, "
                      f"{updated_count} new, {skipped_old} old, {skipped_no_email} no email)")
            
            # Process leads on this page
            for lead_raw in leads_on_page:
                try:
                    lead_data = transform_lead_for_db(lead_raw)
                    
                    # Skip if no email
                    if not lead_data['email']:
                        skipped_no_email += 1
                        continue
                    
                    # Filter by created_date - only process leads created after last sync
                    if cutoff_date:
                        created_date_str = lead_data.get('created_date')
                        if not created_date_str:
                            skipped_old += 1
                            continue
                        
                        # Parse string date to datetime
                        try:
                            if isinstance(created_date_str, str):
                                created_date = datetime.strptime(created_date_str, '%Y-%m-%d %H:%M:%S')
                            elif isinstance(created_date_str, datetime):
                                created_date = created_date_str
                            else:
                                skipped_old += 1
                                continue
                            
                            # Skip if created before last sync
                            if created_date <= cutoff_date:
                                skipped_old += 1
                                continue
                        except:
                            skipped_old += 1
                            continue
                    
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
            
            # Move to next page
            page += 1
        
        elapsed_total = (datetime.now() - start_time).total_seconds() / 60
        print()
        print(f"  ✅ Completed in {elapsed_total:.1f} minutes")
        print(f"  📊 Total fetched: {total_fetched} leads")
        print(f"  ✅ New leads synced: {updated_count}")
        print(f"  ⏭️  Skipped (old): {skipped_old}")
        print(f"  ⏭️  Skipped (no email): {skipped_no_email}")
        
        # Log sync completion (even if 0 new records)
        db.log_sync(
            entity_type='sync_run',
            entity_id=None,
            pabau_id=None,
            email=None,
            action='sync_pabau_leads_completed',
            status='success',
            message=f'Synced {updated_count} leads from {total_fetched} fetched'
        )
        
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
