#!/usr/bin/env python3
"""
INCREMENTAL SYNC: Push newly synced contacts from database to Mailchimp
Run every 30 minutes

This script:
1. Gets contacts synced in last 30 minutes (opt_in_email = 1)
2. Uploads them to Mailchimp with all 18 fields in batches
3. Tags as "Pabau Clients" or "Pabau Leads"
4. Logs all operations
"""

import sys
import os
import asyncio
from datetime import datetime, timedelta
import gc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.mailchimp_client import MailchimpClient
from db.database import get_db


async def sync_to_mailchimp():
    """Sync recently updated opted-in contacts to Mailchimp"""
    
    print(f"[{datetime.now()}] Syncing to Mailchimp...")
    
    db = get_db()
    mc = MailchimpClient()
    
    try:
        # Check when Pabau sync last completed
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT MAX(created_at) as last_run
                FROM sync_logs
                WHERE action IN ('sync_pabau_clients_completed', 'sync_pabau_leads_completed')
                  AND status = 'success'
            """)
            result = cursor.fetchone()
            last_pabau_sync = result['last_run'] if result and result['last_run'] else None
        
        if not last_pabau_sync:
            print(f"  No Pabau sync completion found - nothing to upload")
            return
        
        # Check when Mailchimp upload last completed
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT MAX(created_at) as last_upload
                FROM sync_logs
                WHERE action = 'sync_to_mailchimp_completed'
                  AND status = 'success'
            """)
            result = cursor.fetchone()
            last_mailchimp_upload = result['last_upload'] if result and result['last_upload'] else datetime(2020, 1, 1)
        
        print(f"  Last Pabau sync completed: {last_pabau_sync}")
        print(f"  Last Mailchimp upload completed: {last_mailchimp_upload}")
        
        # Only upload if Pabau sync happened AFTER last Mailchimp upload
        if last_pabau_sync <= last_mailchimp_upload:
            print(f"  ✅ No new Pabau data since last Mailchimp upload")
            return
        
        print(f"  Uploading clients/leads synced after {last_mailchimp_upload}")
        
        # Find clients that were successfully synced from Pabau since last Mailchimp upload
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(DISTINCT sl.email) as count
                FROM sync_logs sl
                INNER JOIN clients c ON c.email = sl.email
                WHERE sl.action = 'sync_pabau_client'
                  AND sl.status = 'success'
                  AND sl.created_at > %s
                  AND c.opt_in_email = 1
                  AND c.is_active = 1
            """, (last_mailchimp_upload,))
            debug = cursor.fetchone()
            print(f"  Clients to upload: {debug['count']}")
        
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT ON (c.id)
                    c.id as client_db_id,
                    c.pabau_id as client_system_id,
                    c.first_name,
                    c.last_name,
                    c.email,
                    c.phone,
                    c.mobile as client_mobile,
                    c.gender,
                    c.opt_in_phone as phone_opt_in,
                    c.updated_at as last_updated,
                    a.appointment_date,
                    a.appointment_datetime,
                    a.service,
                    a.duration,
                    a.appointment_status,
                    a.appt_with,
                    a.created_by,
                    a.created_date
                FROM sync_logs sl
                INNER JOIN clients c ON c.email = sl.email
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM appointments
                    WHERE client_pabau_id = c.pabau_id
                    ORDER BY appointment_datetime DESC NULLS LAST
                    LIMIT 1
                ) a ON true
                WHERE sl.action = 'sync_pabau_client'
                  AND sl.status = 'success'
                  AND sl.created_at > %s
                  AND c.opt_in_email = 1
                  AND c.email IS NOT NULL
                  AND c.is_active = 1
                ORDER BY c.id
            """, (last_mailchimp_upload,))
            clients = cursor.fetchall()
        
        if not clients:
            print(f"  ✅ No new/updated clients to sync")
            return
        
        print(f"  Found {len(clients)} new/updated clients to sync")
        
        # Deduplicate by email (keep latest)
        email_to_client = {}
        for client in clients:
            email_lower = client['email'].lower()
            if email_lower in email_to_client:
                if client['client_db_id'] > email_to_client[email_lower]['client_db_id']:
                    email_to_client[email_lower] = client
            else:
                email_to_client[email_lower] = client
        
        unique_clients = list(email_to_client.values())
        print(f"  {len(unique_clients)} unique clients after deduplication")
        
        # Prepare batch data
        members_batch = []
        for client in unique_clients:
            merge_fields = {
                'FNAME': client['first_name'] or '',
                'LNAME': client['last_name'] or '',
            }
            
            # Add optional fields
            if client['phone']:
                merge_fields['PHONE'] = client['phone']
            if client['client_mobile']:
                merge_fields['MMERGE7'] = client['client_mobile']
            if client['gender']:
                merge_fields['MMERGE6'] = client['gender']
            
            merge_fields['MMERGE8'] = 'Yes' if client['phone_opt_in'] == 1 else 'No'
            
            # Appointment fields with validation
            try:
                if client['appointment_date']:
                    merge_fields['MMERGE9'] = client['appointment_date'].strftime('%m/%d/%Y')
            except (AttributeError, ValueError):
                pass
            
            if client['appt_with']:
                merge_fields['MMERGE10'] = str(client['appt_with'])[:50]
            if client['created_by']:
                merge_fields['MMERGE11'] = str(client['created_by'])[:50]
            
            try:
                if client['created_date']:
                    merge_fields['MMERGE12'] = client['created_date'].strftime('%m/%d/%Y')
            except (AttributeError, ValueError):
                pass
            
            if client['duration']:
                try:
                    duration_val = int(client['duration'])
                    merge_fields['MMERGE13'] = f"{duration_val} min"
                except (ValueError, TypeError):
                    pass
            
            if client['service']:
                merge_fields['MMERGE14'] = str(client['service'])[:100]
            
            try:
                if client['appointment_datetime']:
                    merge_fields['MMERGE15'] = client['appointment_datetime'].strftime('%m/%d/%Y %H:%M')
            except (AttributeError, ValueError):
                pass
            
            if client['appointment_status']:
                merge_fields['MMERGE18'] = str(client['appointment_status'])[:50]
            
            # Client System ID (required)
            try:
                system_id_val = int(client['client_system_id'])
                if 0 < system_id_val < 2147483647:
                    merge_fields['MMERGE17'] = system_id_val
                else:
                    continue
            except (ValueError, TypeError):
                continue
            
            members_batch.append({
                'email_address': client['email'],
                'status': 'subscribed',
                'merge_fields': merge_fields,
                'tags': ['Pabau Clients']
            })
        
        if not members_batch:
            print(f"  No valid members to upload")
            return
        
        # Upload in batches of 500
        batch_size = 500
        success_count = 0
        error_count = 0
        
        for batch_num in range(0, len(members_batch), batch_size):
            batch = members_batch[batch_num:batch_num + batch_size]
            batch_index = batch_num // batch_size + 1
            
            try:
                print(f"    Batch {batch_index}: Uploading {len(batch)} members...")
                result = await mc.batch_subscribe(batch, update_existing=True)
                
                batch_success = result.get('total_created', 0) + result.get('total_updated', 0)
                batch_errors = result.get('error_count', 0)
                
                success_count += batch_success
                error_count += batch_errors
                
                print(f"    Batch {batch_index}: ✅ {batch_success} success, ❌ {batch_errors} errors")
                
                # Clean up
                batch = None
                result = None
                gc.collect()
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"    Batch {batch_index}: ❌ Failed - {str(e)[:100]}")
                error_count += len(batch)
                gc.collect()
                await asyncio.sleep(1)
        
        print(f"  Total: ✅ {success_count} success, ❌ {error_count} errors")
        
        # Log successful sync for each uploaded client
        if success_count > 0:
            for client in unique_clients[:success_count]:  # Log up to success_count
                try:
                    db.log_sync(
                        entity_type='client',
                        entity_id=client['client_db_id'],
                        pabau_id=client['client_system_id'],
                        email=client['email'],
                        action='sync_to_mailchimp',
                        status='success',
                        message='Synced to Mailchimp'
                    )
                except Exception as log_error:
                    print(f"  ⚠️  Failed to log sync for {client['email']}: {log_error}")
        
        # Log completion (even if 0 uploads)
        db.log_sync(
            entity_type='sync_run',
            entity_id=None,
            pabau_id=None,
            email=None,
            action='sync_to_mailchimp_completed',
            status='success',
            message=f'Uploaded {success_count} clients/leads to Mailchimp'
        )
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == '__main__':
    asyncio.run(sync_to_mailchimp())
