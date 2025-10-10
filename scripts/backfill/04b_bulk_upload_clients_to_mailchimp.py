#!/usr/bin/env python3
"""
STEP 4B: BULK upload all opted-in clients to Mailchimp

This script:
1. Queries database for ALL opted-in clients with their latest appointment
2. Prepares all 18 fields from sample-pabau.txt for each client
3. Bulk uploads to Mailchimp "ALL Contacts" list using batch operations (500 per batch)
4. Tags uploaded members as "Pabau Clients"
5. Only uploads clients with opt_in_email = 1 (excluding unsubscribed)

Filtering rules:
- opt_in_email = 1 (opted in for email marketing)
- email IS NOT NULL
- is_active = 1
"""

import sys
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Load environment variables
load_dotenv()

from clients.mailchimp_client import MailchimpClient
from db.database import get_db


async def bulk_upload_clients():
    """Bulk upload all opted-in clients to Mailchimp"""
    
    print("=" * 80)
    print("BULK UPLOAD CLIENTS TO MAILCHIMP")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("")
    
    db = get_db()
    mc = MailchimpClient()
    
    try:
        # Query: Get ALL opted-in clients with their latest appointment
        print("📊 Querying all opted-in clients with latest appointments...")
        print("")
        
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    c.id as client_db_id,
                    c.pabau_id as client_system_id,
                    c.custom_id as client_id,
                    c.first_name,
                    c.last_name,
                    c.email,
                    c.phone,
                    c.mobile as client_mobile,
                    c.gender,
                    c.opt_in_phone as phone_opt_in,
                    c.opt_in_email,
                    a.appointment_date,
                    a.appointment_time,
                    a.appointment_datetime,
                    a.location as appointment_location,
                    a.service,
                    a.duration,
                    a.appointment_status,
                    a.appt_with,
                    a.created_by,
                    a.created_date,
                    a.cancellation_reason
                FROM clients c
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM appointments
                    WHERE client_pabau_id = c.pabau_id
                    ORDER BY appointment_datetime DESC NULLS LAST
                    LIMIT 1
                ) a ON true
                WHERE c.opt_in_email = 1
                  AND c.email IS NOT NULL
                  AND c.is_active = 1
                ORDER BY c.id
            """)
            clients = cursor.fetchall()
        
        if not clients:
            print("⚠️  No opted-in clients found!")
            return
        
        print(f"✅ Found {len(clients)} clients from database")
        print("")
        
        # Deduplicate by email (keep LATEST client record - highest client ID)
        email_to_client = {}
        duplicates_removed = 0
        
        for client in clients:
            email_lower = client['email'].lower()
            if email_lower in email_to_client:
                # Keep the client with higher ID (more recent)
                if client['client_db_id'] > email_to_client[email_lower]['client_db_id']:
                    email_to_client[email_lower] = client
                duplicates_removed += 1
            else:
                email_to_client[email_lower] = client
        
        if duplicates_removed > 0:
            print(f"⚠️  Removed {duplicates_removed} duplicate emails (kept most recent client record)")
        
        unique_clients = list(email_to_client.values())
        print(f"✅ {len(unique_clients)} unique clients to upload")
        print("")
        
        # Use unique_clients from here on
        clients = unique_clients
        
        # Count clients with/without appointments
        with_appt = sum(1 for c in clients if c['appointment_date'] is not None)
        without_appt = len(clients) - with_appt
        
        print(f"   With appointments:    {with_appt}")
        print(f"   Without appointments: {without_appt}")
        print("")
        
        # Prepare all members for batch upload
        print("📦 Preparing batch data...")
        print("")
        
        members_batch = []
        for client in clients:
            # Prepare merge fields using EXISTING Mailchimp field names
            # Only include fields that have values (Mailchimp rejects empty number fields)
            merge_fields = {
                'FNAME': client['first_name'] or '',
                'LNAME': client['last_name'] or '',
            }
            
            # Add optional text fields (only if they have values)
            if client['phone']:
                merge_fields['PHONE'] = client['phone']
            if client['client_mobile']:
                merge_fields['MMERGE7'] = client['client_mobile']
            if client['gender']:
                merge_fields['MMERGE6'] = client['gender']
            
            # Phone opt in
            merge_fields['MMERGE8'] = 'Yes' if client['phone_opt_in'] == 1 else 'No'
            
            # Appointment fields (only if appointment exists)
            # Note: Date fields use MM/DD/YYYY format for Mailchimp
            # Add validation to prevent invalid date values
            try:
                if client['appointment_date']:
                    merge_fields['MMERGE9'] = client['appointment_date'].strftime('%m/%d/%Y')
            except (AttributeError, ValueError):
                pass  # Skip invalid dates
            
            if client['appt_with']:
                merge_fields['MMERGE10'] = str(client['appt_with'])[:50]  # Limit length
            if client['created_by']:
                merge_fields['MMERGE11'] = str(client['created_by'])[:50]
            
            try:
                if client['created_date']:
                    merge_fields['MMERGE12'] = client['created_date'].strftime('%m/%d/%Y')
            except (AttributeError, ValueError):
                pass  # Skip invalid dates
            
            if client['duration']:
                try:
                    # Ensure duration is a valid number
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
                pass  # Skip invalid datetimes
            
            if client['appointment_status']:
                merge_fields['MMERGE18'] = str(client['appointment_status'])[:50]
            
            # Client System ID (pabau_id) - This is the primary identifier
            # Note: We're NOT sending MMERGE16 (custom_id/client_id) because those values
            # often exceed Mailchimp's 32-bit integer limit (2,147,483,647)
            # MMERGE17 (client_system_id) is always valid and is the true Pabau ID
            try:
                system_id_val = int(client['client_system_id'])
                # Validate range (should always be under 100M)
                if 0 < system_id_val < 2147483647:
                    merge_fields['MMERGE17'] = system_id_val
                else:
                    # This is critical - skip client if system ID is invalid
                    continue
            except (ValueError, TypeError):
                # This is a critical field - skip this entire client if invalid
                continue
            
            members_batch.append({
                'email_address': client['email'],
                'status': 'subscribed',
                'merge_fields': merge_fields,
                'tags': ['Pabau Clients']  # Tag to identify synced clients
            })
        
        print(f"✅ Prepared {len(members_batch)} members for batch upload")
        print("")
        
        # Upload in batches of 500 (Mailchimp limit)
        print("📤 Uploading clients to Mailchimp in batches...")
        print(f"   Batch size: 500 members per batch")
        print(f"   Total batches: {(len(members_batch) + 499) // 500}")
        print("")
        
        batch_size = 500
        success_count = 0
        error_count = 0
        start_time = datetime.now()
        
        for batch_num in range(0, len(members_batch), batch_size):
            batch = members_batch[batch_num:batch_num + batch_size]
            batch_index = batch_num // batch_size + 1
            
            try:
                print(f"  Batch {batch_index}: Uploading {len(batch)} members...")
                result = await mc.batch_subscribe(batch, update_existing=True)
                
                # Parse results
                batch_success = result.get('total_created', 0) + result.get('total_updated', 0)
                batch_errors = result.get('error_count', 0)
                
                success_count += batch_success
                error_count += batch_errors
                
                print(f"  Batch {batch_index}: ✅ {batch_success} success, ❌ {batch_errors} errors")
                
                # Show errors if any
                if batch_errors > 0 and 'errors' in result:
                    for err in result['errors'][:5]:  # Show first 5 errors with details
                        email = err.get('email_address', 'N/A')
                        error_msg = err.get('error', 'Unknown error')
                        field_errors = err.get('field_errors', {})
                        print(f"    ⚠️  {email}: {error_msg}")
                        if field_errors:
                            for field, field_error in field_errors.items():
                                print(f"        Field '{field}': {field_error}")
                
                # Clean up after each batch to prevent memory buildup
                import gc
                batch = None
                result = None
                gc.collect()
                
                # Small delay to prevent rate limiting
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"  Batch {batch_index}: ❌ Failed - {str(e)[:100]}")
                error_count += len(batch)
                
                # Clean up on error too
                import gc
                gc.collect()
                await asyncio.sleep(1)
        
        elapsed_total = (datetime.now() - start_time).total_seconds()
        
        print("")
        print("=" * 80)
        print("BULK UPLOAD COMPLETE!")
        print("=" * 80)
        print(f"✅ Successfully uploaded:  {success_count}")
        print(f"❌ Errors:                 {error_count}")
        print(f"⏱️  Time taken:             {elapsed_total / 60:.1f} minutes")
        print(f"📊 Upload rate:            {success_count / elapsed_total:.1f} clients/second")
        print("")
        print(f"Completed at: {datetime.now()}")
        print("")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(bulk_upload_clients())

