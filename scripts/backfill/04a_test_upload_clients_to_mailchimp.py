#!/usr/bin/env python3
"""
STEP 4A: TEST upload clients to Mailchimp (SMALL SAMPLE)

This script:
1. Queries database for 5 opted-in clients with their latest appointment
2. Prepares all 18 fields from sample-pabau.txt
3. Uploads to Mailchimp as subscribed members
4. Tests the complete flow before bulk upload

Fields to upload (18 total):
1. Appointment date, 2. Appointment Location, 3. Client Name, 4. Appt With,
5. Created by, 6. Created date, 7. Duration, 8. Service, 9. Appointment Date/Time,
10. Client ID, 11. Client System ID, 12. Appointment Status, 13. Cancellation reason,
14. Email, 15. Phone, 16. Client mobile, 17. Gender, 18. Phone Opt In
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


async def test_upload_clients():
    """Test upload 5 clients to Mailchimp"""
    
    print("=" * 80)
    print("TEST UPLOAD CLIENTS TO MAILCHIMP (2 SAMPLE)")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("")
    
    db = get_db()
    mc = MailchimpClient()
    
    try:
        # Query: Get 5 opted-in clients with their latest appointment
        print("📊 Querying 5 opted-in clients with latest appointments...")
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
                LIMIT 2
            """)
            clients = cursor.fetchall()
        
        if not clients:
            print("⚠️  No opted-in clients found!")
            return
        
        print(f"✅ Found {len(clients)} clients to upload")
        print("")
        
        # Display clients
        for i, client in enumerate(clients, 1):
            print(f"Client {i}:")
            print(f"  Name: {client['first_name']} {client['last_name']}")
            print(f"  Email: {client['email']}")
            print(f"  Opted in: {client['opt_in_email']}")
            print(f"  Latest appointment: {client['appointment_date'] or 'None'}")
            print("")
        
        # Upload each client to Mailchimp
        print("📤 Uploading clients to Mailchimp...")
        print("")
        
        success_count = 0
        error_count = 0
        
        for i, client in enumerate(clients, 1):
            try:
                # Prepare merge fields (all 18 fields)
                merge_fields = {
                    'APPTDATE': client['appointment_date'].strftime('%d/%m/%Y') if client['appointment_date'] else '',
                    'APPTLOC': client['appointment_location'] or '',
                    'CLIENTNAME': f"{client['first_name']} {client['last_name']}",
                    'APPTWITH': client['appt_with'] or '',
                    'CREATEDBY': client['created_by'] or '',
                    'CREATEDATE': client['created_date'].strftime('%d/%m/%Y %H:%M') if client['created_date'] else '',
                    'DURATION': str(client['duration']) if client['duration'] else '',
                    'SERVICE': client['service'] or '',
                    'APPTDTTM': client['appointment_datetime'].strftime('%d/%m/%Y %H:%M') if client['appointment_datetime'] else '',
                    'CLIENTID': client['client_id'] or '',
                    'SYSID': str(client['client_system_id']),
                    'APPTSTATUS': client['appointment_status'] or '',
                    'CANCELRSN': client['cancellation_reason'] or '',
                    'PHONE': client['phone'] or '',
                    'MOBILE': client['client_mobile'] or '',
                    'GENDER': client['gender'] or '',
                    'PHONEOPTIN': 'Yes' if client['phone_opt_in'] == 1 else 'No'
                }
                
                print(f"  {i}. Uploading {client['email']}...")
                
                # Upload to Mailchimp with "Pabau Clients" tag
                result = await mc.add_or_update_member(
                    email=client['email'],
                    first_name=client['first_name'],
                    last_name=client['last_name'],
                    phone=client['client_mobile'],
                    merge_fields=merge_fields,
                    status='subscribed',
                    tags=['Pabau Clients']
                )
                
                print(f"     ✅ Success! Mailchimp ID: {result.get('id', 'N/A')[:8]}...")
                success_count += 1
                
            except Exception as e:
                print(f"     ❌ Error: {str(e)[:100]}")
                error_count += 1
        
        print("")
        print("=" * 80)
        print("TEST UPLOAD COMPLETE!")
        print("=" * 80)
        print(f"✅ Successfully uploaded:  {success_count}")
        print(f"❌ Errors:                 {error_count}")
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
    asyncio.run(test_upload_clients())

