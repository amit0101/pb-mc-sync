#!/usr/bin/env python3
"""
STEP 2A: Fetch FULL appointment details from Pabau /appointments endpoint TO FILE
This enriches the basic appointment data from /clients with full details:
- Appointment Location, Duration, Status
- Appt With (practitioner name)
- Created by, Created date
- Cancellation reason

This matches ALL fields from sample-pabau.txt
"""

import sys
import os
import asyncio
import csv
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Load environment variables
load_dotenv()

from clients.pabau_client import PabauClient
from db.database import get_db


def parse_pabau_datetime(date_str: str) -> str:
    """Parse Pabau datetime format to ISO"""
    if not date_str:
        return None
    try:
        # Pabau format: "2025-09-13 09:24:19"
        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        return dt.isoformat()
    except:
        return None


async def fetch_appointments_for_client(pabau: PabauClient, client_pabau_id: int):
    """
    Fetch all appointments for a specific client
    
    Returns:
        List of appointment dicts with full details
    """
    try:
        # Call /appointments endpoint with customer_id filter
        response = await pabau._request(
            "GET",
            f"appointments",
            params={"customer_id": client_pabau_id}
        )
        
        appointments_raw = response.get("appointments", [])
        transformed_appointments = []
        
        for appt_raw in appointments_raw:
            details = appt_raw.get('details', {})
            dates = appt_raw.get('dates', {})
            client_info = appt_raw.get('client', [{}])[0] if appt_raw.get('client') else {}
            service_info = appt_raw.get('service', [{}])[0] if appt_raw.get('service') else {}
            
            # Extract practitioner info
            practitioner = details.get('practitioner', {})
            practitioner_name = practitioner.get('practitioner_name')
            
            # Extract created by info
            created_by = details.get('created_by', {})
            created_by_name = created_by.get('name')
            
            # Extract location
            location = details.get('location', {})
            location_name = location.get('name')
            
            # Combine date and time for appointment_datetime
            start_date = dates.get('start_date')  # "2026-03-28"
            start_time = dates.get('start_time')  # "11:00:00"
            appointment_datetime = None
            if start_date and start_time:
                try:
                    dt = datetime.strptime(f"{start_date} {start_time}", '%Y-%m-%d %H:%M:%S')
                    appointment_datetime = dt.isoformat()
                except:
                    pass
            
            transformed_appointments.append({
                'client_pabau_id': client_pabau_id,
                'pabau_appointment_id': details.get('appointment_id'),
                'appointment_date': start_date,
                'appointment_time': start_time,
                'appointment_datetime': appointment_datetime,
                'location': location_name,
                'service': service_info.get('service'),
                'duration': dates.get('duration'),  # In minutes
                'appointment_status': details.get('appointment_status'),
                'appt_with': practitioner_name,
                'created_by': created_by_name,
                'created_date': parse_pabau_datetime(details.get('create_date')),
                'cancellation_reason': details.get('notes') if 'cancel' in details.get('notes', '').lower() else None,
            })
        
        return transformed_appointments
    
    except Exception as e:
        print(f"      ⚠️  Error fetching appointments for client {client_pabau_id}: {e}")
        return []


async def fetch_all_appointments_to_file():
    """Fetch full appointment details for all clients with appointments and save to file"""
    
    print("=" * 80)
    print("FETCH FULL APPOINTMENT DETAILS FROM PABAU → FILE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("")
    
    # Output file
    output_file = "/tmp/pabau_full_appointments.psv"
    
    # Initialize
    pabau = PabauClient()
    db = get_db()
    
    try:
        # Get all clients that have appointments (from our basic appointments table)
        print("📊 Finding clients with appointments in database...")
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT client_pabau_id 
                FROM appointments 
                ORDER BY client_pabau_id
            """)
            client_ids = [row['client_pabau_id'] for row in cursor.fetchall()]
        
        print(f"   Found {len(client_ids)} clients with appointments")
        print(f"   Will fetch full details for their appointments")
        print("")
        
        if not client_ids:
            print("⚠️  No clients with appointments found in database.")
            print("   Please run 01a_fetch_clients_to_file.py and 01c_bulk_load_clients.py first")
            return
        
        print("📥 Fetching full appointment details from Pabau API...")
        print(f"   This will make {len(client_ids)} API calls (1 per client)")
        print(f"   Estimated time: ~{len(client_ids) * 3 / 60:.1f} minutes")
        print("")
        
        # Open file for writing
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'client_pabau_id', 'pabau_appointment_id',
                'appointment_date', 'appointment_time', 'appointment_datetime',
                'location', 'service', 'duration', 'appointment_status',
                'appt_with', 'created_by', 'created_date', 'cancellation_reason'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|')
            writer.writeheader()
            
            total_clients = len(client_ids)
            total_appointments_fetched = 0
            total_appointments_written = 0
            error_count = 0
            
            for i, client_pabau_id in enumerate(client_ids, 1):
                try:
                    # Fetch appointments for this client
                    appointments = await fetch_appointments_for_client(pabau, client_pabau_id)
                    
                    if appointments:
                        total_appointments_fetched += len(appointments)
                        
                        # Write each appointment to file
                        for appt in appointments:
                            writer.writerow(appt)
                            total_appointments_written += 1
                    
                    # Progress - show every 10 clients
                    if i % 10 == 0:
                        print(f"  Progress: {i}/{total_clients} clients ({total_appointments_written} appointments written)", end='\r')
                
                except Exception as e:
                    error_count += 1
                    if error_count <= 10:
                        print(f"\n      ⚠️  Error processing client {client_pabau_id}: {e}")
                
                # Small delay to avoid rate limiting
                if i % 50 == 0:
                    await asyncio.sleep(1)
        
        print(f"\n  Progress: {total_clients}/{total_clients} clients ({total_appointments_written} appointments written)")
        print("")
        
        # Get file size
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        
        print("=" * 80)
        print("FETCH COMPLETE!")
        print("=" * 80)
        print(f"✅ Clients processed:        {total_clients}")
        print(f"✅ Appointments fetched:     {total_appointments_fetched}")
        print(f"✅ Appointments written:     {total_appointments_written}")
        print(f"❌ Errors:                   {error_count}")
        print(f"📁 Output file:              {output_file}")
        print(f"📏 File size:                {file_size:.2f} MB")
        print(f"Completed at:                {datetime.now()}")
        print("")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(fetch_all_appointments_to_file())

