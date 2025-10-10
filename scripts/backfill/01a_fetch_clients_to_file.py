#!/usr/bin/env python3
"""
STEP 1: Fetch ALL clients from Pabau and save to pipe-delimited file
This script fetches data page by page and writes immediately to avoid memory issues
"""

import sys
import os
import asyncio
import csv
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.pabau_client import PabauClient
from utils.transforms import transform_client_for_db, transform_appointments_from_client


async def fetch_clients_to_file():
    """Fetch all clients from Pabau and write to pipe-delimited file"""
    
    output_file_clients = '/tmp/pabau_clients.psv'  # Pipe-separated values
    output_file_appointments = '/tmp/pabau_appointments.psv'  # Appointments file
    
    print("=" * 80)
    print("FETCH CLIENTS + APPOINTMENTS FROM PABAU → FILES")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print(f"Output file (clients): {output_file_clients}")
    print(f"Output file (appointments): {output_file_appointments}")
    print("")
    
    # Initialize
    pabau = PabauClient()
    
    # Define the columns we'll save (matching database schema)
    client_columns = [
        'pabau_id', 'custom_id', 'first_name', 'last_name', 'salutation', 'gender',
        'dob', 'location', 'is_active', 'email', 'phone', 'mobile',
        'opt_in_email', 'opt_in_sms', 'opt_in_phone', 'opt_in_post', 'opt_in_newsletter',
        'created_date', 'created_by_name', 'created_by_id'
    ]
    
    appointment_columns = [
        'client_pabau_id', 'pabau_appointment_id', 'appointment_date', 'appointment_time',
        'appointment_datetime', 'location', 'service', 'duration', 'appointment_status',
        'appt_with', 'created_by', 'created_date', 'cancellation_reason'
    ]
    
    try:
        # Open both files for writing
        with open(output_file_clients, 'w', newline='', encoding='utf-8') as f_clients, \
             open(output_file_appointments, 'w', newline='', encoding='utf-8') as f_appts:
            
            client_writer = csv.DictWriter(f_clients, fieldnames=client_columns, delimiter='|', 
                                          quoting=csv.QUOTE_MINIMAL, extrasaction='ignore')
            appt_writer = csv.DictWriter(f_appts, fieldnames=appointment_columns, delimiter='|', 
                                        quoting=csv.QUOTE_MINIMAL, extrasaction='ignore')
            
            # Write headers
            client_writer.writeheader()
            appt_writer.writeheader()
            
            # Paginate through all records
            page = 1
            total_clients_fetched = 0
            total_clients_written = 0
            total_appointments_written = 0
            
            print("📥 Fetching clients page by page...")
            print("")
            
            while True:
                # Fetch one page
                print(f"📄 Page {page}: Fetching...", end='', flush=True)
                response = await pabau.get_contacts(page=page, page_size=50)
                clients = response.get("clients", [])
                
                if not clients:
                    print(f" No data - pagination complete!")
                    break
                
                total_clients_fetched += len(clients)
                print(f" Got {len(clients)} clients", flush=True)
                
                # Transform and write each client + their appointments immediately
                for client_raw in clients:
                    try:
                        # Transform client to database format
                        client_data = transform_client_for_db(client_raw)
                        
                        # Write client to file
                        client_writer.writerow(client_data)
                        total_clients_written += 1
                        
                        # Extract and write appointments for this client
                        appointments = transform_appointments_from_client(client_raw)
                        for appt_data in appointments:
                            appt_writer.writerow(appt_data)
                            total_appointments_written += 1
                        
                    except Exception as e:
                        print(f"      ⚠️  Error transforming client: {e}")
                
                # Flush to disk every page
                f_clients.flush()
                f_appts.flush()
                
                # Show progress every 100 pages (5000 records)
                if page % 100 == 0:
                    print(f"   ✅ Checkpoint: {total_clients_written} clients, {total_appointments_written} appointments written")
                
                # Check if this is the last page
                if len(clients) < 50:
                    print(f"\n📄 Page {page} returned < 50 clients - this is the last page")
                    break
                
                page += 1
                
                # Clear references to help garbage collection
                del clients
                del response
        
        print("")
        print("=" * 80)
        print("FETCH COMPLETE!")
        print("=" * 80)
        print(f"✅ Clients fetched:      {total_clients_fetched}")
        print(f"✅ Clients written:      {total_clients_written}")
        print(f"✅ Appointments written: {total_appointments_written}")
        print(f"📄 Pages processed:      {page}")
        print(f"📁 Client file:          {output_file_clients}")
        print(f"📏 Client file size:     {os.path.getsize(output_file_clients) / 1024 / 1024:.2f} MB")
        print(f"📁 Appointment file:     {output_file_appointments}")
        print(f"📏 Appointment file size: {os.path.getsize(output_file_appointments) / 1024 / 1024:.2f} MB")
        print("")
        print(f"Completed at: {datetime.now()}")
        print("")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise


if __name__ == '__main__':
    try:
        asyncio.run(fetch_clients_to_file())
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

