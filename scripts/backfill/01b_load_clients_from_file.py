#!/usr/bin/env python3
"""
STEP 2: Load clients from pipe-delimited file into database
This script reads the file and batch inserts into PostgreSQL
"""

import sys
import os
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Load environment variables
load_dotenv()

from db.database import get_db


def load_clients_from_file():
    """Load clients and appointments from PSV files into database"""
    
    input_file_clients = '/tmp/pabau_clients.psv'
    input_file_appointments = '/tmp/pabau_appointments.psv'
    
    print("=" * 80)
    print("LOAD CLIENTS + APPOINTMENTS FROM FILES → DATABASE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print(f"Input file (clients): {input_file_clients}")
    print(f"Input file (appointments): {input_file_appointments}")
    print("")
    
    if not os.path.exists(input_file_clients):
        print(f"❌ Error: File not found: {input_file_clients}")
        print("   Please run 01a_fetch_clients_to_file.py first")
        return
    
    if not os.path.exists(input_file_appointments):
        print(f"⚠️  Warning: Appointment file not found: {input_file_appointments}")
        print("   Will only load clients without appointments")
        load_appointments = False
    else:
        load_appointments = True
    
    # Initialize database
    db = get_db()
    
    # Calculate cutoff date (7 days ago)
    cutoff_date = datetime.now() - timedelta(days=7)
    
    client_success_count = 0
    client_error_count = 0
    skipped_recent_count = 0
    opted_in_count = 0
    appointment_success_count = 0
    appointment_error_count = 0
    
    try:
        # STEP 1: Load clients
        print("📖 Reading client file and inserting into database...")
        print("")
        
        with open(input_file_clients, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='|')
            
            for i, row in enumerate(reader, 1):
                try:
                    # Skip clients created or updated in last 7 days (for testing incremental sync)
                    if row.get('created_date'):
                        try:
                            created_date = datetime.fromisoformat(row['created_date'].replace('Z', '+00:00'))
                            if created_date > cutoff_date:
                                skipped_recent_count += 1
                                if i % 1000 == 0:
                                    print(f"  Progress: {i} rows processed, {success_count} inserted, {skipped_recent_count} skipped (recent)", end='\r')
                                continue
                        except:
                            pass  # If date parsing fails, include the record
                    
                    # Skip if no email
                    if not row.get('email') or row['email'].strip() == '':
                        continue
                    
                    # Convert integer fields
                    for field in ['pabau_id', 'created_by_id', 'is_active', 
                                  'opt_in_email', 'opt_in_sms', 'opt_in_phone', 
                                  'opt_in_post', 'opt_in_newsletter']:
                        if row.get(field) and row[field].strip():
                            try:
                                row[field] = int(row[field])
                            except:
                                row[field] = None
                    
                    # Insert into database
                    db_id = db.upsert_client(row)
                    
                    # Log success
                    db.log_sync(
                        entity_type='client',
                        entity_id=db_id,
                        pabau_id=row.get('pabau_id'),
                        email=row.get('email'),
                        action='backfill_client',
                        status='success',
                        message=f"Client {row.get('first_name')} {row.get('last_name')} loaded from file"
                    )
                    
                    client_success_count += 1
                    if row.get('opt_in_email') == 1:
                        opted_in_count += 1
                    
                    # Progress - show every 1000 for large datasets
                    if i % 1000 == 0:
                        print(f"  Progress: {i} rows processed, {client_success_count} inserted, {skipped_recent_count} skipped (recent)", end='\r')
                
                except Exception as e:
                    client_error_count += 1
                    if client_error_count <= 10:  # Only print first 10 errors
                        print(f"\n      ❌ Error row {i}: {e}")
                    
                    db.log_sync(
                        entity_type='client',
                        entity_id=None,
                        pabau_id=row.get('pabau_id'),
                        email=row.get('email', ''),
                        action='backfill_client',
                        status='error',
                        error_details=str(e)
                    )
        
        print("\n")
        print("Clients loaded!")
        print("")
        
        # STEP 2: Load appointments (if file exists)
        if load_appointments:
            print("📖 Reading appointment file and inserting into database...")
            print("")
            
            with open(input_file_appointments, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='|')
                
                for i, row in enumerate(reader, 1):
                    try:
                        # Convert integer fields
                        if row.get('client_pabau_id') and row['client_pabau_id'].strip():
                            row['client_pabau_id'] = int(row['client_pabau_id'])
                        else:
                            continue  # Skip if no client_pabau_id
                        
                        if row.get('pabau_appointment_id') and row['pabau_appointment_id'].strip():
                            row['pabau_appointment_id'] = int(row['pabau_appointment_id'])
                        else:
                            row['pabau_appointment_id'] = None
                        
                        # Convert None strings to actual None
                        for field in row:
                            if row[field] == 'None' or row[field] == '':
                                row[field] = None
                        
                        # Insert appointment into database
                        try:
                            appt_id = db.upsert_appointment(row)
                            appointment_success_count += 1
                        except Exception as e:
                            # Skip constraint errors (usually duplicate appointments)
                            if 'unique' not in str(e).lower():
                                raise
                            appointment_success_count += 1  # Count as success since it's a duplicate
                        
                        # Progress - show every 1000
                        if i % 1000 == 0:
                            print(f"  Progress: {i} rows processed, {appointment_success_count} inserted", end='\r')
                    
                    except Exception as e:
                        appointment_error_count += 1
                        if appointment_error_count <= 10:  # Only print first 10 errors
                            print(f"\n      ⚠️  Error row {i}: {e}")
            
            print("\n")
            print("Appointments loaded!")
            print("")
        
        print("=" * 80)
        print("LOAD COMPLETE!")
        print("=" * 80)
        print(f"✅ Clients success:        {client_success_count}")
        print(f"⏭️  Clients skipped (7d):  {skipped_recent_count}")
        print(f"❌ Client errors:          {client_error_count}")
        print(f"📧 Opted in for email:     {opted_in_count}")
        if load_appointments:
            print(f"✅ Appointments success:   {appointment_success_count}")
            print(f"❌ Appointment errors:     {appointment_error_count}")
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
        load_clients_from_file()
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

