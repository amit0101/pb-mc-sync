#!/usr/bin/env python3
"""
STEP 2 (FAST): Bulk load clients from pipe-delimited file into database
Uses PostgreSQL COPY command for maximum speed
"""

import sys
import os
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Load environment variables
load_dotenv()


def bulk_load_clients():
    """Bulk load clients and appointments from PSV files into database using batch inserts"""
    
    input_file_clients = '/tmp/pabau_clients.psv'
    input_file_appointments = '/tmp/pabau_appointments.psv'
    
    print("=" * 80)
    print("BULK LOAD CLIENTS + APPOINTMENTS FROM FILES → DATABASE")
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
    
    # Get database URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Error: DATABASE_URL not set")
        return
    
    # Calculate cutoff date (7 days ago)
    cutoff_date = datetime.now() - timedelta(days=7)
    
    client_success_count = 0
    skipped_recent_count = 0
    client_error_count = 0
    appointment_success_count = 0
    appointment_error_count = 0
    
    try:
        # Connect to database
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # STEP 1: Load clients
        print("📖 Reading client file and preparing bulk insert...")
        print("")
        
        # Prepare data for bulk insert
        client_insert_data = []
        
        with open(input_file_clients, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='|')
            
            for i, row in enumerate(reader, 1):
                try:
                    # Skip clients created in last 7 days (for testing incremental sync)
                    if row.get('created_date'):
                        try:
                            created_date = datetime.fromisoformat(row['created_date'].replace('Z', '+00:00'))
                            if created_date > cutoff_date:
                                skipped_recent_count += 1
                                if i % 1000 == 0:
                                    print(f"  Progress: {i} rows processed, {len(client_insert_data)} to insert, {skipped_recent_count} skipped", end='\r')
                                continue
                        except:
                            pass  # If date parsing fails, include the record
                    
                    # Skip if no email
                    if not row.get('email') or row['email'].strip() == '':
                        continue
                    
                    # Convert integer fields (empty string to None)
                    def to_int(val):
                        if val and val.strip():
                            try:
                                return int(val)
                            except:
                                return None
                        return None
                    
                    # Prepare tuple for insert
                    client_insert_data.append((
                        to_int(row.get('pabau_id')),
                        row.get('custom_id') or None,
                        row.get('first_name') or None,
                        row.get('last_name') or None,
                        row.get('salutation') or None,
                        row.get('gender') or None,
                        row.get('dob') or None,
                        row.get('location') or None,
                        to_int(row.get('is_active')) or 1,
                        row.get('email'),
                        row.get('phone') or None,
                        row.get('mobile') or None,
                        to_int(row.get('opt_in_email')) or 0,
                        to_int(row.get('opt_in_sms')) or 0,
                        to_int(row.get('opt_in_phone')) or 0,
                        to_int(row.get('opt_in_post')) or 0,
                        to_int(row.get('opt_in_newsletter')) or 0,
                        row.get('created_date') or None,
                        row.get('created_by_name') or None,
                        to_int(row.get('created_by_id'))
                    ))
                    
                    if i % 1000 == 0:
                        print(f"  Progress: {i} rows processed, {len(client_insert_data)} to insert, {skipped_recent_count} skipped", end='\r')
                
                except Exception as e:
                    client_error_count += 1
                    if client_error_count <= 10:
                        print(f"\n      ⚠️  Error processing row {i}: {e}")
        
        print(f"\n\n📥 Bulk inserting {len(client_insert_data)} clients...")
        
        # Bulk insert using execute_values (much faster than individual inserts)
        insert_query = """
            INSERT INTO clients (
                pabau_id, custom_id, first_name, last_name, salutation, gender,
                dob, location, is_active, email, phone, mobile,
                opt_in_email, opt_in_sms, opt_in_phone, opt_in_post, opt_in_newsletter,
                created_date, created_by_name, created_by_id
            ) VALUES %s
            ON CONFLICT (pabau_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                opt_in_email = EXCLUDED.opt_in_email,
                opt_in_sms = EXCLUDED.opt_in_sms,
                opt_in_phone = EXCLUDED.opt_in_phone,
                updated_at = CURRENT_TIMESTAMP
        """
        
        # Insert in batches of 1000 for progress tracking
        batch_size = 1000
        for i in range(0, len(client_insert_data), batch_size):
            batch = client_insert_data[i:i+batch_size]
            execute_values(cursor, insert_query, batch)
            conn.commit()
            client_success_count += len(batch)
            print(f"  Inserted batch {i//batch_size + 1}: {client_success_count}/{len(client_insert_data)} records", end='\r')
        
        print("\n")
        print("Clients loaded!")
        print("")
        
        # STEP 2: Load appointments (if file exists)
        if load_appointments:
            print("📖 Reading appointment file and preparing bulk insert...")
            print("")
            
            appointment_insert_data = []
            
            with open(input_file_appointments, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='|')
                
                def to_int(val):
                    if val and val.strip() and val != 'None':
                        try:
                            return int(val)
                        except:
                            return None
                    return None
                
                def to_str(val):
                    if val and val != 'None':
                        return val
                    return None
                
                for i, row in enumerate(reader, 1):
                    try:
                        # Skip if no client_pabau_id
                        if not row.get('client_pabau_id') or not row['client_pabau_id'].strip():
                            continue
                        
                        # Prepare tuple for insert
                        appointment_insert_data.append((
                            to_int(row.get('client_pabau_id')),
                            to_int(row.get('pabau_appointment_id')),
                            to_str(row.get('appointment_date')),
                            to_str(row.get('appointment_time')),
                            to_str(row.get('appointment_datetime')),
                            to_str(row.get('location')),
                            to_str(row.get('service')),
                            to_str(row.get('duration')),
                            to_str(row.get('appointment_status')),
                            to_str(row.get('appt_with')),
                            to_str(row.get('created_by')),
                            to_str(row.get('created_date')),
                            to_str(row.get('cancellation_reason'))
                        ))
                        
                        if i % 1000 == 0:
                            print(f"  Progress: {i} rows processed, {len(appointment_insert_data)} to insert", end='\r')
                    
                    except Exception as e:
                        appointment_error_count += 1
                        if appointment_error_count <= 10:
                            print(f"\n      ⚠️  Error processing row {i}: {e}")
            
            # Deduplicate appointments based on unique key (client_pabau_id, appointment_datetime, service)
            # Keep last occurrence of each unique combination
            seen = {}
            unique_appointments = []
            duplicates_removed = 0
            
            for appt in appointment_insert_data:
                # Create unique key from client_pabau_id, appointment_datetime, service
                key = (appt[0], appt[4], appt[6])  # indices: client_pabau_id, appointment_datetime, service
                if key in seen:
                    duplicates_removed += 1
                seen[key] = appt
            
            unique_appointments = list(seen.values())
            
            if duplicates_removed > 0:
                print(f"  ⚠️  Removed {duplicates_removed} duplicate appointments (same client+datetime+service)")
            
            print(f"\n\n📥 Bulk inserting {len(unique_appointments)} unique appointments...")
            
            # Bulk insert appointments
            appointment_insert_query = """
                INSERT INTO appointments (
                    client_pabau_id, pabau_appointment_id,
                    appointment_date, appointment_time, appointment_datetime,
                    location, service, duration, appointment_status,
                    appt_with, created_by, created_date, cancellation_reason
                ) VALUES %s
                ON CONFLICT ON CONSTRAINT appointments_unique_key DO UPDATE SET
                    appointment_date = EXCLUDED.appointment_date,
                    service = EXCLUDED.service,
                    pabau_last_synced_at = CURRENT_TIMESTAMP
            """
            
            # Insert in batches of 1000
            for i in range(0, len(unique_appointments), batch_size):
                batch = unique_appointments[i:i+batch_size]
                execute_values(cursor, appointment_insert_query, batch)
                conn.commit()
                appointment_success_count += len(batch)
                print(f"  Inserted batch {i//batch_size + 1}: {appointment_success_count}/{len(unique_appointments)} records", end='\r')
            
            print("\n")
            print("Appointments loaded!")
            print("")
        
        print("=" * 80)
        print("BULK LOAD COMPLETE!")
        print("=" * 80)
        print(f"✅ Clients inserted:       {client_success_count}")
        print(f"⏭️  Clients skipped (7d):  {skipped_recent_count}")
        print(f"❌ Client errors:          {client_error_count}")
        if load_appointments:
            print(f"✅ Appointments inserted:  {appointment_success_count}")
            print(f"❌ Appointment errors:     {appointment_error_count}")
        print("")
        print("⚠️  NOTE: Last 7 days excluded - use sync script to catch up")
        print("")
        
        # Show summary
        cursor.execute("SELECT COUNT(*) FROM clients")
        total_clients = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM clients WHERE opt_in_email = 1")
        opted_in = cursor.fetchone()[0]
        
        print("Database Summary:")
        print(f"  Clients: {total_clients} total, {opted_in} opted in")
        
        if load_appointments:
            cursor.execute("SELECT COUNT(*) FROM appointments")
            total_appointments = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT client_pabau_id) FROM appointments")
            clients_with_appointments = cursor.fetchone()[0]
            print(f"  Appointments: {total_appointments} total, {clients_with_appointments} unique clients")
        
        print("")
        print(f"Completed at: {datetime.now()}")
        print("")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == '__main__':
    try:
        bulk_load_clients()
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

