#!/usr/bin/env python3
"""
STEP 2B: Bulk load FULL appointment details from file to database
This updates the appointments table with complete details from the /appointments endpoint
"""

import sys
import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Load environment variables
load_dotenv()

from db.database import get_db


def bulk_load_full_appointments():
    """Load full appointment details from PSV file into database"""
    
    print("=" * 80)
    print("BULK LOAD FULL APPOINTMENT DETAILS FROM FILE → DATABASE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    
    input_file = "/tmp/pabau_full_appointments.psv"
    print(f"Input file: {input_file}")
    print("")
    
    db = get_db()
    
    try:
        print("📖 Reading appointment file and preparing bulk insert...")
        print("")
        
        # Read all appointments from file
        appointment_data = []
        
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='|')
            
            for row in reader:
                # Convert empty strings to None
                for key, value in row.items():
                    if value == '':
                        row[key] = None
                
                # Convert numeric fields
                if row.get('duration'):
                    try:
                        row['duration'] = int(row['duration'])
                    except:
                        row['duration'] = None
                
                if row.get('pabau_appointment_id'):
                    try:
                        row['pabau_appointment_id'] = int(row['pabau_appointment_id'])
                    except:
                        row['pabau_appointment_id'] = None
                
                if row.get('client_pabau_id'):
                    try:
                        row['client_pabau_id'] = int(row['client_pabau_id'])
                    except:
                        row['client_pabau_id'] = None
                
                appointment_data.append(row)
                
                # Progress indicator
                if len(appointment_data) % 100 == 0:
                    print(f"  Progress: {len(appointment_data)} rows processed", end='\r')
        
        print(f"  Progress: {len(appointment_data)} rows processed")
        print("")
        
        if not appointment_data:
            print("⚠️  No appointments found in file!")
            return
        
        # Prepare data for bulk insert
        insert_data = []
        for appt in appointment_data:
            insert_data.append((
                appt['client_pabau_id'],
                appt['pabau_appointment_id'],
                appt['appointment_date'],
                appt['appointment_time'],
                appt['appointment_datetime'],
                appt['location'],
                appt['service'],
                appt['duration'],
                appt['appointment_status'],
                appt['appt_with'],
                appt['created_by'],
                appt['created_date'],
                appt['cancellation_reason']
            ))
        
        # Deduplicate based on unique key (client_pabau_id, appointment_datetime, service)
        seen = {}
        unique_data = []
        duplicates_removed = 0
        
        for data in insert_data:
            # indices: 0=client_pabau_id, 4=appointment_datetime, 6=service
            key = (data[0], data[4], data[6])
            if key in seen:
                duplicates_removed += 1
            seen[key] = data
        
        unique_data = list(seen.values())
        
        if duplicates_removed > 0:
            print(f"  ⚠️  Removed {duplicates_removed} duplicate appointments (same client+datetime+service)")
        
        print(f"\n📥 Bulk inserting {len(unique_data)} unique appointments with full details...")
        print("")
        
        # Bulk insert using execute_values
        insert_query = """
            INSERT INTO appointments (
                client_pabau_id, pabau_appointment_id,
                appointment_date, appointment_time, appointment_datetime,
                location, service, duration, appointment_status,
                appt_with, created_by, created_date, cancellation_reason,
                pabau_last_synced_at
            ) VALUES %s
            ON CONFLICT ON CONSTRAINT appointments_unique_key DO UPDATE SET
                pabau_appointment_id = EXCLUDED.pabau_appointment_id,
                location = EXCLUDED.location,
                duration = EXCLUDED.duration,
                appointment_status = EXCLUDED.appointment_status,
                appt_with = EXCLUDED.appt_with,
                created_by = EXCLUDED.created_by,
                created_date = EXCLUDED.created_date,
                cancellation_reason = EXCLUDED.cancellation_reason,
                pabau_last_synced_at = CURRENT_TIMESTAMP
        """
        
        with db.get_cursor() as cursor:
            # Process in batches of 1000
            batch_size = 1000
            inserted = 0
            errors = 0
            
            for i in range(0, len(unique_data), batch_size):
                batch = unique_data[i:i + batch_size]
                try:
                    execute_values(
                        cursor,
                        insert_query,
                        batch,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                        page_size=batch_size
                    )
                    inserted += len(batch)
                    print(f"  Inserted batch {(i // batch_size) + 1}: {inserted}/{len(unique_data)} records", end='\r')
                except Exception as e:
                    errors += len(batch)
                    print(f"\n  ⚠️  Error inserting batch {(i // batch_size) + 1}: {e}")
            
            print(f"\n  Inserted batch {(len(unique_data) // batch_size) + 1}: {inserted}/{len(unique_data)} records")
        
        print("")
        print("Appointments loaded!")
        print("")
        
        # Get summary statistics
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(location) as has_location,
                    COUNT(duration) as has_duration,
                    COUNT(appointment_status) as has_status,
                    COUNT(appt_with) as has_appt_with,
                    COUNT(created_by) as has_created_by,
                    COUNT(pabau_appointment_id) as has_appt_id,
                    COUNT(DISTINCT client_pabau_id) as unique_clients
                FROM appointments
            """)
            stats = cursor.fetchone()
        
        print("=" * 80)
        print("BULK LOAD COMPLETE!")
        print("=" * 80)
        print(f"✅ Appointments updated:       {inserted}")
        print(f"❌ Appointment errors:         {errors}")
        print("")
        print("Database Summary:")
        print(f"  Appointments: {stats['total']} total")
        print(f"  Unique clients: {stats['unique_clients']}")
        print("")
        print("Full Details Coverage:")
        print(f"  Have pabau_appointment_id: {stats['has_appt_id']}/{stats['total']} ({stats['has_appt_id']*100//stats['total']}%)")
        print(f"  Have location: {stats['has_location']}/{stats['total']} ({stats['has_location']*100//stats['total']}%)")
        print(f"  Have duration: {stats['has_duration']}/{stats['total']} ({stats['has_duration']*100//stats['total']}%)")
        print(f"  Have status: {stats['has_status']}/{stats['total']} ({stats['has_status']*100//stats['total']}%)")
        print(f"  Have appt_with: {stats['has_appt_with']}/{stats['total']} ({stats['has_appt_with']*100//stats['total']}%)")
        print(f"  Have created_by: {stats['has_created_by']}/{stats['total']} ({stats['has_created_by']*100//stats['total']}%)")
        print("")
        print(f"Completed at: {datetime.now()}")
        print("")
        
    except FileNotFoundError:
        print(f"❌ Error: File not found: {input_file}")
        print("   Please run 02a_fetch_full_appointments_to_file.py first")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    bulk_load_full_appointments()

