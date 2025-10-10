#!/usr/bin/env python3
"""
STEP 2 (FAST): Bulk load leads from pipe-delimited file into database
Uses PostgreSQL execute_values for maximum speed
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


def bulk_load_leads():
    """Bulk load leads from PSV file into database using batch inserts"""
    
    input_file_leads = '/tmp/pabau_leads.psv'
    
    print("=" * 80)
    print("BULK LOAD LEADS FROM FILE → DATABASE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print(f"Input file: {input_file_leads}")
    print("")
    
    if not os.path.exists(input_file_leads):
        print(f"❌ Error: File not found: {input_file_leads}")
        print("   Please run 02a_fetch_leads_to_file.py first")
        return
    
    # Get database URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Error: DATABASE_URL not set")
        return
    
    # Calculate cutoff date (7 days ago)
    cutoff_date = datetime.now() - timedelta(days=7)
    
    lead_success_count = 0
    skipped_recent_count = 0
    lead_error_count = 0
    
    try:
        # Connect to database
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print("📖 Reading leads file and preparing bulk insert...")
        print("")
        
        # Prepare data for bulk insert
        lead_insert_data = []
        
        with open(input_file_leads, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='|')
            
            for i, row in enumerate(reader, 1):
                try:
                    # Skip leads created in last 7 days (for testing incremental sync)
                    if row.get('created_date'):
                        try:
                            created_date = datetime.fromisoformat(row['created_date'].replace('Z', '+00:00'))
                            if created_date > cutoff_date:
                                skipped_recent_count += 1
                                if i % 1000 == 0:
                                    print(f"  Progress: {i} rows processed, {len(lead_insert_data)} to insert, {skipped_recent_count} skipped", end='\r')
                                continue
                        except:
                            pass  # If date parsing fails, include the record
                    
                    # Skip if no email
                    if not row.get('email') or row['email'].strip() == '':
                        continue
                    
                    # Convert integer fields (empty string to None)
                    def to_int(val):
                        if val and val.strip() and val != 'None':
                            try:
                                return int(val)
                            except:
                                return None
                        return None
                    
                    def to_float(val):
                        if val and val.strip() and val != 'None':
                            try:
                                return float(val)
                            except:
                                return None
                        return None
                    
                    def to_str(val):
                        if val and val != 'None' and val.strip():
                            return val
                        return None
                    
                    # Prepare tuple for insert
                    lead_insert_data.append((
                        to_int(row.get('pabau_id')),
                        to_int(row.get('contact_id')),
                        row.get('email'),
                        to_str(row.get('salutation')),
                        to_str(row.get('first_name')),
                        to_str(row.get('last_name')),
                        to_str(row.get('phone')),
                        to_str(row.get('mobile')),
                        to_str(row.get('dob')),
                        to_str(row.get('mailing_street')),
                        to_str(row.get('mailing_postal')),
                        to_str(row.get('mailing_city')),
                        to_str(row.get('mailing_county')),
                        to_str(row.get('mailing_country')),
                        to_int(row.get('is_active')) or 1,
                        to_str(row.get('lead_status')),
                        to_int(row.get('owner_id')),
                        to_str(row.get('owner_name')),
                        to_int(row.get('location_id')),
                        to_str(row.get('location_name')),
                        to_str(row.get('created_date')),
                        to_str(row.get('updated_date')),
                        to_str(row.get('converted_date')),
                        to_str(row.get('pipeline_name')),
                        to_int(row.get('pipeline_stage_id')),
                        to_str(row.get('pipeline_stage_name')),
                        to_float(row.get('deal_value')),
                        to_int(row.get('opt_in_email_mailchimp')) or 0
                    ))
                    
                    if i % 1000 == 0:
                        print(f"  Progress: {i} rows processed, {len(lead_insert_data)} to insert, {skipped_recent_count} skipped", end='\r')
                
                except Exception as e:
                    lead_error_count += 1
                    if lead_error_count <= 10:
                        print(f"\n      ⚠️  Error processing row {i}: {e}")
        
        print(f"\n\n📥 Bulk inserting {len(lead_insert_data)} leads...")
        
        # Bulk insert using execute_values (much faster than individual inserts)
        insert_query = """
            INSERT INTO leads (
                pabau_id, contact_id, email, salutation, first_name, last_name,
                phone, mobile, dob,
                mailing_street, mailing_postal, mailing_city, mailing_county, mailing_country,
                is_active, lead_status,
                owner_id, owner_name, location_id, location_name,
                created_date, updated_date, converted_date,
                pipeline_name, pipeline_stage_id, pipeline_stage_name,
                deal_value, opt_in_email_mailchimp
            ) VALUES %s
            ON CONFLICT (pabau_id) DO UPDATE SET
                contact_id = EXCLUDED.contact_id,
                email = EXCLUDED.email,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                phone = EXCLUDED.phone,
                mobile = EXCLUDED.mobile,
                is_active = EXCLUDED.is_active,
                lead_status = EXCLUDED.lead_status,
                updated_date = EXCLUDED.updated_date,
                converted_date = EXCLUDED.converted_date,
                pipeline_stage_name = EXCLUDED.pipeline_stage_name,
                opt_in_email_mailchimp = EXCLUDED.opt_in_email_mailchimp
        """
        
        # Insert in batches of 1000 for progress tracking
        batch_size = 1000
        for i in range(0, len(lead_insert_data), batch_size):
            batch = lead_insert_data[i:i+batch_size]
            execute_values(cursor, insert_query, batch)
            conn.commit()
            lead_success_count += len(batch)
            print(f"  Inserted batch {i//batch_size + 1}: {lead_success_count}/{len(lead_insert_data)} records", end='\r')
        
        print("\n")
        print("=" * 80)
        print("BULK LOAD COMPLETE!")
        print("=" * 80)
        print(f"✅ Leads inserted:         {lead_success_count}")
        print(f"⏭️  Leads skipped (7d):    {skipped_recent_count}")
        print(f"❌ Lead errors:            {lead_error_count}")
        print("")
        print("⚠️  NOTE: Last 7 days excluded - use sync script to catch up")
        print("")
        
        # Show summary
        cursor.execute("SELECT COUNT(*) FROM leads")
        total_leads = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE lead_status = 'Open'")
        open_leads = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE lead_status = 'Won'")
        won_leads = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE contact_id IS NOT NULL")
        converted_leads = cursor.fetchone()[0]
        
        print("Database Summary:")
        print(f"  Leads: {total_leads} total")
        print(f"    - Open: {open_leads}")
        print(f"    - Won: {won_leads}")
        print(f"    - Converted to clients: {converted_leads}")
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
        bulk_load_leads()
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

