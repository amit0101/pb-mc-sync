#!/usr/bin/env python3
"""
Fetch ALL leads from Pabau and save to pipe-delimited file
Leads are prospects in the sales pipeline (not yet clients)
"""

import sys
import os
import asyncio
import csv
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.pabau_client import PabauClient
from utils.transforms import transform_lead_for_db


async def fetch_leads_to_file():
    """Fetch all leads from Pabau and write to pipe-delimited file"""
    
    output_file_leads = '/tmp/pabau_leads.psv'  # Pipe-separated values
    
    print("=" * 80)
    print("FETCH LEADS FROM PABAU → FILE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print(f"Output file: {output_file_leads}")
    print("")
    
    # Initialize
    pabau = PabauClient()
    
    # Define the columns we'll save (matching database schema)
    lead_columns = [
        'pabau_id', 'contact_id', 'email', 'salutation', 'first_name', 'last_name',
        'phone', 'mobile', 'dob',
        'mailing_street', 'mailing_postal', 'mailing_city', 'mailing_county', 'mailing_country',
        'is_active', 'lead_status',
        'owner_id', 'owner_name', 'location_id', 'location_name',
        'created_date', 'updated_date', 'converted_date',
        'pipeline_name', 'pipeline_stage_id', 'pipeline_stage_name',
        'deal_value', 'opt_in_email_mailchimp'
    ]
    
    try:
        # Open file for writing
        with open(output_file_leads, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=lead_columns, delimiter='|', 
                                   quoting=csv.QUOTE_MINIMAL, extrasaction='ignore')
            
            # Write header
            writer.writeheader()
            
            # Paginate through all records
            page = 1
            total_fetched = 0
            total_written = 0
            
            print("📥 Fetching leads page by page...")
            print("")
            
            while True:
                # Fetch one page
                print(f"📄 Page {page}: Fetching...", end='', flush=True)
                response = await pabau.get_leads(page=page, page_size=50)
                leads = response.get("leads", [])
                
                if not leads:
                    print(f" No data - pagination complete!")
                    break
                
                total_fetched += len(leads)
                print(f" Got {len(leads)} leads", flush=True)
                
                # Transform and write each lead immediately
                for lead_raw in leads:
                    try:
                        # Transform to database format
                        lead_data = transform_lead_for_db(lead_raw)
                        
                        # Write to file
                        writer.writerow(lead_data)
                        total_written += 1
                        
                    except Exception as e:
                        print(f"      ⚠️  Error transforming lead: {e}")
                
                # Flush to disk every page
                f.flush()
                
                # Show progress every 100 pages (5000 records)
                if page % 100 == 0:
                    print(f"   ✅ Checkpoint: {total_written} leads written to file")
                
                # Check if this is the last page
                if len(leads) < 50:
                    print(f"\n📄 Page {page} returned < 50 leads - this is the last page")
                    break
                
                page += 1
                
                # Clear references to help garbage collection
                del leads
                del response
        
        print("")
        print("=" * 80)
        print("FETCH COMPLETE!")
        print("=" * 80)
        print(f"✅ Leads fetched:  {total_fetched}")
        print(f"✅ Leads written:  {total_written}")
        print(f"📄 Pages processed: {page}")
        print(f"📁 Output file:     {output_file_leads}")
        print(f"📏 File size:       {os.path.getsize(output_file_leads) / 1024 / 1024:.2f} MB")
        print("")
        print(f"Completed at: {datetime.now()}")
        print("")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise


if __name__ == '__main__':
    try:
        asyncio.run(fetch_leads_to_file())
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

