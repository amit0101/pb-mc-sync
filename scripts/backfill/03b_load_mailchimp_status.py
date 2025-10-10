#!/usr/bin/env python3
"""
STEP 3B: Load Mailchimp UNSUBSCRIBED list from file to database

This script:
1. Reads ONLY unsubscribed members from PSV file (email + opt_in_email_mailchimp = 0)
2. Updates clients table: sets opt_in_email_mailchimp = 0 for these emails
3. Updates leads table: sets opt_in_email_mailchimp = 0 for these emails

Important: Only unsubscribed users are updated. All others keep their Pabau opt-in status.
"""

import sys
import os
import csv
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Load environment variables
load_dotenv()

from db.database import get_db


def load_mailchimp_status():
    """Load Mailchimp status from file into database"""
    
    print("=" * 80)
    print("LOAD MAILCHIMP UNSUBSCRIBED LIST FROM FILE → DATABASE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    
    input_file = "/tmp/mailchimp_unsubscribed.psv"
    print(f"Input file: {input_file}")
    print("")
    
    db = get_db()
    
    try:
        print("📖 Reading unsubscribed members file...")
        print("")
        
        # Read unsubscribed members from file
        unsubscribed_emails = []
        
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                unsubscribed_emails.append(row['email'].lower())
                
                if len(unsubscribed_emails) % 100 == 0:
                    print(f"  Progress: {len(unsubscribed_emails)} unsubscribed emails read", end='\r')
        
        print(f"  Progress: {len(unsubscribed_emails)} unsubscribed emails read")
        print("")
        
        if not unsubscribed_emails:
            print("🎉 No unsubscribed members! All contacts remain opted in.")
            print("")
            return
        
        print(f"⚠️  Found {len(unsubscribed_emails)} unsubscribed emails")
        print(f"   These will be set to opt_in_email_mailchimp = 0")
        print("")
        
        # Update clients table - set opt_in_email = 0 for unsubscribed emails
        print("📥 Updating CLIENTS table: setting opt_in_email = 0...")
        print("")
        
        clients_updated = 0
        clients_not_found = 0
        clients_errors = 0
        
        with db.get_cursor() as cursor:
            for i, email in enumerate(unsubscribed_emails, 1):
                try:
                    cursor.execute("""
                        UPDATE clients 
                        SET 
                            opt_in_email = 0,
                            mailchimp_last_synced_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE LOWER(email) = %s
                    """, (email,))
                    
                    if cursor.rowcount > 0:
                        clients_updated += 1
                    else:
                        clients_not_found += 1
                    
                    if i % 10 == 0:
                        print(f"  Progress: {i}/{len(unsubscribed_emails)} emails ({clients_updated} updated)", end='\r')
                
                except Exception as e:
                    clients_errors += 1
                    if clients_errors <= 10:
                        print(f"\n  ⚠️  Error updating client {email}: {e}")
        
        print(f"\n  Progress: {len(unsubscribed_emails)}/{len(unsubscribed_emails)} emails ({clients_updated} updated)")
        print("")
        
        # Update leads table - set opt_in_email_mailchimp = 0 for unsubscribed emails
        print("📥 Updating LEADS table: setting opt_in_email_mailchimp = 0...")
        print("")
        
        leads_updated = 0
        leads_not_found = 0
        leads_errors = 0
        
        with db.get_cursor() as cursor:
            for i, email in enumerate(unsubscribed_emails, 1):
                try:
                    cursor.execute("""
                        UPDATE leads 
                        SET 
                            opt_in_email_mailchimp = 0,
                            mailchimp_last_synced_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE LOWER(email) = %s
                    """, (email,))
                    
                    if cursor.rowcount > 0:
                        leads_updated += 1
                    else:
                        leads_not_found += 1
                    
                    if i % 10 == 0:
                        print(f"  Progress: {i}/{len(unsubscribed_emails)} emails ({leads_updated} updated)", end='\r')
                
                except Exception as e:
                    leads_errors += 1
                    if leads_errors <= 10:
                        print(f"\n  ⚠️  Error updating lead {email}: {e}")
        
        print(f"\n  Progress: {len(unsubscribed_emails)}/{len(unsubscribed_emails)} emails ({leads_updated} updated)")
        print("")
        
        # Summary statistics
        with db.get_cursor() as cursor:
            # Clients stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN opt_in_email = 1 THEN 1 END) as opted_in,
                    COUNT(CASE WHEN opt_in_email = 0 THEN 1 END) as opted_out
                FROM clients
            """)
            clients_stats = cursor.fetchone()
            
            # Leads stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN opt_in_email_mailchimp = 1 THEN 1 END) as opted_in,
                    COUNT(CASE WHEN opt_in_email_mailchimp = 0 THEN 1 END) as opted_out,
                    COUNT(CASE WHEN opt_in_email = 1 THEN 1 END) as opt_in_email_yes
                FROM leads
            """)
            leads_stats = cursor.fetchone()
        
        print("=" * 80)
        print("LOAD COMPLETE!")
        print("=" * 80)
        print("")
        print("CLIENTS:")
        print(f"  ✅ Updated:              {clients_updated}")
        print(f"  ⚠️  Not found:            {clients_not_found}")
        print(f"  ❌ Errors:               {clients_errors}")
        print("")
        print("  Opt-in Status in Database:")
        print(f"    Total clients:         {clients_stats['total']}")
        print(f"    opt_in_email = 1:      {clients_stats['opted_in']}")
        print(f"    opt_in_email = 0:      {clients_stats['opted_out']}")
        print("")
        print("LEADS:")
        print(f"  ✅ Updated:              {leads_updated}")
        print(f"  ⚠️  Not found:            {leads_not_found}")
        print(f"  ❌ Errors:               {leads_errors}")
        print("")
        print("  Opt-in Status in Database:")
        print(f"    Total leads:           {leads_stats['total']}")
        print(f"    opt_in_email_mailchimp = 1:  {leads_stats['opted_in']}")
        print(f"    opt_in_email_mailchimp = 0:  {leads_stats['opted_out']}")
        print(f"    opt_in_email = 1:      {leads_stats['opt_in_email_yes']} (auto-synced)")
        print("")
        print(f"Completed at: {datetime.now()}")
        print("")
        
    except FileNotFoundError:
        print(f"❌ Error: File not found: {input_file}")
        print("   Please run 03a_fetch_mailchimp_status_to_file.py first")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    load_mailchimp_status()

