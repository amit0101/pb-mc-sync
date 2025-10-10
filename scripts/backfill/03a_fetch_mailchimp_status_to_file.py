#!/usr/bin/env python3
"""
STEP 3A: Fetch Mailchimp UNSUBSCRIBED members and save to file

This script:
1. Fetches ONLY unsubscribed members from Mailchimp audience
2. Extracts: email (for matching)
3. Sets opt_in_email_mailchimp = 0 for these emails
4. Saves to PSV file for bulk loading

This data will be used to update opt_in_email_mailchimp = 0 for unsubscribed users in clients and leads tables.
All other contacts remain opted in (default = 1 from Pabau data).
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

from clients.mailchimp_client import MailchimpClient


async def fetch_mailchimp_unsubscribed_to_file():
    """Fetch ONLY unsubscribed Mailchimp members and save to file"""
    
    print("=" * 80)
    print("FETCH MAILCHIMP UNSUBSCRIBED MEMBERS → FILE")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("")
    
    # Output file
    output_file = "/tmp/mailchimp_unsubscribed.psv"
    
    # Initialize Mailchimp client
    mc = MailchimpClient()
    
    try:
        print("📥 Fetching ONLY unsubscribed members from Mailchimp...")
        print(f"   List ID: {mc.list_id}")
        print(f"   Filter: status=unsubscribed")
        print("")
        
        # Fetch ONLY unsubscribed members
        members = await mc.get_all_members(status='unsubscribed')
        
        print(f"✅ Fetched {len(members)} unsubscribed members from Mailchimp")
        print("")
        
        if len(members) == 0:
            print("🎉 No unsubscribed members found! All contacts are opted in.")
            # Create empty file anyway for consistency
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['email', 'opt_in_email_mailchimp']
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|')
                writer.writeheader()
            print("")
            print(f"📁 Created empty file: {output_file}")
            print("")
            return
        
        # Write to file - all these emails get opt_in_email_mailchimp = 0
        print(f"📝 Writing {len(members)} unsubscribed emails to file...")
        print(f"   All will be set to opt_in_email_mailchimp = 0")
        print("")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['email', 'opt_in_email_mailchimp']
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|')
            writer.writeheader()
            
            for member in members:
                email = member.get('email_address', '').lower()
                
                # All these members are unsubscribed, so opt_in_email_mailchimp = 0
                writer.writerow({
                    'email': email,
                    'opt_in_email_mailchimp': 0
                })
        
        # Get file size
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        
        print("")
        print("=" * 80)
        print("FETCH COMPLETE!")
        print("=" * 80)
        print(f"✅ Unsubscribed members fetched:  {len(members)}")
        print(f"📁 Output file:                   {output_file}")
        print(f"📏 File size:                     {file_size:.2f} MB")
        print("")
        print(f"⚠️  These {len(members)} emails will be set to opt_in_email_mailchimp = 0")
        print(f"   All other contacts remain opted in (opt_in_email_mailchimp = 1 from Pabau)")
        print("")
        print(f"Completed at:                     {datetime.now()}")
        print("")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(fetch_mailchimp_unsubscribed_to_file())

