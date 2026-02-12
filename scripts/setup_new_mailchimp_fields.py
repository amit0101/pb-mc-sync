#!/usr/bin/env python3
"""
Setup new Mailchimp merge fields for the field mapping update.

Creates merge fields with clean tag names for:
- 10 new KEEP fields that don't have tags yet
- 10 new ADD computed fields

Run this ONCE before the first sync with the new field mapping.
Safe to re-run (skips fields that already exist).
"""

import asyncio
import sys
import os
from loguru import logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clients.mailchimp_client import MailchimpClient


# All new merge fields to create
NEW_MERGE_FIELDS = [
    # === KEEP fields that need new tags ===
    {"tag": "LEADSRC",  "name": "Lead Source",          "type": "text"},
    {"tag": "PIPELINE", "name": "Pipeline Stage",       "type": "text"},
    {"tag": "POSTCODE", "name": "Postcode",             "type": "text"},
    {"tag": "LEADLOC",  "name": "Lead Location",        "type": "text"},
    {"tag": "LEADAGE",  "name": "Lead Age",             "type": "number"},
    {"tag": "TOTALAPP", "name": "Total Appointments",   "type": "number"},
    {"tag": "CLIENTID", "name": "Client ID",            "type": "text"},
    {"tag": "LEADID",   "name": "Lead ID",              "type": "number"},
    {"tag": "SOURCE",   "name": "Source",               "type": "text"},
    {"tag": "EMOPTIN",  "name": "Email Marketing",      "type": "radio", "choices": ["Yes", "No"]},
    
    # === ADD fields (10 new computed fields) ===
    {"tag": "TOTSPEND", "name": "Total Spend",          "type": "number"},
    {"tag": "AVGSPEND", "name": "Avg Spend",            "type": "number"},
    {"tag": "NEXTAPPT", "name": "Next Appt Date",       "type": "date"},
    {"tag": "ISREAL",   "name": "Is Real Customer",     "type": "radio", "choices": ["Yes", "No"]},
    {"tag": "ISSURGRY", "name": "Is Surgery Client",    "type": "radio", "choices": ["Yes", "No"]},
    {"tag": "ISCONSLT", "name": "Is Consult Only",      "type": "radio", "choices": ["Yes", "No"]},
    {"tag": "ISUNCONV", "name": "Is Unconverted Lead",  "type": "radio", "choices": ["Yes", "No"]},
    {"tag": "ISPRP",    "name": "Is PRP Patient",       "type": "radio", "choices": ["Yes", "No"]},
    {"tag": "PRPOVDUE", "name": "Is PRP Overdue",       "type": "radio", "choices": ["Yes", "No"]},
    {"tag": "ISHIGHV",  "name": "Is High Value Client", "type": "radio", "choices": ["Yes", "No"]},
]


async def main():
    """Create all new merge fields in Mailchimp"""
    mc = MailchimpClient()
    
    # Get existing merge fields
    logger.info("Fetching existing merge fields...")
    try:
        response = await mc._request('GET', f'/lists/{mc.list_id}/merge-fields', params={'count': 100})
        existing_tags = {f['tag'] for f in response.get('merge_fields', [])}
        logger.info(f"Found {len(existing_tags)} existing merge fields: {sorted(existing_tags)}")
    except Exception as e:
        logger.error(f"Failed to fetch existing fields: {e}")
        existing_tags = set()
    
    created = 0
    skipped = 0
    failed = 0
    
    for field_def in NEW_MERGE_FIELDS:
        tag = field_def['tag']
        
        if tag in existing_tags:
            logger.info(f"  ⏭️  {tag} ({field_def['name']}) - already exists, skipping")
            skipped += 1
            continue
        
        try:
            data = {
                'tag': tag,
                'name': field_def['name'],
                'type': field_def['type'],
                'required': False,
                'public': False,
            }
            
            if field_def.get('choices'):
                data['options'] = {'choices': field_def['choices']}
            
            if field_def['type'] == 'date':
                data['options'] = {'date_format': 'MM/DD/YYYY'}
            
            await mc._request('POST', f'/lists/{mc.list_id}/merge-fields', json_data=data)
            logger.success(f"  ✅ Created {tag} ({field_def['name']})")
            created += 1
            
        except Exception as e:
            logger.error(f"  ❌ Failed to create {tag} ({field_def['name']}): {e}")
            failed += 1
        
        await asyncio.sleep(0.3)  # Rate limiting
    
    logger.info("")
    logger.info(f"Done! Created: {created}, Skipped: {skipped}, Failed: {failed}")


if __name__ == "__main__":
    asyncio.run(main())
