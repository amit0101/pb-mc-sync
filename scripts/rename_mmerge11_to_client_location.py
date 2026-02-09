#!/usr/bin/env python3
"""
Rename MMERGE11 field in Mailchimp from "Created by" to "Client Location"

This script updates the merge field name in Mailchimp to reflect the new data source.
"""

import asyncio
import sys
import os
from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clients.mailchimp_client import MailchimpClient


async def rename_mmerge11():
    """Rename MMERGE11 from 'Created by' to 'Client Location'"""
    logger.info("=" * 60)
    logger.info("RENAMING MMERGE11 TO 'Client Location'")
    logger.info("=" * 60)
    logger.info("")
    
    client = MailchimpClient()
    
    try:
        # First, get all merge fields to find MMERGE11's ID
        logger.info("📋 Fetching existing merge fields...")
        response = await client._request(
            "GET",
            f"lists/{client.list_id}/merge-fields?count=200"
        )
        
        mmerge11_field = None
        for field in response.get('merge_fields', []):
            if field['tag'] == 'MMERGE11':
                mmerge11_field = field
                break
        
        if not mmerge11_field:
            logger.error("❌ MMERGE11 field not found!")
            return False
        
        logger.info(f"Found MMERGE11:")
        logger.info(f"  - ID: {mmerge11_field['merge_id']}")
        logger.info(f"  - Current Name: {mmerge11_field['name']}")
        logger.info(f"  - Type: {mmerge11_field['type']}")
        logger.info("")
        
        # Update the field name
        logger.info("🔧 Updating field name to 'Client Location'...")
        update_data = {
            "name": "Client Location"
        }
        
        result = await client._request(
            "PATCH",
            f"lists/{client.list_id}/merge-fields/{mmerge11_field['merge_id']}",
            json_data=update_data
        )
        
        logger.success(f"✅ Successfully renamed MMERGE11!")
        logger.info(f"  - Old Name: {mmerge11_field['name']}")
        logger.info(f"  - New Name: {result.get('name', 'Client Location')}")
        logger.info("")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(rename_mmerge11())
    if success:
        logger.info("=" * 60)
        logger.info("✅ DONE! MMERGE11 is now 'Client Location'")
        logger.info("=" * 60)
    else:
        logger.error("Failed to rename field")
        sys.exit(1)
