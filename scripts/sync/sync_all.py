#!/usr/bin/env python3
"""
MASTER SYNC SCRIPT - Run every 30 minutes
Executes all sync operations in correct order

Order of operations:
1. Fetch Mailchimp unsubscribes → Update database
2. Fetch Pabau updates → Update database
3. Push database opted-in contacts → Mailchimp

This ensures:
- Mailchimp unsubscribes are captured first
- Pabau updates overwrite if needed (Pabau is source of truth for opt-ins)
- Final state is pushed to Mailchimp
"""

import sys
import os
import asyncio
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Import sync functions
import importlib.util

def load_sync_module(filename):
    """Dynamically load a sync module"""
    script_dir = os.path.dirname(__file__)
    spec = importlib.util.spec_from_file_location(filename.replace('.py', ''), os.path.join(script_dir, filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def run_sync_cycle():
    """Run complete sync cycle"""
    
    start_time = datetime.now()
    
    print("=" * 80)
    print("SYNC CYCLE STARTED")
    print("=" * 80)
    print(f"Time: {start_time}")
    print("")
    
    try:
        # STEP 1: Fetch Mailchimp unsubscribes
        print("STEP 1/3: Fetching Mailchimp unsubscribes...")
        print("-" * 80)
        mc_module = load_sync_module('fetch_mailchimp_unsubscribes.py')
        await mc_module.fetch_unsubscribes()
        print("")
        
        # STEP 2: Sync Pabau to Database
        print("STEP 2/3: Syncing Pabau to database...")
        print("-" * 80)
        pabau_module = load_sync_module('sync_pabau_to_db.py')
        await pabau_module.sync_pabau()
        print("")
        
        # STEP 3: Sync Database to Mailchimp
        print("STEP 3/3: Syncing database to Mailchimp...")
        print("-" * 80)
        db_mc_module = load_sync_module('sync_db_to_mailchimp.py')
        await db_mc_module.sync_to_mailchimp()
        print("")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("=" * 80)
        print("SYNC CYCLE COMPLETE ✅")
        print("=" * 80)
        print(f"Duration: {duration:.1f} seconds")
        print(f"Completed at: {end_time}")
        print("")
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("")
        print("=" * 80)
        print("SYNC CYCLE FAILED ❌")
        print("=" * 80)
        print(f"Error: {e}")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Failed at: {end_time}")
        print("")
        raise


if __name__ == '__main__':
    try:
        asyncio.run(run_sync_cycle())
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)

