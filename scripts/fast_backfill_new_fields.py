#!/usr/bin/env python3
"""
Fast backfill: Fetch all clients from Pabau and UPDATE only the new columns
in the existing DB rows. Uses batch UPDATEs for speed.

This does NOT insert new rows — it only updates existing clients
with the new fields (total_spend, avg_spend, etc.)
"""

import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clients.pabau_client import PabauClient
from utils.transforms import transform_client_for_db
from db.database import get_db


async def fast_backfill():
    pabau = PabauClient()
    db = get_db()
    
    print("=" * 60)
    print("FAST BACKFILL: Updating new fields for existing clients")
    print("=" * 60)
    
    page = 1
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    start_time = time.time()
    
    while True:
        # Fetch page from Pabau (fast — ~1.5s)
        try:
            response = await pabau.get_contacts(page=page, page_size=50)
            clients_raw = response.get('clients', [])
        except Exception as e:
            print(f"  Page {page}: Fetch error: {e}")
            break
        
        if not clients_raw:
            print(f"\n  Page {page}: Empty — reached end of data")
            break
        
        # Transform all clients on this page
        updates = []
        for raw in clients_raw:
            try:
                t = transform_client_for_db(raw)
                if not t.get('pabau_id'):
                    continue
                updates.append(t)
            except Exception:
                total_errors += 1
        
        # Batch UPDATE in a single transaction (fast — one round trip)
        if updates:
            try:
                with db.get_cursor() as cursor:
                    for t in updates:
                        cursor.execute("""
                            UPDATE clients SET
                                total_spend = %(total_spend)s,
                                avg_spend = %(avg_spend)s,
                                total_completed = %(total_completed)s,
                                total_pending = %(total_pending)s,
                                total_cancelled = %(total_cancelled)s,
                                total_visits = %(total_visits)s,
                                total_noshow = %(total_noshow)s,
                                next_appt_date = %(next_appt_date)s,
                                last_appt_date = %(last_appt_date)s,
                                first_visit_date = %(first_visit_date)s,
                                last_appt_service = %(last_appt_service)s,
                                next_appt_service = %(next_appt_service)s,
                                primary_source_name = %(primary_source_name)s,
                                primary_source_id = %(primary_source_id)s,
                                age = %(age)s,
                                mailing_postal = %(mailing_postal)s,
                                pabau_last_synced_at = NOW()
                            WHERE pabau_id = %(pabau_id)s
                        """, t)
                        if cursor.rowcount > 0:
                            total_updated += 1
                        else:
                            total_skipped += 1
            except Exception as e:
                print(f"  Page {page}: DB error: {e}")
                total_errors += len(updates)
        
        # Progress every 20 pages
        if page % 20 == 0:
            elapsed = time.time() - start_time
            rate = total_updated / elapsed if elapsed > 0 else 0
            print(f"  Page {page}: {total_updated} updated, {total_skipped} not in DB, "
                  f"{total_errors} errors ({elapsed:.0f}s, {rate:.0f}/s)")
        
        page += 1
    
    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"DONE in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Updated: {total_updated}")
    print(f"  Not in DB: {total_skipped}")
    print(f"  Errors: {total_errors}")
    print(f"  Pages: {page - 1}")
    print(f"{'=' * 60}")
    
    # Verify a sample
    print("\nVerifying sample...")
    with db.get_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN total_spend > 0 THEN 1 END) as has_spend,
                   COUNT(CASE WHEN last_appt_service IS NOT NULL THEN 1 END) as has_service,
                   COUNT(CASE WHEN age IS NOT NULL THEN 1 END) as has_age,
                   COUNT(CASE WHEN primary_source_name IS NOT NULL THEN 1 END) as has_source
            FROM clients
        """)
        row = cursor.fetchone()
        print(f"  Total clients: {row['total']}")
        print(f"  With total_spend > 0: {row['has_spend']}")
        print(f"  With last_appt_service: {row['has_service']}")
        print(f"  With age: {row['has_age']}")
        print(f"  With primary_source_name: {row['has_source']}")
    
    db.close()


if __name__ == '__main__':
    asyncio.run(fast_backfill())
