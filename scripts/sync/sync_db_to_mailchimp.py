#!/usr/bin/env python3
"""
INCREMENTAL SYNC: Push newly synced contacts from database to Mailchimp
Run every 6 hours as Step 3 of the sync cycle

This script:
1. Gets clients synced since last Mailchimp upload (opt_in_email = 1)
2. Gets leads synced since last Mailchimp upload (opt_in_email_mailchimp = 1)
3. Uploads them to Mailchimp with the target 30 fields
4. Tags as "Pabau Clients" or "Pabau Leads"
5. Logs all operations

Target fields (30 total = 20 KEEP + 10 ADD):
  KEEP: First Name, Last Name, Phone, Gender, Mobile, Appointment Date,
        Client Location, Created Date, Service, Lead Source, Pipeline Stage,
        Postcode, Lead Location, Lead Age, Total Appointments, Client ID,
        Client System ID, Lead ID, Source, Email Marketing
  ADD:  total_spend, avg_spend, next_appt_date, is_real_customer,
        is_surgery_client, is_consult_only, is_unconverted_lead,
        is_prp_patient, is_prp_overdue, is_high_value_client
"""

import sys
import os
import asyncio
from datetime import datetime, timedelta, date
import gc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from clients.mailchimp_client import MailchimpClient
from db.database import get_db


def compute_boolean_flags(client):
    """Compute the 10 boolean/computed fields for a client record"""
    total_spend = float(client.get('total_spend') or 0)
    avg_spend = float(client.get('avg_spend') or 0)
    total_completed = int(client.get('total_completed') or 0)
    last_appt_service = str(client.get('last_appt_service') or '').lower()
    last_appt_date = client.get('last_appt_date')
    
    # is_real_customer: total_spend > 25 AND total_completed > 0
    is_real_customer = total_spend > 25 and total_completed > 0
    
    # is_high_value_client: total_spend > 3000
    is_high_value_client = total_spend > 3000
    
    # is_unconverted_lead: no appointments AND no spend
    is_unconverted_lead = total_completed == 0 and total_spend == 0
    
    # is_surgery_client: service contains Hair Transplant or Paid Procedure
    surgery_keywords = ['hair transplant', 'paid procedure']
    is_surgery_client = any(kw in last_appt_service for kw in surgery_keywords)
    # Also check all appointment services if available
    all_services = str(client.get('all_services') or '').lower()
    if all_services:
        is_surgery_client = is_surgery_client or any(kw in all_services for kw in surgery_keywords)
    
    # is_consult_only: had consult/appointment but no surgery
    is_consult_only = total_completed > 0 and not is_surgery_client
    
    # is_prp_patient: service contains PRP
    is_prp_patient = 'prp' in last_appt_service
    if all_services:
        is_prp_patient = is_prp_patient or 'prp' in all_services
    
    # is_prp_overdue: is_prp_patient AND last appointment > 56 days ago
    is_prp_overdue = False
    if is_prp_patient and last_appt_date:
        try:
            if isinstance(last_appt_date, date):
                days_since = (date.today() - last_appt_date).days
            else:
                days_since = (date.today() - datetime.strptime(str(last_appt_date), '%Y-%m-%d').date()).days
            is_prp_overdue = days_since > 56
        except (ValueError, TypeError):
            pass
    
    return {
        'total_spend': total_spend,
        'avg_spend': avg_spend,
        'next_appt_date': client.get('next_appt_date'),
        'is_real_customer': is_real_customer,
        'is_high_value_client': is_high_value_client,
        'is_unconverted_lead': is_unconverted_lead,
        'is_surgery_client': is_surgery_client,
        'is_consult_only': is_consult_only,
        'is_prp_patient': is_prp_patient,
        'is_prp_overdue': is_prp_overdue,
    }


def build_client_merge_fields(client):
    """Build Mailchimp merge fields dict for a client record"""
    flags = compute_boolean_flags(client)
    
    merge_fields = {
        'FNAME': client['first_name'] or '',
        'LNAME': client['last_name'] or '',
    }
    
    # Phone & Mobile
    if client.get('phone'):
        merge_fields['PHONE'] = client['phone']
    if client.get('client_mobile'):
        merge_fields['MMERGE7'] = client['client_mobile']
    
    # Gender
    if client.get('gender'):
        merge_fields['MMERGE6'] = client['gender']
    
    # Appointment Date (from lateral join, fallback to Pabau client_insights)
    try:
        appt_date = client.get('appointment_date') or client.get('last_appt_date')
        if appt_date:
            if isinstance(appt_date, date):
                merge_fields['MMERGE9'] = appt_date.strftime('%m/%d/%Y')
            else:
                merge_fields['MMERGE9'] = datetime.strptime(str(appt_date), '%Y-%m-%d').strftime('%m/%d/%Y')
    except (AttributeError, ValueError):
        pass
    
    # Client Location
    if client.get('client_location'):
        merge_fields['MMERGE11'] = str(client['client_location'])[:50]
    
    # Created Date
    try:
        if client.get('created_date'):
            merge_fields['MMERGE12'] = client['created_date'].strftime('%m/%d/%Y')
    except (AttributeError, ValueError):
        pass
    
    # Service (from lateral join, fallback to Pabau client_insights)
    service = client.get('service') or client.get('last_appt_service')
    if service:
        merge_fields['MMERGE14'] = str(service)[:100]
    
    # Client System ID (pabau_id)
    try:
        system_id_val = int(client['client_system_id'])
        if 0 < system_id_val < 2147483647:
            merge_fields['MMERGE17'] = system_id_val
        else:
            return None  # Skip invalid system ID
    except (ValueError, TypeError):
        return None  # Skip if no valid system ID
    
    # Client ID (custom_id or DB id) → MMERGE16 (number type)
    if client.get('custom_id'):
        try:
            merge_fields['MMERGE16'] = int(client['custom_id'])
        except (ValueError, TypeError):
            pass
    elif client.get('client_db_id'):
        try:
            merge_fields['MMERGE16'] = int(client['client_db_id'])
        except (ValueError, TypeError):
            pass
    
    # Source (primary_source_name from Pabau)
    if client.get('primary_source_name'):
        merge_fields['SOURCE'] = str(client['primary_source_name'])[:100]
    
    # Total Appointments (total_completed)
    merge_fields['MMERGE30'] = int(client.get('total_completed') or 0)
    
    # Email Marketing opt-in status
    merge_fields['EMOPTIN'] = 'Yes' if client.get('opt_in_email') == 1 else 'No'
    
    # Postcode → MMERGE22
    if client.get('mailing_postal'):
        merge_fields['MMERGE22'] = str(client['mailing_postal'])[:20]
    
    # --- ADD fields (10 new computed fields) ---
    merge_fields['TOTSPEND'] = flags['total_spend']
    merge_fields['AVGSPEND'] = flags['avg_spend']
    
    if flags['next_appt_date']:
        try:
            if isinstance(flags['next_appt_date'], date):
                merge_fields['NEXTAPPT'] = flags['next_appt_date'].strftime('%m/%d/%Y')
            else:
                merge_fields['NEXTAPPT'] = str(flags['next_appt_date'])
        except (AttributeError, ValueError):
            pass
    
    merge_fields['ISREAL'] = 'Yes' if flags['is_real_customer'] else 'No'
    merge_fields['ISSURGRY'] = 'Yes' if flags['is_surgery_client'] else 'No'
    merge_fields['ISCONSLT'] = 'Yes' if flags['is_consult_only'] else 'No'
    merge_fields['ISUNCONV'] = 'Yes' if flags['is_unconverted_lead'] else 'No'
    merge_fields['ISPRP'] = 'Yes' if flags['is_prp_patient'] else 'No'
    merge_fields['PRPOVDUE'] = 'Yes' if flags['is_prp_overdue'] else 'No'
    merge_fields['ISHIGHV'] = 'Yes' if flags['is_high_value_client'] else 'No'
    
    return merge_fields


def build_lead_merge_fields(lead):
    """Build Mailchimp merge fields dict for a lead record"""
    merge_fields = {
        'FNAME': lead['first_name'] or '',
        'LNAME': lead['last_name'] or '',
    }
    
    # Phone & Mobile
    if lead.get('phone'):
        merge_fields['PHONE'] = lead['phone']
    if lead.get('mobile'):
        merge_fields['MMERGE7'] = lead['mobile']
    
    # Created Date
    try:
        if lead.get('created_date'):
            merge_fields['MMERGE12'] = lead['created_date'].strftime('%m/%d/%Y')
    except (AttributeError, ValueError):
        pass
    
    # Lead Source
    if lead.get('source_name'):
        merge_fields['MMERGE20'] = str(lead['source_name'])[:100]
    
    # Pipeline Stage
    if lead.get('pipeline_stage_name'):
        merge_fields['MMERGE21'] = str(lead['pipeline_stage_name'])[:100]
    
    # Postcode → MMERGE22
    if lead.get('mailing_postal'):
        merge_fields['MMERGE22'] = str(lead['mailing_postal'])[:20]
    
    # Lead Location
    if lead.get('location_name'):
        merge_fields['MMERGE24'] = str(lead['location_name'])[:50]
    
    # Lead Age (calculated from DOB)
    if lead.get('dob'):
        try:
            if isinstance(lead['dob'], date):
                dob_date = lead['dob']
            else:
                dob_date = datetime.strptime(str(lead['dob']), '%Y-%m-%d').date()
            age = (date.today() - dob_date).days // 365
            if 0 < age < 150:
                merge_fields['MMERGE23'] = age
        except (ValueError, TypeError):
            pass
    
    # Lead ID (pabau_id)
    try:
        lead_id_val = int(lead['pabau_id'])
        if 0 < lead_id_val < 2147483647:
            merge_fields['MMERGE29'] = lead_id_val
    except (ValueError, TypeError):
        pass
    
    # Email Marketing opt-in
    merge_fields['EMOPTIN'] = 'Yes' if lead.get('opt_in_email_mailchimp') == 1 else 'No'
    
    # Unconverted lead flag (leads with no appointments/spend are unconverted by default)
    merge_fields['ISUNCONV'] = 'Yes'
    
    return merge_fields


async def upload_single_batch(mc, batch, batch_index, batch_label):
    """Upload a single batch of members to Mailchimp"""
    try:
        print(f"    [{batch_label}] Batch {batch_index}: Uploading {len(batch)} members...")
        result = await mc.batch_subscribe(batch, update_existing=True)
        batch_success = result.get('total_created', 0) + result.get('total_updated', 0)
        batch_errors = result.get('error_count', 0)
        print(f"    [{batch_label}] Batch {batch_index}: ✅ {batch_success} success, ❌ {batch_errors} errors")
        return batch_success, batch_errors
    except Exception as e:
        print(f"    [{batch_label}] Batch {batch_index}: ❌ Failed - {str(e)[:100]}")
        return 0, len(batch)


async def upload_batch(mc, members_batch, batch_label, db, entity_type='client'):
    """Upload batches of members to Mailchimp with concurrent uploads"""
    batch_size = 500
    concurrency = 5  # 5 parallel uploads (Mailchimp allows 10 concurrent connections)
    success_count = 0
    error_count = 0
    
    # Split into batches of 500
    batches = []
    for batch_num in range(0, len(members_batch), batch_size):
        chunk = members_batch[batch_num:batch_num + batch_size]
        batch_index = batch_num // batch_size + 1
        batches.append((chunk, batch_index))
    
    total_batches = len(batches)
    print(f"    [{batch_label}] {total_batches} batches, {concurrency} concurrent")
    
    # Process in groups of `concurrency`
    for group_start in range(0, total_batches, concurrency):
        group = batches[group_start:group_start + concurrency]
        group_num = group_start // concurrency + 1
        total_groups = (total_batches + concurrency - 1) // concurrency
        
        results = await asyncio.gather(
            *[upload_single_batch(mc, chunk, idx, batch_label) for chunk, idx in group]
        )
        
        for s, e in results:
            success_count += s
            error_count += e
        
        print(f"    [{batch_label}] Group {group_num}/{total_groups} done")
        gc.collect()
        await asyncio.sleep(0.3)
    
    return success_count, error_count


async def sync_clients_to_mailchimp(db, mc, last_mailchimp_upload):
    """Sync recently updated opted-in clients to Mailchimp"""
    
    # Count clients to upload
    with db.get_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT sl.email) as count
            FROM sync_logs sl
            INNER JOIN clients c ON c.email = sl.email
            WHERE sl.action = 'sync_pabau_client'
              AND sl.status = 'success'
              AND sl.created_at > %s
              AND c.opt_in_email = 1
              AND c.is_active = 1
        """, (last_mailchimp_upload,))
        debug = cursor.fetchone()
        print(f"  Clients to upload: {debug['count']}")
    
    # Fetch client data with latest appointment
    with db.get_cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT ON (c.id)
                c.id as client_db_id,
                c.pabau_id as client_system_id,
                c.custom_id,
                c.first_name,
                c.last_name,
                c.email,
                c.phone,
                c.mobile as client_mobile,
                c.gender,
                c.opt_in_email,
                c.location as client_location,
                c.mailing_postal,
                c.total_spend,
                c.avg_spend,
                c.total_completed,
                c.next_appt_date,
                c.last_appt_date,
                c.last_appt_service,
                c.primary_source_name,
                c.created_date,
                a.appointment_date,
                a.service
            FROM sync_logs sl
            INNER JOIN clients c ON c.email = sl.email
            LEFT JOIN LATERAL (
                SELECT *
                FROM appointments
                WHERE client_pabau_id = c.pabau_id
                ORDER BY appointment_datetime DESC NULLS LAST
                LIMIT 1
            ) a ON true
            WHERE sl.action = 'sync_pabau_client'
              AND sl.status = 'success'
              AND sl.created_at > %s
              AND c.opt_in_email = 1
              AND c.email IS NOT NULL
              AND c.is_active = 1
            ORDER BY c.id
        """, (last_mailchimp_upload,))
        clients = cursor.fetchall()
    
    if not clients:
        print(f"  ✅ No new/updated clients to sync")
        return 0, 0
    
    print(f"  Found {len(clients)} new/updated clients to sync")
    
    # Also fetch all appointment services per client for accurate surgery/PRP flags
    client_pabau_ids = [c['client_system_id'] for c in clients if c.get('client_system_id')]
    all_services_map = {}
    if client_pabau_ids:
        try:
            with db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT client_pabau_id, string_agg(DISTINCT service, ', ') as all_services
                    FROM appointments
                    WHERE client_pabau_id = ANY(%s)
                      AND service IS NOT NULL
                    GROUP BY client_pabau_id
                """, (client_pabau_ids,))
                for row in cursor.fetchall():
                    all_services_map[row['client_pabau_id']] = row['all_services']
        except Exception:
            pass  # If appointments table is empty, that's fine
    
    # Deduplicate by email (keep latest)
    email_to_client = {}
    for client in clients:
        email_lower = client['email'].lower()
        # Attach all_services from appointments
        client['all_services'] = all_services_map.get(client['client_system_id'], '')
        if email_lower in email_to_client:
            if client['client_db_id'] > email_to_client[email_lower]['client_db_id']:
                email_to_client[email_lower] = client
        else:
            email_to_client[email_lower] = client
    
    unique_clients = list(email_to_client.values())
    print(f"  {len(unique_clients)} unique clients after deduplication")
    
    # Build batch
    members_batch = []
    skipped = 0
    for client in unique_clients:
        merge_fields = build_client_merge_fields(client)
        if merge_fields is None:
            skipped += 1
            continue
        
        members_batch.append({
            'email_address': client['email'],
            'status': 'subscribed',
            'merge_fields': merge_fields,
            'tags': ['Pabau Clients']
        })
    
    if skipped:
        print(f"  Skipped {skipped} clients (invalid system ID)")
    
    if not members_batch:
        print(f"  No valid client members to upload")
        return 0, 0
    
    success, errors = await upload_batch(mc, members_batch, "Clients", db)
    
    # Log successful syncs
    if success > 0:
        for client in unique_clients[:success]:
            try:
                db.log_sync(
                    entity_type='client',
                    entity_id=client['client_db_id'],
                    pabau_id=client['client_system_id'],
                    email=client['email'],
                    action='sync_to_mailchimp',
                    status='success',
                    message='Synced to Mailchimp'
                )
            except Exception as log_error:
                print(f"  ⚠️  Failed to log sync for {client['email']}: {log_error}")
    
    return success, errors


async def sync_leads_to_mailchimp(db, mc, last_mailchimp_upload):
    """Sync recently updated opted-in leads to Mailchimp"""
    
    # Count leads to upload
    with db.get_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT sl.email) as count
            FROM sync_logs sl
            INNER JOIN leads l ON l.email = sl.email
            WHERE sl.action = 'sync_pabau_lead'
              AND sl.status = 'success'
              AND sl.created_at > %s
              AND l.opt_in_email_mailchimp = 1
              AND l.is_active = 1
        """, (last_mailchimp_upload,))
        debug = cursor.fetchone()
        print(f"  Leads to upload: {debug['count']}")
    
    # Fetch lead data
    with db.get_cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT ON (l.id)
                l.id as lead_db_id,
                l.pabau_id,
                l.first_name,
                l.last_name,
                l.email,
                l.phone,
                l.mobile,
                l.dob,
                l.mailing_postal,
                l.location_name,
                l.source_name,
                l.pipeline_stage_name,
                l.created_date,
                l.opt_in_email_mailchimp
            FROM sync_logs sl
            INNER JOIN leads l ON l.email = sl.email
            WHERE sl.action = 'sync_pabau_lead'
              AND sl.status = 'success'
              AND sl.created_at > %s
              AND l.opt_in_email_mailchimp = 1
              AND l.email IS NOT NULL
              AND l.is_active = 1
            ORDER BY l.id
        """, (last_mailchimp_upload,))
        leads = cursor.fetchall()
    
    if not leads:
        print(f"  ✅ No new/updated leads to sync")
        return 0, 0
    
    print(f"  Found {len(leads)} new/updated leads to sync")
    
    # Deduplicate by email (keep latest)
    email_to_lead = {}
    for lead in leads:
        email_lower = lead['email'].lower()
        if email_lower in email_to_lead:
            if lead['lead_db_id'] > email_to_lead[email_lower]['lead_db_id']:
                email_to_lead[email_lower] = lead
        else:
            email_to_lead[email_lower] = lead
    
    unique_leads = list(email_to_lead.values())
    print(f"  {len(unique_leads)} unique leads after deduplication")
    
    # Build batch
    members_batch = []
    for lead in unique_leads:
        merge_fields = build_lead_merge_fields(lead)
        
        members_batch.append({
            'email_address': lead['email'],
            'status': 'subscribed',
            'merge_fields': merge_fields,
            'tags': ['Pabau Leads']
        })
    
    if not members_batch:
        print(f"  No valid lead members to upload")
        return 0, 0
    
    success, errors = await upload_batch(mc, members_batch, "Leads", db)
    
    # Log successful syncs
    if success > 0:
        for lead in unique_leads[:success]:
            try:
                db.log_sync(
                    entity_type='lead',
                    entity_id=lead['lead_db_id'],
                    pabau_id=lead['pabau_id'],
                    email=lead['email'],
                    action='sync_to_mailchimp',
                    status='success',
                    message='Lead synced to Mailchimp'
                )
            except Exception as log_error:
                print(f"  ⚠️  Failed to log sync for {lead['email']}: {log_error}")
    
    return success, errors


async def sync_to_mailchimp():
    """Sync recently updated opted-in contacts to Mailchimp"""
    
    print(f"[{datetime.now()}] Syncing to Mailchimp...")
    
    db = get_db()
    mc = MailchimpClient()
    
    try:
        # Check when Pabau sync last completed
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT MAX(created_at) as last_run
                FROM sync_logs
                WHERE action IN ('sync_pabau_clients_completed', 'sync_pabau_leads_completed')
                  AND status = 'success'
            """)
            result = cursor.fetchone()
            last_pabau_sync = result['last_run'] if result and result['last_run'] else None
        
        if not last_pabau_sync:
            print(f"  No Pabau sync completion found - nothing to upload")
            return
        
        # Check when Mailchimp upload last completed
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT MAX(created_at) as last_upload
                FROM sync_logs
                WHERE action = 'sync_to_mailchimp_completed'
                  AND status = 'success'
            """)
            result = cursor.fetchone()
            last_mailchimp_upload = result['last_upload'] if result and result['last_upload'] else datetime(2020, 1, 1)
        
        print(f"  Last Pabau sync completed: {last_pabau_sync}")
        print(f"  Last Mailchimp upload completed: {last_mailchimp_upload}")
        
        # Only upload if Pabau sync happened AFTER last Mailchimp upload
        if last_pabau_sync <= last_mailchimp_upload:
            print(f"  ✅ No new Pabau data since last Mailchimp upload")
            return
        
        print(f"  Uploading clients/leads synced after {last_mailchimp_upload}")
        
        # Sync clients
        print(f"\n  --- CLIENTS ---")
        client_success, client_errors = await sync_clients_to_mailchimp(db, mc, last_mailchimp_upload)
        
        # Sync leads
        print(f"\n  --- LEADS ---")
        lead_success, lead_errors = await sync_leads_to_mailchimp(db, mc, last_mailchimp_upload)
        
        total_success = client_success + lead_success
        total_errors = client_errors + lead_errors
        print(f"\n  Total: ✅ {total_success} success ({client_success} clients, {lead_success} leads), ❌ {total_errors} errors")
        
        # Log completion
        db.log_sync(
            entity_type='sync_run',
            entity_id=None,
            pabau_id=None,
            email=None,
            action='sync_to_mailchimp_completed',
            status='success',
            message=f'Uploaded {client_success} clients + {lead_success} leads to Mailchimp'
        )
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == '__main__':
    asyncio.run(sync_to_mailchimp())
