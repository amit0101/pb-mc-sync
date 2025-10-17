"""
Database connection and helper functions
Handles PostgreSQL connection and CRUD operations
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from typing import List, Dict, Optional, Any
from contextlib import contextmanager
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Database:
    """PostgreSQL database wrapper"""
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database connection
        
        Args:
            database_url: PostgreSQL connection URL (or use DATABASE_URL env var)
        """
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise ValueError("DATABASE_URL not provided")
        
        self._conn = None
    
    def connect(self):
        """Establish database connection"""
        if not self._conn or self._conn.closed:
            self._conn = psycopg2.connect(
                self.database_url,
                cursor_factory=RealDictCursor
            )
        return self._conn
    
    def close(self):
        """Close database connection"""
        if self._conn and not self._conn.closed:
            self._conn.close()
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results
        
        Args:
            query: SQL query to execute
            params: Optional parameters for parameterized query
            
        Returns:
            List of dictionaries with query results
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    # ==========================
    # CLIENT OPERATIONS
    # ==========================
    
    def upsert_client(self, client_data: Dict[str, Any]) -> int:
        """
        Insert or update a client
        
        Args:
            client_data: Dictionary with client fields
        
        Returns:
            Database ID of inserted/updated client
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO clients (
                    pabau_id, custom_id, email, first_name, last_name,
                    salutation, gender, dob, location, is_active,
                    phone, mobile,
                    opt_in_email, opt_in_sms, opt_in_phone, opt_in_post, opt_in_newsletter,
                    created_date, created_by_name, created_by_id,
                    pabau_last_synced_at
                ) VALUES (
                    %(pabau_id)s, %(custom_id)s, %(email)s, %(first_name)s, %(last_name)s,
                    %(salutation)s, %(gender)s, %(dob)s, %(location)s, %(is_active)s,
                    %(phone)s, %(mobile)s,
                    %(opt_in_email)s, %(opt_in_sms)s, %(opt_in_phone)s, %(opt_in_post)s, %(opt_in_newsletter)s,
                    %(created_date)s, %(created_by_name)s, %(created_by_id)s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (pabau_id) DO UPDATE SET
                    custom_id = EXCLUDED.custom_id,
                    email = EXCLUDED.email,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    salutation = EXCLUDED.salutation,
                    gender = EXCLUDED.gender,
                    dob = EXCLUDED.dob,
                    location = EXCLUDED.location,
                    is_active = EXCLUDED.is_active,
                    phone = EXCLUDED.phone,
                    mobile = EXCLUDED.mobile,
                    opt_in_email = EXCLUDED.opt_in_email,
                    opt_in_sms = EXCLUDED.opt_in_sms,
                    opt_in_phone = EXCLUDED.opt_in_phone,
                    opt_in_post = EXCLUDED.opt_in_post,
                    opt_in_newsletter = EXCLUDED.opt_in_newsletter,
                    created_date = EXCLUDED.created_date,
                    created_by_name = EXCLUDED.created_by_name,
                    created_by_id = EXCLUDED.created_by_id,
                    pabau_last_synced_at = CURRENT_TIMESTAMP
                RETURNING id
            """, client_data)
            
            result = cursor.fetchone()
            return result['id']
    
    def bulk_upsert_clients(self, clients: List[Dict[str, Any]]) -> int:
        """Bulk insert/update clients"""
        count = 0
        for client in clients:
            self.upsert_client(client)
            count += 1
        return count
    
    def get_client_by_email(self, email: str) -> Optional[Dict]:
        """Get client by email"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM clients WHERE email = %s", (email,))
            return cursor.fetchone()
    
    def get_client_by_pabau_id(self, pabau_id: int) -> Optional[Dict]:
        """Get client by Pabau ID"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM clients WHERE pabau_id = %s", (pabau_id,))
            return cursor.fetchone()
    
    # ==========================
    # APPOINTMENT OPERATIONS
    # ==========================
    
    def upsert_appointment(self, appointment_data: Dict[str, Any]) -> int:
        """
        Insert or update an appointment
        
        Args:
            appointment_data: Dictionary with appointment fields
        
        Returns:
            Database ID of inserted/updated appointment
        """
        with self.get_cursor() as cursor:
            # Check if we have a unique identifier to update
            # Since we don't have pabau_appointment_id from simple appointments array,
            # we use client_pabau_id + appointment_datetime as unique key
            cursor.execute("""
                INSERT INTO appointments (
                    client_pabau_id, pabau_appointment_id,
                    appointment_date, appointment_time, appointment_datetime,
                    location, service, duration, appointment_status,
                    appt_with, created_by, created_date, cancellation_reason,
                    pabau_last_synced_at
                ) VALUES (
                    %(client_pabau_id)s, %(pabau_appointment_id)s,
                    %(appointment_date)s, %(appointment_time)s, %(appointment_datetime)s,
                    %(location)s, %(service)s, %(duration)s, %(appointment_status)s,
                    %(appt_with)s, %(created_by)s, %(created_date)s, %(cancellation_reason)s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT ON CONSTRAINT appointments_unique_key DO UPDATE SET
                    appointment_date = EXCLUDED.appointment_date,
                    appointment_time = EXCLUDED.appointment_time,
                    appointment_datetime = EXCLUDED.appointment_datetime,
                    location = EXCLUDED.location,
                    service = EXCLUDED.service,
                    duration = EXCLUDED.duration,
                    appointment_status = EXCLUDED.appointment_status,
                    appt_with = EXCLUDED.appt_with,
                    created_by = EXCLUDED.created_by,
                    created_date = EXCLUDED.created_date,
                    cancellation_reason = EXCLUDED.cancellation_reason,
                    pabau_last_synced_at = CURRENT_TIMESTAMP
                RETURNING id
            """, appointment_data)
            
            result = cursor.fetchone()
            return result['id'] if result else None
    
    def bulk_upsert_appointments(self, appointments: List[Dict[str, Any]]) -> int:
        """Bulk insert/update appointments"""
        count = 0
        for appointment in appointments:
            result = self.upsert_appointment(appointment)
            if result:
                count += 1
        return count
    
    def get_appointments_by_client(self, client_pabau_id: int) -> List[Dict]:
        """Get all appointments for a client"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM appointments 
                WHERE client_pabau_id = %s 
                ORDER BY appointment_datetime DESC NULLS LAST
            """, (client_pabau_id,))
            return cursor.fetchall()
    
    def link_appointment_to_client_db_id(self, client_pabau_id: int):
        """Update client_db_id for appointments after client is inserted"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE appointments a
                SET client_db_id = c.id
                FROM clients c
                WHERE a.client_pabau_id = c.pabau_id
                  AND a.client_pabau_id = %s
                  AND a.client_db_id IS NULL
            """, (client_pabau_id,))
    
    # ==========================
    # LEAD OPERATIONS
    # ==========================
    
    def upsert_lead(self, lead_data: Dict[str, Any]) -> int:
        """Insert or update a lead"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO leads (
                    pabau_id, contact_id, email, first_name, last_name,
                    salutation, phone, mobile, dob,
                    mailing_street, mailing_postal, mailing_city, mailing_county, mailing_country,
                    is_active, lead_status,
                    owner_id, owner_name, location_id, location_name,
                    created_date, updated_date, converted_date,
                    pipeline_name, pipeline_stage_id, pipeline_stage_name,
                    deal_value, opt_in_email_mailchimp,
                    pabau_last_synced_at
                ) VALUES (
                    %(pabau_id)s, %(contact_id)s, %(email)s, %(first_name)s, %(last_name)s,
                    %(salutation)s, %(phone)s, %(mobile)s, %(dob)s,
                    %(mailing_street)s, %(mailing_postal)s, %(mailing_city)s, %(mailing_county)s, %(mailing_country)s,
                    %(is_active)s, %(lead_status)s,
                    %(owner_id)s, %(owner_name)s, %(location_id)s, %(location_name)s,
                    %(created_date)s, %(updated_date)s, %(converted_date)s,
                    %(pipeline_name)s, %(pipeline_stage_id)s, %(pipeline_stage_name)s,
                    %(deal_value)s, %(opt_in_email_mailchimp)s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (pabau_id) DO UPDATE SET
                    contact_id = EXCLUDED.contact_id,
                    email = EXCLUDED.email,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    salutation = EXCLUDED.salutation,
                    phone = EXCLUDED.phone,
                    mobile = EXCLUDED.mobile,
                    dob = EXCLUDED.dob,
                    mailing_street = EXCLUDED.mailing_street,
                    mailing_postal = EXCLUDED.mailing_postal,
                    mailing_city = EXCLUDED.mailing_city,
                    mailing_county = EXCLUDED.mailing_county,
                    mailing_country = EXCLUDED.mailing_country,
                    is_active = EXCLUDED.is_active,
                    lead_status = EXCLUDED.lead_status,
                    owner_id = EXCLUDED.owner_id,
                    owner_name = EXCLUDED.owner_name,
                    location_id = EXCLUDED.location_id,
                    location_name = EXCLUDED.location_name,
                    created_date = EXCLUDED.created_date,
                    updated_date = EXCLUDED.updated_date,
                    converted_date = EXCLUDED.converted_date,
                    pipeline_name = EXCLUDED.pipeline_name,
                    pipeline_stage_id = EXCLUDED.pipeline_stage_id,
                    pipeline_stage_name = EXCLUDED.pipeline_stage_name,
                    deal_value = EXCLUDED.deal_value,
                    opt_in_email_mailchimp = EXCLUDED.opt_in_email_mailchimp,
                    pabau_last_synced_at = CURRENT_TIMESTAMP
                RETURNING id
            """, lead_data)
            
            result = cursor.fetchone()
            return result['id']
    
    def bulk_upsert_leads(self, leads: List[Dict[str, Any]]) -> int:
        """Bulk insert/update leads"""
        count = 0
        for lead in leads:
            self.upsert_lead(lead)
            count += 1
        return count
    
    def get_lead_by_email(self, email: str) -> Optional[Dict]:
        """Get lead by email"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM leads WHERE email = %s", (email,))
            return cursor.fetchone()
    
    # ==========================
    # MAILCHIMP SYNC OPERATIONS
    # ==========================
    
    def update_client_mailchimp_status(self, email: str, mailchimp_id: str, status: str, tags: List[str] = None):
        """Update Mailchimp-related fields for a client"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE clients 
                SET mailchimp_id = %s,
                    mailchimp_status = %s,
                    mailchimp_tags = %s,
                    mailchimp_last_synced_at = CURRENT_TIMESTAMP
                WHERE email = %s
            """, (mailchimp_id, status, tags or [], email))
    
    def update_lead_mailchimp_status(self, email: str, mailchimp_id: str, status: str, tags: List[str] = None):
        """Update Mailchimp-related fields for a lead"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE leads 
                SET mailchimp_id = %s,
                    mailchimp_status = %s,
                    mailchimp_tags = %s,
                    mailchimp_last_synced_at = CURRENT_TIMESTAMP
                WHERE email = %s
            """, (mailchimp_id, status, tags or [], email))
    
    def update_opt_in_from_mailchimp(self, email: str, opt_in: int):
        """Update opt_in_email based on Mailchimp unsubscribe"""
        with self.get_cursor() as cursor:
            # Try clients first
            cursor.execute("""
                UPDATE clients 
                SET opt_in_email = %s,
                    mailchimp_status = CASE WHEN %s = 1 THEN 'subscribed' ELSE 'unsubscribed' END
                WHERE email = %s
                RETURNING id
            """, (opt_in, opt_in, email))
            
            if cursor.fetchone():
                return 'client'
            
            # Try leads
            cursor.execute("""
                UPDATE leads 
                SET opt_in_email_mailchimp = %s,
                    mailchimp_status = CASE WHEN %s = 1 THEN 'subscribed' ELSE 'unsubscribed' END
                WHERE email = %s
                RETURNING id
            """, (opt_in, opt_in, email))
            
            if cursor.fetchone():
                return 'lead'
            
            return None
    
    def get_opted_in_contacts(self) -> List[Dict]:
        """Get all contacts with opt_in_email = 1"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM v_mailchimp_contacts")
            return cursor.fetchall()
    
    # ==========================
    # LOGGING OPERATIONS
    # ==========================
    
    def log_sync(
        self,
        entity_type: str,
        entity_id: Optional[int],
        pabau_id: Optional[int],
        email: str,
        action: str,
        status: str,
        message: str = None,
        error_details: str = None,
        field_changes: Dict = None
    ):
        """Log a sync operation"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO sync_logs (
                    entity_type, entity_id, pabau_id, email,
                    action, status, message, error_details, field_changes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                entity_type, entity_id, pabau_id, email,
                action, status, message, error_details,
                json.dumps(field_changes) if field_changes else None
            ))
    
    def get_recent_logs(self, limit: int = 100) -> List[Dict]:
        """Get recent sync logs"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM sync_logs 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
    
    def get_logs_by_action(self, action: str, limit: int = 100) -> List[Dict]:
        """Get logs for a specific action"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM sync_logs 
                WHERE action = %s
                ORDER BY created_at DESC 
                LIMIT %s
            """, (action, limit))
            return cursor.fetchall()
    
    # ==========================
    # STATISTICS
    # ==========================
    
    def get_summary(self) -> List[Dict]:
        """Get summary statistics"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM v_summary")
            return cursor.fetchall()
    
    def get_last_sync_time(self, entity_type: str = 'client') -> Optional[datetime]:
        """Get last successful sync time for incremental updates"""
        with self.get_cursor() as cursor:
            if entity_type == 'client':
                cursor.execute("""
                    SELECT MAX(pabau_last_synced_at) as last_sync 
                    FROM clients
                """)
            else:
                cursor.execute("""
                    SELECT MAX(pabau_last_synced_at) as last_sync 
                    FROM leads
                """)
            
            result = cursor.fetchone()
            return result['last_sync'] if result else None


    # ==========================
    # SYNC PROGRESS TRACKING
    # ==========================
    
    def save_pabau_page_progress(self, page_number: int):
        """Save the last page processed for resumable syncs"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_progress (
                    key TEXT PRIMARY KEY,
                    value INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                INSERT INTO sync_progress (key, value, updated_at)
                VALUES ('last_pabau_page', %s, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
            """, (page_number,))
    
    def get_last_pabau_page_processed(self) -> int:
        """Get the last page number processed"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT value FROM sync_progress WHERE key = 'last_pabau_page'
            """)
            result = cursor.fetchone()
            return result['value'] if result else 0
    
    def reset_pabau_page_progress(self):
        """Reset progress (call when sync completes all pages)"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                DELETE FROM sync_progress WHERE key = 'last_pabau_page'
            """)


# Singleton instance
_db = None


def get_db() -> Database:
    """Get database singleton instance"""
    global _db
    if _db is None:
        _db = Database()
    return _db

