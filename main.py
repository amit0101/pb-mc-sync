#!/usr/bin/env python3
"""
Main entry point for Render deployment
Runs both the FastAPI dashboard and background scheduler in separate threads
"""

import threading
import os
from loguru import logger

# Import dashboard and scheduler
from dashboard import app
from scheduler import start_scheduler


def init_database():
    """Initialize database schema if tables don't exist"""
    from db.database import get_db
    
    logger.info("Checking database schema...")
    db = get_db()
    
    try:
        with db.get_cursor() as cursor:
            # Check if clients table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'clients'
                )
            """)
            tables_exist = cursor.fetchone()['exists']
            
            if not tables_exist:
                logger.info("Tables don't exist. Creating schema...")
                
                # Create clients table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clients (
                        id SERIAL PRIMARY KEY,
                        pabau_id INTEGER NOT NULL UNIQUE,
                        custom_id VARCHAR(100),
                        mailchimp_id VARCHAR(100),
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        salutation VARCHAR(50),
                        gender VARCHAR(20),
                        dob DATE,
                        location VARCHAR(100),
                        is_active SMALLINT DEFAULT 1,
                        email VARCHAR(255) NOT NULL UNIQUE,
                        phone VARCHAR(50),
                        mobile VARCHAR(50),
                        opt_in_email SMALLINT DEFAULT 0,
                        opt_in_sms SMALLINT DEFAULT 0,
                        opt_in_phone SMALLINT DEFAULT 0,
                        opt_in_post SMALLINT DEFAULT 0,
                        opt_in_newsletter SMALLINT DEFAULT 0,
                        created_date TIMESTAMP,
                        created_by_name VARCHAR(100),
                        created_by_id INTEGER,
                        total_spend DECIMAL(10,2) DEFAULT 0,
                        avg_spend DECIMAL(10,2) DEFAULT 0,
                        total_completed INTEGER DEFAULT 0,
                        total_pending INTEGER DEFAULT 0,
                        total_cancelled INTEGER DEFAULT 0,
                        total_visits INTEGER DEFAULT 0,
                        total_noshow INTEGER DEFAULT 0,
                        next_appt_date DATE,
                        last_appt_date DATE,
                        first_visit_date TIMESTAMP,
                        last_appt_service VARCHAR(255),
                        next_appt_service VARCHAR(255),
                        primary_source_name VARCHAR(255),
                        primary_source_id INTEGER,
                        age INTEGER,
                        mailing_postal VARCHAR(50),
                        mailchimp_status VARCHAR(20),
                        mailchimp_tags TEXT[],
                        pabau_last_synced_at TIMESTAMP,
                        mailchimp_last_synced_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("  ✅ Created clients table")
                
                # Create leads table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS leads (
                        id SERIAL PRIMARY KEY,
                        pabau_id INTEGER NOT NULL UNIQUE,
                        contact_id INTEGER,
                        mailchimp_id VARCHAR(100),
                        salutation VARCHAR(50),
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        email VARCHAR(255) NOT NULL UNIQUE,
                        phone VARCHAR(50),
                        mobile VARCHAR(50),
                        dob DATE,
                        mailing_street VARCHAR(255),
                        mailing_postal VARCHAR(50),
                        mailing_city VARCHAR(100),
                        mailing_county VARCHAR(100),
                        mailing_country VARCHAR(100),
                        is_active SMALLINT DEFAULT 1,
                        lead_status VARCHAR(50),
                        owner_id INTEGER,
                        owner_name VARCHAR(100),
                        location_id INTEGER,
                        location_name VARCHAR(100),
                        created_date TIMESTAMP,
                        updated_date TIMESTAMP,
                        converted_date TIMESTAMP,
                        pipeline_name VARCHAR(100),
                        pipeline_stage_id INTEGER,
                        pipeline_stage_name VARCHAR(100),
                        deal_value DECIMAL(10,2),
                        opt_in_email_mailchimp SMALLINT DEFAULT 0,
                        opt_in_email SMALLINT DEFAULT 0,
                        source_name VARCHAR(255),
                        source_id INTEGER,
                        mailchimp_status VARCHAR(20),
                        mailchimp_tags TEXT[],
                        pabau_last_synced_at TIMESTAMP,
                        mailchimp_last_synced_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("  ✅ Created leads table")
                
                # Create sync_logs table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sync_logs (
                        id SERIAL PRIMARY KEY,
                        entity_type VARCHAR(20),
                        entity_id INTEGER,
                        pabau_id INTEGER,
                        email VARCHAR(255),
                        action VARCHAR(50),
                        status VARCHAR(20),
                        message TEXT,
                        error_details TEXT,
                        field_changes JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("  ✅ Created sync_logs table")
            else:
                logger.info("✅ Core tables already exist")
                
                # Apply missing columns for field mapping update
                logger.info("  Applying column migrations if needed...")
                migration_columns_clients = [
                    ("total_spend", "DECIMAL(10,2) DEFAULT 0"),
                    ("avg_spend", "DECIMAL(10,2) DEFAULT 0"),
                    ("total_completed", "INTEGER DEFAULT 0"),
                    ("total_pending", "INTEGER DEFAULT 0"),
                    ("total_cancelled", "INTEGER DEFAULT 0"),
                    ("total_visits", "INTEGER DEFAULT 0"),
                    ("total_noshow", "INTEGER DEFAULT 0"),
                    ("next_appt_date", "DATE"),
                    ("last_appt_date", "DATE"),
                    ("first_visit_date", "TIMESTAMP"),
                    ("last_appt_service", "VARCHAR(255)"),
                    ("next_appt_service", "VARCHAR(255)"),
                    ("primary_source_name", "VARCHAR(255)"),
                    ("primary_source_id", "INTEGER"),
                    ("age", "INTEGER"),
                    ("mailing_postal", "VARCHAR(50)"),
                ]
                for col_name, col_type in migration_columns_clients:
                    cursor.execute(f"ALTER TABLE clients ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                
                migration_columns_leads = [
                    ("source_name", "VARCHAR(255)"),
                    ("source_id", "INTEGER"),
                ]
                for col_name, col_type in migration_columns_leads:
                    cursor.execute(f"ALTER TABLE leads ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                
                logger.info("  ✅ Column migrations applied")
            
            # Always ensure appointments table exists (may have been missed)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS appointments (
                    id SERIAL PRIMARY KEY,
                    client_db_id INTEGER,
                    client_pabau_id INTEGER NOT NULL,
                    pabau_appointment_id BIGINT,
                    appointment_date DATE,
                    appointment_time TIME,
                    appointment_datetime TIMESTAMP,
                    location VARCHAR(100),
                    service VARCHAR(255),
                    duration VARCHAR(50),
                    appointment_status VARCHAR(50),
                    appt_with VARCHAR(100),
                    created_by VARCHAR(100),
                    created_date TIMESTAMP,
                    cancellation_reason TEXT,
                    pabau_last_synced_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT appointments_client_required CHECK (client_pabau_id IS NOT NULL),
                    CONSTRAINT appointments_unique_key UNIQUE (client_pabau_id, appointment_datetime, service)
                )
            """)
            logger.info("  ✅ Appointments table ready")
            
            # Always ensure sync_progress table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_progress (
                    key TEXT PRIMARY KEY,
                    value INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes (IF NOT EXISTS is safe to re-run)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_clients_email ON clients(email)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_clients_pabau_id ON clients(pabau_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_clients_opt_in_email ON clients(opt_in_email)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_pabau_id ON leads(pabau_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sync_logs_created ON sync_logs(created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_appointments_client_pabau_id ON appointments(client_pabau_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_appointments_datetime ON appointments(appointment_datetime)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_appointments_status ON appointments(appointment_status)")
            logger.info("  ✅ All indexes ready")
            
            logger.success("✅ Database schema initialized!")
                
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def run_dashboard():
    """Run FastAPI dashboard"""
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    logger.info(f"Starting FastAPI dashboard on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)


def run_scheduler():
    """Run background scheduler"""
    logger.info("Starting background scheduler...")
    start_scheduler()


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting Pabau-Mailchimp Sync Service")
    logger.info("=" * 80)
    
    # Initialize database schema first
    init_database()
    
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Run dashboard in main thread (blocks)
    run_dashboard()
