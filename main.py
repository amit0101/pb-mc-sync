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
                
                # Read and execute schema file
                schema_path = os.path.join(os.path.dirname(__file__), 'DATABASE_SCHEMA_FINAL.sql')
                if os.path.exists(schema_path):
                    with open(schema_path, 'r') as f:
                        schema_sql = f.read()
                    cursor.execute(schema_sql)
                    logger.success("✅ Database schema created successfully!")
                else:
                    logger.error(f"Schema file not found: {schema_path}")
            else:
                logger.info("✅ Database tables already exist")
                
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
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
