#!/usr/bin/env python3
"""
Main entry point for Render deployment
Runs both the FastAPI dashboard and background scheduler in separate threads
"""

import threading
from loguru import logger

# Import dashboard and scheduler
from dashboard import app
from scheduler import start_scheduler


def run_dashboard():
    """Run FastAPI dashboard"""
    import uvicorn
    logger.info("Starting FastAPI dashboard on port 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)


def run_scheduler():
    """Run background scheduler"""
    logger.info("Starting background scheduler...")
    start_scheduler()


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting Pabau-Mailchimp Sync Service")
    logger.info("=" * 80)
    
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Run dashboard in main thread (blocks)
    run_dashboard()
