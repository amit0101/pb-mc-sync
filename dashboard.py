"""
Simple Web Dashboard for Sync Logs
View sync operations and statistics in browser
Run with: uvicorn dashboard:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import os
import threading
from db.database import get_db
from datetime import datetime

app = FastAPI(title="Pabau-Mailchimp Sync Dashboard")

# Start background scheduler in a separate thread
def start_background_scheduler():
    """Start the scheduler in a background thread"""
    from scheduler import start_scheduler
    start_scheduler()

# Start scheduler on app startup
@app.on_event("startup")
async def startup_event():
    """Start background scheduler when app starts"""
    scheduler_thread = threading.Thread(target=start_background_scheduler, daemon=True)
    scheduler_thread.start()

# Templates
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)


@app.get("/health")
async def health_check():
    """Health check endpoint for Render"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    db = get_db()
    
    try:
        # Get summary
        summary = db.get_summary()
        
        # Get recent logs
        recent_logs = db.get_recent_logs(limit=100)
        
        # Get logs by action type
        pabau_sync_logs = db.get_logs_by_action('sync_pabau_client', limit=20)
        mailchimp_sync_logs = db.get_logs_by_action('sync_to_mailchimp', limit=20)
        unsubscribe_logs = db.get_logs_by_action('mailchimp_unsubscribe', limit=20)
        
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "summary": summary,
            "recent_logs": recent_logs,
            "pabau_sync_count": len(pabau_sync_logs),
            "mailchimp_sync_count": len(mailchimp_sync_logs),
            "unsubscribe_count": len(unsubscribe_logs),
            "current_time": datetime.now()
        })
    finally:
        db.close()


@app.get("/api/summary")
async def api_summary():
    """API endpoint for summary data"""
    db = get_db()
    try:
        summary = db.get_summary()
        return {"summary": summary}
    finally:
        db.close()


@app.get("/api/logs")
async def api_logs(limit: int = 100, action: str = None):
    """API endpoint for logs"""
    db = get_db()
    try:
        if action:
            logs = db.get_logs_by_action(action, limit=limit)
        else:
            logs = db.get_recent_logs(limit=limit)
        return {"logs": logs}
    finally:
        db.close()


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

