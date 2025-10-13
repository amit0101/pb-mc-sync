# Deployment Checklist

## Pre-Deployment

- [ ] Push code to GitHub/GitLab
- [ ] Verify `.env` file is NOT committed (should be in `.gitignore`)
- [ ] Test locally with `./start.sh` to ensure everything works

## Render Setup

### 1. PostgreSQL Database
- [ ] Create PostgreSQL database (free tier)
- [ ] Name: `pb-mc-sync-db`
- [ ] Save Internal Database URL
- [ ] Initialize schema: `psql <connection> < db/schema.sql`

### 2. Web Service
- [ ] Create new Web Service
- [ ] Connect Git repository
- [ ] Configure:
  - Build Command: `pip install -r requirements.txt`
  - Start Command: `python main.py`
  - Plan: Free
  
### 3. Environment Variables
Set these in Render Dashboard:
- [ ] `DATABASE_URL` - Internal database URL from Step 1
- [ ] `PABAU_API_KEY` - Your Pabau API key
- [ ] `MAILCHIMP_API_KEY` - Your Mailchimp API key
- [ ] `MAILCHIMP_SERVER` - Your Mailchimp server prefix (e.g., `us19`)
- [ ] `MAILCHIMP_LIST_ID` - Your Mailchimp list/audience ID

## Post-Deployment

- [ ] Wait for build to complete (~5 min)
- [ ] Visit dashboard URL: `https://your-app.onrender.com`
- [ ] Check logs for "Starting background scheduler..."
- [ ] Verify first sync runs successfully
- [ ] Monitor sync logs in dashboard

## Files Needed for Deployment

### Core Application
- ✅ `main.py` - Entry point (runs dashboard + scheduler)
- ✅ `dashboard.py` - FastAPI dashboard
- ✅ `scheduler.py` - Background sync scheduler
- ✅ `requirements.txt` - Python dependencies
- ✅ `templates/dashboard.html` - Dashboard UI

### Sync Scripts
- ✅ `scripts/sync/sync_pabau_to_db.py`
- ✅ `scripts/sync/fetch_mailchimp_unsubscribes.py`
- ✅ `scripts/sync/sync_db_to_mailchimp.py`

### Database & Config
- ✅ `db/database.py` - Database connection
- ✅ `db/schema.sql` - Database schema
- ✅ `config.py` - Settings management

### API Clients
- ✅ `clients/pabau_client.py`
- ✅ `clients/mailchimp_client.py`

### Utilities
- ✅ `utils/transforms.py` - Data transformations

## Optional (Not Needed for Production)
- ❌ `scripts/backfill/` - One-time backfill scripts
- ❌ `scripts/test_*.py` - Test/debugging scripts
- ❌ Data files (`.psv`, `.csv`)

## Quick Commands

### Deploy to Render
```bash
# 1. Commit and push
git add .
git commit -m "Ready for deployment"
git push origin main

# 2. Render will auto-deploy on push
# Monitor logs in Render Dashboard
```

### Initialize Database
```bash
# Get connection from Render Dashboard → Database → Connect
PGPASSWORD=<password> psql -h <host> -U <user> <database> < db/schema.sql
```

### Test Locally
```bash
# Make sure .env has correct values
./start.sh

# Visit http://localhost:8000
```

## Monitoring

### Dashboard
- URL: `https://your-app.onrender.com`
- Shows recent sync logs from database
- Refreshable view of system status

### Render Logs
- Go to Web Service → Logs tab
- Shows real-time sync execution
- Filter for errors with "ERROR" keyword

### Expected Log Messages
```
Starting background scheduler...
Sync will run every 3 hours
Running initial sync...
Step 1: Syncing Pabau to Database...
Step 2: Fetching Mailchimp unsubscribes...
Step 3: Syncing Database to Mailchimp...
Sync cycle completed
```

## Troubleshooting

### Service Won't Start
1. Check environment variables are set
2. Verify DATABASE_URL format: `postgresql://user:pass@host:port/db`
3. Check Render logs for Python errors

### Database Connection Failed
1. Ensure using **Internal Database URL** (not External)
2. Both services must be in same region
3. Check database is active (not suspended)

### Sync Not Running
1. Check logs for scheduler startup message
2. Verify API keys are correct
3. Check for rate limit errors in logs

### Free Tier Issues
- Service spins down after 15 min inactivity
- First request after spin-down takes 50+ seconds
- Database expires after 90 days (upgrade to Starter)

## Cost Breakdown

### Free Tier (Development)
- PostgreSQL: Free (expires in 90 days)
- Web Service: Free (spins down when idle)
- **Total: $0/month**

### Production (Recommended)
- PostgreSQL Starter: $7/month
- Web Service Starter: $7/month
- **Total: $14/month**

## Next Steps

1. ✅ Deploy to Render
2. ✅ Monitor first sync cycle
3. ✅ Verify data integrity
4. Consider upgrading database before 90-day expiration
5. Set up alerts/monitoring if needed
