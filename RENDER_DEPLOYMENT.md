# Render Deployment Guide

## Overview
Deploy the Pabau-Mailchimp sync system to Render with:
- 1 PostgreSQL database (free tier)
- 1 Web service (free tier) running dashboard + background sync

## Prerequisites
- Render account
- Git repository pushed to GitHub/GitLab

---

## Step 1: Create PostgreSQL Database

1. Go to Render Dashboard → **New** → **PostgreSQL**
2. Configure:
   - **Name**: `pb-mc-sync-db`
   - **Database**: `pb_mc_sync`
   - **User**: `pb_mc_sync_user`
   - **Region**: Oregon (us-west)
   - **Plan**: Free
3. Click **Create Database**
4. Wait for provisioning (5-10 minutes)
5. **Save the Internal Database URL** (starts with `postgresql://`)

---

## Step 2: Initialize Database Schema

1. Once database is ready, click **Connect** → **PSQL Command**
2. Copy the command and run it locally:
   ```bash
   PGPASSWORD=<password> psql -h <hostname> -U <user> <database>
   ```
3. Run the schema from `db/schema.sql`:
   ```bash
   PGPASSWORD=<password> psql -h <hostname> -U <user> <database> < db/schema.sql
   ```

---

## Step 3: Create Web Service

1. Go to Render Dashboard → **New** → **Web Service**
2. Connect your Git repository
3. Configure:
   - **Name**: `pb-mc-sync`
   - **Region**: Oregon (us-west) - same as database
   - **Branch**: `main`
   - **Root Directory**: (leave empty)
   - **Runtime**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python main.py`
   - **Plan**: Free

4. **Environment Variables** - Add these:
   ```
   DATABASE_URL=<internal-database-url-from-step-1>
   PABAU_API_KEY=<your-pabau-api-key>
   MAILCHIMP_API_KEY=<your-mailchimp-api-key>
   MAILCHIMP_SERVER=<your-mailchimp-server-prefix>
   MAILCHIMP_LIST_ID=<your-mailchimp-list-id>
   ```

5. Click **Create Web Service**

---

## Step 4: Verify Deployment

1. Wait for the build to complete (~5 minutes)
2. Once deployed, visit your web service URL (e.g., `https://pb-mc-sync.onrender.com`)
3. You should see the dashboard showing sync logs
4. Check the logs to see:
   - "Starting background scheduler..."
   - "Running initial sync..."
   - Sync should run every 3 hours automatically

---

## How It Works

### Main Process (`main.py`)
- Runs FastAPI dashboard on port 8000 (responds to Render health checks)
- Starts background scheduler in a separate thread

### Background Scheduler (`scheduler.py`)
- Runs full sync cycle every 3 hours:
  1. Pabau → Database (takes ~30 min)
  2. Mailchimp unsubscribes → Database (fast)
  3. Database → Mailchimp (fast, only if new data)

### Dashboard (`dashboard.py`)
- Shows recent sync logs
- Accessible at `https://your-app.onrender.com`

---

## Free Tier Limitations

### PostgreSQL Free Tier:
- 256 MB RAM
- 1 GB Storage
- **Expires after 90 days** - must upgrade to paid plan
- For production: **Upgrade to Starter ($7/month)** for:
  - 1 GB RAM
  - 10 GB Storage
  - No expiration
  - Automated daily backups

### Web Service Free Tier:
- 512 MB RAM
- Spins down after 15 min of inactivity
- **First request after spin-down takes 50+ seconds**
- No bandwidth limits
- For production: Consider **Starter ($7/month)** for always-on service

---

## Monitoring

### View Logs:
1. Go to your web service in Render Dashboard
2. Click **Logs** tab
3. You'll see:
   - Sync start/completion messages
   - Number of records synced
   - Any errors

### View Dashboard:
- Visit `https://your-app.onrender.com`
- Shows recent sync activity from database

---

## Troubleshooting

### Service won't start:
- Check **Logs** for Python errors
- Verify all environment variables are set correctly
- Ensure DATABASE_URL is the **Internal Database URL**

### Sync not running:
- Check logs for scheduler messages
- Verify environment variables (PABAU_API_KEY, MAILCHIMP_API_KEY)
- Check database connectivity

### Database connection errors:
- Ensure you're using the **Internal Database URL** (not External)
- Both services must be in the **same region**

---

## Scaling for Production

When ready for production:

1. **Upgrade Database** to Starter plan ($7/month)
   - Removes 90-day expiration
   - Adds daily backups
   - More storage and RAM

2. **Upgrade Web Service** to Starter plan ($7/month)
   - Prevents spin-down
   - Faster sync execution
   - Better reliability

3. **Optional**: Split into separate services:
   - Web Service 1: Dashboard only (can stay free tier)
   - Web Service 2: Background sync worker (upgrade to Starter)

**Total Cost**: $14-21/month for production-grade setup

---

## Next Steps

After deployment:
1. Monitor first few sync cycles
2. Check dashboard for any errors
3. Verify data is syncing correctly
4. Consider upgrading database before 90-day expiration

