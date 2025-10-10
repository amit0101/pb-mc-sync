# Pabau ↔ Mailchimp Sync System

Automated bidirectional sync system between Pabau CRM and Mailchimp marketing platform.

## 🎯 Features

- **Automated Sync**: Runs on scheduled cron jobs
  - Fast sync (every 30 min): Mailchimp ↔ Database
  - Slow sync (every 4 hours): Pabau → Database
- **Web Dashboard**: Real-time view of sync status and logs
- **Smart Filtering**: Only syncs opted-in contacts
- **Incremental Updates**: Only processes new/changed records
- **Robust Error Handling**: Retry logic and comprehensive logging

## 📊 System Architecture

```
┌─────────────────────────────────────────┐
│         RENDER PRODUCTION               │
├─────────────────────────────────────────┤
│                                         │
│  📊 Web Dashboard                       │
│  └─ Real-time sync logs & stats        │
│                                         │
│  🔄 Fast Sync (30 min)                  │
│  ├─ Fetch Mailchimp unsubscribes       │
│  └─ Push DB changes to Mailchimp       │
│                                         │
│  🐌 Slow Sync (4 hours)                 │
│  └─ Fetch Pabau clients → Database     │
│                                         │
│  💾 PostgreSQL Database                 │
│  ├─ clients (42K records)              │
│  ├─ leads (6K records)                 │
│  ├─ appointments (2K records)          │
│  └─ sync_logs                          │
│                                         │
└─────────────────────────────────────────┘
```

## 🚀 Quick Deploy

See [DEPLOY_NOW.md](DEPLOY_NOW.md) for complete deployment instructions.

### Requirements
- Render account
- Pabau API credentials
- Mailchimp API credentials
- PostgreSQL database (Render provides)

### Deploy in 5 minutes

1. **Push to GitHub** (you're here!)
2. **Deploy on Render**: New → Blueprint → Connect repo
3. **Set environment variables** in Render dashboard
4. **Initialize database schema**
5. **View dashboard** at your Render URL

## 📁 Project Structure

```
pb-mc-sync/
├── render.yaml              # Render service configuration
├── dashboard.py             # Web dashboard
├── config.py                # Configuration
├── clients/                 # API clients
│   ├── pabau_client.py      # Pabau API wrapper
│   └── mailchimp_client.py  # Mailchimp API wrapper
├── db/                      # Database layer
│   └── database.py          # PostgreSQL operations
├── scripts/
│   ├── sync/                # Production sync scripts
│   │   ├── sync_pabau_to_db.py
│   │   ├── sync_db_to_mailchimp.py
│   │   ├── fetch_mailchimp_unsubscribes.py
│   │   └── sync_all.py
│   └── backfill/            # One-time data loading
├── templates/
│   └── dashboard.html       # Dashboard UI
└── utils/
    └── transforms.py        # Data transformations
```

## 🔄 Sync Behavior

### Fast Sync (30 minutes)
- **Runtime**: ~30 seconds
- Fetches unsubscribed members from Mailchimp
- Updates `opt_in_email = 0` in database
- Pushes database changes to Mailchimp
- Uses `updated_at` timestamp for incremental sync

### Slow Sync (4 hours)
- **Runtime**: ~20 minutes (fetches 50K records)
- Fetches ALL clients from Pabau API
- Filters by `created_date` in application
- Only inserts new clients
- Updates `pabau_last_synced_at` timestamp

## 💰 Cost

- **PostgreSQL**: $7/month (Starter plan)
- **Web Dashboard**: FREE (free tier)
- **Cron Jobs**: FREE (2 jobs)
- **Total**: $7/month

## 🔧 Environment Variables

Required for deployment:

```bash
# Pabau API
PABAU_API_KEY=your_pabau_api_key
PABAU_API_URL=https://api.pabau.com
PABAU_COMPANY_ID=your_company_id

# Mailchimp API
MAILCHIMP_API_KEY=your_mailchimp_api_key
MAILCHIMP_API_URL=https://usX.api.mailchimp.com
MAILCHIMP_LIST_ID=your_list_id

# Database (auto-set by Render)
DATABASE_URL=postgresql://...
```

## 📊 Dashboard

Once deployed, access your dashboard at:
- **URL**: `https://your-service-name.onrender.com`
- **Features**:
  - Real-time sync statistics
  - Recent sync logs (last 100 operations)
  - Success/error counts
  - Auto-refresh every 30 seconds

## 🛠️ Local Development

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp env.template .env
# Edit .env with your credentials

# Initialize database
psql $DATABASE_URL < DATABASE_SCHEMA_FINAL.sql
```

### Run Dashboard Locally
```bash
uvicorn dashboard:app --reload --port 8000
# Open http://localhost:8000
```

### Run Sync Manually
```bash
# Pabau to Database
python scripts/sync/sync_pabau_to_db.py

# Database to Mailchimp
python scripts/sync/sync_db_to_mailchimp.py

# Fetch Mailchimp unsubscribes
python scripts/sync/fetch_mailchimp_unsubscribes.py

# Run all
python scripts/sync/sync_all.py
```

## 📦 Backfill Scripts

For initial data load (one-time use):

```bash
# 1. Fetch & load clients
python scripts/backfill/01a_fetch_clients_to_file.py
python scripts/backfill/01c_bulk_load_clients.py

# 2. Fetch & load leads
python scripts/backfill/02a_fetch_leads_to_file.py
python scripts/backfill/02b_bulk_load_leads.py

# 3. Load Mailchimp status
python scripts/backfill/03a_fetch_mailchimp_status_to_file.py
python scripts/backfill/03b_load_mailchimp_status.py

# 4. Upload to Mailchimp
python scripts/backfill/04b_bulk_upload_clients_to_mailchimp.py
```

## 📝 Documentation

- [DEPLOY_NOW.md](DEPLOY_NOW.md) - Quick deployment guide
- [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md) - Detailed deployment steps
- [SYNC_SYSTEM_COMPLETE.md](SYNC_SYSTEM_COMPLETE.md) - System documentation

## 🔒 Security Notes

- Never commit `.env` file (already in `.gitignore`)
- Set environment variables in Render dashboard
- Use Render's database connection string (auto-injected)
- Enable IP whitelist on database if needed

## 📈 Monitoring

### Via Dashboard
- Open your Render URL
- View real-time stats and logs
- Check for errors

### Via Database
```sql
-- Check recent syncs
SELECT * FROM sync_logs 
ORDER BY created_at DESC 
LIMIT 20;

-- Check sync counts by action
SELECT action, status, COUNT(*) 
FROM sync_logs 
GROUP BY action, status;

-- Check data counts
SELECT 
    (SELECT COUNT(*) FROM clients) as clients,
    (SELECT COUNT(*) FROM leads) as leads,
    (SELECT COUNT(*) FROM appointments) as appointments;
```

## 🆘 Troubleshooting

### Sync not running
1. Check cron job status in Render dashboard
2. View logs for errors
3. Verify environment variables are set
4. Check database connection

### No data syncing
1. Check `pabau_last_synced_at` timestamps
2. Verify API credentials
3. Check if there are new records to sync
4. Review `sync_logs` table for errors

### Dashboard not loading
1. Check web service status
2. Verify `DATABASE_URL` is set
3. Check database connection
4. View logs for startup errors

## 📄 License

Proprietary - Internal use only

## 👥 Support

For issues or questions, check the documentation or contact the development team.

---

**Status**: ✅ Production Ready

**Last Updated**: October 2025
