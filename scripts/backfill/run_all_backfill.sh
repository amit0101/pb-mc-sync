#!/bin/bash
################################################################################
# Run ALL backfill scripts in order
# Execute once to populate database with initial data
################################################################################

set -e  # Exit on error

echo "================================================================================"
echo "BACKFILL ALL DATA - COMPLETE INITIAL LOAD"
echo "================================================================================"
echo ""
echo "This will:"
echo "  1. Load ALL clients from Pabau → Database"
echo "  2. Load ALL leads from Pabau → Database"
echo "  3. Fetch Mailchimp data for all contacts → Database"
echo ""
echo "⚠️  WARNING: This is a ONE-TIME operation"
echo "   For ongoing sync, use the incremental sync scripts"
echo ""

read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled"
    exit 0
fi

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/../.."

echo ""
echo "================================================================================"
echo "STEP 1/3: Backfill Pabau Clients"
echo "================================================================================"
python scripts/backfill/01_backfill_pabau_clients.py
if [ $? -ne 0 ]; then
    echo "❌ Step 1 failed!"
    exit 1
fi

echo ""
echo "================================================================================"
echo "STEP 2/3: Backfill Pabau Leads"
echo "================================================================================"
python scripts/backfill/02_backfill_pabau_leads.py
if [ $? -ne 0 ]; then
    echo "❌ Step 2 failed!"
    exit 1
fi

echo ""
echo "================================================================================"
echo "STEP 3/3: Backfill Mailchimp Data"
echo "================================================================================"
python scripts/backfill/03_backfill_mailchimp_data.py
if [ $? -ne 0 ]; then
    echo "❌ Step 3 failed!"
    exit 1
fi

echo ""
echo "================================================================================"
echo "✅ ALL BACKFILLS COMPLETE!"
echo "================================================================================"
echo ""
echo "Database is now populated with:"
echo "  - All clients from Pabau"
echo "  - All leads from Pabau"
echo "  - Mailchimp status for all contacts"
echo ""
echo "Next steps:"
echo "  1. Review logs: psql \$DATABASE_URL -c \"SELECT * FROM v_recent_activity LIMIT 20;\""
echo "  2. Check summary: psql \$DATABASE_URL -c \"SELECT * FROM v_summary;\""
echo "  3. Set up incremental sync: See scripts/sync/"
echo ""

