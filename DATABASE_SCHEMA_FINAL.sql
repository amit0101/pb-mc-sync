-- ============================================================================
-- PABAU-MAILCHIMP SYNC - FINAL ALIGNED DATABASE SCHEMA
-- ============================================================================
-- Platform: Render PostgreSQL (Minimum setup)
-- Strategy: Database as single source of truth
-- Design: Field names and data types match actual Pabau/Mailchimp APIs
-- ============================================================================

-- ============================================================================
-- TABLE 1: clients
-- Pabau clients - mirrors Pabau API structure exactly
-- ============================================================================
CREATE TABLE clients (
    -- Database primary key
    id SERIAL PRIMARY KEY,
    
    -- Pabau identifiers (from details object)
    pabau_id INTEGER NOT NULL UNIQUE,           -- details.id (Pabau system ID)
    custom_id VARCHAR(100),                     -- details.custom_id (custom client ID)
    
    -- Mailchimp identifier
    mailchimp_id VARCHAR(100),                  -- MD5 hash of lowercase email
    
    -- Basic info (from details object)
    first_name VARCHAR(100),                    -- details.first_name
    last_name VARCHAR(100),                     -- details.last_name
    salutation VARCHAR(50),                     -- details.salutation
    gender VARCHAR(20),                         -- details.gender
    dob DATE,                                   -- details.DOB
    location VARCHAR(100),                      -- details.location
    is_active SMALLINT DEFAULT 1,               -- details.is_active (0 or 1)
    
    -- Communications (from communications object) - EXACT Pabau field names
    email VARCHAR(255) NOT NULL UNIQUE,         -- communications.email
    phone VARCHAR(50),                          -- communications.phone
    mobile VARCHAR(50),                         -- communications.mobile
    opt_in_email SMALLINT DEFAULT 0,            -- communications.opt_in_email (0 or 1) - SOURCE OF TRUTH
    opt_in_sms SMALLINT DEFAULT 0,              -- communications.opt_in_sms (0 or 1)
    opt_in_phone SMALLINT DEFAULT 0,            -- communications.opt_in_phone (0 or 1)
    opt_in_post SMALLINT DEFAULT 0,             -- communications.opt_in_post (0 or 1)
    opt_in_newsletter SMALLINT DEFAULT 0,       -- communications.opt_in_newsletter (0 or 1)
    
    -- Created info (from created object)
    created_date TIMESTAMP,                     -- created.created_date
    created_by_name VARCHAR(100),               -- created.owner[0].full_name
    created_by_id INTEGER,                      -- created.owner[0].created_by_id
    
    -- Mailchimp sync fields (matches Mailchimp API)
    mailchimp_status VARCHAR(20),               -- 'subscribed', 'unsubscribed', 'cleaned', 'pending'
    mailchimp_tags TEXT[],                      -- Array of tag names
    
    -- Sync timestamps
    pabau_last_synced_at TIMESTAMP,             -- When we last fetched from Pabau
    mailchimp_last_synced_at TIMESTAMP,         -- When we last pushed to Mailchimp
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT clients_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    CONSTRAINT clients_opt_in_email_valid CHECK (opt_in_email IN (0, 1)),
    CONSTRAINT clients_opt_in_sms_valid CHECK (opt_in_sms IN (0, 1)),
    CONSTRAINT clients_opt_in_phone_valid CHECK (opt_in_phone IN (0, 1))
);

-- Indexes
CREATE INDEX idx_clients_email ON clients(email);
CREATE INDEX idx_clients_pabau_id ON clients(pabau_id);
CREATE INDEX idx_clients_mailchimp_id ON clients(mailchimp_id);
CREATE INDEX idx_clients_opt_in_email ON clients(opt_in_email);
CREATE INDEX idx_clients_is_active ON clients(is_active);

COMMENT ON TABLE clients IS 'Pabau clients - field names and types match Pabau API exactly';
COMMENT ON COLUMN clients.opt_in_email IS 'Email marketing consent: 0=opted out, 1=opted in (matches Pabau API)';


-- ============================================================================
-- TABLE 2: leads
-- Pabau leads - mirrors Pabau API structure exactly
-- ============================================================================
CREATE TABLE leads (
    -- Database primary key
    id SERIAL PRIMARY KEY,
    
    -- Pabau identifiers
    pabau_id INTEGER NOT NULL UNIQUE,           -- id from /leads endpoint
    contact_id INTEGER,                         -- contact_id (if converted to client)
    
    -- Mailchimp identifier
    mailchimp_id VARCHAR(100),                  -- MD5 hash of lowercase email
    
    -- Basic info (root level fields in /leads response)
    salutation VARCHAR(50),                     -- salutation
    first_name VARCHAR(100),                    -- first_name
    last_name VARCHAR(100),                     -- last_name
    email VARCHAR(255) NOT NULL UNIQUE,         -- email
    phone VARCHAR(50),                          -- phone
    mobile VARCHAR(50),                         -- mobile
    dob DATE,                                   -- DOB
    
    -- Address fields
    mailing_street VARCHAR(255),                -- mailing_street
    mailing_postal VARCHAR(50),                 -- mailing_postal
    mailing_city VARCHAR(100),                  -- mailing_city
    mailing_county VARCHAR(100),                -- mailing_county
    mailing_country VARCHAR(100),               -- mailing_country
    
    -- Status fields
    is_active SMALLINT DEFAULT 1,               -- is_active (0 or 1)
    lead_status VARCHAR(50),                    -- lead_status ('Open', 'Won', 'Lost')
    
    -- Owner and location (from objects)
    owner_id INTEGER,                           -- owner.id
    owner_name VARCHAR(100),                    -- owner.name
    location_id INTEGER,                        -- location.id
    location_name VARCHAR(100),                 -- location.name
    
    -- Dates (from dates object)
    created_date TIMESTAMP,                     -- dates.created_date
    updated_date TIMESTAMP,                     -- dates.updated_date
    converted_date TIMESTAMP,                   -- dates.converted_date
    
    -- Pipeline (from pipeline object)
    pipeline_name VARCHAR(100),                 -- pipeline.name
    pipeline_stage_id INTEGER,                  -- pipeline.stage.pipeline_stage_id
    pipeline_stage_name VARCHAR(100),           -- pipeline.stage.pipeline_stage_name
    
    -- Deal
    deal_value DECIMAL(10,2),                   -- deal_value
    
    -- Custom field for marketing consent (WORKAROUND for API limitation)
    -- This is from Pabau custom_fields array, converted to match opt_in format
    opt_in_email_mailchimp SMALLINT DEFAULT 0,  -- 0 or 1 (from custom field, matches Pabau format)
    
    -- Derived field for consistency (same as opt_in_email_mailchimp)
    opt_in_email SMALLINT DEFAULT 0,            -- 1 if opt_in_email_mailchimp = 1
    
    -- Mailchimp sync fields
    mailchimp_status VARCHAR(20),               -- 'subscribed', 'unsubscribed', etc.
    mailchimp_tags TEXT[],
    
    -- Sync timestamps
    pabau_last_synced_at TIMESTAMP,
    mailchimp_last_synced_at TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT leads_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    CONSTRAINT leads_opt_in_email_mailchimp_valid CHECK (opt_in_email_mailchimp IN (0, 1)),
    CONSTRAINT leads_opt_in_email_valid CHECK (opt_in_email IN (0, 1))
);

-- Indexes
CREATE INDEX idx_leads_email ON leads(email);
CREATE INDEX idx_leads_pabau_id ON leads(pabau_id);
CREATE INDEX idx_leads_mailchimp_id ON leads(mailchimp_id);
CREATE INDEX idx_leads_opt_in_email ON leads(opt_in_email);
CREATE INDEX idx_leads_consent ON leads(opt_in_email_mailchimp);
CREATE INDEX idx_leads_status ON leads(lead_status);
CREATE INDEX idx_leads_contact_id ON leads(contact_id);

COMMENT ON TABLE leads IS 'Pabau leads - field names match Pabau API exactly';
COMMENT ON COLUMN leads.opt_in_email_mailchimp IS 'From Pabau custom field - converted to 0/1 format matching other opt_in fields';
COMMENT ON COLUMN leads.opt_in_email IS 'Same as opt_in_email_mailchimp for consistency with clients table';


-- ============================================================================
-- TABLE 3: sync_logs
-- Simple operation log
-- ============================================================================
CREATE TABLE sync_logs (
    id SERIAL PRIMARY KEY,
    
    -- What
    entity_type VARCHAR(20),                    -- 'client' or 'lead'
    entity_id INTEGER,                          -- ID in clients or leads table
    pabau_id INTEGER,                           -- Pabau client_id or lead_id
    email VARCHAR(255),
    
    -- Action
    action VARCHAR(50),                         -- 'fetch_from_pabau', 'push_to_mailchimp', 'webhook_unsubscribe', etc.
    status VARCHAR(20),                         -- 'success', 'error', 'skipped'
    
    -- Details
    message TEXT,
    error_details TEXT,
    
    -- Optional: what changed
    field_changes JSONB,                        -- {"opt_in_email": {"from": 0, "to": 1}}
    
    -- When
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_sync_logs_created ON sync_logs(created_at DESC);
CREATE INDEX idx_sync_logs_email ON sync_logs(email);
CREATE INDEX idx_sync_logs_entity ON sync_logs(entity_type, entity_id);
CREATE INDEX idx_sync_logs_status ON sync_logs(status);

COMMENT ON TABLE sync_logs IS 'Simple operation log - all sync activities recorded here';


-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER clients_updated 
    BEFORE UPDATE ON clients
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER leads_updated 
    BEFORE UPDATE ON leads
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();


-- Auto-sync lead opt_in_email from opt_in_email_mailchimp
CREATE OR REPLACE FUNCTION sync_lead_opt_in()
RETURNS TRIGGER AS $$
BEGIN
    -- Set opt_in_email same as opt_in_email_mailchimp for consistency
    NEW.opt_in_email := COALESCE(NEW.opt_in_email_mailchimp, 0);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER leads_sync_opt_in 
    BEFORE INSERT OR UPDATE ON leads
    FOR EACH ROW EXECUTE FUNCTION sync_lead_opt_in();

COMMENT ON FUNCTION sync_lead_opt_in IS 'Auto-set opt_in_email to match opt_in_email_mailchimp';


-- Ensure email uniqueness across both tables
CREATE OR REPLACE FUNCTION check_email_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'clients' THEN
        IF EXISTS(SELECT 1 FROM leads WHERE email = NEW.email) THEN
            RAISE EXCEPTION 'Email % already exists in leads table', NEW.email;
        END IF;
    ELSE  -- leads table
        IF EXISTS(SELECT 1 FROM clients WHERE email = NEW.email) THEN
            RAISE EXCEPTION 'Email % already exists in clients table', NEW.email;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER clients_email_unique 
    BEFORE INSERT OR UPDATE ON clients
    FOR EACH ROW EXECUTE FUNCTION check_email_unique();

CREATE TRIGGER leads_email_unique 
    BEFORE INSERT OR UPDATE ON leads
    FOR EACH ROW EXECUTE FUNCTION check_email_unique();


-- ============================================================================
-- VIEWS
-- ============================================================================

-- All contacts unified view
CREATE VIEW v_all_contacts AS
SELECT 
    'client' as contact_type,
    id,
    pabau_id,
    email,
    first_name,
    last_name,
    mobile,
    opt_in_email,
    mailchimp_status,
    mailchimp_id,
    created_at
FROM clients
WHERE is_active = 1
UNION ALL
SELECT 
    'lead' as contact_type,
    id,
    pabau_id,
    email,
    first_name,
    last_name,
    mobile,
    opt_in_email,
    mailchimp_status,
    mailchimp_id,
    created_at
FROM leads
WHERE is_active = 1;


-- Contacts that should be in Mailchimp (opted in only)
CREATE VIEW v_mailchimp_contacts AS
SELECT 
    'client' as contact_type,
    pabau_id,
    email,
    first_name,
    last_name,
    mobile as phone,
    mailchimp_id,
    mailchimp_status,
    'Pabau Client' as tag,
    CASE WHEN opt_in_email = 1 THEN 'subscribed' ELSE 'unsubscribed' END as desired_status
FROM clients
WHERE opt_in_email = 1  -- Only opted-in clients
UNION ALL
SELECT 
    'lead' as contact_type,
    pabau_id,
    email,
    first_name,
    last_name,
    mobile as phone,
    mailchimp_id,
    mailchimp_status,
    'Pabau Lead' as tag,
    CASE WHEN opt_in_email = 1 THEN 'subscribed' ELSE 'unsubscribed' END as desired_status
FROM leads
WHERE opt_in_email = 1;  -- Only opted-in leads


-- Summary statistics
CREATE VIEW v_summary AS
SELECT 
    'Clients' as category,
    COUNT(*) as total,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active,
    SUM(CASE WHEN opt_in_email = 1 THEN 1 ELSE 0 END) as opted_in_email,
    SUM(CASE WHEN mailchimp_status = 'subscribed' THEN 1 ELSE 0 END) as in_mailchimp,
    SUM(CASE WHEN opt_in_email = 1 AND (mailchimp_status IS NULL OR mailchimp_status != 'subscribed') THEN 1 ELSE 0 END) as needs_sync
FROM clients
UNION ALL
SELECT 
    'Leads' as category,
    COUNT(*) as total,
    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active,
    SUM(CASE WHEN opt_in_email = 1 THEN 1 ELSE 0 END) as opted_in_email,
    SUM(CASE WHEN mailchimp_status = 'subscribed' THEN 1 ELSE 0 END) as in_mailchimp,
    SUM(CASE WHEN opt_in_email = 1 AND (mailchimp_status IS NULL OR mailchimp_status != 'subscribed') THEN 1 ELSE 0 END) as needs_sync
FROM leads;


-- Recent sync activity
CREATE VIEW v_recent_activity AS
SELECT 
    entity_type,
    email,
    action,
    status,
    message,
    created_at
FROM sync_logs
WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY created_at DESC
LIMIT 100;


-- ============================================================================
-- USEFUL QUERIES
-- ============================================================================

-- Find contact by email
/*
SELECT * FROM v_all_contacts WHERE email = 'user@example.com';
*/

-- Get all contacts for Mailchimp sync (opted in only)
/*
SELECT * FROM v_mailchimp_contacts;
*/

-- Find clients that need syncing to Mailchimp
/*
SELECT * FROM clients 
WHERE opt_in_email = 1 
  AND (mailchimp_status IS NULL OR mailchimp_status != 'subscribed')
ORDER BY pabau_id;
*/

-- Check sync logs for errors
/*
SELECT * FROM sync_logs 
WHERE status = 'error' 
  AND created_at > NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC;
*/

-- Summary stats
/*
SELECT * FROM v_summary;
*/

-- Count by opt-in status
/*
SELECT 
    opt_in_email,
    COUNT(*) as count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM clients) as percentage
FROM clients
GROUP BY opt_in_email;
*/


-- ============================================================================
-- RENDER POSTGRESQL SETUP INSTRUCTIONS
-- ============================================================================
/*
STEP 1: Create PostgreSQL in Render
------------------------------------
1. Go to Render Dashboard: https://dashboard.render.com
2. Click "New +" → "PostgreSQL"
3. Settings:
   - Name: pabau-mailchimp-sync
   - Database: pabau_mailchimp (auto-created)
   - User: pabau_user (auto-created)
   - Region: Choose closest to you
   - Plan: 
     * Free: Good for testing (expires after 90 days)
     * Starter ($7/mo): Good for production
4. Click "Create Database"
5. Wait 2-3 minutes for provisioning


STEP 2: Note Connection Details
--------------------------------
After creation, you'll see:
- Internal Database URL: Use this in your app
- External Database URL: Use this for external connections
- PSQL Command: Quick connect command


STEP 3: Run This Schema
------------------------
Method A - Using psql locally:
psql "<External_Database_URL>" < DATABASE_SCHEMA_FINAL.sql

Method B - Using Render Shell:
1. Click "Shell" tab in your database
2. Copy-paste this entire SQL file
3. Execute


STEP 4: Set Environment Variable
---------------------------------
In your Render Web Service:
1. Go to Environment tab
2. Add: DATABASE_URL = <Internal_Database_URL>
3. Save


STEP 5: Verify Setup
--------------------
Connect and check:
psql "<External_Database_URL>"

Then run:
\dt                     -- List tables (should see clients, leads, sync_logs)
\d clients              -- Describe clients table
SELECT * FROM v_summary; -- Check summary view

You're done! Database is ready.


COST ESTIMATE
-------------
PostgreSQL on Render:
- Free: $0/month (90 days, 1GB storage)
- Starter: $7/month (10GB storage, 1GB RAM)
- Standard: $20/month (100GB storage, 2GB RAM)

Recommendation: Start with Free for testing, upgrade to Starter for production.
*/


-- ============================================================================
-- DATA TYPE ALIGNMENT NOTES
-- ============================================================================
/*
PABAU API → DATABASE ALIGNMENT:
- Pabau uses integers (0, 1) for boolean flags → We use SMALLINT (not BOOLEAN)
- Pabau uses VARCHAR for IDs → We store as INTEGER where appropriate
- Pabau timestamps → We use TIMESTAMP (PostgreSQL native)
- Pabau arrays → We use PostgreSQL arrays (TEXT[])

MAILCHIMP API → DATABASE ALIGNMENT:
- Mailchimp status strings → We use VARCHAR(20)
- Mailchimp member_id (MD5 hash) → We use VARCHAR(100)
- Mailchimp tags → We use TEXT[] array

This ensures direct API-to-DB and DB-to-API mapping with no conversion needed!
*/


-- ============================================================================
-- END OF SCHEMA
-- ============================================================================

