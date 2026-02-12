-- ============================================================================
-- Migration 002: Add fields needed for Mailchimp field update
-- ============================================================================
-- Adds columns to clients table for: total_spend, avg_spend, next_appt_date,
-- total_completed, last_appt_date, last_appt_service, primary_source_name, age
-- Adds columns to leads table for: source_name, source_id
-- Safe to run multiple times (uses IF NOT EXISTS)
-- ============================================================================

-- ==============================
-- CLIENTS TABLE - New columns
-- ==============================

-- Business metrics from Pabau client_insights
ALTER TABLE clients ADD COLUMN IF NOT EXISTS total_spend DECIMAL(10,2) DEFAULT 0;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS avg_spend DECIMAL(10,2) DEFAULT 0;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS total_completed INTEGER DEFAULT 0;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS total_pending INTEGER DEFAULT 0;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS total_cancelled INTEGER DEFAULT 0;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS total_visits INTEGER DEFAULT 0;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS total_noshow INTEGER DEFAULT 0;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS next_appt_date DATE;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS last_appt_date DATE;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS first_visit_date TIMESTAMP;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS last_appt_service VARCHAR(255);
ALTER TABLE clients ADD COLUMN IF NOT EXISTS next_appt_service VARCHAR(255);

-- Source info (expanded from Pabau source array)
ALTER TABLE clients ADD COLUMN IF NOT EXISTS primary_source_name VARCHAR(255);
ALTER TABLE clients ADD COLUMN IF NOT EXISTS primary_source_id INTEGER;

-- Calculated fields
ALTER TABLE clients ADD COLUMN IF NOT EXISTS age INTEGER;

-- Address field (for Postcode merge field)
ALTER TABLE clients ADD COLUMN IF NOT EXISTS mailing_postal VARCHAR(50);

-- Indexes for new columns
CREATE INDEX IF NOT EXISTS idx_clients_total_spend ON clients(total_spend);
CREATE INDEX IF NOT EXISTS idx_clients_total_completed ON clients(total_completed);
CREATE INDEX IF NOT EXISTS idx_clients_last_appt_date ON clients(last_appt_date);
CREATE INDEX IF NOT EXISTS idx_clients_next_appt_date ON clients(next_appt_date);
CREATE INDEX IF NOT EXISTS idx_clients_last_appt_service ON clients(last_appt_service);

-- ==============================
-- LEADS TABLE - New columns
-- ==============================

-- Source info (from Pabau lead source object)
ALTER TABLE leads ADD COLUMN IF NOT EXISTS source_name VARCHAR(255);
ALTER TABLE leads ADD COLUMN IF NOT EXISTS source_id INTEGER;

-- Index
CREATE INDEX IF NOT EXISTS idx_leads_source_name ON leads(source_name);
