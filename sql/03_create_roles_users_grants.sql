-- =============================================================================
-- Script : 03_create_roles_users_grants.sql
-- Purpose: Create the DASHBOARD_READONLY_ROLE and grant minimal privileges
--          required for dashboard tools (Tableau, Power BI) to query MART tables
-- Run as : SECURITYADMIN (role creation) and SYSADMIN (grants on objects)
-- Prereq : 01 and 02 scripts must be executed first
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Switch to SECURITYADMIN to manage roles
-- ---------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS DASHBOARD_READONLY_ROLE
    COMMENT = 'Read-only access to MART schema for dashboard and BI tool service accounts';

-- ---------------------------------------------------------------------------
-- Switch to SYSADMIN to grant object-level privileges
-- ---------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- Grant usage on the database so the role can navigate to it
GRANT USAGE ON DATABASE ELECTRICITY_MARKET_DB
    TO ROLE DASHBOARD_READONLY_ROLE;

-- Grant usage on the MART schema so the role can see objects inside it
GRANT USAGE ON SCHEMA ELECTRICITY_MARKET_DB.MART
    TO ROLE DASHBOARD_READONLY_ROLE;

-- Grant SELECT on all tables that currently exist in MART
GRANT SELECT ON ALL TABLES IN SCHEMA ELECTRICITY_MARKET_DB.MART
    TO ROLE DASHBOARD_READONLY_ROLE;

-- Grant SELECT on all tables that will be created in MART in the future
-- (covers dbt-materialised mart models created after this script runs)
GRANT SELECT ON FUTURE TABLES IN SCHEMA ELECTRICITY_MARKET_DB.MART
    TO ROLE DASHBOARD_READONLY_ROLE;

-- Grant SELECT on all views currently in MART (e.g. incremental views)
GRANT SELECT ON ALL VIEWS IN SCHEMA ELECTRICITY_MARKET_DB.MART
    TO ROLE DASHBOARD_READONLY_ROLE;

-- Grant SELECT on future views in MART
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ELECTRICITY_MARKET_DB.MART
    TO ROLE DASHBOARD_READONLY_ROLE;

-- ---------------------------------------------------------------------------
-- Example: Create a service account user for your dashboard tool and assign
--          the role. Replace <WAREHOUSE_NAME> and <PASSWORD> appropriately.
-- ---------------------------------------------------------------------------

-- USE ROLE SECURITYADMIN;
--
-- CREATE USER IF NOT EXISTS DASHBOARD_SVC
--     PASSWORD            = '<strong_password_here>'
--     DEFAULT_ROLE        = DASHBOARD_READONLY_ROLE
--     DEFAULT_WAREHOUSE   = '<WAREHOUSE_NAME>'
--     DEFAULT_NAMESPACE   = 'ELECTRICITY_MARKET_DB.MART'
--     MUST_CHANGE_PASSWORD = FALSE
--     COMMENT             = 'Service account for Tableau / Power BI dashboard connections';
--
-- GRANT ROLE DASHBOARD_READONLY_ROLE TO USER DASHBOARD_SVC;
