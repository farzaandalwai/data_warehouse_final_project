-- =============================================================================
-- Script : 01_create_database_schemas.sql
-- Purpose: Create the ELECTRICITY_MARKET_DB database and all schemas
-- Run as : SYSADMIN or equivalent role with CREATE DATABASE privilege
-- =============================================================================

-- Create the top-level database
CREATE DATABASE IF NOT EXISTS ELECTRICITY_MARKET_DB
    COMMENT = 'Wholesale electricity market analytics warehouse';

USE DATABASE ELECTRICITY_MARKET_DB;

-- ---------------------------------------------------------------------------
-- RAW schema
-- Landing zone for all data ingested directly from source APIs.
-- No transformations applied; data is immutable once loaded.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data loaded directly from EIA and CAISO source systems';

-- ---------------------------------------------------------------------------
-- STAGING schema
-- First transformation layer managed by dbt.
-- Cleans, casts, and standardises RAW data. Materialised as views.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'dbt staging layer: cleaned and typed views over RAW tables';

-- ---------------------------------------------------------------------------
-- INTERMEDIATE schema
-- Second transformation layer managed by dbt.
-- Joins, aggregations, and feature engineering. Materialised as tables.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'dbt intermediate layer: feature-engineered tables for modelling';

-- ---------------------------------------------------------------------------
-- MART schema
-- Final analytics-ready tables consumed by dashboards and reports.
-- Materialised as tables. Granted to dashboard read roles.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS MART
    COMMENT = 'dbt mart layer: analytics-ready tables for dashboards and BI tools';

-- ---------------------------------------------------------------------------
-- ML schema
-- Storage for model training datasets, scored outputs, and predictions.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ML
    COMMENT = 'Machine learning datasets, scored outputs, and forecasts';

-- ---------------------------------------------------------------------------
-- ADHOC schema
-- Scratch space for exploratory analysis and ad-hoc queries.
-- Not managed by dbt; contents may be transient.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ADHOC
    COMMENT = 'Ad-hoc analysis, exploration, and temporary objects';
