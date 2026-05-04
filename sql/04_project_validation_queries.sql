-- =============================================================================
-- Script : 04_project_validation_queries.sql
-- Purpose: Validate the end-to-end pipeline for report screenshots and
--          final presentation evidence.
-- Run in : Snowflake worksheet — execute sections individually or all at once.
-- Note   : All queries are read-only (SELECT / SHOW). Safe to run at any time.
-- =============================================================================


-- =============================================================================
-- SECTION 1 — Database and Schema Validation
-- Confirms that the database and all expected schemas were created correctly.
-- =============================================================================

SHOW DATABASES LIKE 'ELECTRICITY_MARKET_DB';

SHOW SCHEMAS IN DATABASE ELECTRICITY_MARKET_DB;


-- =============================================================================
-- SECTION 2 — RAW CAISO Validation
-- Confirms that 5-minute LMP data was loaded into RAW.CAISO_LMP_5MIN.
-- =============================================================================

-- Total row count
SELECT COUNT(*) AS total_rows
FROM ELECTRICITY_MARKET_DB.RAW.CAISO_LMP_5MIN;

-- Row count by trading hub — expect NP15, SP15, ZP26
SELECT
    TRADING_HUB,
    COUNT(*)                        AS row_count,
    MIN(INTERVAL_START)             AS earliest_interval,
    MAX(INTERVAL_START)             AS latest_interval
FROM ELECTRICITY_MARKET_DB.RAW.CAISO_LMP_5MIN
GROUP BY TRADING_HUB
ORDER BY TRADING_HUB;

-- Sample rows — 10 records from the earliest loaded date
SELECT
    INTERVAL_START,
    INTERVAL_END,
    TRADING_HUB,
    LMP,
    ENERGY_COMPONENT,
    CONGESTION_COMPONENT,
    LOSS_COMPONENT,
    MARKET,
    SOURCE_SYSTEM,
    LOADED_AT
FROM ELECTRICITY_MARKET_DB.RAW.CAISO_LMP_5MIN
ORDER BY INTERVAL_START, TRADING_HUB
LIMIT 10;


-- =============================================================================
-- SECTION 3 — RAW EIA Validation
-- Confirms that hourly grid operations data was loaded into RAW.EIA_HOURLY_OPS.
-- =============================================================================

-- Total row count
SELECT COUNT(*) AS total_rows
FROM ELECTRICITY_MARKET_DB.RAW.EIA_HOURLY_OPS;

-- Row count by balancing authority
SELECT
    BALANCING_AUTHORITY,
    COUNT(*)            AS row_count,
    MIN(PERIOD)         AS earliest_period,
    MAX(PERIOD)         AS latest_period
FROM ELECTRICITY_MARKET_DB.RAW.EIA_HOURLY_OPS
GROUP BY BALANCING_AUTHORITY
ORDER BY BALANCING_AUTHORITY;

-- Sample rows — 10 records ordered by period
SELECT
    PERIOD,
    BALANCING_AUTHORITY,
    DEMAND_MWH,
    DEMAND_FORECAST_MWH,
    NET_GENERATION_MWH,
    INTERCHANGE_MWH,
    SOURCE_SYSTEM,
    LOADED_AT
FROM ELECTRICITY_MARKET_DB.RAW.EIA_HOURLY_OPS
ORDER BY PERIOD
LIMIT 10;


-- =============================================================================
-- SECTION 4 — dbt Object Validation
-- Confirms that dbt created views and tables in the correct schemas.
-- Expected staging views  : stg_caiso_lmp, stg_eia_hourly_ops
-- Expected intermediate   : int_hourly_market_features
-- Expected mart tables    : mart_hourly_market_stress, mart_price_spike_dashboard
-- =============================================================================

SHOW VIEWS IN SCHEMA ELECTRICITY_MARKET_DB.STAGING;

SHOW TABLES IN SCHEMA ELECTRICITY_MARKET_DB.INTERMEDIATE;

SHOW TABLES IN SCHEMA ELECTRICITY_MARKET_DB.MART;


-- =============================================================================
-- SECTION 5 — MART Validation
-- Confirms that mart_hourly_market_stress contains classified market data
-- with all expected metrics and spike flags populated.
-- =============================================================================

-- Total row count
SELECT COUNT(*) AS total_rows
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS;

-- Row count by trading hub — expect equal distribution across NP15, SP15, ZP26
SELECT
    TRADING_HUB,
    COUNT(*)                        AS row_count,
    ROUND(AVG(LMP_AVG), 2)          AS avg_lmp,
    ROUND(MIN(LMP_AVG), 2)          AS min_lmp,
    ROUND(MAX(LMP_AVG), 2)          AS max_lmp,
    SUM(PRICE_SPIKE_FLAG)           AS spike_hours,
    COUNT(DISTINCT STRESS_SEVERITY) AS severity_levels
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
GROUP BY TRADING_HUB
ORDER BY TRADING_HUB;

-- Sample joined market stress rows — key analytical fields
SELECT
    HOUR_UTC,
    TRADING_HUB,
    ROUND(LMP_AVG, 2)               AS LMP_AVG,
    ROUND(DEMAND_MWH, 0)            AS DEMAND_MWH,
    ROUND(DEMAND_FORECAST_MWH, 0)   AS DEMAND_FORECAST_MWH,
    ROUND(NET_GENERATION_MWH, 0)    AS NET_GENERATION_MWH,
    ROUND(INTERCHANGE_MWH, 0)       AS INTERCHANGE_MWH,
    PRICE_SPIKE_FLAG,
    STRESS_SEVERITY
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
ORDER BY HOUR_UTC, TRADING_HUB
LIMIT 20;


-- =============================================================================
-- SECTION 6 — Dashboard-Ready Validation
-- Confirms that mart_price_spike_dashboard is populated with BI-tool-friendly
-- rounded metrics and all required columns for Tableau or Power BI.
-- =============================================================================

SELECT
    HOUR_UTC,
    TRADING_HUB,
    LMP_AVG_USD_MWH,
    SPIKE_THRESHOLD_P95,
    SPIKE_THRESHOLD_P99,
    PRICE_SPIKE_FLAG,
    STRESS_SEVERITY,
    ENERGY_COMPONENT_AVG,
    CONGESTION_COMPONENT_AVG,
    DEMAND_MWH,
    DEMAND_FORECAST_MWH,
    NET_GENERATION_MWH,
    INTERCHANGE_MWH,
    LMP_LAG_1H,
    LMP_LAG_24H,
    PCT_ABOVE_SPIKE_THRESHOLD
FROM ELECTRICITY_MARKET_DB.MART.MART_PRICE_SPIKE_DASHBOARD
ORDER BY HOUR_UTC, TRADING_HUB
LIMIT 20;


-- =============================================================================
-- SECTION 7 — Data Quality Sanity Checks
-- These queries should all return 0. Any non-zero result indicates a data
-- quality issue that should be investigated before the final report.
-- =============================================================================

-- Check 1: Null HOUR_UTC in mart_hourly_market_stress (must be 0)
SELECT
    COUNT(*) AS null_hour_utc_count
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
WHERE HOUR_UTC IS NULL;

-- Check 2: Null LMP_AVG in mart_hourly_market_stress (must be 0)
SELECT
    COUNT(*) AS null_lmp_avg_count
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
WHERE LMP_AVG IS NULL;

-- Check 3: Invalid PRICE_SPIKE_FLAG values — only 0 and 1 are valid (must be 0)
SELECT
    COUNT(*) AS invalid_spike_flag_count
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
WHERE PRICE_SPIKE_FLAG NOT IN (0, 1);

-- Check 4: Null DEMAND_MWH in mart_hourly_market_stress
-- Note: EIA data joins via a LEFT JOIN so nulls are expected when EIA rows
-- are not yet loaded for the date range. Non-zero here is acceptable but
-- worth investigating if the EIA pipeline is running.
SELECT
    COUNT(*) AS null_demand_mwh_count
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
WHERE DEMAND_MWH IS NULL;

-- Summary: spike distribution across severity levels
SELECT
    STRESS_SEVERITY,
    COUNT(*)                AS hour_count,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
GROUP BY STRESS_SEVERITY
ORDER BY hour_count DESC;
