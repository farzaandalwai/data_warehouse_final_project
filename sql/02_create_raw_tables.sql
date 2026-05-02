-- =============================================================================
-- Script : 02_create_raw_tables.sql
-- Purpose: Create raw landing tables in the RAW schema
-- Run as : SYSADMIN or a role with CREATE TABLE privilege on RAW
-- Prereq : 01_create_database_schemas.sql must be executed first
-- =============================================================================

USE DATABASE ELECTRICITY_MARKET_DB;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------------
-- RAW.EIA_HOURLY_OPS
-- Source : EIA Open Data API  (https://api.eia.gov/v2/electricity/rto/)
-- Grain  : One row per hour per balancing authority
-- Notes  : All numeric columns are FLOAT to handle API nulls gracefully.
--          loaded_at is set by the ingestion script, not a DEFAULT, so that
--          the value reflects the actual load time rather than Snowflake DML time.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.EIA_HOURLY_OPS (
    period                  TIMESTAMP_NTZ   NOT NULL COMMENT 'Hour of the operating interval (UTC)',
    balancing_authority     VARCHAR(50)     NOT NULL COMMENT 'EIA balancing authority abbreviation (e.g. CISO, MISO)',
    demand_mwh              FLOAT                    COMMENT 'Hourly electricity demand in megawatt-hours',
    demand_forecast_mwh     FLOAT                    COMMENT 'Day-ahead demand forecast in megawatt-hours',
    net_generation_mwh      FLOAT                    COMMENT 'Net generation across all fuel types in megawatt-hours',
    interchange_mwh         FLOAT                    COMMENT 'Net interchange with neighbouring BAs (positive = export)',
    source_system           VARCHAR(50)              COMMENT 'Identifier for the source system (eia_api)',
    loaded_at               TIMESTAMP_NTZ            COMMENT 'UTC timestamp when this row was loaded into Snowflake'
)
COMMENT = 'Raw hourly electricity operations data from EIA Open Data API';

-- ---------------------------------------------------------------------------
-- RAW.CAISO_LMP_5MIN
-- Source : CAISO OASIS (http://oasis.caiso.com/oasisapi/)
-- Grain  : One row per 5-minute interval per trading hub
-- Notes  : LMP components (energy, congestion, loss) sum to total LMP.
--          market distinguishes real-time (RTM) from day-ahead (DAM).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.CAISO_LMP_5MIN (
    interval_start          TIMESTAMP_NTZ   NOT NULL COMMENT 'Start of the 5-minute interval (UTC)',
    interval_end            TIMESTAMP_NTZ   NOT NULL COMMENT 'End of the 5-minute interval (UTC)',
    trading_hub             VARCHAR(20)     NOT NULL COMMENT 'CAISO trading hub (NP15, SP15, ZP26)',
    lmp                     FLOAT                    COMMENT 'Locational marginal price ($/MWh)',
    energy_component        FLOAT                    COMMENT 'Energy component of LMP ($/MWh)',
    congestion_component    FLOAT                    COMMENT 'Congestion component of LMP ($/MWh)',
    loss_component          FLOAT                    COMMENT 'Loss component of LMP ($/MWh)',
    market                  VARCHAR(10)              COMMENT 'Market type: RTM (real-time) or DAM (day-ahead)',
    source_system           VARCHAR(50)              COMMENT 'Identifier for the source system (caiso_oasis)',
    loaded_at               TIMESTAMP_NTZ            COMMENT 'UTC timestamp when this row was loaded into Snowflake'
)
COMMENT = 'Raw 5-minute locational marginal price data from CAISO OASIS';
