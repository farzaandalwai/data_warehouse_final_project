-- =============================================================================
-- 05_create_ml_tables.sql
-- -----------------------------------------------------------------------------
-- Creates the ML-layer tables in ELECTRICITY_MARKET_DB.ML.
--
-- Tables:
--   PRICE_SPIKE_PREDICTIONS    — stores per-hour model predictions for each
--                                trading hub; consumed by the dashboard spike
--                                forecast panel and Tableau/Power BI reports.
--
--   PRICE_SPIKE_MODEL_METRICS  — stores evaluation metrics for each trained
--                                model version; supports model monitoring,
--                                experiment comparisons, and the final report.
--
-- Run this script after 01_create_database_schemas.sql (which creates the
-- ELECTRICITY_MARKET_DB database and the ML schema).
-- =============================================================================

USE DATABASE ELECTRICITY_MARKET_DB;
USE SCHEMA ML;


-- -----------------------------------------------------------------------------
-- Table 1: PRICE_SPIKE_PREDICTIONS
-- Stores hourly price-spike predictions produced by the ML model.
-- PREDICTED_SPIKE_PROBABILITY supports threshold tuning in the dashboard.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_PREDICTIONS (
    HOUR_UTC                    TIMESTAMP_NTZ   COMMENT 'UTC hour the prediction applies to',
    TRADING_HUB                 STRING          COMMENT 'CAISO hub: NP15, SP15, or ZP26',
    ACTUAL_PRICE_SPIKE_FLAG     INTEGER         COMMENT '1 = actual price spike occurred, 0 = no spike (from MART layer)',
    PREDICTED_PRICE_SPIKE_FLAG  INTEGER         COMMENT '1 = model predicted a spike, 0 = no spike predicted',
    PREDICTED_SPIKE_PROBABILITY FLOAT           COMMENT 'Raw model probability score (0.0 – 1.0)',
    LMP_AVG                     FLOAT           COMMENT 'Average LMP for the hour used as input feature (USD/MWh)',
    DEMAND_MWH                  FLOAT           COMMENT 'Hourly demand in MWh used as input feature',
    MODEL_VERSION               STRING          COMMENT 'Identifier of the model that produced this prediction',
    PREDICTED_AT                TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the prediction was generated'
);


-- -----------------------------------------------------------------------------
-- Table 2: PRICE_SPIKE_MODEL_METRICS
-- Stores evaluation metrics for each trained model version.
-- One row per model version; supports experiment comparisons and reporting.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_MODEL_METRICS (
    MODEL_VERSION       STRING    COMMENT 'Unique identifier for the model version (e.g. v1.0.0, run_20240115)',
    TRAINING_ROW_COUNT  INTEGER   COMMENT 'Number of rows in the training set',
    TEST_ROW_COUNT      INTEGER   COMMENT 'Number of rows in the test / hold-out set',
    ACCURACY            FLOAT     COMMENT 'Overall classification accuracy on the test set',
    PRECISION_SCORE     FLOAT     COMMENT 'Precision (positive predictive value) on the test set',
    RECALL_SCORE        FLOAT     COMMENT 'Recall (sensitivity / true positive rate) on the test set',
    F1_SCORE            FLOAT     COMMENT 'Harmonic mean of precision and recall on the test set',
    ROC_AUC             FLOAT     COMMENT 'Area under the ROC curve on the test set',
    TRAINED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when model training completed'
);
