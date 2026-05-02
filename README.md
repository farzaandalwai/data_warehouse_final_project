# Wholesale Electricity Market Stress and Price Spike Forecasting

## Project Overview

This project delivers an end-to-end data analytics pipeline for monitoring, analyzing, and
forecasting stress conditions and price spike events in the U.S. wholesale electricity market.
It combines raw market data ingestion, cloud data warehousing, automated transformations, and
interactive dashboards into a single, reproducible analytical system.

---

## Business Problem

Wholesale electricity prices are highly volatile and influenced by a complex mix of supply,
demand, weather, generation mix, and transmission congestion. Price spikes — periods where
locational marginal prices (LMPs) rise sharply above baseline — impose significant financial
risk on utilities, large commercial buyers, and energy traders.

Without early detection and forecasting infrastructure, market participants are exposed to:

- Unexpected cost overruns during high-stress grid conditions
- Inability to hedge positions effectively
- Reactive rather than proactive operational decisions

This project addresses that gap by building a scalable analytics and forecasting system that
surfaces stress signals in near real-time and provides daily forward-looking price spike
probability estimates.

---

## Data Sources

### 1. EIA Open Data API
The U.S. Energy Information Administration (EIA) provides free, hourly electricity grid
operations data via its public API. This includes:

- Hourly demand (MWh)
- Demand forecast (MWh)
- Net generation by source (MWh)
- Net interchange between balancing authorities (MWh)

**Coverage:** All major U.S. balancing authorities (BAs), including CAISO, MISO, PJM, ERCOT, and others.
**API:** [https://api.eia.gov/v2/electricity/rto/](https://api.eia.gov/v2/electricity/rto/)

### 2. CAISO OASIS
The California Independent System Operator (CAISO) publishes granular 5-minute and hourly
locational marginal price (LMP) data through its Open Access Same-time Information System
(OASIS). This includes:

- LMP by trading hub (NP15, SP15, ZP26)
- Energy, congestion, and loss components
- Real-time and day-ahead markets

**Coverage:** California (CAISO balancing authority)
**API:** [http://oasis.caiso.com/oasisapi/](http://oasis.caiso.com/oasisapi/)

---

## Architecture

```
Data Sources
    │
    ├── EIA Open Data API  (hourly operations)
    └── CAISO OASIS        (5-min LMP prices)
         │
         ▼
    Apache Airflow (ETL Orchestration)
    ├── eia_hourly_etl_dag.py
    ├── caiso_lmp_etl_dag.py
    └── dbt_run_dag.py
         │
         ▼
    Snowflake — RAW Schema
    ├── RAW.EIA_HOURLY_OPS
    └── RAW.CAISO_LMP_5MIN
         │
         ▼
    dbt ELT Transformations
    ├── Staging  → STAGING.stg_eia_hourly_ops, stg_caiso_lmp
    ├── Intermediate → INTERMEDIATE.int_hourly_market_features
    └── Marts    → MART.mart_hourly_market_stress
                   MART.mart_price_spike_dashboard
         │
         ▼
    Dashboard (Tableau / Power BI)
    ├── Market Overview
    ├── Stress Monitor
    ├── Forecasting / Model Output
    └── Recommendations
```

---

## Tools and Technologies

| Layer             | Tool / Technology                        |
|-------------------|------------------------------------------|
| Data Warehouse    | Snowflake                                |
| Orchestration     | Apache Airflow                           |
| Transformation    | dbt (dbt-core + dbt-snowflake)           |
| Ingestion Scripts | Python (requests, pandas)                |
| ML / Forecasting  | scikit-learn                             |
| Dashboard         | Tableau or Power BI                      |
| Version Control   | GitHub                                   |
| Environment Mgmt  | python-dotenv, environment variables     |

---

## Current Project Scope

The first implementation phase will focus on **CAISO price data** (LMP from CAISO OASIS) as
the primary data source. This includes building the full pipeline — ingestion, loading,
staging, feature engineering, mart tables, and dashboard — end to end for CAISO.

Once the CAISO pipeline is stable and validated, the project will expand to ingest
**EIA hourly operations data** for broader grid-level context, enabling cross-BA analysis
and improved forecasting features such as demand-supply imbalance indicators.

---

## Repository Structure

```
electricity-market-stress-analytics/
├── README.md                        # This file
├── requirements.txt                 # Python dependencies
├── .gitignore
├── sql/                             # Snowflake DDL scripts
│   ├── 01_create_database_schemas.sql
│   ├── 02_create_raw_tables.sql
│   └── 03_create_roles_users_grants.sql
├── airflow/dags/                    # Airflow DAG definitions
├── scripts/                         # Python ETL scripts
├── dbt/electricity_market/          # dbt project
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── dashboards/                      # Dashboard specs and notes
└── report/                          # Final report outline
```

---

## Getting Started

1. Clone this repository.
2. Copy `.env.example` to `.env` and fill in your credentials (Snowflake, EIA API key).
3. Install dependencies: `pip install -r requirements.txt`
4. Run Snowflake DDL scripts in order: `sql/01_` → `sql/02_` → `sql/03_`
5. Configure Airflow and register the DAGs in `airflow/dags/`.
6. Configure the dbt profile (`~/.dbt/profiles.yml`) to point at your Snowflake instance.
7. Trigger the `caiso_lmp_etl_dag` DAG to begin ingestion.
