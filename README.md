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
    ├── caiso_lmp_etl_dag.py    02:00 UTC
    ├── eia_hourly_etl_dag.py   03:00 UTC
    ├── dbt_run_dag.py          04:00 UTC
    └── model_training_dag.py   05:00 UTC
         │
         ▼
    Snowflake — RAW Schema
    ├── RAW.EIA_HOURLY_OPS
    └── RAW.CAISO_LMP_5MIN
         │
         ▼
    dbt ELT Transformations
    ├── STAGING       → stg_eia_hourly_ops, stg_caiso_lmp
    ├── INTERMEDIATE  → int_hourly_market_features
    └── MART          → mart_hourly_market_stress
                        mart_price_spike_dashboard
         │
         ▼
    ML Training (scikit-learn + MLflow)
    ├── Logistic regression — price spike prediction
    ├── ML.PRICE_SPIKE_PREDICTIONS
    └── ML.PRICE_SPIKE_MODEL_METRICS
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

| Layer              | Tool / Technology                              |
|--------------------|------------------------------------------------|
| Data Warehouse     | Snowflake                                      |
| Orchestration      | Apache Airflow (Dockerized)                    |
| Transformation     | dbt (dbt-core + dbt-snowflake)                 |
| Ingestion Scripts  | Python (requests, pandas)                      |
| ML / Forecasting   | scikit-learn (logistic regression)             |
| Experiment Tracking| MLflow                                         |
| Containerization   | Docker, Docker Compose                         |
| Dashboard          | Tableau or Power BI                            |
| Version Control    | GitHub                                         |
| Environment Mgmt   | python-dotenv, environment variables           |

---

## Current Status

### Completed

- Snowflake database, schemas (`RAW`, `STAGING`, `INTERMEDIATE`, `MART`, `ML`), RAW tables, MART tables, and ML tables
- CAISO OASIS API extraction with retry/backoff logic and Snowflake load
- EIA Open Data API extraction with pagination support and Snowflake load
- dbt transformations across all four layers: RAW → STAGING → INTERMEDIATE → MART
- dbt data quality tests (26 passing tests across all models)
- Airflow DAGs for CAISO ETL, EIA ETL, dbt run, and ML model training
- Docker Compose environment for Airflow, PostgreSQL (metadata DB), and MLflow
- Logistic regression price spike prediction model (ROC-AUC 0.987 on current data)
- ML predictions and model metrics written to Snowflake ML schema
- MLflow experiment tracking integrated into the training pipeline

### Key Snowflake Tables

| Table | Description |
|---|---|
| `ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS` | Hourly LMP averages, lag features, spike flags |
| `ELECTRICITY_MARKET_DB.MART.MART_PRICE_SPIKE_DASHBOARD` | Dashboard-ready aggregated view |
| `ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_PREDICTIONS` | Per-hour model predictions for each trading hub |
| `ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_MODEL_METRICS` | Evaluation metrics per trained model version |

---

## Repository Structure

```
electricity-market-stress-analytics/
├── README.md
├── requirements.txt
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── sql/
│   ├── 01_create_database_schemas.sql
│   ├── 02_create_raw_tables.sql
│   ├── 03_create_roles_users_grants.sql
│   ├── 04_project_validation_queries.sql
│   └── 05_create_ml_tables.sql
├── airflow/
│   └── dags/
│       ├── caiso_lmp_etl_dag.py
│       ├── eia_hourly_etl_dag.py
│       ├── dbt_run_dag.py
│       └── model_training_dag.py
├── scripts/
│   ├── extract_caiso_lmp.py
│   ├── extract_eia_hourly.py
│   ├── load_to_snowflake.py
│   └── train_price_spike_model.py
├── dbt/
│   ├── profiles/
│   │   └── profiles.yml
│   └── electricity_market/
│       ├── dbt_project.yml
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── models/
│           ├── sources.yml
│           ├── schema.yml
│           ├── staging/
│           │   ├── stg_caiso_lmp.sql
│           │   └── stg_eia_hourly_ops.sql
│           ├── intermediate/
│           │   └── int_hourly_market_features.sql
│           └── marts/
│               ├── mart_hourly_market_stress.sql
│               └── mart_price_spike_dashboard.sql
├── dashboards/
│   └── README.md
├── report/
│   └── final_report_outline.md
├── logs/
│   └── .gitkeep
└── mlruns/
    └── .gitkeep
```

---

## Getting Started

### Local development (without Docker)

1. Clone this repository.
2. Create a `.env` file in the project root with your Snowflake credentials and EIA API key (see `.env` format in the project).
3. Install dependencies: `pip install -r requirements.txt`
4. Run Snowflake DDL scripts in order: `sql/01_` → `sql/02_` → `sql/03_` → `sql/05_`
5. Extract and load data: `python scripts/extract_caiso_lmp.py` and `python scripts/load_to_snowflake.py`
6. Run dbt transformations from `dbt/electricity_market/`: `dbt run && dbt test`
7. Train the model: `python scripts/train_price_spike_model.py`

### Docker Compose (Airflow + MLflow)

1. Ensure Docker Desktop is running.
2. Place your `.env` file in the project root with all required credentials.
3. Build and start all services:
   ```bash
   docker compose up --build -d
   ```
4. Airflow UI: [http://localhost:8080](http://localhost:8080) (admin / admin)
5. MLflow UI: [http://localhost:5001](http://localhost:5001)
6. Enable and trigger DAGs in the Airflow UI in this order:
   - `caiso_lmp_etl_dag`
   - `eia_hourly_etl_dag`
   - `dbt_run_dag`
   - `model_training_dag`
