"""
dbt_run_dag.py
--------------
Daily Airflow DAG that runs the dbt ELT transformation pipeline after raw
CAISO and EIA data has been loaded into Snowflake by the upstream ETL DAGs.

dbt transforms data across three layers:
  RAW         — source tables loaded by caiso_lmp_etl_dag / eia_hourly_etl_dag
  STAGING     — cleaned and typed views (stg_caiso_lmp, stg_eia_hourly_ops)
  INTERMEDIATE — feature-engineered tables (int_hourly_market_features)
  MART        — analytics-ready tables consumed by dashboards
                (mart_hourly_market_stress, mart_price_spike_dashboard)

This DAG should run after both ETL DAGs complete — schedule is set to
04:00 UTC, 2 hours after the latest ETL DAG (03:00 UTC).

Owner    : group_8
Schedule : Daily at 04:00 UTC
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ---------------------------------------------------------------------------
# dbt project path
#
# Inside Docker Compose, the dbt project is mounted at:
#   ./dbt/electricity_market → /opt/airflow/dbt/electricity_market
# (see the volumes section of docker-compose.yml)
#
# The DBT_PROJECT_DIR environment variable can be set to override this path
# for local development outside Docker. If the variable is not set, the
# Docker mount path is used as the default.
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR",
    "/opt/airflow/dbt/electricity_market",
)


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "group_8",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dbt_run_dag",
    description=(
        "Daily dbt run + test: transforms Snowflake RAW data into "
        "STAGING, INTERMEDIATE, and MART layers for the electricity_market project"
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 4 * * *",   # 04:00 UTC — after both ETL DAGs complete
    catchup=False,
    tags=["dbt", "elt", "snowflake"],
) as dag:

    # Task 1 — dbt run
    # Executes all dbt models in dependency order:
    #   stg_caiso_lmp, stg_eia_hourly_ops (STAGING views)
    #   → int_hourly_market_features (INTERMEDIATE table)
    #   → mart_hourly_market_stress, mart_price_spike_dashboard (MART tables)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    # Task 2 — dbt test
    # Runs all schema tests defined in sources.yml and model YAML files.
    # If no tests are defined yet, dbt exits cleanly with "Nothing to do" —
    # this is expected and the task will still succeed.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    # Models must build successfully before tests are attempted
    dbt_run >> dbt_test
