"""
dbt_run_dag.py
--------------
Airflow DAG that runs the dbt transformation pipeline after raw data has
been loaded into Snowflake by the upstream ETL DAGs.

This DAG triggers dbt run (all models) followed by dbt test to validate
the output. It is intended to run after both caiso_lmp_etl_dag and
eia_hourly_etl_dag have completed successfully.

Schedule : Daily at 04:00 UTC (after ETL DAGs complete)
Owner    : data-engineering
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ---------------------------------------------------------------------------
# Default arguments applied to every task in this DAG
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Path to the dbt project directory on the Airflow worker
# TODO: Update DBT_PROJECT_DIR to the absolute path where the dbt project
#       is deployed on your Airflow worker nodes or Docker image.
#       In a containerised setup this might be /opt/airflow/dbt/electricity_market
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = "/opt/airflow/dbt/electricity_market"

# TODO: If using a virtual environment for dbt, set DBT_EXECUTABLE to the
#       full path of the dbt binary, e.g. /opt/dbt-venv/bin/dbt
DBT_EXECUTABLE = "dbt"

# TODO: Set the dbt profile target (dev / prod) via an Airflow Variable or
#       environment variable (DBT_TARGET) rather than hardcoding it here.
DBT_TARGET = "prod"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dbt_run_dag",
    description="Daily dbt run and test for the electricity_market project",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 4 * * *",   # 04:00 UTC daily — after ETL DAGs finish
    catchup=False,
    tags=["dbt", "transform", "mart"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --target {DBT_TARGET} --profiles-dir ~/.dbt"
        ),
        # TODO: Pass dbt vars for execution date if needed:
        #   dbt run --vars '{"execution_date": "{{ ds }}"}'
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} test --target {DBT_TARGET} --profiles-dir ~/.dbt"
        ),
        # TODO: Add --select flags to scope tests to specific models if needed:
        #   dbt test --select mart_hourly_market_stress mart_price_spike_dashboard
    )

    # Run models first; only test if the run succeeds
    dbt_run >> dbt_test
