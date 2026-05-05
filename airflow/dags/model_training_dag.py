"""
model_training_dag.py
---------------------
Daily Airflow DAG that trains the price spike prediction model after the
dbt ELT pipeline has populated the MART layer.

Execution order (manual dependency — schedule staggered by 1 hour):
  caiso_lmp_etl_dag   02:00 UTC  — loads RAW.CAISO_LMP_5MIN
  eia_hourly_etl_dag  03:00 UTC  — loads RAW.EIA_HOURLY_OPS
  dbt_run_dag         04:00 UTC  — builds STAGING → INTERMEDIATE → MART
  model_training_dag  05:00 UTC  — trains model using MART.MART_HOURLY_MARKET_STRESS

Model source  : ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
Model outputs : ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_PREDICTIONS
                ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_MODEL_METRICS
MLflow        : Logs experiment params, metrics, and the sklearn model artifact
                when the MLflow tracking server is reachable (http://mlflow:5001
                inside Docker Compose; skipped gracefully if unavailable).

Owner    : group_8
Schedule : Daily at 05:00 UTC
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ---------------------------------------------------------------------------
# Training script path
#
# Inside Docker Compose, the scripts/ folder is mounted at /opt/airflow/scripts.
# Override MODEL_SCRIPT_PATH to point elsewhere for local development.
# ---------------------------------------------------------------------------
MODEL_SCRIPT_PATH = os.environ.get(
    "MODEL_SCRIPT_PATH",
    "/opt/airflow/scripts/train_price_spike_model.py",
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
    dag_id="model_training_dag",
    description=(
        "Daily ML training: logistic regression price spike model "
        "trained on MART.MART_HOURLY_MARKET_STRESS, predictions and "
        "metrics written to Snowflake ML schema, run logged in MLflow"
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 5 * * *",   # 05:00 UTC — after dbt_run_dag (04:00 UTC)
    catchup=False,
    tags=["ml", "price-spike", "snowflake", "mlflow"],
) as dag:

    train_price_spike_model = BashOperator(
        task_id="train_price_spike_model",
        bash_command=f"python {MODEL_SCRIPT_PATH}",
    )
