"""
eia_hourly_etl_dag.py
---------------------
Airflow DAG that orchestrates the daily extraction of EIA hourly electricity
operations data from the EIA Open Data API and loads it into Snowflake
RAW.EIA_HOURLY_OPS.

Schedule : Daily at 03:00 UTC (EIA data is typically available by 02:00 UTC)
Owner    : data-engineering
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Default arguments applied to every task in this DAG
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# ---------------------------------------------------------------------------
# Balancing authorities to extract data for
# ---------------------------------------------------------------------------
BALANCING_AUTHORITIES = ["CISO"]   # Start with CAISO; expand list as needed


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def extract_eia_hourly_task(**context) -> None:
    """
    Extract hourly operations data from the EIA API for the execution date
    and push the resulting DataFrame to XCom for the downstream load task.

    TODO: Import extract_eia_hourly from scripts.extract_eia_hourly.
          Ensure the scripts/ directory is on the Python path (configure in
          airflow.cfg or add a PYTHONPATH entry in the Airflow environment).
          Ensure EIA_API_KEY is available as an Airflow Variable or environment
          variable within the worker environment.
    """
    execution_date = context["ds"]  # YYYY-MM-DD string for the DAG run date

    # TODO: Uncomment and connect to the real extraction function:
    # import pandas as pd
    # from scripts.extract_eia_hourly import extract_eia_hourly
    #
    # all_dfs = []
    # for ba in BALANCING_AUTHORITIES:
    #     df = extract_eia_hourly(
    #         start_date=execution_date,
    #         end_date=execution_date,
    #         balancing_authority=ba,
    #     )
    #     all_dfs.append(df)
    #
    # combined_df = pd.concat(all_dfs, ignore_index=True)
    # context["ti"].xcom_push(key="eia_hourly_df", value=combined_df.to_json())

    print(f"[extract_eia_hourly_task] Extraction placeholder for date={execution_date}")


def load_eia_hourly_to_snowflake_task(**context) -> None:
    """
    Pull the extracted EIA DataFrame from XCom and load it into Snowflake.

    TODO: Import load_dataframe_to_table from scripts.load_to_snowflake.
          Deserialise the DataFrame from JSON before passing it to the loader.
    """
    # TODO: Uncomment and connect to the real load function:
    # import pandas as pd
    # from scripts.load_to_snowflake import load_dataframe_to_table
    #
    # raw_json = context["ti"].xcom_pull(task_ids="extract_eia_hourly", key="eia_hourly_df")
    # df = pd.read_json(raw_json)
    # load_dataframe_to_table(df, table_name="EIA_HOURLY_OPS", schema="RAW")

    print("[load_eia_hourly_to_snowflake_task] Load placeholder — Snowflake not yet connected.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="eia_hourly_etl_dag",
    description="Daily extraction of EIA hourly grid operations data into Snowflake RAW",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",   # 03:00 UTC daily
    catchup=False,
    tags=["eia", "hourly", "etl", "raw"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_eia_hourly",
        python_callable=extract_eia_hourly_task,
    )

    load_task = PythonOperator(
        task_id="load_eia_hourly_to_snowflake",
        python_callable=load_eia_hourly_to_snowflake_task,
    )

    # Task dependency: extract must succeed before load runs
    extract_task >> load_task
