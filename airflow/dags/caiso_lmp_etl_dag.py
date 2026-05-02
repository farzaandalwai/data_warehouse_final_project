"""
caiso_lmp_etl_dag.py
--------------------
Airflow DAG that orchestrates the daily extraction of CAISO 5-minute LMP data
from CAISO OASIS and loads it into Snowflake RAW.CAISO_LMP_5MIN.

Schedule : Daily at 02:00 UTC (data for the prior day)
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
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Trading hubs to extract LMP data for
# ---------------------------------------------------------------------------
TRADING_HUBS = ["NP15", "SP15", "ZP26"]


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def extract_caiso_lmp_task(**context) -> None:
    """
    Extract CAISO 5-minute LMP data for the execution date and push the
    resulting DataFrame to XCom for the downstream load task.

    TODO: Import extract_caiso_lmp from scripts.extract_caiso_lmp.
          Ensure the scripts/ directory is on the Python path (configure in
          airflow.cfg or add a PYTHONPATH entry in the Airflow environment).
    """
    execution_date = context["ds"]  # YYYY-MM-DD string for the DAG run date

    # TODO: Uncomment and connect to the real extraction function:
    # from scripts.extract_caiso_lmp import extract_caiso_lmp
    # df = extract_caiso_lmp(
    #     start_date=execution_date,
    #     end_date=execution_date,
    #     trading_hubs=TRADING_HUBS,
    # )
    # context["ti"].xcom_push(key="caiso_lmp_df", value=df.to_json())

    print(f"[extract_caiso_lmp_task] Extraction placeholder for date={execution_date}")


def load_caiso_lmp_to_snowflake_task(**context) -> None:
    """
    Pull the extracted CAISO DataFrame from XCom and load it into Snowflake.

    TODO: Import load_dataframe_to_table from scripts.load_to_snowflake.
          Deserialise the DataFrame from JSON before passing it to the loader.
    """
    # TODO: Uncomment and connect to the real load function:
    # import pandas as pd
    # from scripts.load_to_snowflake import load_dataframe_to_table
    #
    # raw_json = context["ti"].xcom_pull(task_ids="extract_caiso_lmp", key="caiso_lmp_df")
    # df = pd.read_json(raw_json)
    # load_dataframe_to_table(df, table_name="CAISO_LMP_5MIN", schema="RAW")

    print("[load_caiso_lmp_to_snowflake_task] Load placeholder — Snowflake not yet connected.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="caiso_lmp_etl_dag",
    description="Daily extraction of CAISO 5-minute LMP data into Snowflake RAW",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    catchup=False,
    tags=["caiso", "lmp", "etl", "raw"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_caiso_lmp",
        python_callable=extract_caiso_lmp_task,
    )

    load_task = PythonOperator(
        task_id="load_caiso_lmp_to_snowflake",
        python_callable=load_caiso_lmp_to_snowflake_task,
    )

    # Task dependency: extract must succeed before load runs
    extract_task >> load_task
