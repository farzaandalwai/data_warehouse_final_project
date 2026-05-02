"""
caiso_lmp_etl_dag.py
--------------------
Daily ETL DAG that extracts 5-minute CAISO LMP data from CAISO OASIS,
deletes any existing rows for that date (idempotency), and loads the
result into Snowflake RAW.CAISO_LMP_5MIN.

Pipeline  : CAISO OASIS API → extract_caiso_lmp() → delete stale rows
            → load_dataframe_to_table() → Snowflake RAW.CAISO_LMP_5MIN
Schedule  : Daily at 02:00 UTC (prior day data is fully published by then)
Owner     : group_8
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Path setup
# Airflow workers may not have the project root on sys.path.
# We resolve the project root two directories above this DAG file
# (airflow/dags/ → airflow/ → project root) and prepend it so that
# `from scripts.xxx import yyy` resolves correctly at runtime.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.extract_caiso_lmp import extract_caiso_lmp                        # noqa: E402
from scripts.load_to_snowflake import (                                         # noqa: E402
    delete_existing_caiso_rows_for_date,
    load_dataframe_to_table,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRADING_HUBS = ["NP15", "SP15", "ZP26"]


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
# Task callable
# ---------------------------------------------------------------------------

def run_caiso_lmp_etl(**context) -> None:
    """
    Single-task ETL callable that runs the full extract → delete → load cycle
    for the Airflow logical date (ds).

    Steps
    -----
    1. Extract 5-minute LMP data from CAISO OASIS for all three trading hubs.
    2. Raise ValueError if no data was returned (prevents a silent empty load).
    3. Delete any existing rows in RAW.CAISO_LMP_5MIN for that date so that
       re-triggering the DAG is idempotent.
    4. Load the extracted DataFrame into RAW.CAISO_LMP_5MIN.
    """
    load_date: str = context["ds"]   # Airflow logical date — 'YYYY-MM-DD'

    print(f"[caiso_lmp_etl] Starting ETL for date={load_date}, hubs={TRADING_HUBS}")

    # Step 1 — Extract
    df = extract_caiso_lmp(
        start_date=load_date,
        end_date=load_date,
        trading_hubs=TRADING_HUBS,
    )
    print(f"[caiso_lmp_etl] Extracted {len(df):,} rows for date={load_date}.")

    # Step 2 — Guard against empty extractions
    if df.empty:
        raise ValueError(
            f"CAISO extraction returned 0 rows for date={load_date}. "
            "Aborting load to prevent an empty write."
        )

    # Step 3 — Delete existing rows for this date (idempotency)
    delete_existing_caiso_rows_for_date(load_date)

    # Step 4 — Load into Snowflake
    rows_loaded = load_dataframe_to_table(df, "CAISO_LMP_5MIN", schema="RAW")

    print(
        f"[caiso_lmp_etl] Done. {rows_loaded:,} rows loaded into "
        f"RAW.CAISO_LMP_5MIN for date={load_date}."
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="caiso_lmp_etl_dag",
    description=(
        "Daily extraction of CAISO 5-minute LMP data (NP15, SP15, ZP26) "
        "into Snowflake RAW.CAISO_LMP_5MIN"
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",   # 02:00 UTC — prior day data is fully available
    catchup=False,
    tags=["caiso", "etl", "snowflake"],
) as dag:

    extract_load_caiso_lmp = PythonOperator(
        task_id="extract_load_caiso_lmp",
        python_callable=run_caiso_lmp_etl,
    )
