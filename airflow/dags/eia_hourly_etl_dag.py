"""
eia_hourly_etl_dag.py
---------------------
Daily ETL DAG that extracts hourly grid operations data from the EIA Open
Data API v2, deletes any existing rows for that date and balancing authority
(idempotency), and loads the result into Snowflake RAW.EIA_HOURLY_OPS.

Pipeline  : EIA Open Data API v2 → extract_eia_hourly() → delete stale rows
            → load_dataframe_to_table() → Snowflake RAW.EIA_HOURLY_OPS

EIA data for a given day is fully published by ~02:00 UTC the same day,
so this DAG is scheduled one hour after the CAISO DAG (02:00 UTC) to avoid
warehouse contention while still finishing before the dbt DAG (04:00 UTC).

Schedule  : Daily at 03:00 UTC
Owner     : group_8
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Path setup
# Resolve the project root two directories above this DAG file:
#   airflow/dags/eia_hourly_etl_dag.py  →  airflow/  →  project root
# This ensures `from scripts.xxx import yyy` works on any Airflow worker
# regardless of the working directory or PYTHONPATH configuration.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.extract_eia_hourly import extract_eia_hourly           # noqa: E402
from scripts.load_to_snowflake import (                              # noqa: E402
    delete_existing_eia_rows_for_date,
    load_dataframe_to_table,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BALANCING_AUTHORITY = "CISO"   # California ISO — expand list as project grows


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

def run_eia_hourly_etl(**context) -> None:
    """
    Single-task ETL callable that runs the full extract → delete → load cycle
    for the EIA hourly data on the Airflow logical date (ds).

    Steps
    -----
    1. Build EIA date range: T00 through T23 for the logical date.
    2. Extract hourly grid operations from the EIA API for CISO.
    3. Raise ValueError if no data was returned (prevents a silent empty load).
    4. Delete any existing EIA rows for that date + BA (idempotency).
    5. Load the extracted DataFrame into RAW.EIA_HOURLY_OPS.
    """
    load_date: str = context["ds"]   # Airflow logical date — 'YYYY-MM-DD'

    # EIA API v2 hourly endpoint expects 'YYYY-MM-DDTHH' format
    start_date = f"{load_date}T00"
    end_date   = f"{load_date}T23"

    print(
        f"[eia_hourly_etl] Starting ETL for date={load_date}, "
        f"BA={BALANCING_AUTHORITY}"
    )

    # Step 1 — Extract
    df = extract_eia_hourly(
        start_date=start_date,
        end_date=end_date,
        balancing_authority=BALANCING_AUTHORITY,
    )
    print(f"[eia_hourly_etl] Extracted {len(df):,} rows for date={load_date}.")

    # Step 2 — Guard against empty extractions
    if df.empty:
        raise ValueError(
            f"EIA extraction returned 0 rows for date={load_date}, "
            f"BA={BALANCING_AUTHORITY}. "
            "Aborting load to prevent an empty write."
        )

    # Step 3 — Delete existing rows for this date + BA (idempotency)
    delete_existing_eia_rows_for_date(load_date, BALANCING_AUTHORITY)

    # Step 4 — Load into Snowflake
    rows_loaded = load_dataframe_to_table(df, "EIA_HOURLY_OPS", schema="RAW")

    print(
        f"[eia_hourly_etl] Done. {rows_loaded:,} rows loaded into "
        f"RAW.EIA_HOURLY_OPS for date={load_date}, BA={BALANCING_AUTHORITY}."
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="eia_hourly_etl_dag",
    description=(
        "Daily extraction of EIA hourly grid operations data (CISO) "
        "into Snowflake RAW.EIA_HOURLY_OPS"
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",   # 03:00 UTC — after CAISO DAG, before dbt DAG
    catchup=False,
    tags=["eia", "etl", "snowflake"],
) as dag:

    extract_load_eia_hourly = PythonOperator(
        task_id="extract_load_eia_hourly",
        python_callable=run_eia_hourly_etl,
    )
