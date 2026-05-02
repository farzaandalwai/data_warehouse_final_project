"""
load_to_snowflake.py
--------------------
Reusable utilities for connecting to Snowflake and loading pandas DataFrames
into target tables.

Credentials are read exclusively from environment variables — never hardcoded.
Required .env variables:
    SNOWFLAKE_ACCOUNT    — e.g. SFEDU02-EAB27764
    SNOWFLAKE_USER       — Snowflake username
    SNOWFLAKE_PASSWORD   — Snowflake password
    SNOWFLAKE_ROLE       — Role to assume (e.g. TRAINING_ROLE)
    SNOWFLAKE_WAREHOUSE  — Virtual warehouse name
    SNOWFLAKE_DATABASE   — Target database (e.g. ELECTRICITY_MARKET_DB)
    SNOWFLAKE_SCHEMA     — Default schema (e.g. RAW)
"""

import os
import sys

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


load_dotenv()


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Return an authenticated Snowflake connection built from environment variables.

    Raises EnvironmentError if any required variable is missing.
    """
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]

    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required Snowflake environment variables: {missing}. "
            "Check your .env file."
        )

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

    # Explicitly activate warehouse and database in the session.
    # The connection params set defaults but some Snowflake account configurations
    # require an explicit USE statement before DML can execute.
    warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
    database  = os.environ["SNOWFLAKE_DATABASE"]
    conn.cursor().execute(f'USE WAREHOUSE "{warehouse}"')
    conn.cursor().execute(f'USE DATABASE "{database}"')

    return conn


# ---------------------------------------------------------------------------
# Idempotent delete helper
# ---------------------------------------------------------------------------

def delete_existing_caiso_rows_for_date(load_date: str) -> int:
    """
    Delete all rows from RAW.CAISO_LMP_5MIN where DATE(INTERVAL_START) matches
    load_date. This makes repeated loads for the same date idempotent —
    re-running will replace the day's data rather than appending duplicates.

    Parameters
    ----------
    load_date : str
        Date string in 'YYYY-MM-DD' format.

    Returns
    -------
    int
        Number of rows deleted.
    """
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM RAW.CAISO_LMP_5MIN WHERE DATE(INTERVAL_START) = %s",
            (load_date,),
        )
        deleted = cursor.rowcount
        print(
            f"[delete_existing_caiso_rows_for_date] "
            f"Deleted {deleted} existing rows for date={load_date}"
        )
        return deleted
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DataFrame loader
# ---------------------------------------------------------------------------

def load_dataframe_to_table(
    df: pd.DataFrame,
    table_name: str,
    schema: str | None = None,
) -> int:
    """
    Load a pandas DataFrame into a Snowflake table using write_pandas.

    The target table must already exist (managed via sql/02_create_raw_tables.sql).
    Column names are uppercased automatically to match Snowflake conventions.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to load. Columns must match the target table's column names
        (case-insensitive — they are uppercased before loading).
    table_name : str
        Name of the target table, e.g. 'CAISO_LMP_5MIN'.
    schema : str, optional
        Schema override. Uses SNOWFLAKE_SCHEMA from the environment if omitted.

    Returns
    -------
    int
        Number of rows successfully loaded.
    """
    if df.empty:
        print(
            f"[load_dataframe_to_table] DataFrame is empty — "
            f"skipping load to {table_name}."
        )
        return 0

    target_db     = os.environ["SNOWFLAKE_DATABASE"]
    target_schema = schema or os.environ["SNOWFLAKE_SCHEMA"]

    print(
        f"[load_dataframe_to_table] Loading {len(df):,} rows → "
        f"{target_db}.{target_schema}.{table_name.upper()} ..."
    )

    # write_pandas matches on column names — uppercase to align with Snowflake DDL
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    conn = get_snowflake_connection()
    try:
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=target_db,
            schema=target_schema,
            auto_create_table=False,
        )

        if success:
            print(
                f"[load_dataframe_to_table] "
                f"Success — {num_rows:,} rows loaded in {num_chunks} chunk(s)."
            )
        else:
            print("[load_dataframe_to_table] write_pandas reported failure.")

        return num_rows

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main — smoke test: extract CAISO data and load to Snowflake
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Allow running as `python scripts/load_to_snowflake.py` from the project root.
    # sys.path[0] is already the scripts/ directory when the script is run directly,
    # so extract_caiso_lmp is importable as a sibling module.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from extract_caiso_lmp import extract_caiso_lmp

    TEST_DATE = "2024-01-15"
    TEST_HUBS = ["NP15", "SP15", "ZP26"]

    print("=" * 60)
    print("Step 2 smoke test: extract CAISO LMP → load to Snowflake")
    print(f"Date  : {TEST_DATE}")
    print(f"Hubs  : {TEST_HUBS}")
    print("=" * 60)

    # 1. Extract
    print("\n--- Extraction ---")
    df = extract_caiso_lmp(TEST_DATE, TEST_DATE, TEST_HUBS)
    print(f"\nExtracted {len(df):,} rows.")

    # 2. Delete existing rows for the date (idempotency)
    print("\n--- Pre-load delete ---")
    delete_existing_caiso_rows_for_date(TEST_DATE)

    # 3. Load into Snowflake
    print("\n--- Load ---")
    rows_loaded = load_dataframe_to_table(df, table_name="CAISO_LMP_5MIN", schema="RAW")

    print("\n" + "=" * 60)
    print(f"Done. {rows_loaded:,} rows loaded into RAW.CAISO_LMP_5MIN.")
    print("=" * 60)
