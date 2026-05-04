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

# Timestamp columns that must be serialised to strings before write_pandas.
# write_pandas can misinterpret pandas datetime64 values when the target
# Snowflake column type is TIMESTAMP_NTZ, producing corrupted "Invalid Date"
# or impossible negative-year values. Sending explicit strings bypasses that.
# Covers both CAISO columns (interval_start/end) and EIA column (period).
_TIMESTAMP_COLS = ["interval_start", "interval_end", "loaded_at", "period"]
_TIMESTAMP_FMT  = "%Y-%m-%d %H:%M:%S"


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
# DataFrame preparation
# ---------------------------------------------------------------------------

def prepare_dataframe_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a cleaned copy of `df` that is safe to pass to write_pandas.

    Two transformations are applied in order:
      1. Timestamp columns are converted to plain 'YYYY-MM-DD HH:MM:SS' strings.
         This avoids write_pandas misinterpreting pandas datetime64 values as
         corrupted or negative-year timestamps in Snowflake TIMESTAMP_NTZ columns.
      2. All column names are uppercased to match Snowflake DDL conventions.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame as returned by the extraction layer.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame ready for write_pandas.
    """
    df = df.copy()

    # Step 1 — Serialise timestamp columns to strings
    for col in _TIMESTAMP_COLS:
        if col not in df.columns:
            continue

        parsed = pd.to_datetime(df[col], errors="coerce")

        null_count = parsed.isna().sum()
        if null_count > 0:
            print(
                f"[prepare_dataframe_for_snowflake] WARNING: column '{col}' has "
                f"{null_count} value(s) that could not be parsed as a timestamp "
                f"and will be loaded as NULL."
            )

        df[col] = parsed.dt.strftime(_TIMESTAMP_FMT)

    # Step 2 — Uppercase column names to align with Snowflake DDL
    df.columns = [c.upper() for c in df.columns]

    return df


# ---------------------------------------------------------------------------
# Idempotent delete helper
# ---------------------------------------------------------------------------

def delete_existing_caiso_rows_for_date(load_date: str) -> int:
    """
    Delete all rows from RAW.CAISO_LMP_5MIN where DATE(INTERVAL_START) matches
    load_date. This makes repeated loads for the same date idempotent —
    re-running replaces the day's data rather than appending duplicates.

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
# Idempotent delete helper — EIA
# ---------------------------------------------------------------------------

def delete_existing_eia_rows_for_date(
    load_date: str,
    balancing_authority: str = "CISO",
) -> int:
    """
    Delete rows from RAW.EIA_HOURLY_OPS for a specific date and balancing
    authority. Scoping the delete to both dimensions avoids accidentally
    removing data for other BAs loaded on the same day.

    Parameters
    ----------
    load_date : str
        Date string in 'YYYY-MM-DD' format.
    balancing_authority : str
        EIA balancing authority code, e.g. 'CISO'. Defaults to 'CISO'.

    Returns
    -------
    int
        Number of rows deleted.
    """
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM RAW.EIA_HOURLY_OPS "
            "WHERE DATE(PERIOD) = %s AND BALANCING_AUTHORITY = %s",
            (load_date, balancing_authority),
        )
        deleted = cursor.rowcount
        print(
            f"[delete_existing_eia_rows_for_date] "
            f"Deleted {deleted} existing rows for date={load_date}, BA={balancing_authority}"
        )
        return deleted
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Truncate helper
# ---------------------------------------------------------------------------

def truncate_table(table_name: str, schema: str = "RAW") -> None:
    """
    Truncate all rows from schema.table_name.

    Used during the smoke test to clear corrupted data before a clean reload.

    Parameters
    ----------
    table_name : str
        Name of the table to truncate, e.g. 'CAISO_LMP_5MIN'.
    schema : str
        Schema containing the table. Defaults to 'RAW'.
    """
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
        print(f"[truncate_table] Truncated {schema}.{table_name}.")
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
    Prepare and load a pandas DataFrame into a Snowflake table using write_pandas.

    The target table must already exist (managed via sql/02_create_raw_tables.sql).
    Timestamp columns are serialised to strings and column names are uppercased
    before the load — see prepare_dataframe_for_snowflake() for details.

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

    # Serialise timestamps and uppercase columns before sending to Snowflake
    df_clean = prepare_dataframe_for_snowflake(df)

    conn = get_snowflake_connection()
    try:
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df_clean,
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
# Main — smoke test: CAISO + EIA load
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from extract_caiso_lmp import extract_caiso_lmp
    from extract_eia_hourly import extract_eia_hourly

    TEST_DATE               = "2024-01-15"
    TEST_HUBS               = ["NP15", "SP15", "ZP26"]
    TEST_BALANCING_AUTHORITY = "CISO"

    # ------------------------------------------------------------------
    # CAISO
    # ------------------------------------------------------------------
    print("=" * 60)
    print("CAISO LMP  →  RAW.CAISO_LMP_5MIN")
    print(f"Date : {TEST_DATE}  |  Hubs : {TEST_HUBS}")
    print("=" * 60)

    print("\n--- Extract ---")
    caiso_df = extract_caiso_lmp(TEST_DATE, TEST_DATE, TEST_HUBS)
    print(f"Extracted {len(caiso_df):,} rows.")

    print("\n--- Delete existing rows (idempotency) ---")
    delete_existing_caiso_rows_for_date(TEST_DATE)

    print("\n--- Load ---")
    caiso_rows = load_dataframe_to_table(caiso_df, table_name="CAISO_LMP_5MIN", schema="RAW")

    print(f"\n✓ CAISO done — {caiso_rows:,} rows in RAW.CAISO_LMP_5MIN")

    # ------------------------------------------------------------------
    # EIA
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("EIA Hourly  →  RAW.EIA_HOURLY_OPS")
    print(f"Date : {TEST_DATE}  |  BA : {TEST_BALANCING_AUTHORITY}")
    print("=" * 60)

    print("\n--- Extract ---")
    eia_df = extract_eia_hourly(
        start_date=f"{TEST_DATE}T00",
        end_date=f"{TEST_DATE}T23",
        balancing_authority=TEST_BALANCING_AUTHORITY,
    )
    print(f"Extracted {len(eia_df):,} rows.")

    print("\n--- Delete existing rows (idempotency) ---")
    delete_existing_eia_rows_for_date(TEST_DATE, TEST_BALANCING_AUTHORITY)

    print("\n--- Load ---")
    eia_rows = load_dataframe_to_table(eia_df, table_name="EIA_HOURLY_OPS", schema="RAW")

    print(f"\n✓ EIA done — {eia_rows:,} rows in RAW.EIA_HOURLY_OPS")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Smoke test complete")
    print(f"  RAW.CAISO_LMP_5MIN  : {caiso_rows:,} rows loaded")
    print(f"  RAW.EIA_HOURLY_OPS  : {eia_rows:,} rows loaded")
    print("=" * 60)
