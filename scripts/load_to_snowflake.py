"""
load_to_snowflake.py
--------------------
Reusable utilities for connecting to Snowflake and loading pandas DataFrames
into target tables.

Credentials are read exclusively from environment variables — never hardcoded.
Add the required variables to your .env file (see .env.example).

Required environment variables:
    SNOWFLAKE_ACCOUNT    — e.g. xy12345.us-east-1
    SNOWFLAKE_USER       — Snowflake username
    SNOWFLAKE_PASSWORD   — Snowflake password (or use key-pair auth; see TODO below)
    SNOWFLAKE_WAREHOUSE  — Virtual warehouse name
    SNOWFLAKE_DATABASE   — Target database (ELECTRICITY_MARKET_DB)
    SNOWFLAKE_SCHEMA     — Default schema (typically RAW for raw loads)
    SNOWFLAKE_ROLE       — Role to assume (e.g. SYSADMIN or a custom loader role)
"""

import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


# Load environment variables from .env file (if present)
load_dotenv()


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Build and return an authenticated Snowflake connection using credentials
    stored in environment variables.

    Returns
    -------
    snowflake.connector.SnowflakeConnection
        An open Snowflake connection. Callers are responsible for closing it
        (or using it as a context manager).

    Raises
    ------
    EnvironmentError
        If any required environment variable is missing.
    """
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
    ]

    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required Snowflake environment variables: {missing}. "
            "Add them to your .env file."
        )

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
        # TODO: For production, replace password auth with key-pair authentication.
        #       Set private_key_path=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") and
        #       remove the password parameter.
    )

    return conn


# ---------------------------------------------------------------------------
# DataFrame loader
# ---------------------------------------------------------------------------

def load_dataframe_to_table(
    df: pd.DataFrame,
    table_name: str,
    database: str | None = None,
    schema: str | None = None,
    overwrite: bool = False,
) -> int:
    """
    Load a pandas DataFrame into a Snowflake table using write_pandas.

    The target table must already exist in Snowflake with a compatible schema.
    Column names in `df` are matched case-insensitively to Snowflake columns.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to load. Column names should match the target table's columns.
    table_name : str
        Name of the target Snowflake table (uppercase recommended, e.g. 'EIA_HOURLY_OPS').
    database : str, optional
        Override the database from the environment variable.
    schema : str, optional
        Override the schema from the environment variable.
    overwrite : bool
        If True, truncate the table before loading. Defaults to False (append).

    Returns
    -------
    int
        Number of rows successfully loaded.
    """
    if df.empty:
        print(f"[load_dataframe_to_table] DataFrame is empty — skipping load to {table_name}.")
        return 0

    target_db     = database or os.environ.get("SNOWFLAKE_DATABASE", "ELECTRICITY_MARKET_DB")
    target_schema = schema   or os.environ.get("SNOWFLAKE_SCHEMA",   "RAW")

    print(
        f"[load_dataframe_to_table] Loading {len(df)} rows → "
        f"{target_db}.{target_schema}.{table_name} (overwrite={overwrite}) ..."
    )

    conn = get_snowflake_connection()

    try:
        # TODO: write_pandas requires column names to match exactly.
        #       Ensure df.columns are uppercased to align with Snowflake conventions.
        df.columns = [c.upper() for c in df.columns]

        # TODO: For large DataFrames, consider chunking or using Snowflake COPY INTO
        #       with staged files (PUT + COPY INTO) for better throughput.
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database=target_db,
            schema=target_schema,
            overwrite=overwrite,
            auto_create_table=False,  # Table must pre-exist; managed via DDL scripts
        )

        if success:
            print(f"[load_dataframe_to_table] Loaded {num_rows} rows in {num_chunks} chunk(s).")
        else:
            print(f"[load_dataframe_to_table] Load reported failure. Output: {output}")

        return num_rows

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main — quick connection test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Testing Snowflake connection ...")
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_WAREHOUSE()")
    row = cursor.fetchone()
    print(f"Connected: account={row[0]}, user={row[1]}, warehouse={row[2]}")
    conn.close()
    print("Connection test passed.")
