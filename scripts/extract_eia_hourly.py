"""
extract_eia_hourly.py
---------------------
Extracts hourly electricity grid operations data from the EIA Open Data API v2
for a specified balancing authority and date range.

Data source : EIA Open Data API v2
              https://api.eia.gov/v2/electricity/rto/region-data/data/
Auth        : Free API key — register at https://www.eia.gov/opendata/register.php
              Set EIA_API_KEY in .env before running.

EIA response format (LONG):
  Each row is one (period, respondent, type) combination.
  The 'type' column holds the metric code:
    D   = Demand (MWh)
    DF  = Demand Forecast (MWh)
    NG  = Net Generation (MWh)
    TI  = Total Interchange (MWh)
  This script pivots that long format into one wide row per (period, BA).

Output      : pandas DataFrame conforming to the RAW.EIA_HOURLY_OPS schema
"""

import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
EIA_PAGE_SIZE = 5000

# EIA type codes and their target output column names
TYPE_CODE_MAP = {
    "D":  "demand_mwh",
    "DF": "demand_forecast_mwh",
    "NG": "net_generation_mwh",
    "TI": "interchange_mwh",
}

OUTPUT_COLUMNS = [
    "period",
    "balancing_authority",
    "demand_mwh",
    "demand_forecast_mwh",
    "net_generation_mwh",
    "interchange_mwh",
    "source_system",
    "loaded_at",
]


# ---------------------------------------------------------------------------
# Extraction function
# ---------------------------------------------------------------------------

def extract_eia_hourly(
    start_date: str,
    end_date: str,
    balancing_authority: str = "CISO",
) -> pd.DataFrame:
    """
    Extract hourly grid operations data from the EIA Open Data API v2.

    The EIA endpoint returns data in LONG format — one row per
    (period, balancing_authority, type). This function fetches all pages,
    pivots type codes into columns, and returns a wide DataFrame.

    Parameters
    ----------
    start_date : str
        Start of the range in 'YYYY-MM-DDTHH' format (e.g. '2024-01-15T00').
        EIA hourly data uses this format; 'YYYY-MM-DD' is also accepted and
        will be passed through as-is.
    end_date : str
        End of the range in 'YYYY-MM-DDTHH' format (e.g. '2024-01-15T23').
    balancing_authority : str
        EIA balancing authority code. Default is 'CISO' (California ISO).

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with one row per (period, balancing_authority) and
        columns matching RAW.EIA_HOURLY_OPS.
    """
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "EIA_API_KEY is not set. "
            "Register for a free key at https://www.eia.gov/opendata/register.php "
            "and add EIA_API_KEY=<your_key> to your .env file."
        )

    print(f"\n[extract_eia_hourly] BA={balancing_authority}  |  {start_date} → {end_date}")

    all_records: list[dict] = []
    offset = 0

    while True:
        # EIA API v2 parameter names — verified against the v2 documentation.
        # data[]=value requests the numeric value field.
        # facets[respondent][]=<BA> filters to the target balancing authority.
        # No facets[type] filter — we request all types and pivot after.
        params = {
            "api_key":               api_key,
            "frequency":             "hourly",
            "data[]":                "value",
            "facets[respondent][]":  balancing_authority,
            "start":                 start_date,
            "end":                   end_date,
            "sort[0][column]":       "period",
            "sort[0][direction]":    "asc",
            "offset":                offset,
            "length":                EIA_PAGE_SIZE,
        }

        req = requests.Request("GET", EIA_BASE_URL, params=params).prepare()
        # Print URL but mask the key so it never appears in logs
        safe_url = req.url.replace(api_key, "***API_KEY***")
        print(f"[extract_eia_hourly] Request URL:\n  {safe_url}")

        response = requests.get(EIA_BASE_URL, params=params, timeout=30)

        if response.status_code != 200:
            print(f"[extract_eia_hourly] ERROR — HTTP {response.status_code}")
            print(f"[extract_eia_hourly] Response body (first 1000 chars):\n{response.text[:1000]}")
            raise RuntimeError(f"EIA API returned HTTP {response.status_code}")

        payload = response.json()

        # Defensive: surface any API-level error messages
        if "error" in payload:
            print(f"[extract_eia_hourly] API error: {payload['error']}")
            raise RuntimeError(f"EIA API error: {payload['error']}")

        response_block = payload.get("response", {})
        page_data      = response_block.get("data", [])
        total          = int(response_block.get("total", 0))

        print(
            f"[extract_eia_hourly] Page offset={offset} — "
            f"returned {len(page_data)} rows  (total reported: {total})"
        )

        if not page_data:
            break

        # Print raw columns and a sample row on the first page
        if offset == 0 and page_data:
            print(f"[extract_eia_hourly] Raw API columns : {list(page_data[0].keys())}")
            print(f"[extract_eia_hourly] Sample raw row  : {page_data[0]}")

        all_records.extend(page_data)

        # Paginate while there may be more records
        offset += EIA_PAGE_SIZE
        if len(page_data) < EIA_PAGE_SIZE:
            break

    print(f"\n[extract_eia_hourly] Total raw rows fetched: {len(all_records)}")

    if not all_records:
        print("[extract_eia_hourly] No data returned. Returning empty DataFrame.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    raw_df = pd.DataFrame(all_records)
    print(f"[extract_eia_hourly] Raw DataFrame columns : {raw_df.columns.tolist()}")
    print(f"[extract_eia_hourly] Raw DataFrame shape   : {raw_df.shape}")

    result_df = _pivot_and_normalise(raw_df, balancing_authority)

    result_df["source_system"] = "eia_api"
    result_df["loaded_at"]     = datetime.now(timezone.utc).replace(tzinfo=None)

    print(f"\n[extract_eia_hourly] Final row count : {len(result_df)}")
    print(f"[extract_eia_hourly] Final columns   : {result_df.columns.tolist()}")

    return result_df[OUTPUT_COLUMNS]


# ---------------------------------------------------------------------------
# Pivot / normalise helper
# ---------------------------------------------------------------------------

def _pivot_and_normalise(raw_df: pd.DataFrame, balancing_authority: str) -> pd.DataFrame:
    """
    Pivot EIA long-format data into one wide row per (period, balancing_authority).

    EIA returns one row per (period, respondent, type) where type is a metric
    code such as D, DF, NG, or TI. We pivot on `type` so each unique period
    becomes a single row with one column per metric.
    """
    # Detect the column that holds the metric type code (usually 'type')
    type_col = _find_col(raw_df, ["type", "type-name"])

    # Detect the column that holds the numeric value (usually 'value')
    value_col = _find_col(raw_df, ["value"])

    # Detect the period column
    period_col = _find_col(raw_df, ["period"])

    if not all([type_col, value_col, period_col]):
        print(
            f"[_pivot_and_normalise] Could not identify required columns. "
            f"Available: {raw_df.columns.tolist()}"
        )
        print(f"[_pivot_and_normalise] Sample row: {raw_df.iloc[0].to_dict()}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Keep only rows for the four metric types we care about
    known_types = set(TYPE_CODE_MAP.keys())
    filtered = raw_df[raw_df[type_col].isin(known_types)].copy()

    if filtered.empty:
        print(
            f"[_pivot_and_normalise] No rows matched known type codes {known_types}. "
            f"Unique values in '{type_col}': {raw_df[type_col].unique().tolist()}"
        )
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Cast value to numeric
    filtered[value_col] = pd.to_numeric(filtered[value_col], errors="coerce")

    # Pivot: index = period, columns = type code, values = numeric value
    wide = filtered.pivot_table(
        index=period_col,
        columns=type_col,
        values=value_col,
        aggfunc="first",   # Each (period, type) should be unique; first() is safe
    ).reset_index()

    wide.columns.name = None

    # Rename type codes to schema column names
    rename = {period_col: "period"}
    rename.update({k: v for k, v in TYPE_CODE_MAP.items() if k in wide.columns})
    wide = wide.rename(columns=rename)

    # Ensure all four metric columns exist (fill with NaN if missing from API)
    for col in ["demand_mwh", "demand_forecast_mwh", "net_generation_mwh", "interchange_mwh"]:
        if col not in wide.columns:
            print(f"[_pivot_and_normalise] WARNING: '{col}' not present in API response — filling with NaN.")
            wide[col] = float("nan")

    # Parse period to datetime (EIA format: '2024-01-15T00')
    wide["period"] = pd.to_datetime(wide["period"], format="%Y-%m-%dT%H", errors="coerce")

    # Attach balancing authority
    wide["balancing_authority"] = balancing_authority

    return wide


# ---------------------------------------------------------------------------
# Column detection helper
# ---------------------------------------------------------------------------

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column name found in df.columns, or None."""
    for name in candidates:
        if name in df.columns:
            return name
    return None


# ---------------------------------------------------------------------------
# Main — smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    TEST_START              = "2024-01-15T00"
    TEST_END                = "2024-01-15T23"
    TEST_BALANCING_AUTHORITY = "CISO"

    print("=" * 60)
    print("EIA hourly extraction smoke test")
    print(f"Range : {TEST_START} → {TEST_END}")
    print(f"BA    : {TEST_BALANCING_AUTHORITY}")
    print("=" * 60)

    df = extract_eia_hourly(TEST_START, TEST_END, TEST_BALANCING_AUTHORITY)

    print("\n" + "=" * 60)
    print(f"Extracted {len(df):,} rows")
    print(f"Columns : {df.columns.tolist()}")
    print("\nFirst 5 rows:")
    print(df.head().to_string())
    print("=" * 60)
