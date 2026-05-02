"""
extract_eia_hourly.py
---------------------
Extracts hourly electricity grid operations data from the EIA Open Data API
for a specified balancing authority and date range.

Data source : EIA Open Data API  (https://api.eia.gov/v2/electricity/rto/)
Auth        : API key loaded from EIA_API_KEY environment variable
Output      : pandas DataFrame conforming to the RAW.EIA_HOURLY_OPS schema
"""

import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv


# Load environment variables from .env file (if present)
load_dotenv()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# TODO: Confirm the exact EIA API v2 endpoint for hourly RTO operations data.
#       Candidates:
#         - /v2/electricity/rto/region-data/data/      (demand, generation, interchange by region)
#         - /v2/electricity/rto/fuel-type-data/data/   (generation by fuel type)
#       Check https://api.eia.gov/v2/electricity/rto/ for available routes.
EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

# Columns returned by the EIA API that we care about.
# TODO: Verify exact column names from the EIA API response schema.
EIA_RESPONSE_COLUMNS = {
    "period":       "period",            # ISO 8601 hourly timestamp
    "respondent":   "balancing_authority",
    "D":            "demand_mwh",
    "DF":           "demand_forecast_mwh",
    "NG":           "net_generation_mwh",
    "TI":           "interchange_mwh",
}

# Maximum number of rows the EIA API returns per request (pagination limit)
EIA_PAGE_SIZE = 5000

# Expected output columns matching RAW.EIA_HOURLY_OPS schema
EXPECTED_COLUMNS = [
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
    balancing_authority: str,
) -> pd.DataFrame:
    """
    Extract hourly operations data from the EIA Open Data API.

    Parameters
    ----------
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).
    balancing_authority : str
        EIA balancing authority code, e.g. 'CISO' for California ISO.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns matching RAW.EIA_HOURLY_OPS schema.
    """
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "EIA_API_KEY is not set. Add it to your .env file or environment variables."
        )

    print(f"Extracting EIA hourly ops: BA={balancing_authority}, {start_date} to {end_date} ...")

    all_records = []
    offset = 0

    while True:
        # TODO: Verify the correct parameter names for the EIA API v2 filters.
        #       Common parameters: api_key, frequency, data[], facets[respondent][],
        #       start, end, sort[0][column], sort[0][direction], offset, length
        params = {
            "api_key": api_key,
            "frequency": "hourly",
            "data[]": ["D", "DF", "NG", "TI"],
            "facets[respondent][]": balancing_authority,
            "start": start_date,
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": EIA_PAGE_SIZE,
        }

        # TODO: Add retry logic with exponential backoff for transient API errors.
        response = requests.get(EIA_BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        payload = response.json()

        # TODO: Validate the response structure. EIA API v2 wraps data under
        #       payload["response"]["data"]. Check for error keys in the response.
        data = payload.get("response", {}).get("data", [])

        if not data:
            break

        all_records.extend(data)

        # Paginate if more records are available
        total = payload.get("response", {}).get("total", 0)
        offset += EIA_PAGE_SIZE
        if offset >= total:
            break

    if not all_records:
        print("No data returned from EIA API for the given parameters.")
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    df = pd.DataFrame(all_records)

    # TODO: Rename and cast columns to match the expected schema.
    #       EIA may return value columns as strings; cast to float.
    df = _normalise_eia_dataframe(df, balancing_authority)

    df["source_system"] = "eia_api"
    df["loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    return df[EXPECTED_COLUMNS]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _normalise_eia_dataframe(df: pd.DataFrame, balancing_authority: str) -> pd.DataFrame:
    """
    Rename EIA API response columns to the RAW schema column names and cast types.

    TODO: Map actual EIA column names once the API response structure is confirmed.
          The EIA API v2 returns a 'value' column for each requested data[] item,
          which may require pivoting from long to wide format.
    """
    # Placeholder renames — adjust once you inspect a live API response
    rename_map = {col: target for col, target in EIA_RESPONSE_COLUMNS.items() if col in df.columns}
    df = df.rename(columns=rename_map)

    # Ensure balancing_authority is set if it came from the facet filter (not a column)
    if "balancing_authority" not in df.columns:
        df["balancing_authority"] = balancing_authority

    # Cast numeric columns
    for col in ["demand_mwh", "demand_forecast_mwh", "net_generation_mwh", "interchange_mwh"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cast period to datetime
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], utc=True).dt.tz_localize(None)

    return df


# ---------------------------------------------------------------------------
# Main — quick smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    TEST_START = "2024-01-15"
    TEST_END   = "2024-01-15"
    TEST_BA    = "CISO"   # California ISO

    print("Running EIA hourly extraction smoke test ...")
    df = extract_eia_hourly(TEST_START, TEST_END, TEST_BA)

    print(f"\nExtracted {len(df)} rows.")
    print(df.head())
    print("\nColumns:", df.columns.tolist())
