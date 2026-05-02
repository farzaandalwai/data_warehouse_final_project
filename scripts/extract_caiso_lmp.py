"""
extract_caiso_lmp.py
--------------------
Extracts real-time interval LMP data from CAISO OASIS for one or more
trading hubs over a specified date range.

Data source : CAISO OASIS SingleZip endpoint
              http://oasis.caiso.com/oasisapi/SingleZip
No credentials required — CAISO OASIS is publicly accessible.

Output      : pandas DataFrame conforming to the RAW.CAISO_LMP_5MIN schema
"""

import io
import time
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CAISO_OASIS_URL = "http://oasis.caiso.com/oasisapi/SingleZip"

# Map friendly hub names to CAISO OASIS node identifiers
HUB_NODE_MAP = {
    "NP15": "TH_NP15_GEN-APND",
    "SP15": "TH_SP15_GEN-APND",
    "ZP26": "TH_ZP26_GEN-APND",
}

# CAISO LMP_TYPE values and their target column names
LMP_TYPE_MAP = {
    "LMP": "lmp",
    "MCE": "energy_component",
    "MCC": "congestion_component",
    "MCL": "loss_component",
}

# Expected output columns matching RAW.CAISO_LMP_5MIN schema
OUTPUT_COLUMNS = [
    "interval_start",
    "interval_end",
    "trading_hub",
    "lmp",
    "energy_component",
    "congestion_component",
    "loss_component",
    "market",
    "source_system",
    "loaded_at",
]


# ---------------------------------------------------------------------------
# Extraction function
# ---------------------------------------------------------------------------

def extract_caiso_lmp(
    start_date: str,
    end_date: str,
    trading_hubs: list[str],
) -> pd.DataFrame:
    """
    Extract 5-minute interval LMP data from CAISO OASIS for the given date
    range and trading hubs.

    Parameters
    ----------
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).
    trading_hubs : list[str]
        List of friendly hub names: 'NP15', 'SP15', and/or 'ZP26'.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns matching RAW.CAISO_LMP_5MIN schema.
    """
    all_hub_dfs = []

    for hub in trading_hubs:
        hub_upper = hub.upper()
        node_id = HUB_NODE_MAP.get(hub_upper)
        if node_id is None:
            raise ValueError(
                f"Unknown trading hub '{hub}'. Expected one of: {list(HUB_NODE_MAP.keys())}"
            )

        # CAISO OASIS expects datetimes as YYYYMMDDTHH:MM-0000 (colon required in time)
        start_str = f"{start_date.replace('-', '')}T00:00-0000"
        end_str   = f"{end_date.replace('-', '')}T23:55-0000"

        params = {
            "queryname":     "PRC_INTVL_LMP",
            "market_run_id": "RTM",
            "startdatetime": start_str,
            "enddatetime":   end_str,
            "node":          node_id,
            "resultformat":  "6",      # 6 = CSV inside ZIP
            "version":       "1",
        }

        # Build and print the full request URL for debugging
        req = requests.Request("GET", CAISO_OASIS_URL, params=params).prepare()
        print(f"\n[{hub_upper}] Request URL:\n  {req.url}")

        response = requests.get(CAISO_OASIS_URL, params=params, timeout=120)
        print(f"[{hub_upper}] HTTP status: {response.status_code}  |  "
              f"Content-Type: {response.headers.get('Content-Type', 'unknown')}  |  "
              f"Size: {len(response.content):,} bytes")

        if response.status_code != 200:
            print(f"[{hub_upper}] Non-200 response. Body (first 1000 chars):\n"
                  f"{response.text[:1000]}")
            continue

        hub_df = _parse_zip_response(response, hub_upper)
        if hub_df is not None and not hub_df.empty:
            all_hub_dfs.append(hub_df)

        # CAISO enforces a rate limit — wait between hub requests to avoid 429
        if hub != trading_hubs[-1]:
            print(f"[{hub_upper}] Waiting 6 seconds before next request ...")
            time.sleep(6)

    if not all_hub_dfs:
        print("\nNo data extracted for any hub. Returning empty DataFrame.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    combined = pd.concat(all_hub_dfs, ignore_index=True)
    combined["market"]        = "RTM"
    combined["source_system"] = "caiso_oasis"
    combined["loaded_at"]     = datetime.now(timezone.utc).replace(tzinfo=None)

    return combined[OUTPUT_COLUMNS]


# ---------------------------------------------------------------------------
# ZIP / CSV parsing
# ---------------------------------------------------------------------------

def _parse_zip_response(response: requests.Response, hub: str) -> pd.DataFrame | None:
    """
    Extract the CSV file from the CAISO OASIS ZIP response and parse it into
    a pivoted DataFrame with one row per interval per hub.

    CAISO returns rows in long format — one row per (interval, LMP_TYPE) — where
    LMP_TYPE is one of: LMP, MCE (energy), MCC (congestion), MCL (loss).
    We pivot those into a single wide row per interval.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(response.content))
    except zipfile.BadZipFile:
        print(f"[{hub}] BadZipFile error — response is not a valid ZIP.")
        print(f"[{hub}] Response text (first 1000 chars):\n{response.text[:1000]}")
        return None

    # Print the file names inside the ZIP for debugging
    zip_names = zf.namelist()
    print(f"[{hub}] ZIP contents: {zip_names}")

    # CAISO typically puts exactly one CSV in the archive
    csv_files = [n for n in zip_names if n.lower().endswith(".csv")]
    if not csv_files:
        print(f"[{hub}] No CSV file found inside ZIP. Contents: {zip_names}")
        return None

    csv_name = csv_files[0]
    with zf.open(csv_name) as f:
        raw = pd.read_csv(f)

    # Print raw columns for debugging
    print(f"[{hub}] Raw CAISO columns: {raw.columns.tolist()}")
    print(f"[{hub}] Raw row count: {len(raw)}")

    if raw.empty:
        print(f"[{hub}] CSV is empty.")
        return None

    # Normalise column names to uppercase for consistent access
    raw.columns = [c.strip().upper() for c in raw.columns]

    # Identify the LMP_TYPE column (may be named LMP_TYPE or DATA_ITEM)
    lmp_type_col = _find_column(raw, ["LMP_TYPE", "DATA_ITEM"])
    value_col    = _find_column(raw, ["MW", "VALUE"])
    start_col    = _find_column(raw, ["INTERVALSTARTTIME_GMT", "INTERVAL_START_GMT",
                                      "STARTTIME", "START_TIME"])
    end_col      = _find_column(raw, ["INTERVALENDTIME_GMT",   "INTERVAL_END_GMT",
                                      "ENDTIME",   "END_TIME"])

    if not all([lmp_type_col, value_col, start_col, end_col]):
        print(f"[{hub}] Could not identify required columns in: {raw.columns.tolist()}")
        return None

    # Keep only the LMP_TYPE values we care about
    raw = raw[raw[lmp_type_col].isin(LMP_TYPE_MAP.keys())].copy()
    if raw.empty:
        print(f"[{hub}] No matching LMP_TYPE rows after filtering.")
        return None

    # Pivot from long → wide: one row per interval, one column per LMP_TYPE
    pivoted = raw.pivot_table(
        index=[start_col, end_col],
        columns=lmp_type_col,
        values=value_col,
        aggfunc="first",
    ).reset_index()

    # Flatten the MultiIndex column created by pivot_table
    pivoted.columns.name = None

    # Rename columns to schema names
    rename = {
        start_col: "interval_start",
        end_col:   "interval_end",
    }
    rename.update({k: v for k, v in LMP_TYPE_MAP.items() if k in pivoted.columns})
    pivoted = pivoted.rename(columns=rename)

    # Ensure all four component columns exist (fill with NaN if absent)
    for col in ["lmp", "energy_component", "congestion_component", "loss_component"]:
        if col not in pivoted.columns:
            pivoted[col] = float("nan")

    # Parse interval timestamps to datetime
    pivoted["interval_start"] = pd.to_datetime(pivoted["interval_start"], utc=True).dt.tz_localize(None)
    pivoted["interval_end"]   = pd.to_datetime(pivoted["interval_end"],   utc=True).dt.tz_localize(None)

    # Add the hub name
    pivoted["trading_hub"] = hub

    return pivoted


# ---------------------------------------------------------------------------
# Column detection helper
# ---------------------------------------------------------------------------

def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column name found in df, or None."""
    for name in candidates:
        if name in df.columns:
            return name
    return None


# ---------------------------------------------------------------------------
# Main — smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    TEST_START = "2024-01-15"
    TEST_END   = "2024-01-15"
    TEST_HUBS  = ["NP15", "SP15", "ZP26"]

    print("=" * 60)
    print("CAISO LMP extraction smoke test")
    print(f"Date range : {TEST_START} to {TEST_END}")
    print(f"Hubs       : {TEST_HUBS}")
    print("=" * 60)

    df = extract_caiso_lmp(TEST_START, TEST_END, TEST_HUBS)

    print("\n" + "=" * 60)
    print(f"Total rows extracted : {len(df)}")
    print(f"Columns              : {df.columns.tolist()}")
    print("\nFirst 5 rows:")
    print(df.head().to_string())
