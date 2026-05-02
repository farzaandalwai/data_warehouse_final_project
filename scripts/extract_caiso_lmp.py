"""
extract_caiso_lmp.py
--------------------
Extracts 5-minute Locational Marginal Price (LMP) data from the CAISO OASIS API
for one or more trading hubs over a specified date range.

Data source : CAISO OASIS  (http://oasis.caiso.com/oasisapi/)
Output      : pandas DataFrame conforming to the RAW.CAISO_LMP_5MIN schema
"""

import requests
import pandas as pd
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# TODO: Confirm the correct CAISO OASIS endpoint and report type.
#       Common base URL: http://oasis.caiso.com/oasisapi/SingleZip
#       Report name for real-time LMP by hub: PRC_HASP_LMP or PRC_LMP
CAISO_OASIS_BASE_URL = "http://oasis.caiso.com/oasisapi/SingleZip"

# TODO: Verify the correct queryname for 5-minute real-time LMP data.
#       Options include: PRC_INTVL_LMP (interval LMP), PRC_HASP_LMP (HASP)
CAISO_QUERY_NAME = "PRC_INTVL_LMP"

# CAISO OASIS returns data in a ZIP file containing XML or CSV.
# TODO: Handle ZIP extraction and parsing of the CAISO response format.
CAISO_RESPONSE_FORMAT = "6"  # 6 = CSV (check CAISO OASIS API documentation)

# Expected output columns matching RAW.CAISO_LMP_5MIN schema
EXPECTED_COLUMNS = [
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
    Extract 5-minute LMP data from CAISO OASIS for the given date range and hubs.

    Parameters
    ----------
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).
    trading_hubs : list[str]
        List of CAISO trading hub identifiers, e.g. ['NP15', 'SP15', 'ZP26'].

    Returns
    -------
    pd.DataFrame
        DataFrame with columns matching RAW.CAISO_LMP_5MIN schema.
    """
    all_records = []

    for hub in trading_hubs:
        print(f"Extracting CAISO LMP for hub={hub}, {start_date} to {end_date} ...")

        # TODO: Map friendly hub names (NP15, SP15, ZP26) to the node IDs
        #       expected by the CAISO OASIS API (e.g. TH_NP15_GEN-APND).
        caiso_node_id = _resolve_hub_node_id(hub)

        # TODO: Build the correct query parameters per the CAISO OASIS API spec.
        #       Required params typically include: queryname, startdatetime, enddatetime,
        #       market_run_id, node, version, resultformat.
        params = {
            "queryname": CAISO_QUERY_NAME,
            "startdatetime": f"{start_date}T00:00-0000",
            "enddatetime": f"{end_date}T23:55-0000",
            "market_run_id": "RTM",
            "node": caiso_node_id,
            "version": 1,
            "resultformat": CAISO_RESPONSE_FORMAT,
        }

        # TODO: Make the HTTP request, handle rate limiting and retries.
        response = requests.get(CAISO_OASIS_BASE_URL, params=params, timeout=60)
        response.raise_for_status()

        # TODO: CAISO returns a ZIP file. Extract the CSV from the archive and
        #       parse it into a DataFrame. The parsing logic below is a placeholder.
        raw_df = _parse_caiso_response(response, hub)

        all_records.append(raw_df)

    if not all_records:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    result_df = pd.concat(all_records, ignore_index=True)
    result_df["source_system"] = "caiso_oasis"
    result_df["loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    return result_df[EXPECTED_COLUMNS]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _resolve_hub_node_id(hub: str) -> str:
    """
    Map a short trading hub name to the full CAISO OASIS node identifier.

    TODO: Verify these node IDs against the CAISO OASIS Node/Resource List report.
          The correct suffix depends on the query type (e.g. -APND for aggregated).
    """
    hub_map = {
        "NP15": "TH_NP15_GEN-APND",
        "SP15": "TH_SP15_GEN-APND",
        "ZP26": "TH_ZP26_GEN-APND",
    }
    node_id = hub_map.get(hub.upper())
    if node_id is None:
        raise ValueError(f"Unknown trading hub: '{hub}'. Expected one of {list(hub_map.keys())}")
    return node_id


def _parse_caiso_response(response: requests.Response, hub: str) -> pd.DataFrame:
    """
    Parse the raw CAISO OASIS HTTP response into a DataFrame.

    TODO: Implement ZIP extraction and CSV parsing.
          CAISO returns a .zip file containing one or more CSV files.
          Steps:
            1. io.BytesIO(response.content) → ZipFile
            2. Extract the CSV member from the archive
            3. pd.read_csv(csv_bytes)
            4. Rename CAISO columns to match the schema:
               - INTERVALSTARTTIME_GMT → interval_start
               - INTERVALENDTIME_GMT   → interval_end
               - MW (or LMP_TYPE breakdown) → lmp / energy_component / etc.
    """
    # Placeholder: return an empty DataFrame with the correct schema
    return pd.DataFrame(columns=[
        "interval_start", "interval_end", "trading_hub",
        "lmp", "energy_component", "congestion_component", "loss_component", "market",
    ])


# ---------------------------------------------------------------------------
# Main — quick smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    TEST_START = "2024-01-15"
    TEST_END   = "2024-01-15"
    TEST_HUBS  = ["NP15", "SP15", "ZP26"]

    print("Running CAISO LMP extraction smoke test ...")
    df = extract_caiso_lmp(TEST_START, TEST_END, TEST_HUBS)

    print(f"\nExtracted {len(df)} rows.")
    print(df.head())
    print("\nColumns:", df.columns.tolist())
