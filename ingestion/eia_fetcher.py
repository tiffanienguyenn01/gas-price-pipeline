"""
ingestion/eia_fetcher.py
========================
Fetches weekly retail gas price data from the EIA API.
Saves raw JSON (bronze) and clean Parquet (silver) to disk.

Run manually to test:
    python ingestion/eia_fetcher.py
"""

# ── SECTION 1: IMPORTS ───────────────────────────────────────────────────────
# Built-in Python tools (no installation needed)
import json
import os
from datetime import datetime
from pathlib import Path

# Installed packages (from requirements.txt)
import pandas as pd
import requests
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from pydantic import BaseModel, ValidationError


# ── SECTION 2: CONFIGURATION ─────────────────────────────────────────────────
# Read secrets from .env file into memory
load_dotenv()

EIA_API_KEY         = os.getenv("EIA_API_KEY")
RAW_DATA_PATH       = Path(os.getenv("RAW_DATA_PATH", "./data/raw"))
PROCESSED_DATA_PATH = Path(os.getenv("PROCESSED_DATA_PATH", "./data/processed"))

EIA_BASE_URL = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"

PRODUCT_CODES = {
    "regular":  "EMM_EPMR_PTE_NUS_DPG",
    "midgrade": "EMM_EPMM_PTE_NUS_DPG",
    "premium":  "EMM_EPMP_PTE_NUS_DPG",
    "diesel":   "EMM_EPMD_PTE_NUS_DPG",
}


# ── SECTION 3: DATA SCHEMA ───────────────────────────────────────────────────
# Pydantic validates every row from the API against this blueprint.
# If a row doesn't match, it's caught and logged — not silently corrupted.
class GasPriceRecord(BaseModel):
    period: str           # date string  e.g. "2024-01-08"
    series_id: str        # EIA internal ID
    value: float | None   # price in $/gal — can be None (missing weeks)
    unit: str             # always "$/gal"
    product_name: str     # "regular" | "midgrade" | "premium" | "diesel"


# ── SECTION 4: API CALL ──────────────────────────────────────────────────────
# @retry wraps the function with automatic retry logic:
#   - Try up to 3 times
#   - Wait 2s, then 4s, then 8s between attempts (exponential back-off)
#   - If all 3 fail, raise the error so we know about it
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _call_eia_api(series_id: str, start_date: str, end_date: str) -> dict:
    params = {
        "api_key":              EIA_API_KEY,
        "frequency":            "weekly",
        "data[]":               "value",
        "facets[series][]":     series_id,
        "start":                start_date,
        "end":                  end_date,
        "sort[0][column]":      "period",
        "sort[0][direction]":   "asc",
        "length":               5000,
        "offset":               0,
    }
    logger.info(f"Calling EIA API  →  series: {series_id}")
    response = requests.get(EIA_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


# ── SECTION 5: SAVE RAW JSON (BRONZE LAYER) ──────────────────────────────────
# Save the API response exactly as received — untouched.
# "Raw is sacred" — if cleaning logic has a bug, re-process without re-calling the API.
def _save_raw_json(data: dict, product_name: str, run_date: str) -> Path:
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    filepath = RAW_DATA_PATH / f"eia_{product_name}_{run_date}.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Raw JSON saved  →  {filepath}")
    return filepath


# ── SECTION 6: VALIDATE ROWS ─────────────────────────────────────────────────
# Pull records out of the nested API response and validate each one.
# Bad rows are skipped and logged — pipeline keeps running.
def _parse_and_validate(raw_data: dict, product_name: str) -> list[GasPriceRecord]:
    records_raw = raw_data.get("response", {}).get("data", [])

    if not records_raw:
        logger.warning(f"No data returned for {product_name}")
        return []

    validated, skipped = [], 0

    for row in records_raw:
        try:
            record = GasPriceRecord(
                period       = row["period"],
                series_id    = row["series"],
                value        = float(row["value"]) if row.get("value") is not None else None,
                unit         = row.get("units", "$/gal"),
                product_name = product_name,
            )
            validated.append(record)
        except (ValidationError, KeyError) as e:
            logger.warning(f"Skipping bad row: {row}  |  {e}")
            skipped += 1

    logger.info(f"{product_name}: {len(validated)} valid rows, {skipped} skipped")
    return validated


# ── SECTION 7: CONVERT TO DATAFRAME ─────────────────────────────────────────
# Turn validated records into a pandas DataFrame (in-memory spreadsheet table).
# Also convert date strings to real datetimes and add an audit timestamp.
def _to_dataframe(records: list[GasPriceRecord]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame([r.model_dump() for r in records])
    df["period"]      = pd.to_datetime(df["period"])
    df["ingested_at"] = datetime.utcnow().isoformat()
    df = df.sort_values("period").reset_index(drop=True)

    logger.info(
        f"DataFrame: {df.shape[0]} rows  |  "
        f"{df['period'].min().date()} to {df['period'].max().date()}"
    )
    return df


# ── SECTION 8: SAVE PARQUET (SILVER LAYER) ───────────────────────────────────
# Save clean DataFrame as Parquet — compressed, typed, fast for BigQuery.
# Folder name pattern  product=regular/  is Hive partitioning.
def _save_parquet(df: pd.DataFrame, product_name: str, run_date: str) -> Path | None:
    if df.empty:
        return None

    product_dir = PROCESSED_DATA_PATH / f"product={product_name}"
    product_dir.mkdir(parents=True, exist_ok=True)

    filepath = product_dir / f"eia_{run_date}.parquet"
    df.to_parquet(filepath, index=False, engine="pyarrow")

    size_kb = filepath.stat().st_size / 1024
    logger.info(f"Parquet saved  →  {filepath}  ({size_kb:.1f} KB)")
    return filepath


# ── SECTION 9: MAIN ORCHESTRATOR ─────────────────────────────────────────────
# Calls all steps above for every product. This is what Airflow will call in Phase 2.
def fetch_all_gas_prices(
    start_date: str = "2020-01-01",
    end_date: str | None = None,
) -> pd.DataFrame:

    if not EIA_API_KEY:
        raise EnvironmentError(
            "EIA_API_KEY is not set.\n"
            "Open .env and paste your key from https://www.eia.gov/opendata/"
        )

    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    run_date   = datetime.today().strftime("%Y-%m-%d")
    all_frames = []

    for product_name, series_id in PRODUCT_CODES.items():
        logger.info(f"═══ {product_name.upper()} ═══")

        raw_data = _call_eia_api(series_id, start_date, end_date)   # A. Call API
        _save_raw_json(raw_data, product_name, run_date)             # B. Save raw JSON
        records  = _parse_and_validate(raw_data, product_name)      # C. Validate
        df       = _to_dataframe(records)                           # D. DataFrame
        _save_parquet(df, product_name, run_date)                   # E. Save Parquet

        all_frames.append(df)

    combined = (
        pd.concat(all_frames, ignore_index=True)
        if all_frames else pd.DataFrame()
    )

    logger.success(f"Done! {len(combined):,} total rows fetched successfully")
    return combined


# ── SECTION 10: ENTRY POINT ──────────────────────────────────────────────────
# Only runs when you execute this file directly: python ingestion/eia_fetcher.py
# Does NOT run when Airflow imports and calls fetch_all_gas_prices() in Phase 2.
if __name__ == "__main__":
    logger.add("logs/ingestion_{time:YYYY-MM-DD}.log", rotation="1 day", retention="7 days")

    df = fetch_all_gas_prices(start_date="2022-01-01")


    print("\n Ingestion complete!")
    print(f"  Rows    : {len(df):,}")
    print(f"  Columns : {list(df.columns)}")
