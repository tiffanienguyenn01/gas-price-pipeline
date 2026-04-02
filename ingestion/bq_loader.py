"""
ingestion/bq_loader.py
======================
Loads Parquet files from data/processed/ into BigQuery.
Fixes the datetime column before loading so dates display correctly.
"""

import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from loguru import logger

load_dotenv()

PROJECT_ID    = os.getenv("GCP_PROJECT_ID")
DATASET_ID    = os.getenv("GCP_DATASET", "gas_prices")
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_PATH", "./data/processed"))

SOURCE_TABLE_MAP = {
    "product=regular":  "raw_eia_regular",
    "product=midgrade": "raw_eia_midgrade",
    "product=premium":  "raw_eia_premium",
    "product=diesel":   "raw_eia_diesel",
}


def get_client():
    return bigquery.Client(project=PROJECT_ID)


def ensure_dataset(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    try:
        client.create_dataset(dataset_ref)
        logger.info(f"Dataset created: {PROJECT_ID}.{DATASET_ID}")
    except Conflict:
        logger.info(f"Dataset already exists: {PROJECT_ID}.{DATASET_ID}")


def load_parquet(client, parquet_path, table_name):
    """
    Load one Parquet file into BigQuery.
    Reads with pandas first to fix the datetime column,
    then loads the cleaned DataFrame directly into BigQuery.
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # Read the Parquet file into pandas
    df = pd.read_parquet(parquet_path)

    # Fix the period column — convert to plain date string
    # BigQuery understands DATE type cleanly from a string like "2024-01-08"
    df['period'] = pd.to_datetime(df['period']).dt.date

    logger.info(f"Loading {len(df):,} rows → {table_ref}")
    logger.info(f"Sample dates: {df['period'].head(3).tolist()}")

    # Define the BigQuery schema explicitly so period becomes DATE not INTEGER
    schema = [
        bigquery.SchemaField("period",       "DATE"),
        bigquery.SchemaField("series_id",    "STRING"),
        bigquery.SchemaField("value",        "FLOAT"),
        bigquery.SchemaField("unit",         "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("ingested_at",  "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load directly from the DataFrame — no need to write a file
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    logger.success(f"Done: {table.num_rows:,} rows in {table_ref}")
    return table.num_rows


def load_all():
    client = get_client()
    ensure_dataset(client)

    total = 0
    for folder_name, table_name in SOURCE_TABLE_MAP.items():
        source_dir = PROCESSED_DIR / folder_name

        if not source_dir.exists():
            logger.warning(f"Skipping {folder_name} — folder not found")
            continue

        parquet_files = sorted(source_dir.glob("*.parquet"))
        if not parquet_files:
            logger.warning(f"No Parquet files in {source_dir}")
            continue

        latest = parquet_files[-1]
        rows = load_parquet(client, latest, table_name)
        total += rows

    logger.success(f"All done! {total:,} total rows loaded into BigQuery")
    return total


if __name__ == "__main__":
    logger.add("logs/bq_loader_{time:YYYY-MM-DD}.log", rotation="1 day")
    load_all()
