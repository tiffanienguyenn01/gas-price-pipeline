from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "tiffanie",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="gas_price_pipeline",
    description="Weekly gas price ingestion from EIA API",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1",
    catchup=False,
    tags=["gas-prices", "ingestion", "eia"],
) as dag:

    def fetch_gas_prices(**context):
        import sys, os
        sys.path.insert(0, "/opt/airflow")
        from ingestion.eia_fetcher import fetch_all_gas_prices
        start_date = os.getenv("INGESTION_START_DATE", "2020-01-01")
        df = fetch_all_gas_prices(start_date=start_date)
        print(f"Fetched {len(df):,} rows of gas price data")
        return {"rows_fetched": len(df)}

    def load_to_bigquery(**context):
        import sys, os
        sys.path.insert(0, "/opt/airflow")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/secrets/gcp_key.json"
        from ingestion.bq_loader import load_all
        total = load_all()
        print(f"Loaded {total:,} rows into BigQuery")
        return {"rows_loaded": total}

    def export_for_tableau(**context):
        import os
        import pandas as pd
        from google.cloud import bigquery
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/secrets/gcp_key.json"
        client = bigquery.Client(project="gas-price-pipeline")
        query = """
            SELECT *
            FROM `gas-price-pipeline.gas_price_dbt.mart_gas_prices`
            ORDER BY price_date, product
        """
        print("Exporting mart_gas_prices to CSV...")
        # Use list(job.result()) instead of to_dataframe()
        # to avoid needing bigquery.readsessions.create permission
        job = client.query(query)
        result = job.result()
        columns = [field.name for field in result.schema]
        rows = list(result)
        if not rows:
            print("No rows returned!")
            return {"rows_exported": 0}
        data = [{col: getattr(row, col) for col in columns} for row in rows]
        df = pd.DataFrame(data)
        print(f"Rows exported: {len(df):,}")
        output_path = "/opt/airflow/data/gas_prices_tableau.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved to {output_path}")
        run_date = context["ds"]
        dated_path = f"/opt/airflow/data/gas_prices_tableau_{run_date}.csv"
        df.to_csv(dated_path, index=False)
        print(f"Dated backup saved to {dated_path}")
        return {"rows_exported": len(df)}

    task_fetch = PythonOperator(
        task_id="fetch_gas_prices",
        python_callable=fetch_gas_prices,
    )

    task_load = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    task_export = PythonOperator(
        task_id="export_for_tableau",
        python_callable=export_for_tableau,
    )

    task_fetch >> task_load >> task_export
