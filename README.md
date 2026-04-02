# Gas Price Analytics Pipeline

An end-to-end data engineering project that automatically ingests, transforms,
and visualizes U.S. retail gas price data using modern open-source tools.

## Live Dashboard
[View on Looker Studio](https://lookerstudio.google.com/reporting/fa3d2056-8497-41e1-a7da-67a4ef4204c6)

## Architecture

```
EIA API → Airflow (Docker) → BigQuery → dbt → Looker Studio
```

## Tools
| Layer | Tool |
|---|---|
| Ingestion | Python, requests, Pydantic |
| Orchestration | Apache Airflow (Docker) |
| Warehouse | Google BigQuery |
| Transform | dbt |
| Dashboard | Looker Studio |
| CI/CD | GitHub Actions |

## Pipeline
Runs automatically every Monday at 6am:
1. Fetch weekly gas prices from EIA API
2. Validate and save as Parquet
3. Load to BigQuery
4. Export clean CSV backup

## Data Sources
| Source | Data | Frequency |
|---|---|---|
| EIA Open Data | Retail gas prices by grade | Weekly |
| EIA Open Data | WTI crude oil spot price | Weekly |
| FRED (St. Louis Fed) | CPI inflation | Monthly |

## Key Metrics
- 300+ weeks of gas price history (2020-2026)
- 3 products tracked: regular, midgrade, premium
- Week-over-week and year-over-year price changes
- Monthly seasonality analysis

## Setup
1. Clone the repo
2. Copy `.env.example` to `.env` and fill in your keys
3. Run `pip install -r requirements.txt`
4. Run `docker compose up -d` to start Airflow
5. Trigger the DAG in the Airflow UI at `localhost:8080`
