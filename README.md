# Automated Gas Price Analytics: End-to-End Data Engineering Pipeline

## Executive Summary
This project builds a fully automated end-to-end data pipeline that ingests weekly U.S. retail gas price data from the EIA Open Data API, transforms it using dbt, and visualizes it through a live Looker Studio dashboard. By examining price trends, week-over-week changes, year-over-year comparisons, and monthly seasonality across regular, midgrade, and premium grades, the analysis uncovers how gas prices fluctuate in response to macroeconomic events such as the COVID-19 pandemic and the 2022 Russia-Ukraine war. The pipeline runs automatically every Monday, enabling stakeholders to monitor current prices, assess price volatility, and understand seasonal demand patterns without manual intervention.

## Business Problem
Gas price volatility directly impacts consumer spending, logistics costs, and business planning. Without a systematic way to track and analyze weekly price movements across product grades, it is difficult to identify pricing trends, anticipate seasonal demand shifts, or understand how macroeconomic events affect fuel costs. This project addresses the lack of an automated, scalable system for monitoring gas price data and surfacing actionable insights through interactive visualizations.

## Methodology
- Built an automated ingestion pipeline using Python to pull weekly retail gas price data from the U.S. Energy Information Administration (EIA) Open Data API, with Pydantic validation, exponential backoff retry logic, and raw JSON preservation following medallion architecture principles.
- Orchestrated the pipeline using Apache Airflow running inside Docker, scheduling weekly DAG runs that handle ingestion, loading, transformation, and dashboard export automatically.
- Loaded clean Parquet files into Google BigQuery as the cloud data warehouse, separating raw bronze tables from transformed silver and gold layers.
- Transformed raw data using dbt, building staging and mart models that calculate week-over-week price changes, year-over-year comparisons, monthly seasonality, and price tier classifications using SQL window functions.
- Visualized key insights in Looker Studio through an interactive live dashboard connected directly to BigQuery, enabling real-time monitoring of current prices, historical trends, and volatility patterns.
- Implemented CI/CD using GitHub Actions to automatically run dbt tests on every code push, ensuring data model integrity.

## Skills
- **Python**: requests, Pydantic, pandas, pyarrow, python-dotenv, loguru, tenacity
- **SQL**: CTEs, window functions (LAG, PARTITION BY), CASE statements, GROUP BY, aggregate functions
- **dbt**: staging models, mart models, source definitions, schema tests, data documentation
- **Airflow**: DAG authoring, PythonOperator, task dependencies, scheduling, retry logic
- **Docker**: Docker Compose, container orchestration, volume mounting, environment management
- **BigQuery**: dataset management, table loading, query optimization, partitioning
- **Looker Studio**: live BigQuery connection, scorecard design, time series charts, filter controls
- **GitHub Actions**: CI/CD pipeline, automated dbt testing, secrets management

## Results and Business Recommendations

**Results**: The pipeline tracks 300+ weeks of U.S. retail gas prices from 2020 to 2026 across three product grades. Regular gas peaked at $5.01/gallon in mid-2022 following the Russia-Ukraine conflict, representing a +14% week-over-week spike — the largest in the dataset. COVID-19 caused the steepest price drops in early 2020, with weekly declines reaching -6%. Gas prices follow a consistent seasonal pattern, averaging $0.40/gallon higher in summer months (June-July) compared to winter (December-January). As of March 2026, regular gas sits at $3.99/gallon, up 26% year-over-year — the highest YoY increase since the post-pandemic recovery period.

**Business Recommendations**: Logistics and transportation businesses should plan fuel budgets around the mid-year seasonal peak and lock in fuel contracts before June. Retailers and e-commerce businesses should factor rising fuel costs into shipping pricing strategies during high-volatility periods. Further analysis should incorporate crude oil prices and CPI data to build a predictive model for gas price forecasting, enabling more proactive planning.

## Pipeline Architecture
```
EIA API → Python ingestion → Parquet (bronze)
        → BigQuery (silver) → dbt models (gold)
        → Looker Studio dashboard (live, auto-updating)
```

## Data Sources
| Source | Data | Frequency |
|---|---|---|
| EIA Open Data | Retail gas prices by grade (regular, midgrade, premium) | Weekly |
| EIA Open Data | WTI crude oil spot price | Weekly |
| FRED (St. Louis Fed) | CPI inflation index | Monthly |

## Tools & Technologies
| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python, requests, Pydantic | API calls, validation, Parquet storage |
| Orchestration | Apache Airflow (Docker) | Weekly scheduling, retries, monitoring |
| Warehouse | Google BigQuery | Cloud analytical warehouse |
| Transform | dbt Core | SQL models, tests, documentation |
| Dashboard | Looker Studio | Live interactive dashboard |
| CI/CD | GitHub Actions | Automated dbt tests on every push |

## Live Dashboard
[View Live Dashboard](https://lookerstudio.google.com/reporting/fa3d2056-8497-41e1-a7da-67a4ef4204c6)

## Setup
1. Clone the repo
2. Copy `.env.example` to `.env` and fill in your EIA API key and GCP credentials
3. Run `pip install -r requirements.txt`
4. Run `docker compose up -d` to start Airflow
5. Trigger the `gas_price_pipeline` DAG in the Airflow UI at `localhost:8080`
6. View your live dashboard in Looker Studio
