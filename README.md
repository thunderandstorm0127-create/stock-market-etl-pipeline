# Stock Market ETL Pipeline 
## Overview
End-to-end ETL pipeline that extracts daily Amazon (AMZN) stock data from Polygon API, transforms it using python and pandas,and loads into SQLite database orchestrated by Apache Airflow running on Docker.

## Architecture
Polygon API -> Airflow Dag -> Extract Task -> Transform Task -> Load Task -> SQLite Database

## Tech Stack
- Apache Airflow (orchestration)
- Python (programming)
- Pandas (library for Data transformation)
- SQLite (data Storage)
- Docker (containerization)
- Astro CLI (local Airflow environment)

## Pipeline Steps
1. Extract → fetches daily AMZN stock data from Polygon API 
2. Transform → cleans and flattens JSON to tabular format
3. Load → stores data in SQLite database

## Prerequisites
- Docker Desktop
- Ubuntu app for Windows (WSL)
- Astro CLI → install using:
  `winget install -e --id Astronomer.Astro`

## How to Run
1. Clone this repository in WSL:
   `git clone https://github.com/thunderandstorm0127/stock-market-etl-pipeline.git`
2. Generate free API key from polygon.io
3. Add your API key in `dags/market_etl.py`
4. Update `start_date` to recent date (free tier only allows recent data)
5. Optionally change stock ticker from AMZN to any other
6. Run `astro dev start` in Ubuntu terminal
7. Go to Airflow UI → Admin → Connections → create new:
   - Connection Id: `market_database_conn`
   - Connection Type: `SQLite`
   - Host: `/usr/local/airflow/market_data.db`
8. Enable DAG toggle in Airflow UI → pipeline runs automatically
9. To verify data: run `astro dev bash` then `python3 dags/query_db.py`