from airflow import DAG
from airflow.sdk import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta
import requests
import time
import pandas as pd
with DAG(
    dag_id="market_etl",
    start_date=datetime(2026, 5, 1, 9),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    @task()
    def hit_polygon_api(**context):
        stock_ticker= "AMZN"
        polygon_api_key="V9Dh7yrCqXKjE3yxPdlXg0RBpfUFB0ps"
        ds = context.get("ds")
        url = f"https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}"
        response = requests.get(url)
        print(response.json())
        
        time.sleep(15)

        return response.json()

    
    @task
    def flatten_market_data(polygon_response, **context):
        ds = context.get("ds")
        columns ={
            "status": "closed",
            "from": ds,
            "symbol": "AMZN",
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None
        }
        flattened_record=[]
        for header_name, default_value in columns.items():
            flattened_record.append(
                polygon_response.get(header_name, default_value))
        flattened_dataframe= pd.DataFrame(
            [flattened_record], columns = columns.keys()
        )
        print(flattened_dataframe)
        return flattened_dataframe
    @task
    def load_market_data(flattened_dataframe):
        market_database_hook = SqliteHook("market_database_conn")
        db_engine = market_database_hook.get_sqlalchemy_engine()
        flattened_dataframe.to_sql(
            name = "market_data",
            con = db_engine,
            if_exists = "append",
            index = False
        )
        print("Data loaded successfully!")
    
    raw_data = hit_polygon_api()
    flattened_dataframe = flatten_market_data(raw_data)
    load_market_data(flattened_dataframe)