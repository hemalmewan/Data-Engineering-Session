##===========================
## Import Required Libraries
##===========================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

##============================
## User Define Python Modules
##============================
from weather_extractor.weather_extractor import (
    run_extraction_pipeline as extract_weather_data
)

from snowflake.data_loader.snowflake_loader import (
    main as load_weather_data
)


default_args = {
    "owner": "Data Engineering Session",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="weather_hourly_ingestion",
    default_args=default_args,
    description="Extract weather API data and load into staging",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",      # Every hour
    catchup=False,
    tags=["weather", "staging"]
) as dag:


    extract_task = PythonOperator(
        task_id="extract_weather_api",
        python_callable=extract_weather_data
    )


    load_task = PythonOperator(
        task_id="load_weather_staging",
        python_callable=load_weather_data
    )


    extract_task >> load_task