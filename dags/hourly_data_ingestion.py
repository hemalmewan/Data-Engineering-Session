"""Airflow DAG: hourly weather ingestion.

Defines the ``weather_hourly_ingestion`` DAG, which runs every hour to pull
current weather data and land it in Snowflake's staging table. The DAG chains
three tasks:

1. ``extract_weather_api`` -> fetch weather from the OpenWeather API and save
   raw JSON to disk (:func:`weather_extractor.weather_extractor.run_extraction_pipeline`).
2. ``create_schemas`` -> ensure the Snowflake database, schema, and tables
   exist (:func:`snowflake.db.schema.create_schemas`).
3. ``load_weather_staging`` -> load the raw JSON into ``WEATHER_STAGING``
   (:func:`snowflake.data_loader.snowflake_loader.main`).

Schedule: ``0 * * * *`` (top of every hour), no catchup.
"""

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

from snowflake.db.schema import (
    create_schemas
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

    schema_task = PythonOperator(
        task_id="create_schemas",
        python_callable=create_schemas
    )


    load_task = PythonOperator(
        task_id="load_weather_staging",
        python_callable=load_weather_data
    )


    extract_task >> schema_task >> load_task