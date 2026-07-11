"""Airflow DAG: daily weather analytics.

Defines the ``weather_daily_analytics`` DAG, which runs once a day to validate
and aggregate the ingested weather data. The DAG chains three tasks:

1. ``data_quality_check`` -> fail fast if the staging table has nulls or
   duplicates (:func:`snowflake.data_analysis.data_analysis.validate_data`).
2. ``aggregate_weather_data`` -> compute per-city aggregates into
   ``WEATHER_ANALYTICS``
   (:func:`snowflake.data_analysis.aggregate_splitting.analyze_weather_data`).
3. ``split_city_tables`` -> materialise one table per city
   (:func:`snowflake.data_analysis.aggregate_splitting.split_city_tables`).

Schedule: ``0 0 * * *`` (every midnight), no catchup.
"""

##==========================
## Import Required Libraries
##==========================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

##==============================
## User Define Python Modules
##==============================
from snowflake.data_analysis.data_analysis import (
    validate_data as validate
)

from snowflake.data_analysis.aggregate_splitting import (
    analyze_weather_data,
    split_city_tables
)


default_args = {
    "owner": "Data Engineering Session",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="weather_daily_analytics",
    default_args=default_args,
    description="Daily weather aggregation pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",      # Every midnight
    catchup=False,
    tags=["weather", "analytics"]
) as dag:


    quality_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=validate
    )


    analytics_task = PythonOperator(
        task_id="aggregate_weather_data",
        python_callable=analyze_weather_data
    )


    city_task = PythonOperator(
        task_id="split_city_tables",
        python_callable=split_city_tables
    )


    quality_task >> analytics_task >> city_task


