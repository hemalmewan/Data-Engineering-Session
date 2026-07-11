"""Aggregate weather data and split it into per-city tables.

This module contains the analytics stages of the daily pipeline, run after the
data quality gate passes:

- :func:`analyze_weather_data`: aggregates the raw ``WEATHER_STAGING`` rows
  (averages, min/max) grouped by city and country, and writes the result into
  ``WEATHER_ANALYTICS``.
- :func:`split_city_tables`: materialises one table per city from the analytics
  table for convenient per-city querying.

Both functions are wired into the ``weather_daily_analytics`` DAG as the
``aggregate_weather_data`` and ``split_city_tables`` tasks respectively.
"""

##===========================
## Import Required Libraries
##===========================
from loguru import logger
from snowflake import connector
import time

##===========================
## User Define Python Modules
##===========================
from config.config import SnowflakeConfig

## create an object
snowflake_config=SnowflakeConfig()

##===========================
## Connect to Snowflake
##===========================
conn=connector.connect(
    account=snowflake_config.account,
    user=snowflake_config.user,
    password=snowflake_config.password,
    role=snowflake_config.role,
    warehouse=snowflake_config.warehouse,
    database=snowflake_config.database,
    schema=snowflake_config.schema
)


##===========================
## Weather Data Analysis
##===========================
def analyze_weather_data():
    """Aggregate staging data into the analytics table.

    Computes per-``(city, country)`` averages of temperature, humidity, and
    wind speed, along with the min and max temperature, from
    ``WEATHER_STAGING`` and inserts the results into ``WEATHER_ANALYTICS``.
    The connection is closed in the ``finally`` block.

    Raises:
        Exception: Re-raised if the aggregation query fails.
    """
    logger.info("Starting weather data analysis....")
    cursor=conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO WEATHER_ANALYTICS
            (
                city,
                country,
                avg_temperature,
                avg_humidity,
                avg_wind_speed,
                max_temperature,
                min_temperature
            )
            SELECT
                city,
                country,
                AVG(temperature) as avg_temperature,
                AVG(humidity) as avg_humidity,
                AVG(wind_speed) as avg_wind_speed,
                MAX(temperature) as max_temperature,
                MIN(temperature) as min_temperature
            FROM WEATHER_STAGING
            GROUP BY city,country;
            """
        )

        conn.commit()
        logger.info("Successfully inserted weather data into analytics table")

    except Exception as e:
        logger.error("Error analyzing weather data: {}",e)
        raise

    finally:
        cursor.close()
        conn.close()
        logger.info("Snowflake connection closed")



##=============================
## Split the Tables City wise
##=============================
def split_city_tables():
    """Create one analytics table per city.

    Reads the distinct cities from ``WEATHER_ANALYTICS`` and, for each one,
    creates (or replaces) a dedicated table named after the city (uppercased,
    spaces replaced with underscores) containing only that city's rows. The
    connection is closed in the ``finally`` block.

    Raises:
        Exception: Re-raised if reading cities or creating a table fails.
    """
    logger.info("Creating city-wise tables....")
    cursor=conn.cursor()

    try:
        cursor.execute("SELECT DISTINCT city FROM WEATHER_ANALYTICS")
        cities=cursor.fetchall()
        logger.info("Found {} cities",len(cities))

        for (city,) in cities:

            ## make the valid SQL table name
            table_name=city.upper().replace(" ","_")

            cursor.execute(f"""
              CREATE OR REPLACE TABLE {table_name} AS
              SELECT 
                city,
                country,
                avg_temperature,
                avg_wind_speed,
                max_temperature,
                min_temperature,
                created_at
              FROM WEATHER_ANALYTICS
              WHERE city ='{city}'
            """)

            logger.info(f"Created table:{table_name}")
        
        conn.commit()
    
    except Exception as e:
        logger.error("Error creating city tables: {}",e)
        raise
    
    finally:
        cursor.close()
        conn.close()
        logger.info("Snowflake connection closed")

            
