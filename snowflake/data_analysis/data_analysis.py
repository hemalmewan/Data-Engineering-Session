"""Data quality validation for the staging weather table.

This module performs the quality-gate stage of the daily analytics pipeline.
It runs checks against the ``WEATHER_STAGING`` table to ensure the data is fit
for aggregation:

- :func:`is_null`: detects missing values in required columns.
- :func:`is_duplicate`: detects duplicate ``(city, observation_time)`` rows.
- :func:`validate_data`: orchestrates both checks and fails the pipeline if
  either problem is found.

:func:`validate_data` is wired into the ``data_quality_check`` task of the
``weather_daily_analytics`` DAG and runs before any aggregation happens.
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

##=======================
## Analyze wheather data
##=======================
def is_null() -> bool:
    """Check for missing values in the staging table.

    Counts rows in ``WEATHER_STAGING`` where any required column (city,
    temperature, humidity, wind_speed, weather_condition, observation_time)
    is ``NULL``.

    Returns:
        ``True`` if at least one row has a null in a required column,
        otherwise ``False``.

    Raises:
        Exception: Re-raised if the query fails.
    """
    cursor=conn.cursor()

    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM WEATHER_STAGING
            WHERE city IS NULL
                OR temperature IS NULL
                OR humidity IS NULL
                OR wind_speed IS NULL
                OR weather_condition IS NULL
                OR observation_time IS NULL
            """
        )

        count=cursor.fetchone()[0]
        return count > 0

    except Exception as e:
        logger.error("Error checking for null values: {}",e)
        raise

    finally:
        cursor.close()
            

def is_duplicate() -> bool:
    """Check for duplicate observations in the staging table.

    Groups ``WEATHER_STAGING`` by ``(city, observation_time)`` and counts any
    groups that appear more than once.

    Returns:
        ``True`` if any duplicate ``(city, observation_time)`` combination
        exists, otherwise ``False``.

    Raises:
        Exception: Re-raised if the query fails.
    """
    cursor=conn.cursor()

    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM (
              SELECT city,observation_time
              FROM WEATHER_STAGING
              GROUP BY city,observation_time
              HAVING COUNT(*) >1   
            )
            """
        )

        duplocates=cursor.fetchone()[0]

        return duplocates > 0

    except Exception as e:
        logger.error("Error checking for duplicate values: {}",e)
        raise

    finally:
        cursor.close()


def remove_duplicates():

    cursor = conn.cursor()

    try:
        logger.info("Removing duplicate records...")

        cursor.execute("""
            DELETE FROM WEATHER_STAGING
            WHERE (city, observation_time) IN (
                SELECT city, observation_time
                FROM (
                    SELECT
                        city,
                        observation_time,
                        ROW_NUMBER() OVER (
                            PARTITION BY city, observation_time
                            ORDER BY observation_time
                        ) AS rn
                    FROM WEATHER_STAGING
                )
                WHERE rn > 1
            )
        """)

        conn.commit()

        logger.info("Duplicate records removed.")

    except Exception as e:
        logger.error("Error removing duplicates: {}", e)
        raise

    finally:
        cursor.close()


def validate_data():

    logger.info("Validating weather data")

    if is_null():
        raise Exception("NULL values found.")

    if is_duplicate():
        logger.warning("Duplicate records found.")
        remove_duplicates()

        if is_duplicate():
            raise Exception("Unable to remove duplicate records.")

    logger.info("Data validation passed.")