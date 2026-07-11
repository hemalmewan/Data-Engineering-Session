"""Create the Snowflake database objects used by the pipeline.

This module provisions all the Snowflake structures the pipeline depends on:
the ``WEATHER_DB`` database, the ``RAW`` schema, the ``WEATHER_STAGING`` table
(raw ingested rows), and the ``WEATHER_ANALYTICS`` table (aggregated results).

All statements use ``IF NOT EXISTS`` so the module is idempotent and safe to
run on every DAG execution. :func:`create_schemas` is wired into the
``create_schemas`` task of the ``weather_hourly_ingestion`` DAG.
"""

# ===========================
# Import Required Libraries
# ===========================
from snowflake import connector
from loguru import logger
from dotenv import load_dotenv

##============================
## User Define Python Modules
##============================
from config.config import SnowflakeConfig

## create an object 
snowflake_config = SnowflakeConfig()

# ===========================
# Connect to Snowflake
# ===========================
conn = connector.connect(
    account=snowflake_config.account,
    user=snowflake_config.user,
    password=snowflake_config.password,
    role=snowflake_config.role,
    warehouse=snowflake_config.warehouse
)

# ===========================
# Create Database Objects
# ===========================
def create_schemas():
    """Create the database, schema, and tables if they do not exist.

    Executes, in order: create/use the database, create/use the schema,
    create the staging table, and create the analytics table. Every statement
    is idempotent (``IF NOT EXISTS``), so repeated runs are harmless. The
    Snowflake connection is closed in the ``finally`` block.

    Raises:
        Exception: Re-raised if any DDL statement fails.
    """
    logger.info("Creating database objects...")
    cursor = conn.cursor()

    try:
        # Create Database
        cursor.execute(
            f"""
            CREATE DATABASE IF NOT EXISTS {snowflake_config.database}
            """
        )
        logger.info("Database created")

        # Use Database
        cursor.execute(
            f"""
            USE DATABASE {snowflake_config.database}
            """
        )

        # Create Schema
        cursor.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {snowflake_config.schema}
            """
        )

        logger.info("Schema created")

        # Use Schema
        cursor.execute(
            f"""
            USE SCHEMA {snowflake_config.schema}
            """
        )

        # Create Staging Table
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {snowflake_config.staging_table}
            (
                city STRING,
                country STRING,
                temperature FLOAT,
                humidity FLOAT,
                wind_speed FLOAT,
                weather_condition STRING,
                observation_time TIMESTAMP
            )
            """
        )

        logger.info("Weather staging table created")

        # Create Analytics Table
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {snowflake_config.analytics_table}
            (
                city STRING,
                country STRING,
                avg_temperature FLOAT,
                avg_humidity FLOAT,
                avg_wind_speed FLOAT,
                max_temperature FLOAT,
                min_temperature FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )

        logger.info("Weather analytics table created")

    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
        logger.info("Snowflake connection closed")


# if __name__=="__main__":
# # # #     #create_schemas()

#     cursor=conn.cursor()
#     cursor.execute(
#     """
#     DROP DATABASE IF EXISTS WEATHER_DB;
#     """
#     )
#     cursor.close()
#     conn.close()
    
