# ===========================
# Import Required Libraries
# ===========================
import snowflake.connector
from loguru import logger
from dotenv import load_dotenv
import os

##============================
## User Define Python Modules
##============================
from config.config import SnowflakeConfig

## create an object 
snowflake_config = SnowflakeConfig()

# ===========================
# Connect to Snowflake
# ===========================
conn = snowflake.connector.connect(
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
                humidity INTEGER,
                weather_condition STRING,
                wind_speed FLOAT,
                observation_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
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
                avg_temperature FLOAT,
                avg_humidity FLOAT,
                max_temperature FLOAT,
                min_temperature FLOAT,
                record_date DATE,
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


if __name__ == "__main__":
    create_schemas()