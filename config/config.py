"""Central configuration for the weather data pipeline.

This module loads environment variables (from a ``.env`` file when present)
and exposes two configuration classes used throughout the project:

- :class:`OpenWeatherConfig`: settings for the OpenWeather API client.
- :class:`SnowflakeConfig`: credentials and object names for Snowflake.

The database, schema, and table names are defined as module-level constants
so they stay consistent across the loader, schema-creation, and analytics
modules.
"""

##===========================
## Import Required Libraries
##===========================
from dotenv import load_dotenv
from loguru import logger
import os

##==========================
## Load Environment Variables
##==========================
load_dotenv()


##==============================
## Define DB,SCHEMA and TABLES
##==============================
_DB_NAME = "WEATHER_DB"
_DB_SCHEMA = "RAW"
_STAGING_TABLE = "WEATHER_STAGING"
_ANALYTICS_TABLE = "WEATHER_ANALYTICS"

##==========================
## Open Weather Config
##==========================
class OpenWeatherConfig:
    """Configuration for the OpenWeather API client.

    Attributes:
        api_key: OpenWeather API key, read from the ``WEAHTER_API_KEY``
            environment variable.
        base_url: OpenWeather "current weather" endpoint URL.
        units: Measurement units. ``"metric"`` yields Celsius, ``"imperial"``
            yields Fahrenheit.
        time_out: HTTP request timeout in seconds.
    """
    api_key = os.getenv("WEAHTER_API_KEY","")
    base_url = os.getenv("BASE_URL","https://api.openweathermap.org/data/2.5/weather")
    units:str = "metric"  ## "metric" -----> celsius and "imperial" -> Fahrenheit
    time_out:int=10 ## Request timeout in seconds
    
    
    

##====================
## Snowflake Config
##====================
class SnowflakeConfig:
    """Connection settings and object names for Snowflake.

    Credentials are read from environment variables, while the database,
    schema, and table names come from the module-level constants so every
    stage of the pipeline targets the same objects.

    Attributes:
        account: Snowflake account identifier.
        user: Snowflake username.
        password: Snowflake password.
        role: Role to assume for the session.
        warehouse: Virtual warehouse used to run queries.
        database: Target database name (``WEATHER_DB``).
        schema: Target schema name (``RAW``).
        staging_table: Table holding raw ingested weather records.
        analytics_table: Table holding aggregated analytics results.
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT","")
    user = os.getenv("SNOWFLAKE_USER","")
    password = os.getenv("SNOWFLAKE_PASSWORD","")
    role = os.getenv("SNOWFLAKE_ROLE","")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE","")
    database:str =_DB_NAME  ## database name
    schema:str=_DB_SCHEMA   ## database schema
    staging_table:str=_STAGING_TABLE ## staging table where we store raw data
    analytics_table:str=_ANALYTICS_TABLE ## analytics table where we store cleaned data
    

