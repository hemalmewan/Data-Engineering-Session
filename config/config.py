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
    api_key = os.getenv("WEAHTER_API_KEY","")
    base_url = os.getenv("BASE_URL","https://api.openweathermap.org/data/2.5/weather")
    units:str = "metric"  ## "metric" -----> celsius and "imperial" -> Fahrenheit
    time_out:int=10 ## Request timeout in seconds
    
    
    

##====================
## Snowflake Config
##====================
class SnowflakeConfig:
    account = os.getenv("SNOWFLAKE_ACCOUNT","")
    user = os.getenv("SNOWFLAKE_USER","")
    password = os.getenv("SNOWFLAKE_PASSWORD","")
    role = os.getenv("SNOWFLAKE_ROLE","")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE","")
    database:str =_DB_NAME  ## database name
    schema:str=_DB_SCHEMA   ## database schema
    staging_table:str=_STAGING_TABLE ## staging table where we store raw data
    analytics_table:str=_ANALYTICS_TABLE ## analytics table where we store cleaned data
    

