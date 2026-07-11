"""Load raw weather JSON files into the Snowflake staging table.

This module is the loading stage of the hourly ingestion pipeline. It reads
the JSON files produced by :mod:`weather_extractor.weather_extractor`, flattens
each OpenWeather record into a relational row, and bulk-inserts the rows into
the ``WEATHER_STAGING`` table.

A module-level Snowflake connection is opened on import using
:class:`config.config.SnowflakeConfig`, and :func:`main` is the callable wired
into the ``load_weather_staging`` task of the ``weather_hourly_ingestion`` DAG.
"""

##===========================
## Import Required Libraries
##===========================
from loguru import logger
from snowflake import connector
import json
from pathlib import Path
from typing import Optional,List,Dict,Any

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

##===================
## RAW Data Directory
##===================
_DATA_DIRECTORY = Path("/usr/local/airflow/data/raw")

##=====================
## Load Raw JSON Files
##=====================
def load_raw_json_files() -> List[Dict[str,Any]]:
    """Read all raw weather JSON files from the raw data directory.

    Scans :data:`_DATA_DIRECTORY` for ``*.json`` files and loads their
    contents. Files containing a list of records are flattened into the
    result, while files containing a single record are appended directly.

    Returns:
        A combined list of weather record dictionaries across all files.
        Empty if no JSON files are found.

    Raises:
        Exception: Re-raised if any file cannot be read or parsed.
    """
    logger.info("Loading raw JSON files....")

    records=[]
    try:
        json_files=list(_DATA_DIRECTORY.glob("*.json")) ## number of json files
        logger.info("Found {} JSON files",len(json_files))

        if not json_files:
            logger.warning("No JSON files found")
            return records
        
        for file in json_files:
            logger.info(f"Reading file: {file.name}")
            with open(file,"r",encoding="utf-8") as f:
                data=json.load(f)

                if isinstance(data,list): ## if JSON contains list of records
                    records.extend(data)

                elif isinstance(data,dict): ## if JSON contains single records
                    records.append(data)

        
        logger.info("Loaded {} records from JSON files",len(records))
    
    except Exception as e:
        logger.error("Error loading JSON files: {}",e)
        raise
    
    return records


##=============================
## Insert Data Into Snowflake
##=============================
def insert_weather_data(records:List[Dict[str,Any]]):
    """Insert weather records into the ``WEATHER_STAGING`` table.

    Flattens each nested OpenWeather record into a row of
    ``(city, country, temperature, humidity, wind_speed, weather_condition,
    observation_time)`` and bulk-inserts them in a single transaction.

    Args:
        records: List of weather record dictionaries. Each item may be a dict
            or a JSON-encoded string. If the list is empty, the function
            returns without touching Snowflake.

    Raises:
        Exception: Re-raised if the insert fails; the transaction is not
            committed in that case.
    """
    if not records:
        logger.warning("No records to insert")
        return
    
    cursor=conn.cursor()

    logger.info("Inserting {} records into Snowflake....",len(records))

    try:
        insert_query="""
          INSERT INTO WEATHER_STAGING
          (
            city,
            country,
            temperature,
            humidity,
            wind_speed,
            weather_condition,
            observation_time
          )
          VALUES(%s,%s,%s,%s,%s,%s,%s)
        """

        rows=[]

        for record in records:
            record=json.loads(record) if isinstance(record,str) else record

            rows.append(
                (
                record.get("name"),
                record.get("sys","").get("country",""),
                record.get("main").get("temp"),
                record.get("main").get("humidity"),
                record.get("wind").get("speed"),
                record.get("weather")[0]["description"],
                record.get("_extracted_at")
                )
            )
        
        cursor.executemany(insert_query,rows)
        conn.commit()

        logger.info("Successfully inserted {} records",len(rows))

    except Exception as e:
        logger.error("Error inserting data into Snowflake: {}",e)
        raise

    finally:
        cursor.close()

    

##===============
## Main Pipeline
##===============
def main():
    """Run the full staging-load stage.

    Loads all raw JSON files from disk and inserts the parsed records into the
    Snowflake staging table. The module-level connection is always closed in
    the ``finally`` block. This is the callable invoked by the
    ``load_weather_staging`` Airflow task.

    Raises:
        Exception: Re-raised if loading or inserting fails.
    """
    logger.info("Staring Snowflake Loader Pipeline")

    try:
        raw_data=load_raw_json_files()
        insert_weather_data(raw_data)
    
    except Exception as e:
        logger.error("Error in Snowflake Loader Pipeline: {}",e)
        raise
    
    finally:
        conn.close()
        logger.info("Snowflake Loader Pipeline completed")
    


