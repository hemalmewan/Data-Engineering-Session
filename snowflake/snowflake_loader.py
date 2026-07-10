##===========================
## Import Required Libraries
##===========================
from loguru import logger
import snowflake
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
conn=snowflake.connector.connect(
    account=snowflake_config.account,
    user=snowflake_config.user,
    password=snowflake_config.password,
    role=snowflake_config.role,
    warehouse=snowflake_config.warehouse,
    database=snowflake_config.database,
    schema=snowflake_config.schema

)

##====================
## RAW Data Directory
##====================
_DATA_DIRECTORY=Path(__file__).parents[1]/"data"/"raw"

##=====================
## Load Raw JSON Files
##=====================
def load_raw_json_files() -> List[Dict[str,Any]]:
    logger.info("Loading raw JSON files....")

    records=[]
    try:
        json_files=list(_DATA_DIRECTORY.glob("*.json")) ## number of json files
        logger.info("Found %s JSON files",len(json_files))

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

        
        logger.info("Loaded %s records from JSON files",len(records))
    
    except Exception as e:
        logger.error("Error loading JSON files: %s",e)
        raise
    
    return records


##=============================
## Insert Data Into Snowflake
##=============================
def insert_weather_data(records:List[Dict[str,Any]]):

    if not records:
        logger.warning("No records to insert")
        return
    
    cursor=conn.cursor()

    logger.info("Inserting %s records into Snowflake....",len(records))

    try:
        insert_query="""
          INSERT INTO WEATHER_STAGING
          (
            city,
            country,
            temperature,
            humidity,
            weather_condition,
            wind_speed,
            observation_time
          )
          VALUES(%s,%s,%s,%s,%s,%s,%s)
        """

        rows=[]

        for record in records:
            record=json.loads(record) if isinstance(record,str) else record

            rows.append(
                (

                record.get("city"),
                record.get("country"),
                record.get("temperature"),
                record.get("humidity"),
                record.get("weather_condition"),
                record.get("wind_speed"),
                record.get("observation_time")

                )
            )
        
        cursor.executemany(insert_query,rows)
        conn.commit()

        logger.info("Successfully inserted %s records",len(rows))

    except Exception as e:
        logger.error("Error inserting data into Snowflake: %s",e)
        raise

    finally:
        cursor.close()

    

##===============
## Main Pipeline
##===============
def main():
    logger.info("Staring Snowflake Loader Pipeline")

    try:
        raw_data=load_raw_json_files()
        insert_weather_data(raw_data)
    
    except Exception as e:
        logger.error("Error in Snowflake Loader Pipeline: %s",e)
        raise
    
    finally:
        conn.close()
        logger.info("Snowflake Loader Pipeline completed")
    


if __name__=="__main__":
    main()