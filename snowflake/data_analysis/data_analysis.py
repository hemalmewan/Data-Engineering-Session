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
cursor=conn.cursor()

##=======================
## Analyze wheather data
##=======================
def is_null() -> bool:

    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM WEATHER_STATING
            WHERE city IS NULL
                OR temperature IS NULL
                OR humadity IS NULL
                OR wind_speed IS NULL
                OR record_time IS NULL
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
    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM (
              SELECT city,record_time
              FROM WHEATHER_STAGING
              GROUP BY city,record_time
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



def validate_data():
    logger.info("Validating weather data")

    null_found=is_null()
    duplicate_found=is_duplicate()

    if null_found or duplicate_found:
        logger.warning("Data validation failed. Null values or duplicates found")

        raise Exception("Data quality check failed. Stopping pipeline.")
    
    logger.info("Data validation passed")