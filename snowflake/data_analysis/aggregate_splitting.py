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

            
