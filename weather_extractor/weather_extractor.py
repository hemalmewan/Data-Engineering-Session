"""Weather data extraction module.

This module is the first stage of the weather data pipeline. It fetches
current weather observations from the OpenWeather API for a fixed set of
cities and persists the raw JSON responses to the local ``data/raw``
directory so they can later be loaded into Snowflake.

Key components
--------------
- :class:`WeatherExtractor`: thin client around the OpenWeather API that
  fetches weather for one or many cities and saves the results to disk.
- :func:`run_extraction_pipeline`: convenience entry point wired into the
  ``weather_hourly_ingestion`` Airflow DAG.

Environment
-----------
The API key and base URL are read from :class:`config.config.OpenWeatherConfig`,
which in turn loads them from environment variables (see ``.env.sample``).
"""

##==========================
## Import Required Libraries
##==========================
import json
from loguru import logger
from typing import Dict,Any,List,Optional
from datetime import datetime,timezone
import requests
from pathlib import Path

##==================
## Root Directory
##==================
_ROOT_DIRECTORY=Path(__file__).parents[1]

##===================
## RAW Data Directory
##===================
_RAW_DATA_DIRECTORY = Path("/usr/local/airflow/data/raw")

##============================
## User Define Python Modules
##============================
from config.config import OpenWeatherConfig

## create an object
weather_config=OpenWeatherConfig()

_CITIES=["Colombo","Dubai","New York","London","Sydney"]

class WeatherExtractor:
    """Client for fetching weather data from the OpenWeather API.

    The extractor wraps the OpenWeather "current weather" endpoint and
    provides helpers to fetch a single city, fetch multiple cities, and
    persist the collected records to a JSON file.

    Attributes:
        api_key: API key used to authenticate requests. Falls back to the
            value configured in :class:`OpenWeatherConfig` when not provided.
        base_url: OpenWeather endpoint URL. Falls back to the configured
            default when not provided.
    """

    def __init__(self, api_key:Optional[str]=None,base_url:Optional[str]=None):
        """Initialise the extractor and validate credentials.

        Args:
            api_key: Optional API key override. If ``None``, the key from
                :class:`OpenWeatherConfig` is used.
            base_url: Optional base URL override. If ``None``, the URL from
                :class:`OpenWeatherConfig` is used.

        Raises:
            ValueError: If no API key is available from either the argument
                or the configuration.
        """
        self.api_key=api_key if api_key is not None else weather_config.api_key
        self.base_url=base_url if base_url is not None else weather_config.base_url

        if not self.api_key:
            logger.error("API Key is required to make API calls")
            raise ValueError("API Key is required")
        
    ##=====================
    ## fetch weather data
    ##=====================

    def fetch_city_weather(self,city:str) -> Dict[str,Any]:
        """Fetch the current weather for a single city.

        Sends a request to the OpenWeather API and augments the response with
        an ``_extracted_at`` UTC timestamp so downstream stages know when the
        record was collected.

        Args:
            city: City name to query (e.g. ``"Colombo"``).

        Returns:
            The parsed JSON response as a dictionary, with an added
            ``_extracted_at`` ISO-8601 timestamp field.

        Raises:
            requests.HTTPError: If the API returns a non-2xx status code.
        """
        params={
            "q":city,
            "appid":self.api_key,
            "units":weather_config.units
        }

        response=requests.get(
            self.base_url,params=params,timeout=weather_config.time_out
        )

        if response.status_code!=200: ## check whether response is recived or not
            logger.error("Failed to fetch weather data for %s: %s",city,response.status_code)
            raise
        
        response.raise_for_status()
        data=response.json()

        data["_extracted_at"]=datetime.now(timezone.utc).isoformat()

        return data
    
    def fetch_multiple_cities(
        self,
        cities:List[str]
        ) -> List[Dict[str,Any]]:
        """Fetch weather data for a list of cities.

        Iterates over the given cities and collects each successful response.
        Failures for individual cities are logged and skipped so that one bad
        city does not abort the whole batch.

        Args:
            cities: List of city names to fetch. If falsy, the module-level
                default city list (:data:`_CITIES`) is used.

        Returns:
            A list of weather record dictionaries, one per successfully
            fetched city.
        """
        cities=cities or _CITIES
        results:List[Dict[str,Any]]=[]

        for city in cities:
            try:
                logger.info("Fetching weather for {}",city)
                results.append(self.fetch_city_weather(city))
            except Exception as e:
                logger.error("Failed to fetch weather data for {}: {}",city,e)

        return results
    
    def save_to_file(
        self,
        data:List[Dict[str,Any]], 
        output_dir:str=_RAW_DATA_DIRECTORY
        ) -> str:
        """Persist weather records to a timestamped JSON file.

        Creates the output directory if it does not exist and writes the
        records to a file named ``weather_<UTC timestamp>.json``.

        Args:
            data: List of weather record dictionaries to save.
            output_dir: Destination directory. Defaults to the pipeline's raw
                data directory (:data:`_RAW_DATA_DIRECTORY`).

        Returns:
            The absolute path of the written JSON file as a string.
        """
        Path(output_dir).mkdir(parents=True,exist_ok=True) ## create a directory if not exists
        timestamp_str=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") ## create a timestamp

        filepath=Path(output_dir) / f"weather_{timestamp_str}.json"

        with open(filepath,"w",encoding="utf-8") as f:
            json.dump(data,f,ensure_ascii=False,indent=2)
        
        logger.info("Save {} records to {}",len(data),filepath)
        return str(filepath)
    


def run_extraction_pipeline(
    cities:Optional[List[str]]=None,
    output_dir:str=_RAW_DATA_DIRECTORY
    ) -> str:
    """Run the end-to-end weather extraction stage.

    Instantiates a :class:`WeatherExtractor`, fetches weather for the given
    cities, and saves the collected records to disk. This is the callable
    invoked by the ``extract_weather_api`` task in the
    ``weather_hourly_ingestion`` DAG.

    Args:
        cities: Optional list of city names to fetch. If ``None``, the
            extractor's default city list is used.
        output_dir: Directory to write the raw JSON file to. Defaults to the
            pipeline's raw data directory.

    Returns:
        The path to the saved JSON file.
    """
    extractor=WeatherExtractor()

    logger.info("Starting weather extraction")
    data=extractor.fetch_multiple_cities(cities)
    logger.info("Weather extraction completed")

    file_path=extractor.save_to_file(data,output_dir)

    return file_path


# if __name__=="__main__":
#     run_extraction_pipeline()
    
    



            
    

        
