##==========================
## Import Required Libraries
##==========================
import json
from loguru import logger
from typing import Dict,Any,List,Optional
from datetime import datetime,timezone
import requests
from pathlib import Path

##============================
## User Define Python Modules
##============================
from config.config import OpenWeatherConfig

## create an object
weather_config=OpenWeatherConfig()

_CITIES=["Colombo","Dubai","New York","London","Sydney"]

class WeatherExtractor:
    def __init__(self, api_key:Optional[str]=None,base_url:Optional[str]=None):
        self.api_key=api_key if api_key is not None else weather_config.api_key
        self.base_url=base_url if base_url is not None else weather_config.base_url

        if not self.api_key:
            logger.error("API Key is required to make API calls")
            raise ValueError("API Key is required")
        
    ##=====================
    ## fetch weather data
    ##=====================

    def fetch_city_weather(self,city:str) -> Dict[str,Any]:
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
    
    def fetch_multiple_cities(self,cities:List[str]) -> List[Dict[str,Any]]:
        cities=cities or _CITIES
        results=List[Dict[str,Any]]=[]

        for city in cities:
            try:
                logger.info("Fetching weather for %s",city)
                results.append(self.fetch_city_weather(city))
            except Exception as e:
                logger.error("Failed to fetch weather data for %s: %s",city,e)

        return results
    
    def save_to_file(self,data:List[Dict[str,Any]], output_dir:str="data/raw") -> str:
        Path(output_dir).mkdir(parents=True,exist_ok=True) ## create a directory if not exists
        timestamp_str=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") ## create a timestamp

        filepath=Path(output_dir) / f"weather_{timestamp_str}.json"

        with open(filepath,"w",encoding="utf-8") as f:
            json.dump(data,f,ensure_ascii=False,indent=2)
        
        logger.info("Save %d records to %s",len(data),filepath)
        return str(filepath)
    


def run_extraction_pipeline(cities:Optional[List[str]]=None,output_dir:str="data/raw") -> str:
    
    extractor=WeatherExtractor()

    logger.info("Starting weather extraction")
    data=extractor.fetch_multiple_cities(cities)
    logger.info("Weather extraction completed")

    file_path=extractor.save_to_file(data,output_dir)

    return file_path


if __name__=="__main__":
    path=run_extraction_pipeline()
    print(f"Weather data saved to {path}")

    
    
    



            
    

        
