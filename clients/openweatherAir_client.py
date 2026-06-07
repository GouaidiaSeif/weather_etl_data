from typing import Any, Dict, Optional
from datetime import datetime
from requests import RequestException

from clients.base_client import APIClient
from config.towns import Town
from utils.logger import get_logger
from utils.retry import retry_with_backoff, RetryableError

logger = get_logger(__name__)

class OpenWeatherAirQualityClient(APIClient):
    """
    Air pollution forecast data current hour + next 96 hours (4days).
    
    The hourly data includes:
    - dt: Unix timestamp (1st element is current hour)
    - coord.lon: Longitude
    - coord.lat: Latitude
    - list.main.aqi: Air Quality Index value :1 to 5 (good to very poor)
    - list.components.co : Co concentration (μg/m3)
    - list.components.no : No concentration (μg/m3)
    - list.components.no2 : No2 concentration (μg/m3)
    - list.components.o3 : O3 concentration (μg/m3)
    - list.components.so2 : So2 concentration (μg/m3)
    - list.components.pm_25 : PM25 concentration (μg/m3)
    - list.components.pm10 : PM10 concentration (μg/m3)
    - list.components.nh3 : nh3 concentration (μg/m3)
    """
    BASE_URL = "https://api.openweathermap.org/data/2.5"

    def __init__(self, api_key: str, timeout: int = 30, max_retries: int = 3):
        super().__init__(api_key, self.BASE_URL, timeout, max_retries)
        logger.info("OpenWeather AirQuality client initialized")

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        retryable_exceptions=(RetryableError, RequestException)
    )
    
    def fetch_air_quality_forecast(self, town: Town) -> Dict[str, Any]:
        """  Fetch pollution forecast for one town
            Args:
                town: Town object with coordinates
                
            Returns:
                Dict[str, Any]: Raw API response with hourly data
        """
        
        endpoint = "/air_pollution/forecast"

        params = {
            "lat": town.lat,
            "lon": town.lon,
            "appid": self._api_key,
        }

        logger.info(f"Fetching air quality forecast for {town.name}")

        try:
            response = self._make_request(endpoint, params)

            response["_metadata"] = {
                "town_name": town.name,
                "town_lat": town.lat,
                "town_lon": town.lon,
                "fetched_at": datetime.utcnow().isoformat(),
                "api_source": "openweather_air_quality",
            }

            if "list" not in response:
                    logger.error(f"Unexpected response structure: {list(response.keys())}")
                    raise RetryableError("Response missing 'list' data")

            logger.info(f"Successfully fetched {len(response['list'])} records for {town.name}")
            return response

        except RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                if e.response.status_code in (429, 500, 502, 503, 504):
                    logger.warning(f"Retryable error {e.response.status_code} for {town.name}: {e}")
                    raise RetryableError(f"HTTP {e.response.status_code}", e) from e
            
            logger.error(f"Failed to fetch pollution forecast for {town.name}: {e}")
            raise

    def fetch_data(self, town: Town, **kwargs) -> Dict[str, Any]:
        return self.fetch_air_quality_forecast(town)