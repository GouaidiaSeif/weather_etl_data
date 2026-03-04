from typing import Any, Dict, Optional
from datetime import datetime
from requests import RequestException

from clients.base_client import APIClient
from config.towns import Town
from utils.logger import get_logger
from utils.retry import retry_with_backoff, RetryableError

logger = get_logger(__name__)


class OpenWeatherClient(APIClient):
    """
    The hourly data includes:
    - dt: Unix timestamp
    - temp: Temperature
    - feels_like: Feels-like temperature
    - pressure: Atmospheric pressure (hPa)
    - humidity: Relative humidity (%)
    - uvi: UV index
    - clouds: Cloud coverage (%)
    - visibility: Visibility (meters)
    - wind_speed: Wind speed
    - wind_deg: Wind direction (degrees)
    - wind_gust: Wind gusts
    - pop: Probability of precipitation
    - rain.1h: Rain volume (mm/h)
    - snow.1h: Snow volume (mm/h)
    - weather: Weather conditions array
    """
    
    BASE_URL = "https://api.openweathermap.org/data/3.0"
    
    def __init__(self, api_key: str, timeout: int = 30, max_retries: int = 3):
        """Initialize the OpenWeather client.
        
        Args:
            api_key: OpenWeatherMap API key
            timeout: Request timeout in seconds
            max_retries: Maximum retries for connection errors
        """
        super().__init__(api_key, self.BASE_URL, timeout, max_retries)
        logger.info("OpenWeather client initialized")
    
    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        retryable_exceptions=(RetryableError, RequestException)
    )
    def fetch_hourly_data(
        self,
        town: Town,
        units: str = "metric",
        lang: str = "en"
    ) -> Dict[str, Any]:
        """  
        Args:
            town: Town object with coordinates
            units: Unit system (metric, imperial, standard)
            lang: Language for weather descriptions
            
        Returns:
            Dict[str, Any]: Raw API response with hourly data
        """
        endpoint = "/onecall"
        
        params = {
            "lat": town.lat,
            "lon": town.lon,
            "appid": self._api_key,
            "units": units,
            "lang": lang,
            "exclude": "minutely,daily,alerts"
        }
        
        logger.info(f"Fetching weather data for {town.name} ({town.lat}, {town.lon})")
        
        try:
            response = self._make_request(endpoint, params)
            
            # Add metadata for tracking
            response["_metadata"] = {
                "town_name": town.name,
                "town_lat": town.lat,
                "town_lon": town.lon,
                "fetched_at": datetime.utcnow().isoformat(),
                "api_source": "openweather",
                "units": units
            }
            
            # Validate response has expected data
            if "hourly" not in response:
                logger.error(f"Unexpected response structure: {list(response.keys())}")
                raise RetryableError("Response missing 'hourly' data")
            
            hourly_count = len(response.get("hourly", []))
            logger.info(f"Successfully fetched {hourly_count} hourly records for {town.name}")
            
            return response
            
        except RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                if status_code in (429, 500, 502, 503, 504):
                    logger.warning(f"Retryable error {status_code} for {town.name}: {e}")
                    raise RetryableError(f"HTTP {status_code}", e) from e
            
            logger.error(f"Failed to fetch weather for {town.name}: {e}")
            raise
    
    def fetch_data(self, town: Town, **kwargs) -> Dict[str, Any]:
        """Fetch weather data (implements base class interface).
        Args:
            town: Town to fetch data for
            **kwargs: Additional parameters
            
        Returns:
            Dict[str, Any]: Raw API response
        """
        return self.fetch_hourly_data(town, **kwargs)
    
    def fetch_all_towns(
        self,
        towns: list,
        units: str = "metric",
        lang: str = "en"
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch weather data for multiple towns.
        Args:
            towns: List of Town objects
            units: Unit system
            lang: Language for descriptions
            
        Returns:
            Dict[str, Dict[str, Any]]: Mapping of town names to their data
        """
        results = {}
        
        for town in towns:
            try:
                data = self.fetch_hourly_data(town, units, lang)
                results[town.name] = data
            except Exception as e:
                logger.error(f"Failed to fetch data for {town.name}: {e}")
                results[town.name] = {"error": str(e), "town": town.name}
        
        return results
