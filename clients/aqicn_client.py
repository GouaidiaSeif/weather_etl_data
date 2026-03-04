from typing import Any, Dict, Optional
from datetime import datetime
from requests import RequestException

from clients.base_client import APIClient
from config.towns import Town
from utils.logger import get_logger
from utils.retry import retry_with_backoff, RetryableError

logger = get_logger(__name__)


class AQICNClient(APIClient):
    """Client for AQICN Air Quality API.
    API returns:
    - status: API response status
    - data.aqi: Air Quality Index value
    - data.idx: Station ID
    - data.time: Timestamp information
    - data.city: City information
    - data.iaqi: Individual air quality indices (PM2.5, PM10, O3, etc.)
    """
    
    BASE_URL = "https://api.waqi.info/feed"
    
    def __init__(self, api_key: str, timeout: int = 30, max_retries: int = 3):
        """
        Args:
            api_key: AQICN API token
            timeout: Request timeout in seconds
            max_retries: Maximum retries for connection errors
        """

        super().__init__(api_key, "", timeout, max_retries)
        logger.info("AQICN client initialized")
    
    def _make_aqicn_request(self, url: str) -> Dict[str, Any]:
        """Make a request to AQICN API.
        
        Args:
            url: Full API URL
            
        Returns:
            Dict[str, Any]: JSON response
        """
        import requests
        
        params = {"token": self._api_key}
        
        logger.debug(f"Making AQICN request to {url}")
        
        response = self._session.get(
            url,
            params=params,
            timeout=self._timeout
        )
        
        response.raise_for_status()
        
        return response.json()
    
    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        retryable_exceptions=(RetryableError, RequestException)
    )
    def fetch_by_coordinates(self, town: Town) -> Dict[str, Any]:
        """Fetch air quality data by coordinates.
        Args:
            town: Town object with coordinates
            
        Returns:
            Dict[str, Any]: Raw API response with air quality data
        """
        url = f"https://api.waqi.info/feed/geo:{town.lat};{town.lon}/"
        
        logger.info(f"Fetching air quality for {town.name} ({town.lat}, {town.lon})")
        
        try:
            response = self._make_aqicn_request(url)
            
            
            response["_metadata"] = {
                "town_name": town.name,
                "town_lat": town.lat,
                "town_lon": town.lon,
                "fetched_at": datetime.utcnow().isoformat(),
                "api_source": "aqicn"
            }
            
            # Check API status
            if response.get("status") != "ok":
                error_msg = response.get("data", "Unknown error")
                logger.error(f"AQICN API error for {town.name}: {error_msg}")
                raise RetryableError(f"AQICN API error: {error_msg}")
            
            aqi = response.get("data", {}).get("aqi", "N/A")
            logger.info(f"Successfully fetched AQI {aqi} for {town.name}")
            
            return response
            
        except RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                if status_code in (429, 500, 502, 503, 504):
                    logger.warning(f"Retryable error {status_code} for {town.name}: {e}")
                    raise RetryableError(f"HTTP {status_code}", e) from e
            
            logger.error(f"Failed to fetch air quality for {town.name}: {e}")
            raise
    
    def fetch_by_city(self, city_name: str) -> Dict[str, Any]:
        """Fetch air quality data by city name.
        
        Args:
            city_name: Name of the city
            
        Returns:
            Dict[str, Any]: Raw API response with air quality data
        """
        url = f"https://api.waqi.info/feed/{city_name}/"
        
        logger.info(f"Fetching air quality for city: {city_name}")
        
        try:
            response = self._make_aqicn_request(url)
            
            
            response["_metadata"] = {
                "city_name": city_name,
                "fetched_at": datetime.utcnow().isoformat(),
                "api_source": "aqicn"
            }
            
            
            if response.get("status") != "ok":
                error_msg = response.get("data", "Unknown error")
                logger.error(f"AQICN API error for {city_name}: {error_msg}")
                raise RetryableError(f"AQICN API error: {error_msg}")
            
            aqi = response.get("data", {}).get("aqi", "N/A")
            logger.info(f"Successfully fetched AQI {aqi} for {city_name}")
            
            return response
            
        except RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                if status_code in (429, 500, 502, 503, 504):
                    logger.warning(f"Retryable error {status_code} for {city_name}: {e}")
                    raise RetryableError(f"HTTP {status_code}", e) from e
            
            logger.error(f"Failed to fetch air quality for {city_name}: {e}")
            raise
    
    def fetch_data(self, town: Town, **kwargs) -> Dict[str, Any]:
        """Fetch air quality data (implements base class interface).
        
        Args:
            town: Town to fetch data for
            **kwargs: Additional parameters

        Returns:
            Dict[str, Any]: Raw API response
        """
        return self.fetch_by_coordinates(town, **kwargs)
