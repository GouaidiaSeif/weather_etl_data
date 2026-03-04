"""Base API client class providing common functionality for all API clients.
- HTTP session management
- Request/response handling
- Error handling
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.logger import get_logger

logger = get_logger(__name__)


class APIClient(ABC):
    """Abstract base class for API clients.
    
    Provides common HTTP functionality and session management.
    """
    
    def __init__(
        self,
        api_key: str,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """Initialize the API client.
        
        Args:
            api_key: API key for authentication
            base_url: Base URL for the API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
        """
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        
        # Create session with retry strategy
        self._session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Make an HTTP GET request.
        
        Args:
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            headers: HTTP headers
            
        Returns:
            Dict[str, Any]: JSON response
            
        Raises:
            requests.RequestException: If the request fails
        """
        url = f"{self._base_url}{endpoint}"
        
        logger.debug(f"Making request to {url}")
        
        response = self._session.get(
            url,
            params=params,
            headers=headers,
            timeout=self._timeout
        )
        
        response.raise_for_status()
        
        return response.json()
    
    def close(self):
        """Close the HTTP session."""
        self._session.close()
    
    @abstractmethod
    def fetch_data(self, *args, **kwargs) -> Dict[str, Any]:
        """Fetch data from the API.
        
        This method must be implemented by subclasses.
        
        Returns:
            Dict[str, Any]: Raw API response
        """
        pass
