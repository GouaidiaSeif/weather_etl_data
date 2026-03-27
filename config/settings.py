import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    """
        openweather_api_key: API key for OpenWeatherMap
        aqicn_api_key: API token for AQICN
        data_raw_path: Path to store raw API responses
        data_processed_path: Path to store processed data
        log_path: Path to store log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        request_timeout: HTTP request timeout in seconds
        max_retries: Maximum retries for failed API calls
        
        MongoDB settings:
        mongodb_uri: Full MongoDB connection URI (optional, overrides other settings)
        mongodb_host: MongoDB host
        mongodb_port: MongoDB port
        mongodb_database: MongoDB database name
        mongodb_username: MongoDB username (optional)
        mongodb_password: MongoDB password (optional)
    """
    openweather_api_key: str
    aqicn_api_key: str
    data_raw_path: Path
    data_processed_path: Path
    log_path: Path = field(default_factory=lambda: Path("data/logs"))
    log_level: str = "INFO"
    request_timeout: int = 30
    max_retries: int = 3
    
    # MongoDB settings
    mongodb_uri: Optional[str] = None
    mongodb_host: str = "localhost"
    mongodb_port: int = 27017
    mongodb_database: str = "weather_etl"
    mongodb_username: Optional[str] = None
    mongodb_password: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Create settings from environment variables.
        Returns:
            Settings: Configured settings instance
        Raises:
            ValueError: If required environment variables are missing
        """
        
        openweather_key = os.getenv("OPENWEATHER_API_KEY")
        aqicn_key = os.getenv("AQICN_API_KEY")
        
        if not openweather_key:
            raise ValueError(
                "OPENWEATHER_API_KEY environment variable is required. "
                "Get one at: https://openweathermap.org/api "
            )
        
        if not aqicn_key:
            raise ValueError(
                "AQICN_API_KEY environment variable is required. "
                "Get one at: https://aqicn.org/data-platform/token/ "
            )
        
        project_root = Path(__file__).parent.parent
        
        # MongoDB configuration
        mongodb_uri = os.getenv("MONGODB_URI")
        mongodb_host = os.getenv("MONGODB_HOST", "localhost")
        mongodb_port = int(os.getenv("MONGODB_PORT", "27017"))
        mongodb_database = os.getenv("MONGODB_DATABASE", "weather_etl")
        mongodb_username = os.getenv("MONGODB_USERNAME")
        mongodb_password = os.getenv("MONGODB_PASSWORD")
        
        return cls(
            openweather_api_key=openweather_key,
            aqicn_api_key=aqicn_key,
            data_raw_path=Path(os.getenv("DATA_RAW_PATH", project_root / "data" / "raw")),
            data_processed_path=Path(os.getenv("DATA_PROCESSED_PATH", project_root / "data" / "processed")),
            log_path=Path(os.getenv("LOG_PATH", project_root / "data" / "logs")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            # MongoDB settings
            mongodb_uri=mongodb_uri,
            mongodb_host=mongodb_host,
            mongodb_port=mongodb_port,
            mongodb_database=mongodb_database,
            mongodb_username=mongodb_username,
            mongodb_password=mongodb_password,
        )


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance.
    Returns:
        Settings: Application settings
    """
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings


def reset_settings():
    """Reset the global settings instance.
    Useful for testing.
    """
    global _settings
    _settings = None