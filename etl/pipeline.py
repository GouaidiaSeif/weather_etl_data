"""Main ETL pipeline for weather and air quality data with Hive partitioning.

Orchestrates the extraction of data from multiple APIs,
stores raw data in hive-style partitions, and prepares it for downstream processing.

Both Weather and Air Quality APIs are called hourly to ensure data synchronization.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from clients.openweather_client import OpenWeatherClient
from clients.aqicn_client import AQICNClient
from config.settings import Settings, get_settings
from config.towns import FRENCH_TOWNS, Town
from storage.hive_storage import HivePartitionedStorage
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ETLResult:
    """Result of an ETL operation."""
    success: bool
    town: str
    api_source: str
    filepaths: List[Path] = None
    error: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)
        if self.filepaths is None:
            self.filepaths = []


class WeatherETLPipeline:
    """ETL pipeline with Hive-style partitioning for weather and air quality data.
    
    Storage Structure:
        data/raw/
        ├── city=paris/
        │   └── year=2026/
        │       └── month=02/
        │           └── day=08/
        │               ├── weather_13_raw.json       (1 PM weather)
        │               ├── weather_14_raw.json       (2 PM weather)
        │               ├── air_quality_13_raw.json   (1 PM air quality)
        │               └── air_quality_14_raw.json   (2 PM air quality)
    
    Both weather and air quality data are fetched hourly and saved with the same hour.
    The hour is extracted from each API response to ensure synchronization.
    
    Usage:
        with WeatherETLPipeline() as pipeline:
            # Run hourly ETL (both weather and air quality)
            results = pipeline.run_hourly(hours_back=1)
    """
    
    def __init__(
        self,
        settings: Optional[Settings] = None,
        towns: Optional[List[Town]] = None
    ):
        """Initialize the ETL pipeline.
        
        Args:
            settings: Application settings
            towns: List of towns to process
        """
        self._settings = settings or get_settings()
        self._towns = towns or FRENCH_TOWNS
        
        self._storage = HivePartitionedStorage(self._settings.data_raw_path)
        
        self._weather_client: Optional[OpenWeatherClient] = None
        self._air_quality_client: Optional[AQICNClient] = None
        
        logger.info(
            f"Initialized WeatherETLPipeline for {len(self._towns)} towns: "
            f"{[t.name for t in self._towns]}"
        )
    
    def __enter__(self):
        """Context manager entry - initialize API clients."""
        self._weather_client = OpenWeatherClient(
            api_key=self._settings.openweather_api_key,
            timeout=self._settings.request_timeout,
            max_retries=self._settings.max_retries
        )
        
        self._air_quality_client = AQICNClient(
            api_key=self._settings.aqicn_api_key,
            timeout=self._settings.request_timeout,
            max_retries=self._settings.max_retries
        )
        
        logger.debug("API clients initialized")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        if self._weather_client:
            self._weather_client.close()
        if self._air_quality_client:
            self._air_quality_client.close()
        logger.debug("API clients closed")
    
    def extract_weather(self, town: Town) -> Optional[Dict[str, Any]]:
        """Extract weather data for a town."""
        if not self._weather_client:
            raise RuntimeError("Pipeline not initialized. Use context manager.")
        
        try:
            return self._weather_client.fetch_hourly_data(town)
        except Exception as e:
            logger.error(f"Failed to extract weather for {town.name}: {e}")
            return None
    
    def extract_air_quality(self, town: Town) -> Optional[Dict[str, Any]]:
        """Extract air quality data for a town."""
        if not self._air_quality_client:
            raise RuntimeError("Pipeline not initialized. Use context manager.")
        
        try:
            return self._air_quality_client.fetch_by_coordinates(town)
        except Exception as e:
            logger.error(f"Failed to extract air quality for {town.name}: {e}")
            return None
    
    def process_town_weather(self, town: Town, reference_hour: datetime, hours_back: int = 1) -> ETLResult:
        """Process weather data for a single town.
        
        Args:
            town: Town to process
            reference_hour: The reference UTC hour to use for filename consistency
            hours_back: Number of past hours to extract
            
        Returns:
            ETLResult: Result of the operation
        """
        timestamp = datetime.now(timezone.utc)
        
        logger.info(f"Processing weather data for {town.name} (reference hour: {reference_hour.hour} UTC)")
        weather_data = self.extract_weather(town)
        
        if weather_data:
            try:
                filepaths = self._storage.save_weather_hourly_records(
                    api_response=weather_data,
                    city_name=town.name,
                    target_hour=reference_hour,
                    hours_back=hours_back
                )
                
                if filepaths:
                    return ETLResult(
                        success=True,
                        town=town.name,
                        api_source="openweather",
                        filepaths=filepaths,
                        timestamp=timestamp
                    )
                else:
                    return ETLResult(
                        success=False,
                        town=town.name,
                        api_source="openweather",
                        error="No hourly data saved",
                        timestamp=timestamp
                    )
            except Exception as e:
                logger.error(f"Failed to save weather data for {town.name}: {e}")
                return ETLResult(
                    success=False,
                    town=town.name,
                    api_source="openweather",
                    error=str(e),
                    timestamp=timestamp
                )
        else:
            return ETLResult(
                success=False,
                town=town.name,
                api_source="openweather",
                error="Extraction failed",
                timestamp=timestamp
            )
    
    def process_town_air_quality(self, town: Town, reference_hour: datetime) -> ETLResult:
        """Process air quality data for a single town.
        
        Args:
            town: Town to process
            reference_hour: The reference UTC hour to use for filename consistency
            
        Returns:
            ETLResult: Result of the operation
        """
        timestamp = datetime.now(timezone.utc)
        
        logger.info(f"Processing air quality data for {town.name} (reference hour: {reference_hour.hour} UTC)")
        air_quality_data = self.extract_air_quality(town)
        
        if air_quality_data:
            try:
                # Use the reference hour to ensure filename matches weather data
                filepath = self._storage.save_air_quality_data(
                    api_response=air_quality_data,
                    city_name=town.name,
                    target_hour=reference_hour
                )
                
                return ETLResult(
                    success=True,
                    town=town.name,
                    api_source="aqicn",
                    filepaths=[filepath],
                    timestamp=timestamp
                )
            except Exception as e:
                logger.error(f"Failed to save air quality data for {town.name}: {e}")
                return ETLResult(
                    success=False,
                    town=town.name,
                    api_source="aqicn",
                    error=str(e),
                    timestamp=timestamp
                )
        else:
            return ETLResult(
                success=False,
                town=town.name,
                api_source="aqicn",
                error="Extraction failed",
                timestamp=timestamp
            )
    
    def process_town(self, town: Town, reference_hour: datetime, hours_back: int = 1) -> List[ETLResult]:
        """Process a single town - extract and store both data types.
        
        Args:
            town: Town to process
            reference_hour: The reference UTC hour to use for both APIs
            hours_back: Number of past hours to extract for weather
            
        Returns:
            List[ETLResult]: Results for both weather and air quality
        """
        results = []
        
        # Process weather data with reference hour
        weather_result = self.process_town_weather(town, reference_hour, hours_back)
        results.append(weather_result)
        
        # Process air quality data with the SAME reference hour
        air_quality_result = self.process_town_air_quality(town, reference_hour)
        results.append(air_quality_result)
        
        return results
    
    def run_hourly(self, hours_back: int = 1) -> Dict[str, List[ETLResult]]:
        """Run the hourly ETL pipeline for all towns (both weather and air quality).

        Args:
            hours_back: Number of past hours to extract for weather (default: 1)
            
        Returns:
            Dict[str, List[ETLResult]]: Results by town
        """
        # Generate a single reference hour for this entire ETL run
        # This ensures all files across all cities use the same hour
        reference_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        
        logger.info(f"Starting hourly ETL pipeline for {len(self._towns)} towns")
        logger.info(f"Reference hour (UTC): {reference_hour}")
        logger.info(f"Weather: extracting past {hours_back} hour(s)")
        logger.info(f"Air Quality: extracting current hour")
        
        all_results: Dict[str, List[ETLResult]] = {}
        
        for town in self._towns:
            logger.info(f"Processing town: {town.name}")
            results = self.process_town(town, reference_hour, hours_back)
            all_results[town.name] = results
            
            success_count = sum(1 for r in results if r.success)
            total_files = sum(len(r.filepaths) for r in results if r.filepaths)
            logger.info(
                f"Completed {town.name}: {success_count}/{len(results)} operations, "
                f"{total_files} files saved"
            )
        
        total_ops = sum(len(r) for r in all_results.values())
        total_success = sum(sum(1 for r in results if r.success) for results in all_results.values())
        total_files = sum(
            len(r.filepaths) 
            for results in all_results.values() 
            for r in results 
            if r.filepaths
        )
        logger.info(
            f"Hourly ETL completed: {total_success}/{total_ops} operations, "
            f"{total_files} files saved"
        )
        
        return all_results
    
    def get_summary(self, results: Dict[str, List[ETLResult]]) -> Dict[str, Any]:
        """Generate a summary of ETL results."""
        total_ops = 0
        total_success = 0
        total_files = 0
        api_stats: Dict[str, Dict[str, int]] = {}
        
        for town_results in results.values():
            for result in town_results:
                total_ops += 1
                if result.success:
                    total_success += 1
                if result.filepaths:
                    total_files += len(result.filepaths)
                
                if result.api_source not in api_stats:
                    api_stats[result.api_source] = {"success": 0, "failed": 0, "files": 0}
                
                if result.success:
                    api_stats[result.api_source]["success"] += 1
                else:
                    api_stats[result.api_source]["failed"] += 1
                
                if result.filepaths:
                    api_stats[result.api_source]["files"] += len(result.filepaths)
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_towns": len(results),
            "total_operations": total_ops,
            "successful_operations": total_success,
            "failed_operations": total_ops - total_success,
            "total_files_saved": total_files,
            "success_rate": total_success / total_ops if total_ops > 0 else 0,
            "api_breakdown": api_stats
        }


def run_hourly_etl_job(
    settings: Optional[Settings] = None,
    towns: Optional[List[Town]] = None,
    hours_back: int = 1
) -> Dict[str, Any]:
    """Convenience function to run an hourly ETL job (both weather and air quality).
    
    Args:
        settings: Optional settings
        towns: Optional list of towns
        hours_back: Number of past hours to extract for weather (default: 1)
        
    Returns:
        Dict[str, Any]: Summary of the ETL job
    """
    with WeatherETLPipeline(settings, towns) as pipeline:
        results = pipeline.run_hourly(hours_back)
        summary = pipeline.get_summary(results)
    
    return summary