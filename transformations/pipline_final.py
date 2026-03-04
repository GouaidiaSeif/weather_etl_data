"""
Main ETL pipeline for weather and air quality data with Hive partitioning.

Architecture:
    RAW    → data/raw/      (original API responses)
    SILVER → data/silver/   (cleaned & standardized datasets)
    GOLD   → data/gold/     (daily aggregated analytics)

v3 IMPROVEMENTS:
- Extract-First Architecture: All cities extracted before any processing
- Updated transformers handle actual API data structure correctly
- Hour extraction from _storage.hour_timestamp
- Comprehensive field coverage
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json

from clients.openweather_client import OpenWeatherClient
from clients.aqicn_client import AQICNClient
from config.settings import Settings, get_settings
from config.towns import FRENCH_TOWNS, Town
from storage.hive_storage import HivePartitionedStorage
from utils.logger import get_logger

from transformations.improved_weather_transformer import WeatherTransformer
from transformations.improved_air_quality_transformer import AirQualityTransformer
from transformations.improved_gold_pipeline import GoldPipeline

logger = get_logger(__name__)


# =====================================================
# DATA MODELS
# =====================================================

@dataclass
class ExtractedData:
    """Raw extracted data for a town."""
    town: Town
    weather_data: Optional[Dict[str, Any]] = None
    air_quality_data: Optional[Dict[str, Any]] = None
    weather_raw_path: Optional[Path] = None
    air_quality_raw_path: Optional[Path] = None
    weather_error: Optional[str] = None
    air_quality_error: Optional[str] = None


@dataclass
class ETLResult:
    """Result of an ETL operation."""
    success: bool
    town: str
    api_source: str
    filepaths: List[Path] = field(default_factory=list)
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# =====================================================
# MAIN PIPELINE - EXTRACT FIRST ARCHITECTURE
# =====================================================

class WeatherETLPipeline:
    """ETL pipeline with extract-first architecture."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        towns: Optional[List[Town]] = None,
    ):
        self._settings = settings or get_settings()
        self._towns = towns or FRENCH_TOWNS

        self._raw_base = Path(self._settings.data_raw_path)
        self._silver_base = Path("data/silver")
        self._gold_base = Path("data/gold")

        self._storage = HivePartitionedStorage(self._raw_base)

        self._weather_client: Optional[OpenWeatherClient] = None
        self._air_quality_client: Optional[AQICNClient] = None

        logger.info(f"WeatherETLPipeline initialized for {len(self._towns)} towns")

    def __enter__(self):
        self._weather_client = OpenWeatherClient(
            api_key=self._settings.openweather_api_key,
            timeout=self._settings.request_timeout,
            max_retries=self._settings.max_retries,
        )
        self._air_quality_client = AQICNClient(
            api_key=self._settings.aqicn_api_key,
            timeout=self._settings.request_timeout,
            max_retries=self._settings.max_retries,
        )
        logger.info("API clients initialized")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._weather_client:
            self._weather_client.close()
        if self._air_quality_client:
            self._air_quality_client.close()
        logger.info("API clients closed")

    # =====================================================
    # PHASE 1: EXTRACTION (All cities)
    # =====================================================

    def _extract_all_cities(self, reference_hour: datetime) -> List[ExtractedData]:
        """Extract data from all APIs for all cities."""
        logger.info("=" * 60)
        logger.info("PHASE 1: EXTRACTION - Fetching data from all APIs")
        logger.info("=" * 60)
        
        extracted_data: List[ExtractedData] = []
        
        for town in self._towns:
            logger.info(f"Extracting data for {town.name}...")
            data = ExtractedData(town=town)
            
            # Extract weather
            try:
                data.weather_data = self._weather_client.fetch_hourly_data(town)
                hourly_count = len(data.weather_data.get('hourly', []))
                logger.info(f"  ✓ Weather: {hourly_count} hourly records")
            except Exception as e:
                data.weather_error = str(e)
                logger.error(f"  ✗ Weather extraction failed: {e}")
            
            # Extract air quality
            try:
                data.air_quality_data = self._air_quality_client.fetch_by_coordinates(town)
                aqi = data.air_quality_data.get("data", {}).get("aqi", "N/A")
                logger.info(f"  ✓ Air Quality: AQI {aqi}")
            except Exception as e:
                data.air_quality_error = str(e)
                logger.error(f"  ✗ Air Quality extraction failed: {e}")
            
            extracted_data.append(data)
        
        success_count = sum(1 for d in extracted_data if d.weather_data or d.air_quality_data)
        logger.info(f"Extraction complete: {success_count}/{len(self._towns)} cities have data")
        return extracted_data

    # =====================================================
    # PHASE 2: SAVE RAW (Bronze layer)
    # =====================================================

    def _save_all_raw(self, extracted_data: List[ExtractedData], reference_hour: datetime) -> None:
        """Save all extracted data to raw storage (Bronze layer)."""
        logger.info("=" * 60)
        logger.info("PHASE 2: BRONZE - Saving all raw data")
        logger.info("=" * 60)
        
        for data in extracted_data:
            town = data.town
            
            # Save weather raw
            if data.weather_data:
                try:
                    paths = self._storage.save_weather_hourly_records(
                        api_response=data.weather_data,
                        city_name=town.name,
                        target_hour=reference_hour,
                        hours_back=1,
                    )
                    if paths:
                        data.weather_raw_path = paths[0]
                        logger.info(f"  ✓ {town.name}: Saved weather raw → {paths[0].name}")
                except Exception as e:
                    data.weather_error = str(e)
                    logger.error(f"  ✗ {town.name}: Failed to save weather raw: {e}")
            
            # Save air quality raw
            if data.air_quality_data:
                try:
                    path = self._storage.save_air_quality_data(
                        api_response=data.air_quality_data,
                        city_name=town.name,
                        target_hour=reference_hour,
                    )
                    data.air_quality_raw_path = path
                    logger.info(f"  ✓ {town.name}: Saved air quality raw → {path.name}")
                except Exception as e:
                    data.air_quality_error = str(e)
                    logger.error(f"  ✗ {town.name}: Failed to save air quality raw: {e}")

    # =====================================================
    # PHASE 3: TRANSFORM (Silver layer)
    # =====================================================

    def _transform_all(self, extracted_data: List[ExtractedData]) -> List[ETLResult]:
        """Transform all raw data to silver layer."""
        logger.info("=" * 60)
        logger.info("PHASE 3: SILVER - Transforming all raw data")
        logger.info("=" * 60)
        
        results: List[ETLResult] = []
        
        for data in extracted_data:
            town = data.town
            
            # Transform weather
            if data.weather_raw_path and data.weather_raw_path.exists():
                try:
                    with open(data.weather_raw_path) as f:
                        raw_data = json.load(f)
                    
                    cleaned = WeatherTransformer.transform(raw_data, city_name=town.name)
                    silver_path = self._save_silver(data.weather_raw_path, cleaned)
                    
                    results.append(ETLResult(
                        success=True,
                        town=town.name,
                        api_source="openweather",
                        filepaths=[silver_path]
                    ))
                    logger.info(f"  ✓ {town.name}: Weather transformed → {silver_path.name}")
                except Exception as e:
                    results.append(ETLResult(
                        success=False,
                        town=town.name,
                        api_source="openweather",
                        error=str(e)
                    ))
                    logger.error(f"  ✗ {town.name}: Weather transform failed: {e}")
            
            # Transform air quality
            if data.air_quality_raw_path and data.air_quality_raw_path.exists():
                try:
                    with open(data.air_quality_raw_path) as f:
                        raw_data = json.load(f)
                    
                    cleaned = AirQualityTransformer.transform(raw_data, city_name=town.name)
                    silver_path = self._save_silver(data.air_quality_raw_path, cleaned)
                    
                    results.append(ETLResult(
                        success=True,
                        town=town.name,
                        api_source="aqicn",
                        filepaths=[silver_path]
                    ))
                    logger.info(f"  ✓ {town.name}: Air quality transformed → {silver_path.name}")
                except Exception as e:
                    results.append(ETLResult(
                        success=False,
                        town=town.name,
                        api_source="aqicn",
                        error=str(e)
                    ))
                    logger.error(f"  ✗ {town.name}: Air quality transform failed: {e}")
        
        success_count = sum(1 for r in results if r.success)
        logger.info(f"Transform complete: {success_count}/{len(results)} operations successful")
        return results

    def _save_silver(self, raw_path: Path, cleaned_record: Dict[str, Any]) -> Path:
        """Save cleaned record to silver layer."""
        raw_path_str = str(raw_path)
        silver_path_str = raw_path_str.replace("_raw.json", "_cleaned.json")
        silver_path_str = silver_path_str.replace("/raw/", "/silver/")
        silver_path_str = silver_path_str.replace("\\raw\\", "\\silver\\")
        silver_path = Path(silver_path_str)

        silver_path.parent.mkdir(parents=True, exist_ok=True)

        with open(silver_path, "w") as f:
            json.dump(cleaned_record, f, indent=4)

        return silver_path

    # =====================================================
    # PHASE 4: GOLD AGGREGATION
    # =====================================================

    def _run_gold(self) -> None:
        """Run gold aggregation on all silver data."""
        logger.info("=" * 60)
        logger.info("PHASE 4: GOLD - Aggregating silver data")
        logger.info("=" * 60)
        
        try:
            gold = GoldPipeline(
                silver_base_path=self._silver_base,
                gold_base_path=self._gold_base,
            )
            gold.run()
            logger.info("✓ Gold aggregation completed")
        except Exception as e:
            logger.error(f"✗ Gold aggregation failed: {e}")
            raise

    # =====================================================
    # MAIN RUN
    # =====================================================

    def run_hourly(self, hours_back: int = 1) -> Tuple[List[ExtractedData], List[ETLResult]]:
        """Run the complete ETL pipeline with extract-first architecture."""
        reference_hour = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )
        
        logger.info("=" * 70)
        logger.info(f"Starting ETL pipeline at {reference_hour.isoformat()}")
        logger.info("Architecture: Extract → Bronze → Silver → Gold")
        logger.info("=" * 70)
        
        # Phase 1: Extract all data
        extracted_data = self._extract_all_cities(reference_hour)
        
        # Phase 2: Save all raw data
        self._save_all_raw(extracted_data, reference_hour)
        
        # Phase 3: Transform all data
        results = self._transform_all(extracted_data)
        
        # Phase 4: Gold aggregation
        self._run_gold()
        
        logger.info("=" * 70)
        logger.info("ETL pipeline completed")
        logger.info("=" * 70)
        
        return extracted_data, results

    def get_summary(self, results: List[ETLResult]) -> Dict[str, Any]:
        """Generate a summary of ETL results."""
        total_ops = len(results)
        total_success = sum(1 for r in results if r.success)
        total_files = sum(len(r.filepaths) for r in results if r.filepaths)
        
        api_stats: Dict[str, Dict[str, int]] = {}
        for result in results:
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
            "total_towns": len(set(r.town for r in results)),
            "total_operations": total_ops,
            "successful_operations": total_success,
            "failed_operations": total_ops - total_success,
            "total_files_saved": total_files,
            "success_rate": total_success / total_ops if total_ops > 0 else 0,
            "api_breakdown": api_stats
        }


# =====================================================
# ENTRY POINT
# =====================================================

def run_hourly_etl_job(
    settings: Optional[Settings] = None,
    towns: Optional[List[Town]] = None,
    hours_back: int = 1,
) -> Dict[str, Any]:
    """Run the complete ETL pipeline and return a summary."""
    with WeatherETLPipeline(settings, towns) as pipeline:
        _, results = pipeline.run_hourly(hours_back)
        summary = pipeline.get_summary(results)
    
    return summary
