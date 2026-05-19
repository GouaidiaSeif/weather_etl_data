"""
Main ETL pipeline for weather and air quality data with Hive partitioning.

Architecture:
    RAW    → MinIO (or local data/raw/) — original API responses only
    SILVER → MongoDB — cleaned & standardized datasets
    GOLD   → MongoDB — daily aggregated analytics

v3 IMPROVEMENTS:
- Extract-First Architecture: All cities extracted before any processing
- Updated transformers handle actual API data structure correctly
- Hour extraction from _storage.hour_timestamp
- Comprehensive field coverage
- MongoDB integration for Silver and Gold layers

v3.1
- MongoDB integration for gold : add 3 collections for air quality, weather and combined data
"""

import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

# Allow running as: python transformations/pipline_final.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from clients.openweather_client import OpenWeatherClient
from clients.aqicn_client import AQICNClient
from config.settings import Settings, get_settings
from config.towns import FRENCH_TOWNS, Town
from storage.data_store import DataStore, create_raw_store
from storage.hive_storage import HivePartitionedStorage
from storage.mongodb_storage import MongoDBStorage
from utils.logger import get_logger
from utils.timezone_utils import floor_to_paris_hour, get_reference_hour_paris, paris_date_str

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
    weather_raw_keys: List[str] = field(default_factory=list)
    air_quality_raw_key: Optional[str] = None
    weather_error: Optional[str] = None
    air_quality_error: Optional[str] = None


@dataclass
class ETLResult:
    """Result of an ETL operation."""
    success: bool
    town: str
    api_source: str
    object_keys: List[str] = field(default_factory=list)
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# =====================================================
# MAIN PIPELINE - EXTRACT FIRST ARCHITECTURE
# =====================================================

class WeatherETLPipeline:
    """ETL pipeline with extract-first architecture and MongoDB integration."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        towns: Optional[List[Town]] = None,
    ):
        self._settings = settings or get_settings()
        self._towns = towns or FRENCH_TOWNS

        self._raw_store: DataStore = create_raw_store(self._settings)
        self._raw_prefix = "raw"

        self._storage = HivePartitionedStorage(self._raw_store, prefix=self._raw_prefix)

        # Initialize MongoDB storage (for Silver and Gold layers only)
        self._mongodb = MongoDBStorage(self._settings)

        self._weather_client: Optional[OpenWeatherClient] = None
        self._air_quality_client: Optional[AQICNClient] = None

        logger.info(f"WeatherETLPipeline initialized for {len(self._towns)} towns")

    def __enter__(self):
        """Context manager entry - initialize API clients and MongoDB."""
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

        # Connect to MongoDB
        if self._mongodb.connect():
            logger.info("MongoDB connected successfully")
        else:
            logger.warning("MongoDB connection failed - continuing without MongoDB")

        logger.info("API clients initialized")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        if self._weather_client:
            self._weather_client.close()
        if self._air_quality_client:
            self._air_quality_client.close()

        # Close MongoDB connection
        self._mongodb.close()

        logger.info("API clients and MongoDB closed")

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
                logger.info(f"  OK Weather: {hourly_count} hourly records")
            except Exception as e:
                data.weather_error = str(e)
                logger.error(f"  FAIL Weather extraction failed: {e}")

            # Extract air quality
            try:
                data.air_quality_data = self._air_quality_client.fetch_by_coordinates(town)
                aqi = data.air_quality_data.get("data", {}).get("aqi", "N/A")
                logger.info(f"  OK Air Quality: AQI {aqi}")
            except Exception as e:
                data.air_quality_error = str(e)
                logger.error(f"  FAIL Air Quality extraction failed: {e}")

            extracted_data.append(data)

        success_count = sum(1 for d in extracted_data if d.weather_data or d.air_quality_data)
        logger.info(f"Extraction complete: {success_count}/{len(self._towns)} cities have data")
        return extracted_data

    # =====================================================
    # PHASE 2: SAVE RAW (Bronze layer - filesystem only)
    # =====================================================

    def _save_all_raw(self, extracted_data: List[ExtractedData], reference_hour: datetime) -> None:
        """Save all extracted data to raw storage (Bronze layer - filesystem only)."""
        logger.info("=" * 60)
        logger.info("PHASE 2: BRONZE - Saving all raw data (MinIO / object storage only)")
        logger.info("=" * 60)

        for data in extracted_data:
            town = data.town

            # Save weather raw (filesystem only - NOT MongoDB)
            if data.weather_data:
                try:
                    paths = self._storage.save_weather_hourly_records(
                        api_response=data.weather_data,
                        city_name=town.name,
                        target_hour=reference_hour,
                        hours_back=1,
                    )
                    if paths:
                        data.weather_raw_keys = paths
                        logger.info(
                            f"  OK {town.name}: Saved {len(paths)} weather raw file(s) "
                            f"-> {self._raw_store.key_name(paths[0])}"
                            + (f" (+{len(paths) - 1} more)" if len(paths) > 1 else "")
                        )
                except Exception as e:
                    data.weather_error = str(e)
                    logger.error(f"  FAIL {town.name}: Failed to save weather raw: {e}")

            # Save air quality raw (filesystem only - NOT MongoDB)
            if data.air_quality_data:
                try:
                    path = self._storage.save_air_quality_data(
                        api_response=data.air_quality_data,
                        city_name=town.name,
                        target_hour=reference_hour,
                    )
                    data.air_quality_raw_key = path
                    logger.info(f"  OK {town.name}: Saved air quality raw -> {self._raw_store.key_name(path)}")
                except Exception as e:
                    data.air_quality_error = str(e)
                    logger.error(f"  FAIL {town.name}: Failed to save air quality raw: {e}")

    def _link_raw_keys_from_storage(
        self,
        extracted_data: List[ExtractedData],
        reference_hour: datetime,
    ) -> None:
        """Point silver transform at MinIO raw files (including ones saved in earlier runs)."""
        ref_paris = floor_to_paris_hour(reference_hour)
        hour_label = ref_paris.strftime("%H")

        for data in extracted_data:
            town = data.town
            weather_keys = list(dict.fromkeys(data.weather_raw_keys))

            expected_weather = self._storage.hourly_object_key(
                town.name, reference_hour, "openweather"
            )
            if self._raw_store.exists(expected_weather) and expected_weather not in weather_keys:
                weather_keys.append(expected_weather)

            if not weather_keys:
                weather_keys = self._storage.list_raw_keys_for_city_day(
                    town.name, ref_paris, "openweather"
                )
                if weather_keys:
                    logger.info(
                        f"  LINK {town.name}: Found {len(weather_keys)} weather raw file(s) in MinIO"
                    )

            data.weather_raw_keys = weather_keys

            if data.air_quality_raw_key and self._raw_store.exists(data.air_quality_raw_key):
                continue

            expected_aq = self._storage.hourly_object_key(
                town.name, reference_hour, "aqicn"
            )
            if self._raw_store.exists(expected_aq):
                data.air_quality_raw_key = expected_aq
                continue

            for key in self._storage.list_raw_keys_for_city_day(
                town.name, ref_paris, "aqicn"
            ):
                if f"air_quality_{hour_label}_raw" in key:
                    data.air_quality_raw_key = key
                    logger.info(f"  LINK {town.name}: Air quality raw from MinIO -> {self._raw_store.key_name(key)}")
                    break

    # =====================================================
    # PHASE 3: TRANSFORM (Silver layer - filesystem + MongoDB)
    # =====================================================

    def _transform_all(self, extracted_data: List[ExtractedData]) -> List[ETLResult]:
        """Transform all raw data to silver layer (MongoDB only)."""
        logger.info("=" * 60)
        logger.info("PHASE 3: SILVER - Transforming all raw data")
        logger.info("=" * 60)

        results: List[ETLResult] = []

        for data in extracted_data:
            town = data.town

            # Transform weather (one MongoDB document per hourly raw file)
            for weather_key in data.weather_raw_keys:
                if not self._raw_store.exists(weather_key):
                    logger.warning(f"  WARN {town.name}: Weather raw missing: {weather_key}")
                    continue
                try:
                    raw_data = self._raw_store.get_json(weather_key)
                    cleaned = WeatherTransformer.transform(raw_data, city_name=town.name)
                    mongo_id = self._mongodb.insert_silver_weather(cleaned, town.name)
                    if mongo_id:
                        logger.info(
                            f"  OK {town.name}: Weather silver -> MongoDB {mongo_id} "
                            f"({self._raw_store.key_name(weather_key)})"
                        )
                    else:
                        logger.warning(f"  WARN {town.name}: Weather silver MongoDB insert failed")

                    results.append(ETLResult(
                        success=bool(mongo_id),
                        town=town.name,
                        api_source="openweather",
                        object_keys=[mongo_id] if mongo_id else [],
                    ))
                except Exception as e:
                    results.append(ETLResult(
                        success=False,
                        town=town.name,
                        api_source="openweather",
                        error=str(e),
                    ))
                    logger.error(f"  FAIL {town.name}: Weather transform failed: {e}")

            # Transform air quality
            if data.air_quality_raw_key and self._raw_store.exists(data.air_quality_raw_key):
                try:
                    raw_data = self._raw_store.get_json(data.air_quality_raw_key)
                    cleaned = AirQualityTransformer.transform(raw_data, city_name=town.name)
                    mongo_id = self._mongodb.insert_silver_air_quality(cleaned, town.name)
                    if mongo_id:
                        logger.info(f"  OK {town.name}: Air quality silver -> MongoDB {mongo_id}")
                    else:
                        logger.warning(f"  WARN {town.name}: Air quality silver MongoDB insert failed")

                    results.append(ETLResult(
                        success=bool(mongo_id),
                        town=town.name,
                        api_source="aqicn",
                        object_keys=[mongo_id] if mongo_id else [],
                    ))
                except Exception as e:
                    results.append(ETLResult(
                        success=False,
                        town=town.name,
                        api_source="aqicn",
                        error=str(e)
                    ))
                    logger.error(f"  FAIL {town.name}: Air quality transform failed: {e}")

        success_count = sum(1 for r in results if r.success)
        logger.info(f"Transform complete: {success_count}/{len(results)} operations successful")
        return results

    # =====================================================
    # PHASE 4: GOLD (MongoDB only)
    # =====================================================

    def _run_gold(self, reference_hour: datetime) -> List[ETLResult]:
        """Run gold aggregation and persist to MongoDB."""
        logger.info("=" * 60)
        logger.info("PHASE 4: GOLD - Aggregating silver data")
        logger.info("=" * 60)

        results: List[ETLResult] = []

        try:
            aggregation_date = paris_date_str(reference_hour)
            gold = GoldPipeline(
                mongodb=self._mongodb,
                aggregation_date_paris=aggregation_date,
            )
            gold.run()
            logger.info("OK Gold aggregation completed (MongoDB)")
            stats = self._mongodb.get_stats()
            gold_count = (
                stats.get("gold_weather_daily", 0)
                + stats.get("gold_air_quality_daily", 0)
                + stats.get("gold_daily", 0)
            )
            for town in self._towns:
                results.append(ETLResult(
                    success=gold_count > 0,
                    town=town.name,
                    api_source="gold_aggregation",
                ))
        except Exception as e:
            logger.error(f"FAIL Gold aggregation failed: {e}")
            for town in self._towns:
                results.append(ETLResult(
                    success=False,
                    town=town.name,
                    api_source="gold_aggregation",
                    error=str(e),
                ))

        success_count = sum(1 for r in results if r.success)
        logger.info(f"Gold complete: {success_count}/{len(results)} towns successful")
        return results

    # =====================================================
    # MAIN EXECUTION
    # =====================================================

    def run_hourly(self, hours_back: int = 1) -> Dict[str, Any]:
        """Run the complete hourly ETL pipeline.

        Args:
            hours_back: Number of past hours to extract for weather

        Returns:
            Dict with summary of operations
        """
        reference_hour = get_reference_hour_paris()
        logger.info("=" * 70)
        logger.info(
            f"Starting hourly ETL — reference Paris hour: {reference_hour.isoformat()}"
        )
        logger.info("Flow: Extract -> RAW (MinIO) -> Transform -> Silver (MongoDB) -> Aggregate -> Gold (MongoDB)")
        logger.info("=" * 70)

        # Phase 1: Extract all cities
        extracted_data = self._extract_all_cities(reference_hour)

        # Phase 2: Save raw (Bronze - filesystem only)
        self._save_all_raw(extracted_data, reference_hour)

        # Resolve MinIO keys so silver/gold run even when this hour's save returned no paths
        self._link_raw_keys_from_storage(extracted_data, reference_hour)

        # Phase 3: Transform (Silver - filesystem + MongoDB)
        silver_results = self._transform_all(extracted_data)

        # Phase 4: Gold (filesystem via GoldPipeline.run() + MongoDB)
        gold_results = self._run_gold(reference_hour)

        # Compile summary
        all_results = silver_results + gold_results
        successful = sum(1 for r in all_results if r.success)
        total_files = sum(len(r.object_keys) for r in all_results if r.success)

        summary = {
            "timestamp": reference_hour.isoformat(),
            "total_towns": len(self._towns),
            "total_operations": len(all_results),
            "successful_operations": successful,
            "failed_operations": len(all_results) - successful,
            "success_rate": successful / len(all_results) if all_results else 0,
            "total_files_saved": total_files,
            "api_breakdown": self._breakdown_by_api(all_results),
        }

        # Add MongoDB stats if available
        mongodb_stats = self._mongodb.get_stats()
        if mongodb_stats:
            summary['mongodb_stats'] = mongodb_stats

        logger.info("=" * 60)
        logger.info("ETL COMPLETE")
        logger.info(f"  Success rate: {summary['success_rate']:.1%}")
        logger.info(f"  Files saved: {total_files}")
        if mongodb_stats:
            logger.info(f"  MongoDB documents: {sum(mongodb_stats.values())}")
        logger.info("=" * 60)

        return summary

    def _breakdown_by_api(self, results: List[ETLResult]) -> Dict[str, Dict[str, int]]:
        """Break down results by API source."""
        breakdown: Dict[str, Dict[str, int]] = {}

        for result in results:
            api = result.api_source
            if api not in breakdown:
                breakdown[api] = {"success": 0, "failed": 0, "files": 0}

            if result.success:
                breakdown[api]["success"] += 1
                breakdown[api]["files"] += len(result.object_keys)
            else:
                breakdown[api]["failed"] += 1

        return breakdown


# =====================================================
# PUBLIC API
# =====================================================

def run_hourly_etl_job(hours_back: int = 1) -> Dict[str, Any]:
    """Run the hourly ETL job (public API for scheduler).

    Args:
        hours_back: Number of past hours to extract for weather

    Returns:
        Dict with summary of operations
    """
    with WeatherETLPipeline() as pipeline:
        return pipeline.run_hourly(hours_back)


if __name__ == "__main__":
    summary = run_hourly_etl_job()
    print(json.dumps(summary, indent=2, default=str))