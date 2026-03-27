"""
Main ETL pipeline for weather and air quality data with Hive partitioning.

Architecture:
    RAW    → data/raw/      (original API responses - filesystem only)
    SILVER → data/silver/   (cleaned & standardized datasets - filesystem + MongoDB)
    GOLD   → data/gold/     (daily aggregated analytics - filesystem + MongoDB)

v3 IMPROVEMENTS:
- Extract-First Architecture: All cities extracted before any processing
- Updated transformers handle actual API data structure correctly
- Hour extraction from _storage.hour_timestamp
- Comprehensive field coverage
- MongoDB integration for Silver and Gold layers
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
from storage.mongodb_storage import MongoDBStorage
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
    """ETL pipeline with extract-first architecture and MongoDB integration."""

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
    # PHASE 2: SAVE RAW (Bronze layer - filesystem only)
    # =====================================================

    def _save_all_raw(self, extracted_data: List[ExtractedData], reference_hour: datetime) -> None:
        """Save all extracted data to raw storage (Bronze layer - filesystem only)."""
        logger.info("=" * 60)
        logger.info("PHASE 2: BRONZE - Saving all raw data (filesystem only)")
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
                        data.weather_raw_path = paths[0]
                        logger.info(f"  ✓ {town.name}: Saved weather raw → {paths[0].name}")
                except Exception as e:
                    data.weather_error = str(e)
                    logger.error(f"  ✗ {town.name}: Failed to save weather raw: {e}")

            # Save air quality raw (filesystem only - NOT MongoDB)
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
    # PHASE 3: TRANSFORM (Silver layer - filesystem + MongoDB)
    # =====================================================

    def _transform_all(self, extracted_data: List[ExtractedData]) -> List[ETLResult]:
        """Transform all raw data to silver layer (filesystem + MongoDB)."""
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

                    # Insert to MongoDB (Silver layer)
                    mongo_id = self._mongodb.insert_silver_weather(cleaned, town.name)
                    if mongo_id:
                        logger.info(f"  ✓ {town.name}: Weather transformed → {silver_path.name}, MongoDB → {mongo_id}")
                    else:
                        logger.info(f"  ✓ {town.name}: Weather transformed → {silver_path.name}")

                    results.append(ETLResult(
                        success=True,
                        town=town.name,
                        api_source="openweather",
                        filepaths=[silver_path]
                    ))
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

                    # Insert to MongoDB (Silver layer)
                    mongo_id = self._mongodb.insert_silver_air_quality(cleaned, town.name)
                    if mongo_id:
                        logger.info(f"  ✓ {town.name}: Air quality transformed → {silver_path.name}, MongoDB → {mongo_id}")
                    else:
                        logger.info(f"  ✓ {town.name}: Air quality transformed → {silver_path.name}")

                    results.append(ETLResult(
                        success=True,
                        town=town.name,
                        api_source="aqicn",
                        filepaths=[silver_path]
                    ))
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

    def _save_silver(self, raw_path: Path, cleaned_data: Dict[str, Any]) -> Path:
        """Save cleaned data to silver layer."""
        relative = raw_path.relative_to(self._raw_base)

        silver_filename = raw_path.stem.replace("_raw", "_cleaned") + ".json"
        silver_path = self._silver_base / relative.parent / silver_filename
        silver_path.parent.mkdir(parents=True, exist_ok=True)

        with open(silver_path, 'w') as f:
            json.dump(cleaned_data, f, indent=2, default=str)

        return silver_path

    # =====================================================
    # PHASE 4: GOLD (filesystem via GoldPipeline.run() + MongoDB)
    # =====================================================

    def _run_gold(self) -> List[ETLResult]:
        """Run gold aggregation using GoldPipeline.run(), then persist to MongoDB."""
        logger.info("=" * 60)
        logger.info("PHASE 4: GOLD - Aggregating silver data")
        logger.info("=" * 60)

        results: List[ETLResult] = []
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Run the gold pipeline (writes all files to filesystem)
        try:
            gold = GoldPipeline(
                silver_base_path=self._silver_base,
                gold_base_path=self._gold_base,
            )
            gold.run()
            logger.info("✓ Gold aggregation completed (filesystem)")
        except Exception as e:
            logger.error(f"✗ Gold aggregation failed: {e}")
            for town in self._towns:
                results.append(ETLResult(
                    success=False,
                    town=town.name,
                    api_source="gold_aggregation",
                    error=str(e)
                ))
            return results

        # After gold files are written, insert into MongoDB per town
        for town in self._towns:
            gold_paths = []
            try:
                # Collect the gold files written by GoldPipeline.run() for this town
                for subdir in ["weather_daily", "air_quality_daily", "combined_daily"]:
                    p = self._gold_base / subdir / f"city={town.name.lower()}" / f"{date_str}.json"
                    if p.exists():
                        gold_paths.append(p)

                # Read combined file for MongoDB insertion (if it exists)
                combined_path = (
                    self._gold_base / "combined_daily"
                    / f"city={town.name.lower()}" / f"{date_str}.json"
                )
                if combined_path.exists():
                    with open(combined_path) as f:
                        combined_data = json.load(f)
                    mongo_id = self._mongodb.insert_gold_daily(combined_data, town.name, date_str)
                    if mongo_id:
                        logger.info(f"  ✓ {town.name}: Gold MongoDB → {mongo_id}")
                else:
                    logger.warning(f"  ⚠ {town.name}: No combined gold file found at {combined_path}")

                results.append(ETLResult(
                    success=True,
                    town=town.name,
                    api_source="gold_aggregation",
                    filepaths=gold_paths
                ))

            except Exception as e:
                logger.error(f"  ✗ {town.name}: Gold MongoDB insert failed: {e}")
                results.append(ETLResult(
                    success=False,
                    town=town.name,
                    api_source="gold_aggregation",
                    error=str(e)
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
        reference_hour = datetime.now(timezone.utc)
        logger.info("=" * 70)
        logger.info(f"Starting hourly ETL at {reference_hour.isoformat()}")
        logger.info("Flow: Extract → RAW (Bronze/filesystem) → Transform → Silver (fs+MongoDB) → Aggregate → Gold (fs+MongoDB)")
        logger.info("=" * 70)

        # Phase 1: Extract all cities
        extracted_data = self._extract_all_cities(reference_hour)

        # Phase 2: Save raw (Bronze - filesystem only)
        self._save_all_raw(extracted_data, reference_hour)

        # Phase 3: Transform (Silver - filesystem + MongoDB)
        silver_results = self._transform_all(extracted_data)

        # Phase 4: Gold (filesystem via GoldPipeline.run() + MongoDB)
        gold_results = self._run_gold()

        # Compile summary
        all_results = silver_results + gold_results
        successful = sum(1 for r in all_results if r.success)
        total_files = sum(len(r.filepaths) for r in all_results if r.success)

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
                breakdown[api]["files"] += len(result.filepaths)
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