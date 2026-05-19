#!/usr/bin/env python3
"""Load silver + gold in MongoDB from raw files already stored in MinIO/local raw.

Use when bronze (raw) is up to date but MongoDB collections were not updated.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import get_settings
from config.towns import FRENCH_TOWNS
from storage.data_store import create_raw_store
from storage.hive_storage import HivePartitionedStorage
from storage.mongodb_storage import MongoDBStorage
from transformations.improved_weather_transformer import WeatherTransformer
from transformations.improved_air_quality_transformer import AirQualityTransformer
from transformations.improved_gold_pipeline import GoldPipeline
from utils.logger import get_logger
from utils.timezone_utils import get_reference_hour_paris

logger = get_logger(__name__)


def main() -> None:
    settings = get_settings()
    raw_store = create_raw_store(settings)
    raw_store.ensure_ready()
    hive = HivePartitionedStorage(raw_store, prefix="raw")
    ref = get_reference_hour_paris()

    mongodb = MongoDBStorage(settings)
    if not mongodb.connect():
        raise SystemExit("Could not connect to MongoDB")

    try:
        for town in FRENCH_TOWNS:
            weather_keys = hive.list_raw_keys_for_city_day(town.name, ref, "openweather")
            for key in weather_keys:
                try:
                    raw = raw_store.get_json(key)
                    cleaned = WeatherTransformer.transform(raw, city_name=town.name)
                    doc_id = mongodb.insert_silver_weather(cleaned, town.name)
                    logger.info(f"{town.name} weather silver <- {raw_store.key_name(key)}: {doc_id}")
                except Exception as e:
                    logger.error(f"{town.name} weather {key}: {e}")

            aq_keys = hive.list_raw_keys_for_city_day(town.name, ref, "aqicn")
            for key in aq_keys:
                try:
                    raw = raw_store.get_json(key)
                    cleaned = AirQualityTransformer.transform(raw, city_name=town.name)
                    doc_id = mongodb.insert_silver_air_quality(cleaned, town.name)
                    logger.info(f"{town.name} air quality silver <- {raw_store.key_name(key)}: {doc_id}")
                except Exception as e:
                    logger.error(f"{town.name} air quality {key}: {e}")

        logger.info("Running gold aggregation (all silver for today in Paris)...")
        GoldPipeline(mongodb=mongodb, aggregation_date_paris=None).run()
        logger.info("MongoDB stats: %s", mongodb.get_stats())
    finally:
        mongodb.close()


if __name__ == "__main__":
    main()
