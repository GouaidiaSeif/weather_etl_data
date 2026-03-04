#!/usr/bin/env python3
"""Test script to verify the ETL pipeline setup.

This script tests:
1. Environment variables are set
2. API keys are valid
3. Data directories are writable
4. ETL pipeline can be initialized
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import Settings, get_settings
from config.towns import FRENCH_TOWNS
from clients.openweather_client import OpenWeatherClient
from clients.aqicn_client import AQICNClient
from storage.hive_storage import HivePartitionedStorage
from utils.logger import get_logger

logger = get_logger(__name__)


def test_environment_variables():
    """Test that required environment variables are set."""
    logger.info("Testing environment variables...")
    
    try:
        settings = get_settings()
        logger.info(f"  OPENWEATHER_API_KEY: {'*' * 10}{settings.openweather_api_key[-4:]}")
        logger.info(f"  AQICN_API_KEY: {'*' * 10}{settings.aqicn_api_key[-4:]}")
        logger.info("  Environment variables: OK")
        return True
    except ValueError as e:
        logger.error(f"  Environment variables: FAILED - {e}")
        return False


def test_data_directories():
    """Test that data directories are writable."""
    logger.info("Testing data directories...")
    
    try:
        settings = get_settings()
        
        # Test raw directory
        settings.data_raw_path.mkdir(parents=True, exist_ok=True)
        test_file = settings.data_raw_path / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
        logger.info(f"  Raw directory ({settings.data_raw_path}): OK")
        
        # Test processed directory
        settings.data_processed_path.mkdir(parents=True, exist_ok=True)
        test_file = settings.data_processed_path / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
        logger.info(f"  Processed directory ({settings.data_processed_path}): OK")
        
        return True
    except Exception as e:
        logger.error(f"  Data directories: FAILED - {e}")
        return False


def test_openweather_api():
    """Test OpenWeatherMap API connection."""
    logger.info("Testing OpenWeatherMap API...")
    
    try:
        settings = get_settings()
        client = OpenWeatherClient(
            api_key=settings.openweather_api_key,
            timeout=settings.request_timeout,
            max_retries=1
        )
        
        # Test with Paris
        paris = FRENCH_TOWNS[0]
        data = client.fetch_hourly_data(paris)
        
        if "hourly" in data:
            hourly_count = len(data["hourly"])
            logger.info(f"  Fetched {hourly_count} hourly records for {paris.name}")
            logger.info("  OpenWeatherMap API: OK")
            client.close()
            return True
        else:
            logger.error("  OpenWeatherMap API: FAILED - No hourly data in response")
            client.close()
            return False
            
    except Exception as e:
        logger.error(f"  OpenWeatherMap API: FAILED - {e}")
        return False


def test_aqicn_api():
    """Test AQICN API connection."""
    logger.info("Testing AQICN API...")
    
    try:
        settings = get_settings()
        client = AQICNClient(
            api_key=settings.aqicn_api_key,
            timeout=settings.request_timeout,
            max_retries=1
        )
        
        # Test with Paris
        paris = FRENCH_TOWNS[0]
        data = client.fetch_by_coordinates(paris)
        
        if data.get("status") == "ok":
            aqi = data.get("data", {}).get("aqi", "N/A")
            logger.info(f"  Fetched AQI {aqi} for {paris.name}")
            logger.info("  AQICN API: OK")
            client.close()
            return True
        else:
            logger.error(f"  AQICN API: FAILED - Status: {data.get('status')}")
            client.close()
            return False
            
    except Exception as e:
        logger.error(f"  AQICN API: FAILED - {e}")
        return False


def test_storage():
    """Test storage backend."""
    logger.info("Testing storage backend...")
    
    try:
        settings = get_settings()
        storage = HivePartitionedStorage(settings.data_raw_path)
        
        # Test saving weather data (using the unified save_hourly_data method)
        from datetime import datetime, timezone
        test_data = {"test": "data", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        filepath = storage.save_hourly_data(
            data=test_data,
            api_source="openweather",
            city_name="test_city"
        )
        logger.info(f"  Saved test weather data to {filepath}")
        
        # Test saving air quality data (using the unified save_hourly_data method)
        filepath = storage.save_hourly_data(
            data=test_data,
            api_source="aqicn",
            city_name="test_city"
        )
        logger.info(f"  Saved test air quality data to {filepath}")
        
        # Clean up
        import shutil
        test_partition = settings.data_raw_path / "city=test_city"
        if test_partition.exists():
            shutil.rmtree(test_partition)
        
        logger.info("  Storage backend: OK")
        return True
        
    except Exception as e:
        logger.error(f"  Storage backend: FAILED - {e}")
        return False


def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("Weather ETL Pipeline Setup Test")
    logger.info("=" * 60)
    
    tests = [
        ("Environment Variables", test_environment_variables),
        ("Data Directories", test_data_directories),
        ("OpenWeatherMap API", test_openweather_api),
        ("AQICN API", test_aqicn_api),
        ("Storage Backend", test_storage),
    ]
    
    results = []
    for name, test_func in tests:
        logger.info("")
        result = test_func()
        results.append((name, result))
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("Test Summary")
    logger.info("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "PASS" if result else "FAIL"
        logger.info(f"  {name}: {status}")
    
    logger.info("")
    logger.info(f"Total: {passed}/{total} tests passed")
    logger.info("=" * 60)
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
