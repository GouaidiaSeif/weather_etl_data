"""
Usage:
    python fetch_data.py                    # Fetch both weather and air quality for all towns
    python fetch_data.py --weather          # Fetch weather only
    python fetch_data.py --air-quality      # Fetch air quality only
    python fetch_data.py --town paris       # Fetch for specific town
    python fetch_data.py --hours 3          # Extract past 3 hours of weather
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from etl.pipeline import run_hourly_etl_job
from config.towns import FRENCH_TOWNS
from utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch weather and air quality data with Hive partitioning"
    )
    
    # Data type selection
    data_group = parser.add_mutually_exclusive_group()
    data_group.add_argument(
        "--weather",
        action="store_true",
        help="Fetch weather data only"
    )
    data_group.add_argument(
        "--air-quality",
        action="store_true",
        help="Fetch air quality data only"
    )
    
    # Town selection
    parser.add_argument(
        "--town",
        type=str,
        help="Specific town to fetch (default: all towns)"
    )
    
    # Weather options
    parser.add_argument(
        "--hours",
        type=int,
        default=1,
        help="Number of past hours to extract for weather (default: 1)"
    )
    
    # Other options
    parser.add_argument(
        "--list-towns",
        action="store_true",
        help="List available towns and exit"
    )
    
    args = parser.parse_args()
    
    if args.list_towns:
        print("Available towns:")
        for town in FRENCH_TOWNS:
            print(f"  - {town.name} ({town.lat}, {town.lon})")
        return 0
    
    # Filter towns if specified
    if args.town:
        towns = [t for t in FRENCH_TOWNS if t.name.lower() == args.town.lower()]
        if not towns:
            logger.error(f"Town '{args.town}' not found. Available towns:")
            for t in FRENCH_TOWNS:
                logger.error(f"  - {t.name}")
            return 1
    else:
        towns = None
    
    # Determine which data to fetch 
    fetch_weather = not args.air_quality  # Default True unless --air-quality specified
    fetch_air_quality = not args.weather   # Default True unless --weather specified
    
    logger.info("=" * 60)
    logger.info(f"Starting ETL job at {datetime.utcnow().isoformat()}")
    if fetch_weather and fetch_air_quality:
        logger.info("Mode: Both Weather and Air Quality")
    elif fetch_weather:
        logger.info("Mode: Weather only")
    else:
        logger.info("Mode: Air Quality only")
    
    if towns:
        logger.info(f"Towns: {[t.name for t in towns]}")
    else:
        logger.info(f"Towns: all ({len(FRENCH_TOWNS)} towns)")
    logger.info("=" * 60)
    
    try:
        
        summary = run_hourly_etl_job(towns=towns, hours_back=args.hours)
        
        print("\n" + "=" * 60)
        print("ETL Job Summary")
        print("=" * 60)
        print(f"Timestamp: {summary['timestamp']}")
        print(f"Towns processed: {summary['total_towns']}")
        print(f"Operations: {summary['successful_operations']}/{summary['total_operations']} successful")
        print(f"Success rate: {summary['success_rate']:.1%}")
        print(f"Total files saved: {summary['total_files_saved']}")
        
        print("\nAPI Breakdown:")
        for api_name, stats in summary['api_breakdown'].items():
            print(f"  {api_name}:")
            print(f"    Success: {stats['success']}")
            print(f"    Failed: {stats['failed']}")
            print(f"    Files: {stats.get('files', 0)}")
        
        print("=" * 60)
        
        return 0 if summary['success_rate'] > 0 else 1
        
    except Exception as e:
        logger.error(f"ETL job failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
