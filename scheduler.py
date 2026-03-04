"""Scheduler for running the ETL pipeline every hour.

This script uses APScheduler to run the ETL job every hour:
- Weather data: Fetched, transformed to Silver, aggregated to Gold
- Air quality data: Fetched, transformed to Silver, aggregated to Gold

FIXED: Now uses transformations.pipline_final which includes:
  1. Extraction from APIs (OpenWeather + AQICN)
  2. RAW storage (Bronze layer)
  3. Transformation to Silver layer
  4. Aggregation to Gold layer

Logs are saved to both console and text files in the directory specified by settings.log_path.
"""

import sys
import signal
import logging
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

sys.path.insert(0, str(Path(__file__).parent))

# FIXED: Import from pipline_final which has the COMPLETE pipeline (extract + transform + gold)
from transformations.pipline_final import run_hourly_etl_job
from config.settings import get_settings

# Global scheduler instance
scheduler = BackgroundScheduler()


def setup_scheduler_logging(log_dir: Path) -> logging.Logger:
    """Setup logging to both console and file.
    
    Args:
        log_dir: Directory to save log files (from settings.log_path)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get the root logger and clear existing handlers to prevent duplicates
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG)
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler - INFO and above
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler - DEBUG and above
    try:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"scheduler_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
        
        # Log the file location
        root_logger.info(f"Scheduler logging to file: {log_file}")
        
    except Exception as e:
        root_logger.error(f"Failed to setup file logging at {log_dir}: {e}")
        root_logger.warning("Continuing with console logging only")
    
    # Get the specific logger for this module
    logger = logging.getLogger(__name__)
    return logger


def job_listener(event):
    """Listen for job events and log results."""
    logger = logging.getLogger(__name__)
    if event.exception:
        logger.error(f"ETL job failed with exception: {event.exception}")
    else:
        logger.info("ETL job completed successfully")


def run_scheduled_etl():
    """Run the hourly ETL job (extract + transform + gold aggregation).
    
    This function orchestrates the complete ETL flow:
    1. Extract weather and air quality data from APIs
    2. Save RAW data (Bronze layer)
    3. Transform to Silver layer (cleaned & standardized)
    4. Aggregate to Gold layer (daily analytics with KPIs)
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 70)
    logger.info(f"Starting hourly ETL job at {datetime.now(timezone.utc).isoformat()}")
    logger.info("Flow: Extract → RAW (Bronze) → Transform → Silver → Aggregate → Gold")
    logger.info("=" * 70)
    
    try:
        # This runs the COMPLETE pipeline from pipline_final.py:
        # - Extracts from both APIs for all towns
        # - Saves RAW data to data/raw/
        # - Transforms and saves SILVER data to data/silver/
        # - Aggregates and saves GOLD data to data/gold/
        summary = run_hourly_etl_job(hours_back=1)
        
        logger.info("-" * 70)
        logger.info("Hourly ETL Summary:")
        logger.info(f"  Timestamp: {summary['timestamp']}")
        logger.info(f"  Towns processed: {summary['total_towns']}")
        logger.info(f"  Operations: {summary['successful_operations']}/{summary['total_operations']} successful")
        logger.info(f"  Success rate: {summary['success_rate']:.1%}")
        logger.info(f"  Total files saved: {summary['total_files_saved']}")
        
        for api_name, stats in summary['api_breakdown'].items():
            logger.info(f"  {api_name}: {stats['success']} success, {stats['failed']} failed, {stats.get('files', 0)} files")
        
        # Log layer information
        logger.info("-" * 70)
        logger.info("Data Layers:")
        logger.info("  Bronze (RAW):     data/raw/city={name}/year=YYYY/month=MM/day=DD/*_raw.json")
        logger.info("  Silver (Clean):   data/silver/city={name}/year=YYYY/month=MM/day=DD/*_cleaned.json")
        logger.info("  Gold (Analytics): data/gold/weather_daily/city={name}/YYYY-MM-DD.json")
        logger.info("                    data/gold/air_quality_daily/city={name}/YYYY-MM-DD.json")
        logger.info("                    data/gold/combined_daily/city={name}/YYYY-MM-DD.json")
        logger.info("-" * 70)
        
    except Exception as e:
        logger.error(f"Hourly ETL job failed: {e}", exc_info=True)
        raise


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger = logging.getLogger(__name__)
    logger.info(f"Received signal {signum}, shutting down scheduler...")
    scheduler.shutdown(wait=True)
    logger.info("Scheduler stopped")
    sys.exit(0)


def main():
    """Main entry point for the scheduler."""
    # Get settings and setup logging first
    settings = get_settings()
    logger = setup_scheduler_logging(settings.log_path)
    
    logger.info("=" * 70)
    logger.info("Weather ETL Scheduler (Complete Pipeline)")
    logger.info(f"Log directory: {settings.log_path}")
    logger.info("=" * 70)
    logger.info("This scheduler runs every hour with FULL pipeline:")
    logger.info("  Phase 1: EXTRACTION")
    logger.info("    - Weather ETL: Fetch from OpenWeather API")
    logger.info("    - Air Quality ETL: Fetch from AQICN API")
    logger.info("  Phase 2: BRONZE LAYER (RAW)")
    logger.info("    - Save original API responses")
    logger.info("  Phase 3: SILVER LAYER (Transformed)")
    logger.info("    - Clean, validate, and standardize data")
    logger.info("  Phase 4: GOLD LAYER (Aggregated)")
    logger.info("    - Daily aggregations with KPIs")
    logger.info("    - Cross-domain analytics")
    logger.info("=" * 70)
    logger.info("Data is stored in hive-style partitions:")
    logger.info("  Weather:       city={name}/year={YYYY}/month={MM}/day={DD}/weather_{HH}_raw.json")
    logger.info("  Air Quality:   city={name}/year={YYYY}/month={MM}/day={DD}/air_quality_{HH}_raw.json")
    logger.info("=" * 70)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    
    # Hourly job for complete ETL pipeline
    scheduler.add_job(
        run_scheduled_etl,
        trigger=IntervalTrigger(hours=1),
        id='hourly_etl_complete',
        name='Hourly Weather & Air Quality ETL (Full Pipeline)',
        replace_existing=True,
        misfire_grace_time=1800  # 30 minutes grace time
    )
    
    logger.info("Running initial ETL job...")
    run_scheduled_etl()
    
    scheduler.start()
    logger.info("Scheduler started. ETL job will run every hour.")
    logger.info("Press Ctrl+C to stop.")
    
    try:
        while True:
            import time
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        signal_handler(signal.SIGINT, None)


if __name__ == "__main__":
    main()
