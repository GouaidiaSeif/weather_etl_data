"""Scheduler for running the ETL pipeline every hour.

Data is stored in:
  - MinIO: Bronze (raw) layer only
  - MongoDB: Silver (cleaned) and Gold (aggregated) layers
"""

import sys
import signal
import logging
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

sys.path.insert(0, str(Path(__file__).parent))

from transformations.pipline_final import run_hourly_etl_job
from config.settings import get_settings

scheduler = BlockingScheduler()


def setup_scheduler_logging(log_dir: Path) -> logging.Logger:
    """Setup logging to both console and file."""
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG)

    console_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    try:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"scheduler_{timestamp}.log"

        file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        root_logger.info(f"Scheduler logging to file: {log_file}")

    except Exception as e:
        root_logger.error(f"Failed to setup file logging at {log_dir}: {e}")
        root_logger.warning("Continuing with console logging only")

    return logging.getLogger(__name__)


def job_listener(event):
    """Listen for job events and log results."""
    logger = logging.getLogger(__name__)
    if event.exception:
        logger.error(f"ETL job failed with exception: {event.exception}")
    else:
        logger.info("ETL job completed successfully")


def run_scheduled_etl():
    """Run the hourly ETL job."""
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info(f"Starting hourly ETL job at {datetime.now(timezone.utc).isoformat()}")
    logger.info("Flow: Extract -> RAW (MinIO) -> Silver (MongoDB) -> Gold (MongoDB)")
    logger.info("=" * 70)

    try:
        summary = run_hourly_etl_job(hours_back=1)

        logger.info("-" * 70)
        logger.info("Hourly ETL Summary:")
        logger.info(f"  Timestamp: {summary['timestamp']}")
        logger.info(f"  Towns processed: {summary['total_towns']}")
        logger.info(f"  Operations: {summary['successful_operations']}/{summary['total_operations']} successful")
        logger.info(f"  Success rate: {summary['success_rate']:.1%}")

        if "mongodb_stats" in summary:
            logger.info("-" * 70)
            logger.info("MongoDB collections:")
            for collection, count in summary["mongodb_stats"].items():
                logger.info(f"  {collection}: {count} documents")

        logger.info("-" * 70)
        logger.info("Data layers:")
        logger.info("  Bronze (RAW):     MinIO bucket -> raw/city=.../*_raw.json")
        logger.info("  Silver (Clean):   MongoDB -> silver_weather, silver_air_quality")
        logger.info("  Gold (Analytics): MongoDB -> gold_weather_daily, gold_air_quality_daily, gold_daily")
        logger.info("-" * 70)

    except Exception as e:
        logger.error(f"Hourly ETL job failed: {e}", exc_info=True)
        raise


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger = logging.getLogger(__name__)
    logger.info(f"Received signal {signum}, shutting down scheduler...")
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
    sys.exit(0)


def main():
    """Main entry point for the scheduler."""
    settings = get_settings()
    logger = setup_scheduler_logging(settings.log_path)

    logger.info("=" * 70)
    logger.info("Weather ETL Scheduler")
    logger.info(f"Log directory: {settings.log_path}")
    logger.info("Storage: MinIO (raw) + MongoDB (silver, gold)")
    logger.info("=" * 70)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    scheduler.add_job(
        run_scheduled_etl,
        trigger=CronTrigger(minute=5, timezone="Europe/Paris"),
        id="hourly_etl_complete",
        name="Hourly Weather & Air Quality ETL",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )

    logger.info("Running initial ETL job on startup...")
    run_scheduled_etl()

    logger.info("Scheduler started. ETL runs at :05 every hour (Europe/Paris).")
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        signal_handler(signal.SIGINT, None)


if __name__ == "__main__":
    main()
