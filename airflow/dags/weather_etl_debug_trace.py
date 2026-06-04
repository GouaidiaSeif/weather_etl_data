
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable

from config.settings import get_settings
from storage.mongodb_storage import MongoDBStorage
from storage.data_store import create_raw_store
from storage.hive_storage import HivePartitionedStorage
from transformations.pipline_final import run_hourly_etl_job
from config.towns import FRENCH_TOWNS

PARIS_TZ = pendulum.timezone("Europe/Paris")


@dag(
    dag_id="weather_etl_debug_trace",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=PARIS_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "weather-etl", "retries": 0},
    tags=["weather", "etl", "debug"],
)
def weather_etl_debug_trace() -> None:
    """Manual DAG for deep diagnostics while keeping business logic identical."""

    @task(task_id="debug_context")
    def debug_context_task() -> Dict[str, str]:
        logger = logging.getLogger("airflow.task")
        settings = get_settings()

        context = {
            "timezone": "Europe/Paris",
            "storage_backend": settings.storage_backend,
            "minio_endpoint": settings.minio_endpoint,
            "minio_bucket": settings.minio_bucket,
            "mongodb_host": settings.mongodb_host,
            "mongodb_port": str(settings.mongodb_port),
            "mongodb_database": settings.mongodb_database,
            "local_timezone_setting": settings.local_timezone,
            "discord_enabled": str(bool(settings.discord_webhook_url)),
            "towns_count": str(len(FRENCH_TOWNS)),
        }
        logger.info("Debug context: %s", json.dumps(context, indent=2))
        logger.info(
            "Phase details: extract APIs -> save RAW MinIO -> silver transform MongoDB -> gold aggregate MongoDB -> alerts"
        )
        return context

    @task(task_id="run_pipeline_debug")
    def run_pipeline_debug_task() -> Dict[str, Any]:
        logger = logging.getLogger("airflow.task")
        summary = run_hourly_etl_job(hours_back=1)
        logger.info("Full ETL summary: %s", json.dumps(summary, indent=2, default=str))
        return summary

    @task(task_id="validate_summary_schema")
    def validate_summary_schema_task(summary: Dict[str, Any]) -> Dict[str, Any]:
        required = [
            "timestamp",
            "total_towns",
            "total_operations",
            "successful_operations",
            "failed_operations",
            "success_rate",
            "api_breakdown",
        ]
        missing = [key for key in required if key not in summary]
        if missing:
            raise AirflowException(f"Summary schema mismatch. Missing keys: {missing}")
        return summary

    @task(task_id="validate_success_threshold")
    def validate_success_threshold_task(summary: Dict[str, Any]) -> Dict[str, Any]:
        logger = logging.getLogger("airflow.task")
        threshold = float(Variable.get("weather_etl_debug_min_success_rate", default_var="0.80"))
        success_rate = float(summary.get("success_rate", 0.0))
        logger.info("Success rate %.3f vs threshold %.3f", success_rate, threshold)
        if success_rate < threshold:
            raise AirflowException(
                f"Success rate {success_rate:.3f} is below debug threshold {threshold:.3f}"
            )
        return summary

    @task(task_id="snapshot_mongodb_counts")
    def snapshot_mongodb_counts_task() -> Dict[str, int]:
        logger = logging.getLogger("airflow.task")
        settings = get_settings()
        mongodb = MongoDBStorage(settings)
        try:
            if not mongodb.connect():
                raise AirflowException("Failed to connect to MongoDB for debug snapshot")
            stats = mongodb.get_stats()
            logger.info("MongoDB collections snapshot: %s", json.dumps(stats, indent=2))
            return stats
        finally:
            mongodb.close()

    @task(task_id="snapshot_minio_raw_keys")
    def snapshot_minio_raw_keys_task() -> Dict[str, List[str]]:
        logger = logging.getLogger("airflow.task")
        settings = get_settings()
        raw_store = create_raw_store(settings)
        storage = HivePartitionedStorage(raw_store, prefix="raw")
        reference = pendulum.now("UTC").replace(minute=0, second=0, microsecond=0)

        samples: Dict[str, List[str]] = {}
        for town in FRENCH_TOWNS:
            weather_keys = storage.list_raw_keys_for_city_day(town.name, reference, "openweather")
            aq_keys = storage.list_raw_keys_for_city_day(town.name, reference, "aqicn")
            # aq_forcst_keys = storage.list_raw_keys_for_city_day(town.name, reference, "openweather_air_quality")
            sample = (weather_keys[:2] + aq_keys[:2])[:4]
            if sample:
                samples[town.name] = sample

        logger.info("MinIO RAW key samples: %s", json.dumps(samples, indent=2))
        return samples

    @task(task_id="emit_debug_report")
    def emit_debug_report_task(
        summary: Dict[str, Any],
        mongo_stats: Dict[str, int],
        minio_samples: Dict[str, List[str]],
    ) -> None:
        logger = logging.getLogger("airflow.task")
        report = {
            "generated_at": pendulum.now("UTC").isoformat(),
            "summary": summary,
            "mongodb_stats": mongo_stats,
            "minio_samples": minio_samples,
            "flow_notes": {
                "bronze": "RAW in MinIO only",
                "silver": "MongoDB collections: silver_weather, silver_air_quality",
                "gold": "MongoDB collections: gold_weather_daily, gold_air_quality_daily, gold_daily",
                "alerts": "Discord notifications driven by AlertService env webhooks",
            },
        }
        logger.info("DEBUG REPORT: %s", json.dumps(report, indent=2, default=str))

    context = debug_context_task()
    summary = run_pipeline_debug_task()
    validated = validate_summary_schema_task(summary)
    threshold_checked = validate_success_threshold_task(validated)
    mongo = snapshot_mongodb_counts_task()
    minio = snapshot_minio_raw_keys_task()
    context >> threshold_checked
    emit_debug_report_task(threshold_checked, mongo, minio)


weather_etl_debug_trace()
