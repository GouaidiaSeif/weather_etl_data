
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from alerts.discord import send_discord_alert
from alerts.models import AlertEvent
from config.settings import get_settings
from storage.data_store import create_raw_store
from storage.hive_storage import HivePartitionedStorage
from storage.mongodb_storage import MongoDBStorage
from transformations.pipline_final import run_hourly_etl_job

PARIS_TZ = pendulum.timezone("Europe/Paris")


def _flow_details() -> Dict[str, Any]:
    """Return structured pipeline flow details for trace/debug logs."""
    return {
        "phase_1_extract": "API calls: OpenWeather (weather) + AQICN (air quality) per city",
        "phase_2_bronze_minio": "Save RAW responses to MinIO bucket (raw/city=.../year=.../month=.../day=...)",
        "phase_3_silver_mongodb": "Transform raw payloads and write cleaned documents to MongoDB silver collections",
        "phase_4_gold_mongodb": "Aggregate daily analytics and write MongoDB gold collections",
        "alerts_discord": "AlertService emits immediate, digest, and ops notifications based on env webhooks",
    }


def _send_airflow_discord_notification(
    *,
    settings: Any,
    alert_key: str,
    title: str,
    message: str,
    severity: str = "info",
) -> bool:
    """Send a DAG-level Discord notification through existing webhook config."""
    webhook = (
        settings.discord_webhook_ops
        or settings.discord_webhook_immediate
        or settings.discord_webhook_url
    )
    event = AlertEvent(
        alert_key=alert_key,
        channel="ops",
        title=title,
        message=message,
        severity=severity,
        city=None,
        metadata={"source": "airflow_dag_notifications"},
    )
    return send_discord_alert(webhook, event)


@dag(
    dag_id="weather_etl_hourly_parity",
    schedule="5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=PARIS_TZ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "weather-etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["weather", "etl", "production", "parity"],
)
def weather_etl_hourly_parity() -> None:
    """Run the production hourly ETL job under Airflow orchestration."""

    @task(task_id="step_0_preflight_context")
    def step_0_preflight_context_task() -> Dict[str, str]:
        logger = logging.getLogger("airflow.task")
        settings = get_settings()
        context = get_current_context()
        dag_run = context.get("dag_run")
        payload = {
            "timezone": "Europe/Paris",
            "storage_backend": settings.storage_backend,
            "minio_endpoint": settings.minio_endpoint,
            "minio_bucket": settings.minio_bucket,
            "mongodb_host": settings.mongodb_host,
            "mongodb_port": str(settings.mongodb_port),
            "mongodb_database": settings.mongodb_database,
            "discord_webhook_enabled": str(bool(settings.discord_webhook_url)),
            "dag_id": str(context.get("dag").dag_id if context.get("dag") else "unknown"),
            "run_id": str(dag_run.run_id if dag_run else "unknown"),
            "logical_date": str(context.get("logical_date")),
            "started_at_utc": pendulum.now("UTC").isoformat(),
        }
        logger.info("Preflight context: %s", json.dumps(payload, indent=2))
        _send_airflow_discord_notification(
            settings=settings,
            alert_key=f"airflow:start:{payload['dag_id']}:{payload['run_id']}",
            title="Airflow ETL run started",
            message=(
                f"DAG `{payload['dag_id']}` started.\n"
                f"Run ID: `{payload['run_id']}`\n"
                f"Logical date: `{payload['logical_date']}`\n"
                f"Storage: MinIO bucket `{payload['minio_bucket']}` + MongoDB `{payload['mongodb_database']}`"
            ),
            severity="info",
        )
        return payload

    @task(task_id="step_1_extract_api_details")
    def step_1_extract_api_details_task(context: Dict[str, str]) -> None:
        logger = logging.getLogger("airflow.task")
        logger.info("Step 1 / Extract details")
        logger.info(
            "API calls in pipeline: OpenWeather hourly data + AQICN air quality data per city"
        )
        logger.info(
            "Each town is processed with a shared reference hour to keep weather/air alignment."
        )
        logger.info("Context snapshot: %s", json.dumps(context, indent=2))

    @task(task_id="step_2_bronze_minio_details")
    def step_2_bronze_minio_details_task() -> None:
        logger = logging.getLogger("airflow.task")
        logger.info("Step 2 / Bronze details")
        logger.info(
            "RAW API responses are saved in MinIO with hive-like paths: raw/city=.../year=.../month=.../day=..."
        )
        logger.info("This preserves original payloads for replay/debugging and silver backfill.")

    @task(task_id="step_3_silver_transform_details")
    def step_3_silver_transform_details_task() -> None:
        logger = logging.getLogger("airflow.task")
        logger.info("Step 3 / Silver details")
        logger.info("Raw weather files are transformed and inserted into MongoDB silver_weather.")
        logger.info(
            "Raw air-quality files are transformed and inserted into MongoDB silver_air_quality."
        )

    @task(task_id="step_4_gold_aggregation_details")
    def step_4_gold_aggregation_details_task() -> None:
        logger = logging.getLogger("airflow.task")
        logger.info("Step 4 / Gold details")
        logger.info(
            "Daily analytics are computed and inserted into MongoDB: gold_weather_daily, gold_air_quality_daily, gold_daily."
        )
        logger.info(
            "AlertService notifications are emitted by pipeline internals using existing Discord webhook env vars."
        )

    @task(task_id="step_5_run_hourly_pipeline")
    def step_5_run_hourly_pipeline_task() -> Dict[str, Any]:
        logger = logging.getLogger("airflow.task")
        details = _flow_details()

        logger.info("=" * 80)
        logger.info("AIRFLOW PRODUCTION PARITY RUN")
        logger.info("Schedule: minute 5 of each hour (Europe/Paris), catchup disabled")
        logger.info("Flow details: %s", json.dumps(details, indent=2))
        logger.info("Entrypoint: transformations.pipline_final.run_hourly_etl_job(hours_back=1)")
        logger.info("=" * 80)

        summary = run_hourly_etl_job(hours_back=1)

        logger.info("ETL summary: %s", json.dumps(summary, indent=2, default=str))
        failed_operations = int(summary.get("failed_operations", 0))
        if failed_operations > 0:
            raise AirflowException(
                f"ETL reported {failed_operations} failed operation(s). "
                "Failing task to trigger DAG retry."
            )
        logger.info("Pipeline + Discord alerts execution completed")
        return summary

    @task(task_id="step_6_validate_summary")
    def step_6_validate_summary_task(summary: Dict[str, Any]) -> Dict[str, Any]:
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
            raise ValueError(f"Summary is missing required keys: {missing}")
        return summary

    @task(task_id="step_7_log_run_diagnostics")
    def step_7_log_run_diagnostics_task(summary: Dict[str, Any]) -> None:
        logger = logging.getLogger("airflow.task")
        logger.info("-" * 80)
        logger.info("Run diagnostics")
        logger.info("Timestamp: %s", summary.get("timestamp"))
        logger.info(
            "Operations: %s/%s success",
            summary.get("successful_operations"),
            summary.get("total_operations"),
        )
        logger.info("Success rate: %.2f", float(summary.get("success_rate", 0.0)))
        logger.info("API breakdown: %s", json.dumps(summary.get("api_breakdown", {}), indent=2))
        logger.info("MongoDB stats: %s", json.dumps(summary.get("mongodb_stats", {}), indent=2))
        logger.info("-" * 80)

    @task(task_id="step_8_postrun_storage_probe")
    def step_8_postrun_storage_probe_task() -> None:
        logger = logging.getLogger("airflow.task")
        settings = get_settings()

        # Check MongoDB availability and counts.
        mongodb = MongoDBStorage(settings)
        try:
            if mongodb.connect():
                logger.info("MongoDB probe OK")
                logger.info(
                    "MongoDB collection counts: %s",
                    json.dumps(mongodb.get_stats(), indent=2),
                )
            else:
                logger.warning("MongoDB probe failed")
        finally:
            mongodb.close()

        # Check MinIO/raw availability for current hour.
        raw_store = create_raw_store(settings)
        storage = HivePartitionedStorage(raw_store, prefix="raw")
        reference = pendulum.now("UTC").replace(minute=0, second=0, microsecond=0)
        logger.info("MinIO probe reference hour (UTC): %s", reference.isoformat())
        logger.info(
            "Sample probe key format: %s",
            storage.hourly_object_key("paris", reference, "openweather"),
        )

    @task(task_id="step_9_notify_discord_run_summary")
    def step_9_notify_discord_run_summary_task(
        summary: Dict[str, Any],
        context_payload: Dict[str, str],
    ) -> None:
        """Send parsed execution summary to Discord after DAG completion."""
        logger = logging.getLogger("airflow.task")
        settings = get_settings()
        success = int(summary.get("successful_operations", 0))
        total = int(summary.get("total_operations", 0))
        failed = int(summary.get("failed_operations", max(total - success, 0)))
        success_rate = float(summary.get("success_rate", 0.0))
        mongo_stats = summary.get("mongodb_stats", {})
        elapsed_seconds = (
            pendulum.now("UTC") - pendulum.parse(context_payload["started_at_utc"])
        ).total_seconds()

        parsed_payload = {
            "dag_id": context_payload.get("dag_id"),
            "run_id": context_payload.get("run_id"),
            "timestamp": summary.get("timestamp"),
            "operations": {"success": success, "failed": failed, "total": total},
            "success_rate": round(success_rate, 4),
            "total_towns": summary.get("total_towns"),
            "api_breakdown": summary.get("api_breakdown", {}),
            "mongodb_stats": mongo_stats,
            "elapsed_seconds": round(float(elapsed_seconds), 2),
        }
        logger.info("Parsed Airflow execution payload: %s", json.dumps(parsed_payload, indent=2))

        severity = "critical" if failed > 0 else "info"
        sent = _send_airflow_discord_notification(
            settings=settings,
            alert_key=f"airflow:end:{context_payload.get('dag_id')}:{context_payload.get('run_id')}",
            title="Airflow ETL run finished",
            message=(
                f"DAG `{context_payload.get('dag_id')}` finished.\n"
                f"Run ID: `{context_payload.get('run_id')}`\n"
                f"Ops: {success}/{total} success (failed: {failed})\n"
                f"Success rate: {success_rate:.1%}\n"
                f"Elapsed: {round(float(elapsed_seconds), 2)}s\n"
                f"MongoDB stats: {json.dumps(mongo_stats)}"
            ),
            severity=severity,
        )
        logger.info("Discord run summary notification sent=%s", sent)

    preflight = step_0_preflight_context_task()
    extract = step_1_extract_api_details_task(preflight)
    bronze = step_2_bronze_minio_details_task()
    silver = step_3_silver_transform_details_task()
    gold = step_4_gold_aggregation_details_task()
    run = step_5_run_hourly_pipeline_task()
    validated = step_6_validate_summary_task(run)
    diagnostics = step_7_log_run_diagnostics_task(validated)
    probe = step_8_postrun_storage_probe_task()
    notify = step_9_notify_discord_run_summary_task(validated, preflight)

    preflight >> extract >> bronze >> silver >> gold >> run >> validated >> diagnostics >> probe >> notify


weather_etl_hourly_parity()
