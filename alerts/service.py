"""Orchestrate alert evaluation, dedupe, and Discord dispatch."""

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from config.settings import Settings
from alerts.discord import send_discord_alert
from alerts.models import AlertEvent
from alerts.rules import (
    evaluate_etl_failures,
    evaluate_gold_digest,
    evaluate_silver_air_quality,
    evaluate_silver_weather,
)
from alerts.store import AlertStore
from utils.logger import get_logger

if TYPE_CHECKING:
    from storage.mongodb_storage import MongoDBStorage

logger = get_logger(__name__)


class AlertService:
    """Silver immediate alerts, gold daily digest, and ETL ops rollup via Discord."""

    def __init__(
        self,
        settings: Settings,
        mongodb: Optional["MongoDBStorage"] = None,
    ):
        self._settings = settings
        self._store = AlertStore(mongodb) if mongodb else None
        if self._store:
            self._store.ensure_indexes()

    @property
    def enabled(self) -> bool:
        return bool(
            self._settings.discord_webhook_immediate
            or self._settings.discord_webhook_digest
            or self._settings.discord_webhook_ops
            or self._settings.discord_webhook_url
        )

    def _webhook_for_channel(self, channel: str) -> Optional[str]:
        if channel == "immediate":
            return (
                self._settings.discord_webhook_immediate
                or self._settings.discord_webhook_url
            )
        if channel == "digest":
            return (
                self._settings.discord_webhook_digest
                or self._settings.discord_webhook_url
            )
        if channel == "ops":
            return (
                self._settings.discord_webhook_ops
                or self._settings.discord_webhook_url
            )
        return self._settings.discord_webhook_url

    def _dispatch(self, event: AlertEvent) -> bool:
        if self._store and self._store.was_sent(event.alert_key):
            logger.debug("Alert already sent: %s", event.alert_key)
            return False

        url = self._webhook_for_channel(event.channel)
        if not send_discord_alert(url, event):
            return False

        if self._store:
            if not self._store.mark_sent(event.alert_key, event.channel):
                logger.debug("Alert dedupe race: %s", event.alert_key)
        return True

    def process_silver_weather(self, cleaned: Dict[str, Any]) -> bool:
        events = evaluate_silver_weather(cleaned)
        return any(self._dispatch(e) for e in events)

    def process_silver_air_quality(self, cleaned: Dict[str, Any]) -> bool:
        events = evaluate_silver_air_quality(cleaned)
        return any(self._dispatch(e) for e in events)

    def process_daily_digest(
        self,
        mongodb: "MongoDBStorage",
        date_paris: str,
    ) -> bool:
        weather = mongodb.find_gold_weather_analytics_for_date(date_paris)
        air = mongodb.find_gold_air_quality_analytics_for_date(date_paris)
        event = evaluate_gold_digest(weather, air, date_paris)
        if not event:
            return False
        return self._dispatch(event)

    def process_etl_failures(
        self,
        silver_results: List[Any],
        gold_results: List[Any],
        extracted_data: List[Any],
        reference_hour_iso: str,
    ) -> bool:
        event = evaluate_etl_failures(
            silver_results, gold_results, extracted_data, reference_hour_iso
        )
        if not event:
            return False
        return self._dispatch(event)
