"""
Test Discord alert notifications without running the full ETL.

Usage (from project root):
  python scripts/test_alerts.py --ping
  python scripts/test_alerts.py --type all
  python scripts/test_alerts.py --type weather --force
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alerts.discord import send_discord_alert
from alerts.models import AlertEvent
from alerts.rules import (
    evaluate_etl_failures,
    evaluate_gold_digest,
    evaluate_silver_air_quality,
    evaluate_silver_weather,
)
from alerts.service import AlertService
from config.settings import get_settings
from storage.mongodb_storage import MongoDBStorage
from utils.timezone_utils import paris_date_str, get_reference_hour_paris


def _suffix(force: bool) -> str:
    return f":test:{uuid4().hex[:8]}" if force else ""


def cmd_ping(settings) -> bool:
    """Send a minimal embed to the default webhook."""
    url = settings.discord_webhook_url or settings.discord_webhook_immediate
    if not url:
        print("ERROR: Set DISCORD_WEBHOOK_URL (or DISCORD_WEBHOOK_IMMEDIATE) in .env")
        return False

    event = AlertEvent(
        alert_key="test:ping",
        channel="immediate",
        title="Test connexion — Weather ETL",
        severity="info",
        message=(
            "Si vous voyez ce message, le webhook Discord fonctionne.\n"
            f"Horodatage UTC : {datetime.now(timezone.utc).isoformat()}"
        ),
    )
    ok = send_discord_alert(url, event)
    print("OK ping envoyé" if ok else "ÉCHEC ping (voir les logs)")
    return ok


def cmd_types(settings, alert_type: str, force: bool, use_mongo: bool) -> int:
    mongodb = None
    if use_mongo:
        mongodb = MongoDBStorage(settings)
        if not mongodb.connect():
            print("WARN: MongoDB indisponible — tests sans déduplication persistée")
            mongodb = None

    service = AlertService(settings, mongodb)
    if not service.enabled:
        print("ERROR: Aucun DISCORD_WEBHOOK_* configuré dans .env")
        return 1

    suf = _suffix(force)
    ref = get_reference_hour_paris()
    date_paris = paris_date_str(ref)
    hour = ref.hour
    sent = 0

    def dispatch_manual(event: AlertEvent) -> None:
        nonlocal sent
        if force:
            event = AlertEvent(
                alert_key=event.alert_key + suf,
                channel=event.channel,
                title=event.title + " [TEST]",
                message=event.message,
                severity=event.severity,
                city=event.city,
                metadata=event.metadata,
            )
        url = service._webhook_for_channel(event.channel)
        if send_discord_alert(url, event):
            sent += 1
            print(f"  OK {event.channel}: {event.title}")
        else:
            print(f"  FAIL {event.channel}: {event.title}")

    if alert_type in ("weather", "all"):
        sample = {
            "city": "paris",
            "date_paris": date_paris,
            "hour_paris": hour,
            "hour_formatted": f"{hour:02d}:00",
            "weather_severity": "extreme",
            "temperature_celsius": 38.5,
            "wind_speed_mps": 22.0,
            "uvi": 9.2,
            "weather_main": "clear",
        }
        if force:
            for ev in evaluate_silver_weather(sample):
                dispatch_manual(ev)
        else:
            if service.process_silver_weather(sample):
                sent += 1
                print("  OK immediate: weather (via AlertService)")
            else:
                print("  SKIP immediate: weather (déjà envoyé ou règle non déclenchée)")

    if alert_type in ("aqi", "all"):
        sample = {
            "city": "lyon",
            "date_paris": date_paris,
            "hour_paris": hour,
            "hour_formatted": f"{hour:02d}:00",
            "aqi": 165,
            "alert_level": "unhealthy",
            "pm25": 78.0,
            "pm10": 95.0,
            "_data_quality": {"aqi_present": True},
            "health_risk": {
                "outdoor_activity": "avoid",
                "mask_recommended": True,
            },
        }
        if force:
            for ev in evaluate_silver_air_quality(sample):
                dispatch_manual(ev)
        else:
            if service.process_silver_air_quality(sample):
                sent += 1
                print("  OK immediate: air quality (via AlertService)")
            else:
                print("  SKIP immediate: air quality (déjà envoyé ou règle non déclenchée)")

    if alert_type in ("digest", "all"):
        weather = [{
            "city": "paris",
            "extreme_weather_flag": True,
            "is_trusted": True,
            "min_temperature": -2,
            "max_temperature": 36,
            "max_wind_speed": 24,
            "max_severity": "extreme",
        }]
        air = [{
            "city": "marseille",
            "significant_pollution_flag": True,
            "is_trusted": True,
            "max_aqi": 180,
            "avg_aqi": 125,
            "unhealthy_hours_count": 8,
            "unhealthy_hours_percent": 33.3,
        }]
        event = evaluate_gold_digest(weather, air, date_paris)
        if event:
            if force:
                dispatch_manual(event)
            elif service._dispatch(event):
                sent += 1
                print("  OK digest: daily summary")
            else:
                print("  SKIP digest (clé déjà envoyée aujourd'hui — utilisez --force)")
        else:
            print("  FAIL digest: aucun événement généré")

    if alert_type in ("ops", "all"):
        class FakeResult:
            def __init__(self, town, api, ok, err=None):
                self.town = town
                self.api_source = api
                self.success = ok
                self.error = err

        class FakeExtract:
            def __init__(self, name, w_err=None, aq_err=None):
                self.town = type("T", (), {"name": name})()
                self.weather_data = None if w_err else {"ok": True}
                self.air_quality_data = None if aq_err else {"ok": True}
                self.weather_error = w_err
                self.air_quality_error = aq_err

        event = evaluate_etl_failures(
            silver_results=[
                FakeResult("nice", "openweather", False, "Missing hourly data"),
            ],
            gold_results=[],
            extracted_data=[
                FakeExtract("toulouse", w_err="API timeout"),
            ],
            reference_hour_iso=ref.isoformat(),
        )
        if event:
            if force:
                event = AlertEvent(
                    alert_key=event.alert_key + suf,
                    channel=event.channel,
                    title=event.title + " [TEST]",
                    message=event.message,
                    severity=event.severity,
                    metadata=event.metadata,
                )
                dispatch_manual(event)
            elif service._dispatch(event):
                sent += 1
                print("  OK ops: ETL failure rollup")
            else:
                print("  SKIP ops (clé déjà envoyée pour cette heure — utilisez --force)")
        else:
            print("  FAIL ops: aucun événement généré")

    if mongodb:
        mongodb.close()

    print(f"\nTerminé ({sent} envoi(s) via service, ou voir lignes OK ci-dessus avec --force).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Tester les alertes Discord Weather ETL")
    parser.add_argument(
        "--ping",
        action="store_true",
        help="Un seul message de test sur DISCORD_WEBHOOK_URL",
    )
    parser.add_argument(
        "--type",
        choices=["weather", "aqi", "digest", "ops", "all"],
        default="all",
        help="Type d'alerte à simuler (défaut: all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Contourne la déduplication Mongo (clés uniques + envoi direct)",
    )
    parser.add_argument(
        "--no-mongo",
        action="store_true",
        help="Ne pas se connecter à MongoDB",
    )
    args = parser.parse_args()
    settings = get_settings()

    print("Webhooks configurés:")
    print(f"  URL (fallback): {'oui' if settings.discord_webhook_url else 'non'}")
    print(f"  IMMEDIATE: {'oui' if settings.discord_webhook_immediate else 'non'}")
    print(f"  DIGEST: {'oui' if settings.discord_webhook_digest else 'non'}")
    print(f"  OPS: {'oui' if settings.discord_webhook_ops else 'non'}")
    print()

    if args.ping:
        return 0 if cmd_ping(settings) else 1

    return cmd_types(settings, args.type, args.force, use_mongo=not args.no_mongo)


if __name__ == "__main__":
    raise SystemExit(main())
