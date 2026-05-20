"""Discord webhook delivery."""

from typing import Any, Dict, Optional

import requests

from alerts.models import AlertEvent
from utils.logger import get_logger

logger = get_logger(__name__)

DISCORD_CONTENT_LIMIT = 1900

SEVERITY_COLORS = {
    "info": 0x3498DB,
    "warning": 0xF39C12,
    "critical": 0xE74C3C,
}


def _truncate(text: str, limit: int = DISCORD_CONTENT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def send_discord_alert(
    webhook_url: Optional[str],
    event: AlertEvent,
) -> bool:
    """POST an alert embed to a Discord webhook. Returns True if sent."""
    if not webhook_url:
        logger.debug("Discord webhook not configured — skipping alert: %s", event.alert_key)
        return False

    embed: Dict[str, Any] = {
        "title": event.title[:256],
        "description": _truncate(event.message),
        "color": SEVERITY_COLORS.get(event.severity, SEVERITY_COLORS["warning"]),
    }
    if event.city:
        embed["footer"] = {"text": event.city.title()}

    payload = {
        "username": "Weather ETL",
        "embeds": [embed],
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=15)
        if response.status_code == 204 or response.status_code == 200:
            logger.info("Discord alert sent: %s", event.alert_key)
            return True
        logger.error(
            "Discord webhook failed (%s): %s",
            response.status_code,
            response.text[:500],
        )
        return False
    except requests.RequestException as e:
        logger.error("Discord webhook request error: %s", e)
        return False
