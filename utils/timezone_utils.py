"""Timezone helpers — France (Europe/Paris) alignment for the ETL pipeline."""

from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

PARIS_TZ = ZoneInfo("Europe/Paris")


def get_reference_hour_paris(now_utc: Optional[datetime] = None) -> datetime:
    """Current hour floored in Europe/Paris (timezone-aware)."""
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    paris = now_utc.astimezone(PARIS_TZ)
    return paris.replace(minute=0, second=0, microsecond=0)


def floor_to_paris_hour(dt: datetime) -> datetime:
    """Floor any aware datetime to the start of its Paris local hour."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    paris = dt.astimezone(PARIS_TZ)
    return paris.replace(minute=0, second=0, microsecond=0)


def paris_hour(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PARIS_TZ).hour


def paris_date_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PARIS_TZ).strftime("%Y-%m-%d")


def format_hour_paris(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PARIS_TZ).strftime("%H:00")


def parse_storage_timestamp(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
