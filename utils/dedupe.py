"""Deduplicate silver records by Paris local hour (keep latest transform)."""

from typing import Any, Dict, List


def dedupe_records_by_hour(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep one record per hour — latest transformed_at wins."""
    by_hour: Dict[int, Dict[str, Any]] = {}
    for record in records:
        hour = record.get("hour_paris", record.get("hour"))
        if hour is None:
            continue
        existing = by_hour.get(hour)
        if existing is None or record.get("transformed_at", "") >= existing.get("transformed_at", ""):
            by_hour[hour] = record
    return sorted(by_hour.values(), key=lambda r: r.get("hour_paris", r.get("hour", 0)))
