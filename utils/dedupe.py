"""Deduplicate silver records by Paris local hour (keep latest transform)."""

from typing import Any, Dict, List

from transformations.transformationscommon_cleaning import record_sort_timestamp


def dedupe_records_by_hour(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep one record per hour — latest transformed_at wins."""
    by_hour: Dict[int, Dict[str, Any]] = {}
    for record in records:
        hour = record.get("hour_paris", record.get("hour"))
        if hour is None:
            continue
        existing = by_hour.get(hour)
        record_ts = record_sort_timestamp(record)
        existing_ts = record_sort_timestamp(existing) if existing else ""
        if existing is None or record_ts >= existing_ts:
            by_hour[hour] = record
    return sorted(by_hour.values(), key=lambda r: r.get("hour_paris", r.get("hour", 0)))
