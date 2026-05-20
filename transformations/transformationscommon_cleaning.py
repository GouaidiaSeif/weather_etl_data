"""
Common cleaning utilities for silver/gold layers.
"""

from typing import Any, Dict, List, Optional

# Gold daily trust thresholds
MIN_TRUSTED_HOURS = 18
MIN_DATA_QUALITY_SCORE = 0.7


def normalize_city_name(city: str) -> str:
    return city.strip().lower().replace(" ", "_")


def optional_float(value: Any) -> Optional[float]:
    """Parse numeric value; return None for missing or invalid."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def optional_int(value: Any) -> Optional[int]:
    """Parse integer value; return None for missing or invalid."""
    f = optional_float(value)
    if f is None:
        return None
    return int(round(f))


def record_sort_timestamp(record: Dict[str, Any]) -> str:
    """Best timestamp for dedupe (latest transform wins)."""
    lineage = record.get("_lineage") or {}
    return (
        record.get("transformed_at")
        or lineage.get("transformed_at")
        or ""
    )


def daily_trust_flags(
    hours_with_metric: int,
    hours_total: int,
    data_quality_score: float,
) -> Dict[str, Any]:
    """Coverage and trust metadata for gold daily records."""
    coverage_pct = (
        round(hours_with_metric / hours_total * 100, 1) if hours_total else 0.0
    )
    is_trusted = (
        hours_with_metric >= MIN_TRUSTED_HOURS
        and data_quality_score >= MIN_DATA_QUALITY_SCORE
    )
    return {
        "hours_with_metric": hours_with_metric,
        "hours_total": hours_total,
        "coverage_pct": coverage_pct,
        "is_trusted": is_trusted,
    }
