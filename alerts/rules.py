"""Alert rule evaluation for silver, gold, and ETL operations."""

from typing import Any, Dict, List, Optional

from alerts.models import AlertEvent

# Silver — immediate (high impact)
WEATHER_SEVERITY_ALERT = frozenset({"severe", "extreme"})
AQI_ALERT_LEVELS = frozenset({
    "unhealthy_sensitive",
    "unhealthy",
    "very_unhealthy",
    "hazardous",
})


def evaluate_silver_weather(cleaned: Dict[str, Any]) -> List[AlertEvent]:
    """Fire when hourly weather severity is severe or extreme."""
    severity = cleaned.get("weather_severity")
    if severity not in WEATHER_SEVERITY_ALERT:
        return []

    city = cleaned.get("city", "unknown")
    date_paris = cleaned.get("date_paris", "?")
    hour = cleaned.get("hour_paris", cleaned.get("hour", "?"))

    temp = cleaned.get("temperature_celsius")
    wind = cleaned.get("wind_speed_mps")
    uvi = cleaned.get("uvi")

    return [
        AlertEvent(
            alert_key=f"immediate:weather:{city}:{date_paris}:{hour}:severity:{severity}",
            channel="immediate",
            title=f"Weather alert — {city.title()}",
            severity="critical" if severity == "extreme" else "warning",
            city=city,
            message=(
                f"**{city.title()}** · {date_paris} {cleaned.get('hour_formatted', hour)}\n"
                f"Severity: **{severity}**\n"
                f"Temp: {temp}°C · Wind: {wind} m/s · UVI: {uvi}\n"
                f"Condition: {cleaned.get('weather_main', 'unknown')}"
            ),
            metadata={"severity": severity, "date_paris": date_paris, "hour": hour},
        )
    ]


def evaluate_silver_air_quality(cleaned: Dict[str, Any]) -> List[AlertEvent]:
    """Fire when hourly AQI alert level is unhealthy or worse."""
    if not cleaned.get("_data_quality", {}).get("aqi_present", cleaned.get("aqi") is not None):
        return []

    alert_level = cleaned.get("alert_level")
    if alert_level not in AQI_ALERT_LEVELS:
        return []

    city = cleaned.get("city", "unknown")
    date_paris = cleaned.get("date_paris", "?")
    hour = cleaned.get("hour_paris", cleaned.get("hour", "?"))
    aqi = cleaned.get("aqi")
    health = cleaned.get("health_risk") or {}

    severity = "critical" if alert_level in ("very_unhealthy", "hazardous") else "warning"

    return [
        AlertEvent(
            alert_key=f"immediate:aqi:{city}:{date_paris}:{hour}:level:{alert_level}",
            channel="immediate",
            title=f"Air quality alert — {city.title()}",
            severity=severity,
            city=city,
            message=(
                f"**{city.title()}** · {date_paris} {cleaned.get('hour_formatted', hour)}\n"
                f"AQI: **{aqi}** ({alert_level.replace('_', ' ')})\n"
                f"PM2.5: {cleaned.get('pm25')} · PM10: {cleaned.get('pm10')}\n"
                f"Outdoor: {health.get('outdoor_activity', 'n/a')} · "
                f"Mask: {'yes' if health.get('mask_recommended') else 'no'}"
            ),
            metadata={"alert_level": alert_level, "aqi": aqi, "date_paris": date_paris},
        )
    ]


def evaluate_gold_digest(
    weather_records: List[Dict[str, Any]],
    air_records: List[Dict[str, Any]],
    date_paris: str,
) -> Optional[AlertEvent]:
    """One daily digest when any city has extreme weather or significant pollution."""
    weather_hits = [
        r for r in weather_records
        if r.get("extreme_weather_flag") and r.get("is_trusted", True)
    ]
    air_hits = [
        r for r in air_records
        if r.get("significant_pollution_flag") and r.get("is_trusted", True)
    ]

    if not weather_hits and not air_hits:
        return None

    lines = [f"**Daily digest — {date_paris}**\n"]

    if weather_hits:
        lines.append("**Extreme weather**")
        for r in sorted(weather_hits, key=lambda x: x.get("city", "")):
            lines.append(
                f"• {r.get('city', '?').title()}: "
                f"temp {r.get('min_temperature')}–{r.get('max_temperature')}°C, "
                f"max wind {r.get('max_wind_speed')} m/s, "
                f"severity {r.get('max_severity')}"
            )

    if air_hits:
        lines.append("\n**Significant pollution**")
        for r in sorted(air_hits, key=lambda x: x.get("city", "")):
            lines.append(
                f"• {r.get('city', '?').title()}: "
                f"max AQI {r.get('max_aqi')}, avg {r.get('avg_aqi')}, "
                f"unhealthy hours {r.get('unhealthy_hours_count')} "
                f"({r.get('unhealthy_hours_percent')}%)"
            )

    return AlertEvent(
        alert_key=f"digest:{date_paris}",
        channel="digest",
        title=f"Daily weather & air quality digest ({date_paris})",
        severity="warning",
        message="\n".join(lines),
        metadata={
            "date_paris": date_paris,
            "weather_cities": len(weather_hits),
            "air_cities": len(air_hits),
        },
    )


def evaluate_etl_failures(
    silver_results: List[Any],
    gold_results: List[Any],
    extracted_data: List[Any],
    reference_hour_iso: str,
) -> Optional[AlertEvent]:
    """Ops rollup when extract, transform, or gold steps fail."""
    lines: List[str] = []
    failed_extract = [
        d for d in extracted_data
        if d.weather_error or d.air_quality_error
        or (not d.weather_data and not d.air_quality_data)
    ]

    failed_transform = [r for r in silver_results if not r.success]
    failed_gold = [r for r in gold_results if not r.success]

    if not failed_extract and not failed_transform and not failed_gold:
        return None

    lines.append(f"**ETL failure rollup** · ref hour `{reference_hour_iso}`\n")

    if failed_extract:
        lines.append("**Extract / raw**")
        for d in failed_extract:
            town = d.town.name
            if d.weather_error:
                lines.append(f"• {town} weather: {d.weather_error}")
            if d.air_quality_error:
                lines.append(f"• {town} air quality: {d.air_quality_error}")
            if not d.weather_data and not d.air_quality_data and not d.weather_error:
                lines.append(f"• {town}: no data returned")

    if failed_transform:
        lines.append("\n**Silver transform**")
        for r in failed_transform:
            lines.append(f"• {r.town} [{r.api_source}]: {r.error or 'unknown error'}")

    if failed_gold:
        lines.append("\n**Gold aggregation**")
        for r in failed_gold:
            lines.append(f"• {r.town} [{r.api_source}]: {r.error or 'failed'}")

    return AlertEvent(
        alert_key=f"ops:{reference_hour_iso}",
        channel="ops",
        title="ETL pipeline failures",
        severity="critical" if len(failed_transform) > len(failed_extract) else "warning",
        message="\n".join(lines[:40]),
        metadata={
            "failed_extract": len(failed_extract),
            "failed_transform": len(failed_transform),
            "failed_gold": len(failed_gold),
        },
    )
