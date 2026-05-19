"""Load silver-layer feature tables from MongoDB (ETL cleaned_data documents)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Project root on path when running scripts from MachineLearning/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import Settings, get_settings
from storage.mongodb_storage import MongoDBStorage


def _weather_row(cleaned: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "city": (cleaned.get("city") or "").lower(),
        "date_paris": cleaned.get("date_paris"),
        "hour_paris": cleaned.get("hour_paris", cleaned.get("hour")),
        "timestamp_paris": cleaned.get("timestamp_paris"),
        "temp": cleaned.get("temperature_celsius"),
        "humidity": cleaned.get("humidity_percent"),
        "pressure": cleaned.get("pressure_hpa"),
        "wind_speed": cleaned.get("wind_speed_mps"),
        "wind_gust": cleaned.get("wind_gust_mps"),
        "clouds": cleaned.get("cloud_coverage_percent"),
        "weather_main": cleaned.get("weather_main"),
    }


def _aqi_row(cleaned: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "city": (cleaned.get("city") or "").lower(),
        "date_paris": cleaned.get("date_paris"),
        "hour_paris": cleaned.get("hour_paris", cleaned.get("hour")),
        "timestamp_paris": cleaned.get("timestamp_paris"),
        "aqi": cleaned.get("aqi"),
        "pm25": cleaned.get("pm25"),
        "pm10": cleaned.get("pm10"),
        "no2": cleaned.get("no2"),
        "o3": cleaned.get("o3"),
        "alert_level": cleaned.get("alert_level"),
    }


def _records_to_df(records: List[Dict[str, Any]], row_fn) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = [row_fn(r) for r in records]
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["city", "date_paris"], how="any")
    if df.empty:
        return df
    df["hour_paris"] = pd.to_numeric(df["hour_paris"], errors="coerce").astype("Int64")
    return df


def load_silver_weather_df(
    date_paris: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> pd.DataFrame:
    """Hourly cleaned weather rows from ``silver_weather``."""
    mongo = MongoDBStorage(settings or get_settings())
    if not mongo.connect():
        raise ConnectionError("Could not connect to MongoDB for silver_weather")
    try:
        records = mongo.iter_silver_weather_records(date_paris)
        return _records_to_df(records, _weather_row)
    finally:
        mongo.close()


def load_silver_air_quality_df(
    date_paris: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> pd.DataFrame:
    """Hourly cleaned air quality rows from ``silver_air_quality``."""
    mongo = MongoDBStorage(settings or get_settings())
    if not mongo.connect():
        raise ConnectionError("Could not connect to MongoDB for silver_air_quality")
    try:
        records = mongo.iter_silver_air_quality_records(date_paris)
        return _records_to_df(records, _aqi_row)
    finally:
        mongo.close()


def load_merged_dataset(
    date_paris: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> pd.DataFrame:
    """Inner-join weather and AQI on city + Paris date + hour."""
    df_weather = load_silver_weather_df(date_paris, settings)
    df_aqi = load_silver_air_quality_df(date_paris, settings)

    if df_weather.empty or df_aqi.empty:
        return pd.DataFrame()

    merge_keys = ["city", "date_paris", "hour_paris"]
    df = pd.merge(df_weather, df_aqi, on=merge_keys, how="inner", suffixes=("_wx", "_aq"))

    if "timestamp_paris_wx" in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp_paris_wx"], errors="coerce")
    elif "date_paris" in df.columns:
        df["datetime"] = pd.to_datetime(
            df["date_paris"] + " " + df["hour_paris"].astype(str).str.zfill(2) + ":00",
            errors="coerce",
        )

    return df.sort_values(["city", "datetime"]).reset_index(drop=True)


def get_collection_counts(settings: Optional[Settings] = None) -> Dict[str, int]:
    """Return document counts for silver/gold collections."""
    mongo = MongoDBStorage(settings or get_settings())
    if not mongo.connect():
        return {}
    try:
        return mongo.get_stats()
    finally:
        mongo.close()
