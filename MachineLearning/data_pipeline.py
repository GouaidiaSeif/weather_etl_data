"""Build silver/gold feature tables from MongoDB and optional local CSV cache."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.towns import FRENCH_TOWNS
from MachineLearning.mongo_loader import load_merged_dataset
from MachineLearning.paths import GOLD_CSV, SILVER_CSV, ensure_ml_dirs


def _city_codes() -> dict:
    return {town.name.lower(): idx for idx, town in enumerate(FRENCH_TOWNS)}


def build_silver(date_paris: Optional[str] = None, save_csv: bool = True) -> pd.DataFrame:
    """Load merged silver features from MongoDB; optionally write ``dataset_silver.csv``."""
    ensure_ml_dirs()
    df = load_merged_dataset(date_paris=date_paris)

    if df.empty:
        print("No merged silver rows in MongoDB (check silver_weather + silver_air_quality).")
        return df

    df = df.drop_duplicates(subset=["city", "date_paris", "hour_paris"])
    if save_csv:
        df.to_csv(SILVER_CSV, index=False)
        print(f"Silver dataset: {len(df)} rows -> {SILVER_CSV}")
    return df


def build_gold(
    silver_df: Optional[pd.DataFrame] = None,
    date_paris: Optional[str] = None,
    save_csv: bool = True,
) -> pd.DataFrame:
    """Feature matrix for AQI regression from silver data."""
    ensure_ml_dirs()

    if silver_df is None:
        if SILVER_CSV.exists() and date_paris is None:
            silver_df = pd.read_csv(SILVER_CSV)
        else:
            silver_df = build_silver(date_paris=date_paris, save_csv=save_csv)

    if silver_df.empty:
        return silver_df

    df = silver_df.copy()
    if "datetime" not in df.columns or df["datetime"].isna().all():
        df["datetime"] = pd.to_datetime(
            df["date_paris"] + " " + df["hour_paris"].astype(int).astype(str).str.zfill(2) + ":00",
            errors="coerce",
        )
    else:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    codes = _city_codes()
    df["hour"] = df["datetime"].dt.hour
    df["day"] = df["datetime"].dt.day
    df["month"] = df["datetime"].dt.month
    df["city_code"] = df["city"].map(codes)

    feature_cols = [
        "city_code",
        "temp",
        "humidity",
        "pressure",
        "wind_speed",
        "hour",
        "month",
        "aqi",
    ]
    gold = df[feature_cols].dropna(subset=["aqi", "city_code"])

    if save_csv:
        gold.to_csv(GOLD_CSV, index=False)
        print(f"Gold dataset: {len(gold)} rows -> {GOLD_CSV}")

    return gold


def build_all(date_paris: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build silver then gold in one call."""
    silver = build_silver(date_paris=date_paris)
    gold = build_gold(silver_df=silver, save_csv=True)
    return silver, gold


if __name__ == "__main__":
    build_all()
