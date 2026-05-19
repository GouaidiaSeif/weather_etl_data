"""Flag AQI readings that deviate strongly from the trained model."""

from __future__ import annotations

import sys
from pathlib import Path

import joblib
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.towns import FRENCH_TOWNS
from MachineLearning.data_pipeline import build_gold
from MachineLearning.paths import AQI_MODEL_PATH, GOLD_CSV


def city_code_map() -> dict:
    return {idx: town.name for idx, town in enumerate(FRENCH_TOWNS)}


def detect_anomalies() -> pd.DataFrame:
    if not AQI_MODEL_PATH.exists():
        raise FileNotFoundError(f"Train a model first: {AQI_MODEL_PATH}")

    if GOLD_CSV.exists():
        df = pd.read_csv(GOLD_CSV)
    else:
        df = build_gold(save_csv=True)

    if df.empty:
        print("No gold dataset rows available.")
        return df

    model = joblib.load(AQI_MODEL_PATH)
    X = df.drop(columns=["aqi"])
    real = df["aqi"]
    preds = model.predict(X)

    df = df.copy()
    df["predicted_aqi"] = preds
    df["error"] = (real - preds).abs()

    threshold = df["error"].mean() + 2 * df["error"].std()
    df["anomaly"] = df["error"] > threshold
    df["city"] = df["city_code"].map(city_code_map())

    anomalies = df[df["anomaly"]]

    print("\n==============================")
    print(f"Total anomalies: {len(anomalies)}")
    print("==============================\n")

    if anomalies.empty:
        print("No anomalies detected.")
    else:
        print(
            anomalies[["city", "aqi", "predicted_aqi", "error"]]
            .sort_values("error", ascending=False)
            .head(20)
        )

    return df


if __name__ == "__main__":
    detect_anomalies()
