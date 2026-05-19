"""Predict AQI from weather / time features using the trained model."""

from __future__ import annotations

import sys
from pathlib import Path

import joblib
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from MachineLearning.paths import AQI_MODEL_PATH


def load_model():
    if not AQI_MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {AQI_MODEL_PATH}. Run train_aqi.py first."
        )
    return joblib.load(AQI_MODEL_PATH)


def predict_aqi(input_dict: dict) -> float:
    model = load_model()
    feature_names = list(model.feature_names_in_)
    input_df = pd.DataFrame([input_dict]).reindex(columns=feature_names)
    return float(model.predict(input_df)[0])


if __name__ == "__main__":
    sample = {
        "city_code": 0,
        "temp": 12.0,
        "humidity": 60.0,
        "pressure": 1015.0,
        "wind_speed": 3.0,
        "hour": 10,
        "month": 5,
    }
    print(f"Predicted AQI: {predict_aqi(sample):.2f}")
