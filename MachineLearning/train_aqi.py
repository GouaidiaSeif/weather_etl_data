"""Train AQI regression model from MongoDB-backed gold features."""

from __future__ import annotations

import sys
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from MachineLearning.data_pipeline import build_gold
from MachineLearning.paths import AQI_MODEL_PATH, GOLD_CSV, ensure_ml_dirs


def train(min_rows: int = 10) -> None:
    ensure_ml_dirs()

    if GOLD_CSV.exists():
        df = pd.read_csv(GOLD_CSV)
    else:
        df = build_gold(save_csv=True)

    print(f"Dataset loaded: {len(df)} rows")

    if len(df) < min_rows:
        print(f"Not enough data to train (need at least {min_rows} rows). Run ETL + backfill first.")
        return

    X = df.drop(columns=["aqi"])
    y = df["aqi"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    print(f"MAE: {mae:.2f}")

    joblib.dump(model, AQI_MODEL_PATH)
    print(f"Model saved -> {AQI_MODEL_PATH}")


if __name__ == "__main__":
    train()
