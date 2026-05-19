"""Paths for ML artifacts (models, cached CSVs) relative to this package."""

from pathlib import Path

ML_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ML_ROOT.parent

DATA_DIR = ML_ROOT / "data"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
MODELS_DIR = ML_ROOT / "models"

SILVER_CSV = SILVER_DIR / "dataset_silver.csv"
GOLD_CSV = GOLD_DIR / "dataset_gold.csv"
AQI_MODEL_PATH = MODELS_DIR / "aqi_model.pkl"


def ensure_ml_dirs() -> None:
    """Create data and model directories if missing."""
    for path in (SILVER_DIR, GOLD_DIR, MODELS_DIR):
        path.mkdir(parents=True, exist_ok=True)
