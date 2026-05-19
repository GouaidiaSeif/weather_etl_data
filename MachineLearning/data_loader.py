"""Load ML datasets from MongoDB silver collections."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from MachineLearning.mongo_loader import get_collection_counts, load_merged_dataset


def load_dataset(date_paris: Optional[str] = None) -> pd.DataFrame:
    """Merged hourly weather + AQI features (primary entry point for notebooks/scripts)."""
    return load_merged_dataset(date_paris=date_paris)


if __name__ == "__main__":
    stats = get_collection_counts()
    print("MongoDB collections:", stats)
    df = load_dataset()
    print(df.head())
    print(f"\nMerged dataset shape: {df.shape}")
