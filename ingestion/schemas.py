"""Единая схема OHLCV-данных для всех источников."""

from typing import List

import pandas as pd

OHLCV_COLUMNS: List[str] = [
    "ts",
    "symbol",
    "asset_type",
    "source",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "ingested_at",
]

OPTIONAL_COLUMNS: List[str] = [
    "quote_volume",
    "trades_count",
    "close_time",
]


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Приводит DataFrame к единому формату OHLCV."""
    if df.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    result = df.copy()

    for col in OHLCV_COLUMNS:
        if col not in result.columns:
            if col == "ingested_at":
                result[col] = pd.Timestamp.now(tz="UTC")
            else:
                result[col] = None

    return result[OHLCV_COLUMNS]
