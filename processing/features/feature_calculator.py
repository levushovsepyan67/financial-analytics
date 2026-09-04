"""Расчёт технических индикаторов (pandas — для Airflow и локального запуска)."""

import logging
from typing import List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = [
    "ts", "symbol", "asset_type", "close", "volume",
    "log_return", "sma_7", "sma_30", "ema_14",
    "volatility_14d", "volatility_30d", "rsi_14",
    "volume_zscore", "price_range_pct", "calculated_at",
]


def calculate_features(df: pd.DataFrame) -> pd.DataFrame:
    """Рассчитывает полный набор фичей для OHLCV DataFrame."""
    if df.empty:
        return pd.DataFrame(columns=FEATURE_COLUMNS)

    result_parts: List[pd.DataFrame] = []

    for symbol, group in df.groupby("symbol"):
        g = group.sort_values("ts").copy()
        g["prev_close"] = g["close"].shift(1)
        g["log_return"] = np.log(g["close"] / g["prev_close"])
        g["sma_7"] = g["close"].rolling(7, min_periods=1).mean()
        g["sma_30"] = g["close"].rolling(30, min_periods=1).mean()
        g["ema_14"] = g["close"].ewm(span=14, adjust=False).mean()
        g["volatility_14d"] = g["log_return"].rolling(14, min_periods=2).std()
        g["volatility_30d"] = g["log_return"].rolling(30, min_periods=2).std()

        vol_mean = g["volume"].expanding().mean()
        vol_std = g["volume"].expanding().std()
        g["volume_zscore"] = (g["volume"] - vol_mean) / vol_std.replace(0, np.nan)
        g["price_range_pct"] = (g["high"] - g["low"]) / g["close"]

        gain = g["log_return"].clip(lower=0)
        loss = (-g["log_return"]).clip(lower=0)
        avg_gain = gain.rolling(14, min_periods=1).mean()
        avg_loss = loss.rolling(14, min_periods=1).mean()
        g["rsi_14"] = np.where(
            avg_loss == 0,
            100.0,
            100 - 100 / (1 + avg_gain / avg_loss),
        )

        g = g[g["prev_close"].notna()]
        g["calculated_at"] = pd.Timestamp.now(tz="UTC")
        result_parts.append(g)

    if not result_parts:
        return pd.DataFrame(columns=FEATURE_COLUMNS)

    result = pd.concat(result_parts, ignore_index=True)
    available = [c for c in FEATURE_COLUMNS if c in result.columns]
    logger.info("Рассчитано %s строк фичей для %s символов", len(result), result["symbol"].nunique())
    return result[available]
