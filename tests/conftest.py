"""Общие фикстуры для тестов."""

import pandas as pd
import pytest


@pytest.fixture
def sample_ohlcv():
    """Тестовый OHLCV DataFrame."""
    return pd.DataFrame({
        "ts": pd.date_range("2024-01-01", periods=100, freq="h", tz="UTC"),
        "symbol": ["TEST"] * 100,
        "asset_type": ["stock"] * 100,
        "source": ["test"] * 100,
        "open": [100.0 + i * 0.1 for i in range(100)],
        "high": [101.0 + i * 0.1 for i in range(100)],
        "low": [99.0 + i * 0.1 for i in range(100)],
        "close": [100.5 + i * 0.1 for i in range(100)],
        "volume": [1000 + i * 10 for i in range(100)],
        "ingested_at": pd.Timestamp.now(tz="UTC"),
    })


@pytest.fixture
def sample_features(sample_ohlcv):
    """DataFrame с фичами для тестов моделей."""
    df = sample_ohlcv.copy()
    df["log_return"] = df["close"].pct_change().apply(lambda x: __import__("math").log(1 + x) if x == x else 0)
    df["volume_zscore"] = 0.0
    df["volatility_14d"] = 0.01
    df["price_range_pct"] = (df["high"] - df["low"]) / df["close"]
    return df.dropna()
