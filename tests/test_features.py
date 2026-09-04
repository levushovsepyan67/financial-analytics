"""Тесты расчёта фичей."""

import pandas as pd

from processing.features.feature_calculator import calculate_features


class TestFeatureCalculator:
    def test_calculate_features_columns(self, sample_ohlcv):
        features = calculate_features(sample_ohlcv)
        assert not features.empty
        assert "log_return" in features.columns
        assert "sma_7" in features.columns
        assert "rsi_14" in features.columns
        assert "volatility_14d" in features.columns

    def test_log_return_not_all_nan(self, sample_ohlcv):
        features = calculate_features(sample_ohlcv)
        assert features["log_return"].notna().sum() > 0

    def test_empty_input(self):
        result = calculate_features(pd.DataFrame())
        assert result.empty

    def test_multiple_symbols(self, sample_ohlcv):
        df = pd.concat([sample_ohlcv, sample_ohlcv.assign(symbol="TEST2")])
        features = calculate_features(df)
        assert features["symbol"].nunique() == 2
