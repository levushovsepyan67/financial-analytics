"""Тесты моделей аномалий и риска."""

import numpy as np
import pandas as pd
import pytest

from models.anomaly.ensemble import AnomalyEnsemble
from models.anomaly.zscore_detector import ZScoreDetector
from models.portfolio.markowitz import MarkowitzOptimizer
from models.risk.var_calculator import VaRCalculator


class TestVaRCalculator:
    @pytest.fixture
    def returns(self):
        np.random.seed(42)
        return pd.Series(np.random.normal(-0.001, 0.02, 500))

    def test_var_historical_is_negative(self, returns):
        calc = VaRCalculator()
        var = calc.historical_var(returns, confidence=0.95)
        assert var < 0

    def test_var_99_worse_than_95(self, returns):
        calc = VaRCalculator()
        var_95 = calc.historical_var(returns, confidence=0.95)
        var_99 = calc.historical_var(returns, confidence=0.99)
        assert var_99 <= var_95

    def test_cvar_worse_than_var(self, returns):
        calc = VaRCalculator()
        var = calc.historical_var(returns, 0.95)
        cvar = calc.cvar(returns, 0.95)
        assert cvar <= var

    def test_calculate_all_keys(self, returns):
        calc = VaRCalculator()
        result = calc.calculate_all(returns)
        assert "var_95_hist" in result
        assert "cvar_95" in result


class TestZScoreDetector:
    def test_detect_returns_dataframe(self, sample_features):
        detector = ZScoreDetector(threshold=1.0, window=5)
        sample_features.loc[50, "log_return"] = 0.5
        anomalies = detector.detect(sample_features)
        assert isinstance(anomalies, pd.DataFrame)

    def test_no_anomalies_on_stable_data(self, sample_features):
        detector = ZScoreDetector(threshold=10.0, window=5)
        sample_features["log_return"] = 0.001
        anomalies = detector.detect(sample_features)
        assert len(anomalies) == 0


class TestAnomalyEnsemble:
    def test_ensemble_threshold(self, sample_features):
        ensemble = AnomalyEnsemble(min_votes=2)
        result = ensemble.detect(sample_features)
        assert isinstance(result, pd.DataFrame)

    def test_count_critical_empty(self):
        ensemble = AnomalyEnsemble()
        assert ensemble.count_critical(pd.DataFrame()) == 0


class TestMarkowitz:
    def test_optimize_returns_weights(self):
        np.random.seed(42)
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        df = pd.DataFrame({
            "ts": np.tile(dates, 3),
            "symbol": ["A"] * 100 + ["B"] * 100 + ["C"] * 100,
            "log_return": np.random.normal(0.001, 0.02, 300),
        })
        optimizer = MarkowitzOptimizer()
        result = optimizer.optimize(df)
        if result:
            assert "weights" in result
            assert abs(sum(result["weights"].values()) - 1.0) < 0.01
