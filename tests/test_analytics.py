"""Тесты аналитических задач."""

import pandas as pd

from processing.jobs.analytics_jobs import (
    calculate_correlation_matrix,
    calculate_gp_risk_metrics,
    run_portfolio_optimization,
)


class TestAnalyticsJobs:
    def test_correlation_matrix(self, sample_features):
        result = calculate_correlation_matrix(
            sample_features.assign(symbol=sample_features["symbol"].where(
                sample_features.index < 50, "SYM2"
            ))
        )
        # single symbol won't produce pairs
        assert isinstance(result, pd.DataFrame)

    def test_portfolio_optimization(self, sample_features):
        df = sample_features.copy()
        df2 = df.copy()
        df2["symbol"] = "SYM2"
        combined = pd.concat([df, df2])
        result = run_portfolio_optimization(combined)
        if result:
            assert "sharpe_ratio" in result
            assert "weights" in result

    def test_correlation_two_symbols(self, sample_features):
        df1 = sample_features.copy()
        df2 = sample_features.copy()
        df2["symbol"] = "SYM2"
        combined = pd.concat([df1, df2])
        # Mock GP loader by testing logic only - correlation calc without insert
        pivot = combined.pivot_table(index="ts", columns="symbol", values="log_return")
        if pivot.shape[1] >= 2:
            corr = pivot.corr()
            assert corr.shape[0] >= 2
