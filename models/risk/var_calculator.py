"""Расчёт Value at Risk и связанных метрик."""

import logging
from typing import Dict

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class VaRCalculator:
    """VaR тремя методами + CVaR."""

    def historical_var(
        self,
        returns: pd.Series,
        confidence: float = 0.95,
        horizon: int = 1,
    ) -> float:
        if returns.empty:
            return 0.0
        var = np.percentile(returns.dropna(), (1 - confidence) * 100)
        return float(var * np.sqrt(horizon))

    def parametric_var(
        self,
        returns: pd.Series,
        confidence: float = 0.95,
        horizon: int = 1,
    ) -> float:
        if returns.empty:
            return 0.0
        mu = returns.mean()
        sigma = returns.std()
        z = stats.norm.ppf(1 - confidence)
        return float((mu + z * sigma) * np.sqrt(horizon))

    def monte_carlo_var(
        self,
        returns: pd.Series,
        confidence: float = 0.95,
        horizon: int = 10,
        n_sim: int = 10000,
    ) -> float:
        if returns.empty:
            return 0.0
        mu = returns.mean()
        sigma = returns.std()
        simulations = np.random.normal(mu, sigma, size=(n_sim, horizon))
        total_returns = simulations.sum(axis=1)
        return float(np.percentile(total_returns, (1 - confidence) * 100))

    def cvar(self, returns: pd.Series, confidence: float = 0.95) -> float:
        if returns.empty:
            return 0.0
        var = self.historical_var(returns, confidence)
        tail = returns[returns <= var]
        if tail.empty:
            return var
        return float(tail.mean())

    def calculate_all(self, returns: pd.Series) -> Dict[str, float]:
        return {
            "var_95_hist": self.historical_var(returns, 0.95),
            "var_99_hist": self.historical_var(returns, 0.99),
            "var_95_param": self.parametric_var(returns, 0.95),
            "var_99_param": self.parametric_var(returns, 0.99),
            "var_95_mc": self.monte_carlo_var(returns, 0.95, horizon=10),
            "cvar_95": self.cvar(returns, 0.95),
        }
