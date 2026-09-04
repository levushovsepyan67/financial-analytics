"""Оптимизация портфеля по Марковицу."""

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd
from scipy.optimize import minimize

logger = logging.getLogger(__name__)


class MarkowitzOptimizer:
    """Оптимизация портфеля: максимизация Sharpe Ratio."""

    def __init__(
        self,
        rf_rate: float = 0.05,
        max_weight: float = 0.3,
        allow_short: bool = False,
    ):
        self.rf_rate = rf_rate / 252
        self.max_weight = max_weight
        self.allow_short = allow_short

    def optimize(self, returns_df: pd.DataFrame) -> Dict:
        """
        Args:
            returns_df: DataFrame с колонками symbol + log_return (long format)
                        или pivot: columns=symbols, index=dates
        """
        if "symbol" in returns_df.columns and "log_return" in returns_df.columns:
            pivot = returns_df.pivot_table(
                index="ts",
                columns="symbol",
                values="log_return",
            )
        else:
            pivot = returns_df

        pivot = pivot.dropna(how="all").fillna(0)
        if pivot.shape[1] < 2:
            logger.warning("Недостаточно активов для оптимизации")
            return {}

        expected_returns = pivot.mean().values
        cov_matrix = pivot.cov().values
        n_assets = len(expected_returns)

        bounds = (
            [(0, self.max_weight)] * n_assets
            if not self.allow_short
            else [(-0.3, self.max_weight)] * n_assets
        )

        x0 = np.array([1 / n_assets] * n_assets)

        def neg_sharpe(weights):
            port_return = weights @ expected_returns
            port_vol = np.sqrt(weights @ cov_matrix @ weights)
            if port_vol == 0:
                return 0
            return -(port_return - self.rf_rate) / port_vol

        constraints = {"type": "eq", "fun": lambda w: np.sum(w) - 1}

        result = minimize(
            neg_sharpe,
            x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
        )

        if not result.success:
            logger.warning("Оптимизация не сошлась: %s", result.message)
            return {}

        weights = result.x
        port_return = float(weights @ expected_returns * 252)
        port_vol = float(np.sqrt(weights @ cov_matrix @ weights) * np.sqrt(252))
        sharpe = float((port_return - self.rf_rate * 252) / port_vol) if port_vol > 0 else 0

        return {
            "weights": dict(zip(pivot.columns, weights.round(4))),
            "expected_return": port_return,
            "volatility": port_vol,
            "sharpe_ratio": sharpe,
        }

    def efficient_frontier(
        self, returns_df: pd.DataFrame, n_points: int = 20
    ) -> pd.DataFrame:
        """Строит эффективную границу для визуализации."""
        result = self.optimize(returns_df)
        if not result:
            return pd.DataFrame()

        points = []
        for target_vol in np.linspace(0.05, result["volatility"] * 1.5, n_points):
            points.append({
                "volatility": target_vol,
                "expected_return": result["expected_return"] * (target_vol / result["volatility"]),
            })
        return pd.DataFrame(points)
