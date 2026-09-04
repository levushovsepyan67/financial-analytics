"""Аналитические задачи для PostgreSQL/Greenplum слоя."""

import logging
from datetime import datetime

import numpy as np
import pandas as pd

from models.portfolio.markowitz import MarkowitzOptimizer
from models.risk.metrics import calculate_risk_metrics
from processing.loaders.greenplum_loader import GreenplumLoader

logger = logging.getLogger(__name__)


def load_returns_to_gp(features_df: pd.DataFrame, gp_host: str = None) -> int:
    """Загружает log_return в market_returns."""
    if features_df.empty:
        return 0

    returns_df = features_df[["ts", "symbol", "asset_type", "close", "log_return", "volume"]].copy()
    returns_df = returns_df.dropna(subset=["log_return"])

    loader = GreenplumLoader(host=gp_host) if gp_host else GreenplumLoader()
    return loader.insert_dataframe(returns_df, "market_returns")


def calculate_correlation_matrix(features_df: pd.DataFrame, gp_host: str = None) -> pd.DataFrame:
    """Считает корреляционную матрицу и сохраняет в GP."""
    if features_df.empty:
        return pd.DataFrame()

    pivot = features_df.pivot_table(index="ts", columns="symbol", values="log_return").dropna(how="all")
    if pivot.shape[1] < 2:
        return pd.DataFrame()

    corr = pivot.corr()
    today = datetime.now().date()
    rows = []

    for s1 in corr.columns:
        for s2 in corr.columns:
            if s1 >= s2:
                continue
            val = corr.loc[s1, s2]
            if not np.isnan(val):
                rows.append({
                    "calculated_date": today,
                    "symbol_1": s1,
                    "symbol_2": s2,
                    "correlation": round(val, 6),
                    "observations": len(pivot),
                    "period_days": 252,
                })

    result = pd.DataFrame(rows)
    if not result.empty:
        loader = GreenplumLoader(host=gp_host) if gp_host else GreenplumLoader()
        loader.insert_dataframe(result, "correlation_matrix")
        logger.info("Корреляционная матрица: %s пар", len(result))

    return result


def calculate_gp_risk_metrics(features_df: pd.DataFrame, gp_host: str = None) -> pd.DataFrame:
    """Рассчитывает VaR/Sharpe и сохраняет в risk_metrics_gp."""
    if features_df.empty:
        return pd.DataFrame()

    rows = []
    for symbol in features_df["symbol"].unique():
        metrics = calculate_risk_metrics(features_df, symbol)
        if metrics:
            rows.append({
                "calculated_at": metrics["calculated_at"],
                "symbol": symbol,
                "var_95": metrics.get("var_95_hist"),
                "var_99": metrics.get("var_99_hist"),
                "cvar_95": metrics.get("cvar_95"),
                "sharpe_ratio": metrics.get("sharpe_ratio"),
                "observations": metrics.get("observations"),
            })

    result = pd.DataFrame(rows)
    if not result.empty:
        loader = GreenplumLoader(host=gp_host) if gp_host else GreenplumLoader()
        loader.insert_dataframe(result, "risk_metrics_gp")
        logger.info("GP risk metrics: %s символов", len(result))

    return result


def run_portfolio_optimization(features_df: pd.DataFrame) -> dict:
    """Оптимизация портфеля Марковица."""
    if features_df.empty:
        return {}

    optimizer = MarkowitzOptimizer()
    return optimizer.optimize(features_df)
