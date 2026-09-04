"""Дополнительные риск-метрики: Sharpe, Sortino, Drawdown."""

import numpy as np
import pandas as pd


def sharpe_ratio(returns: pd.Series, rf_rate: float = 0.0) -> float:
    if returns.empty or returns.std() == 0:
        return 0.0
    excess = returns.mean() - rf_rate
    return float(excess / returns.std() * np.sqrt(252))


def sortino_ratio(returns: pd.Series, rf_rate: float = 0.0) -> float:
    if returns.empty:
        return 0.0
    downside = returns[returns < rf_rate]
    if downside.empty or downside.std() == 0:
        return 0.0
    return float((returns.mean() - rf_rate) / downside.std() * np.sqrt(252))


def max_drawdown(prices: pd.Series) -> float:
    if prices.empty:
        return 0.0
    cumulative = (1 + prices).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative - running_max) / running_max
    return float(drawdown.min())


def calculate_risk_metrics(df: pd.DataFrame, symbol: str) -> dict:
    """Считает полный набор риск-метрик для одного символа."""
    from models.risk.var_calculator import VaRCalculator

    symbol_df = df[df["symbol"] == symbol].sort_values("ts")
    if symbol_df.empty or "log_return" not in symbol_df.columns:
        return {}

    returns = symbol_df["log_return"].dropna()
    if len(returns) < 10:
        return {}

    calc = VaRCalculator()
    var_metrics = calc.calculate_all(returns)

    return {
        "symbol": symbol,
        **var_metrics,
        "sharpe_ratio": sharpe_ratio(returns),
        "sortino_ratio": sortino_ratio(returns),
        "max_drawdown": max_drawdown(returns),
        "observations": len(returns),
        "calculated_at": pd.Timestamp.now(tz="UTC"),
    }
