"""GARCH детектор аномалий волатильности."""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class GARCHDetector:
    """Детектирует аномалии через стандартизированные остатки GARCH(1,1)."""

    def __init__(self, threshold: float = 3.0):
        self.threshold = threshold

    def detect(self, df: pd.DataFrame, value_col: str = "log_return") -> pd.DataFrame:
        if df.empty or value_col not in df.columns or len(df) < 50:
            return pd.DataFrame()

        try:
            from arch import arch_model
        except ImportError:
            logger.warning("arch не установлен, GARCH детектор пропущен")
            return pd.DataFrame()

        returns = df[value_col].dropna() * 100
        if len(returns) < 50:
            return pd.DataFrame()

        try:
            model = arch_model(returns, vol="Garch", p=1, q=1, rescale=False)
            fitted = model.fit(disp="off", show_warning=False)
            std_residuals = fitted.std_resid
        except Exception as exc:
            logger.warning("GARCH не сошёлся: %s", exc)
            return pd.DataFrame()

        result = df.loc[returns.index].copy()
        result["std_residual"] = std_residuals.values
        result["anomaly_score"] = result["std_residual"].abs()
        result["is_anomaly"] = result["anomaly_score"] > self.threshold
        result["detector"] = "garch"

        anomalies = result[result["is_anomaly"]].copy()
        anomalies["anomaly_type"] = "volatility_break"
        anomalies["severity"] = anomalies["anomaly_score"].apply(self._severity)
        anomalies["description"] = anomalies.apply(
            lambda r: f"GARCH residual={r['std_residual']:.2f}", axis=1
        )
        return anomalies

    @staticmethod
    def _severity(score: float) -> str:
        if score >= 8:
            return "critical"
        if score >= 5:
            return "high"
        if score >= 3:
            return "medium"
        return "low"
