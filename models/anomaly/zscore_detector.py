"""Z-score детектор аномалий."""

import numpy as np
import pandas as pd


class ZScoreDetector:
    """Детектирует аномалии через rolling Z-score."""

    def __init__(self, threshold: float = 3.0, window: int = 30):
        self.threshold = threshold
        self.window = window

    def detect(self, df: pd.DataFrame, value_col: str = "log_return") -> pd.DataFrame:
        if df.empty or value_col not in df.columns:
            return pd.DataFrame()

        result = df.copy()
        rolling_mean = result[value_col].rolling(self.window, min_periods=5).mean()
        rolling_std = result[value_col].rolling(self.window, min_periods=5).std()

        result["zscore"] = (result[value_col] - rolling_mean) / rolling_std.replace(0, np.nan)
        result["is_anomaly"] = result["zscore"].abs() > self.threshold
        result["anomaly_score"] = result["zscore"].abs().fillna(0)
        result["detector"] = "zscore"

        anomalies = result[result["is_anomaly"]].copy()
        anomalies["anomaly_type"] = "price_spike"
        anomalies["severity"] = anomalies["anomaly_score"].apply(self._severity)
        anomalies["description"] = anomalies.apply(
            lambda r: f"Z-score={r['zscore']:.2f} для {r.get('symbol', 'N/A')}", axis=1
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
