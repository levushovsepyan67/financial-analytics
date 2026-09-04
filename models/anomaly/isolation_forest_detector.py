"""Isolation Forest детектор аномалий."""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest


class IsolationForestDetector:
    """Многомерный детектор аномалий на основе Isolation Forest."""

    FEATURE_COLUMNS = [
        "log_return",
        "volume_zscore",
        "volatility_14d",
        "price_range_pct",
    ]

    def __init__(self, contamination: float = 0.01, n_estimators: int = 100):
        self.contamination = contamination
        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=42,
        )

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        features = [c for c in self.FEATURE_COLUMNS if c in df.columns]
        if len(features) < 2:
            return pd.DataFrame()

        matrix = df[features].fillna(0).values
        predictions = self.model.fit_predict(matrix)
        scores = -self.model.score_samples(matrix)

        result = df.copy()
        result["is_anomaly"] = predictions == -1
        result["anomaly_score"] = scores
        result["detector"] = "isolation_forest"

        anomalies = result[result["is_anomaly"]].copy()
        anomalies["anomaly_type"] = "volatility_break"
        anomalies["severity"] = anomalies["anomaly_score"].apply(self._severity)
        anomalies["description"] = "Isolation Forest: многомерная аномалия"
        return anomalies

    @staticmethod
    def _severity(score: float) -> str:
        if score >= 0.8:
            return "critical"
        if score >= 0.6:
            return "high"
        if score >= 0.4:
            return "medium"
        return "low"
