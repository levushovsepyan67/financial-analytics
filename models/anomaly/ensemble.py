"""Ансамбль детекторов аномалий (голосование 2 из 3)."""

import logging
from typing import List

import pandas as pd

from models.anomaly.garch_detector import GARCHDetector
from models.anomaly.isolation_forest_detector import IsolationForestDetector
from models.anomaly.zscore_detector import ZScoreDetector

logger = logging.getLogger(__name__)


class AnomalyEnsemble:
    """Комбинирует Z-score, Isolation Forest и GARCH детекторы."""

    def __init__(self, min_votes: int = 2):
        self.min_votes = min_votes
        self.zscore = ZScoreDetector()
        self.iforest = IsolationForestDetector()
        self.garch = GARCHDetector()

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        all_anomalies: List[pd.DataFrame] = []

        for symbol, group in df.groupby("symbol"):
            z_anomalies = self.zscore.detect(group)
            if_anomalies = self.iforest.detect(group)
            g_anomalies = self.garch.detect(group)

            for det_df in [z_anomalies, if_anomalies, g_anomalies]:
                if not det_df.empty:
                    all_anomalies.append(det_df[["ts", "symbol", "detector", "anomaly_score"]])

        if not all_anomalies:
            return pd.DataFrame()

        combined = pd.concat(all_anomalies, ignore_index=True)
        votes = (
            combined.groupby(["ts", "symbol", "detector"])
            .agg({"anomaly_score": "max"})
            .reset_index()
        )

        vote_counts = (
            votes.groupby(["ts", "symbol"])
            .agg(
                votes=("detector", "count"),
                ensemble_score=("anomaly_score", "mean"),
                detectors=("detector", lambda x: ",".join(sorted(x))),
            )
            .reset_index()
        )

        confirmed = vote_counts[vote_counts["votes"] >= self.min_votes].copy()
        if confirmed.empty:
            logger.info("Ансамбль: аномалий не обнаружено")
            return pd.DataFrame()

        result = confirmed.merge(
            df[["ts", "symbol", "close"]].drop_duplicates(),
            on=["ts", "symbol"],
            how="left",
        )
        result = result.rename(columns={"close": "close_price"})
        result["anomaly_type"] = "price_spike"
        result["detector"] = "ensemble"
        result["severity"] = result["ensemble_score"].apply(self._severity)
        result["description"] = result.apply(
            lambda r: f"Ансамбль ({r['detectors']}): score={r['ensemble_score']:.2f}",
            axis=1,
        )
        result["detected_at"] = pd.Timestamp.now(tz="UTC")

        logger.info("Ансамбль обнаружил %s аномалий", len(result))
        return result

    @staticmethod
    def _severity(score: float) -> str:
        if score >= 8:
            return "critical"
        if score >= 5:
            return "high"
        if score >= 3:
            return "medium"
        return "low"

    def count_critical(self, anomalies: pd.DataFrame) -> int:
        if anomalies.empty:
            return 0
        return int((anomalies["severity"].isin(["high", "critical"])).sum())
