"""Полный локальный запуск pipeline (без Airflow)."""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.connectors.binance import BinanceConnector
from ingestion.connectors.moex import MoexConnector
from ingestion.connectors.yahoo_finance import YahooFinanceConnector
from models.anomaly.ensemble import AnomalyEnsemble
from processing.features.feature_calculator import calculate_features
from processing.jobs.analytics_jobs import (
    calculate_correlation_matrix,
    calculate_gp_risk_metrics,
    load_returns_to_gp,
    run_portfolio_optimization,
)
from processing.loaders.clickhouse_loader import ClickHouseLoader
from processing.pipeline_helpers import ingest_dataframe

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    with open(PROJECT_ROOT / "config" / "symbols.yaml", encoding="utf-8") as f:
        symbols = yaml.safe_load(f)

    all_dfs = []

    # 1. Ingestion
    logger.info("=== MOEX ===")
    moex = MoexConnector()
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    df_moex = moex.fetch_ohlcv(symbols["stocks_moex"][:5], "10m", start, end)
    if not df_moex.empty:
        ingest_dataframe(df_moex, "moex", "stock")
        all_dfs.append(df_moex)

    logger.info("=== Yahoo Stocks ===")
    df_yahoo = YahooFinanceConnector().fetch_ohlcv(symbols["stocks_yahoo"][:5], "1h", "5d", "stock")
    if not df_yahoo.empty:
        ingest_dataframe(df_yahoo, "yahoo_finance", "stock")
        all_dfs.append(df_yahoo)

    logger.info("=== Binance ===")
    df_crypto = BinanceConnector().fetch_ohlcv(symbols["crypto_binance"][:5], "5m", limit=200)
    if not df_crypto.empty:
        ingest_dataframe(df_crypto, "binance", "crypto")
        all_dfs.append(df_crypto)

    if not all_dfs:
        logger.error("Нет данных для обработки")
        return

    import pandas as pd
    combined = pd.concat(all_dfs, ignore_index=True)

    # 2. Features
    logger.info("=== Features ===")
    features = calculate_features(combined)
    try:
        loader = ClickHouseLoader()
        loader.insert_features(features)
        logger.info("Features: %s строк в ClickHouse", len(features))
    except Exception as exc:
        logger.warning("ClickHouse недоступен: %s", exc)

    # 3. Anomaly
    logger.info("=== Anomaly Detection ===")
    ensemble = AnomalyEnsemble()
    anomalies = ensemble.detect(features)
    logger.info("Аномалий: %s", len(anomalies))

    # 4. Greenplum analytics
    logger.info("=== Greenplum Analytics ===")
    try:
        load_returns_to_gp(features)
        calculate_correlation_matrix(features)
        calculate_gp_risk_metrics(features)
    except Exception as exc:
        logger.warning("Analytics DB недоступен: %s", exc)

    # 5. Portfolio
    logger.info("=== Portfolio Optimization ===")
    portfolio = run_portfolio_optimization(features)
    if portfolio:
        logger.info("Sharpe: %.3f, weights: %s", portfolio.get("sharpe_ratio", 0), portfolio.get("weights"))

    logger.info("=== Pipeline завершён ===")


if __name__ == "__main__":
    main()
