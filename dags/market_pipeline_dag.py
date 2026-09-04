"""
Главный DAG платформы финансовой аналитики.

Полный pipeline (каждые 15 минут):
  ingest (MOEX + Yahoo + Binance) → features → ClickHouse →
  anomaly detection → Greenplum analytics → risk → portfolio → Telegram
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from ingestion.connectors.binance import BinanceConnector
from ingestion.connectors.moex import MoexConnector
from ingestion.connectors.yahoo_finance import YahooFinanceConnector
from ingestion.utils.telegram_notifier import send_telegram_message
from models.anomaly.ensemble import AnomalyEnsemble
from models.risk.metrics import calculate_risk_metrics
from processing.features.feature_calculator import calculate_features
from processing.jobs.analytics_jobs import (
    calculate_correlation_matrix,
    calculate_gp_risk_metrics,
    load_returns_to_gp,
    run_portfolio_optimization,
)
from processing.loaders.clickhouse_loader import ClickHouseLoader
from processing.pipeline_helpers import ingest_dataframe

logger = logging.getLogger(__name__)

SYMBOLS_PATH = PROJECT_ROOT / "config" / "symbols.yaml"
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
GP_HOST = os.getenv("GP_HOST", "analytics-db")


def _load_symbols() -> dict:
    with open(SYMBOLS_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def ingest_moex(**context):
    symbols = _load_symbols().get("stocks_moex", ["SBER", "GAZP"])
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    df = MoexConnector().fetch_ohlcv(symbols=symbols, interval="10m", start_date=start_date, end_date=end_date)
    rows, s3_key = ingest_dataframe(df, source="moex", asset_type="stock", clickhouse_host=CH_HOST)

    context["ti"].xcom_push(key="moex_rows", value=rows)
    context["ti"].xcom_push(key="moex_s3_key", value=s3_key or "")
    logger.info("MOEX: %s строк", rows)


def ingest_yahoo_stocks(**context):
    symbols = _load_symbols().get("stocks_yahoo", ["AAPL", "MSFT"])
    df = YahooFinanceConnector().fetch_ohlcv(symbols=symbols, interval="1h", period="5d", asset_type="stock")
    rows, s3_key = ingest_dataframe(df, source="yahoo_finance", asset_type="stock", clickhouse_host=CH_HOST)

    context["ti"].xcom_push(key="yahoo_rows", value=rows)
    context["ti"].xcom_push(key="yahoo_s3_key", value=s3_key or "")
    logger.info("Yahoo stocks: %s строк", rows)


def ingest_yahoo_forex(**context):
    symbols = _load_symbols().get("forex_yahoo", ["EURUSD=X"])
    df = YahooFinanceConnector().fetch_ohlcv(symbols=symbols, interval="1h", period="5d", asset_type="forex")
    rows, s3_key = ingest_dataframe(df, source="yahoo_finance", asset_type="forex", clickhouse_host=CH_HOST)

    context["ti"].xcom_push(key="forex_rows", value=rows)
    context["ti"].xcom_push(key="forex_s3_key", value=s3_key or "")
    logger.info("Yahoo forex: %s строк", rows)


def ingest_crypto(**context):
    symbols = _load_symbols().get("crypto_binance", ["BTCUSDT", "ETHUSDT"])
    df = BinanceConnector().fetch_ohlcv(symbols=symbols, interval="5m", limit=500)
    rows, s3_key = ingest_dataframe(df, source="binance", asset_type="crypto", clickhouse_host=CH_HOST)

    context["ti"].xcom_push(key="crypto_rows", value=rows)
    context["ti"].xcom_push(key="crypto_s3_key", value=s3_key or "")
    logger.info("Binance: %s строк", rows)


def calculate_and_load_features(**context):
    """Pandas-расчёт фичей + загрузка в ClickHouse (основной путь для Airflow)."""
    loader = ClickHouseLoader(host=CH_HOST)
    df = loader.execute_query("""
        SELECT ts, symbol, asset_type, source, open, high, low, close, volume
        FROM market_ticks
        WHERE ts >= now() - INTERVAL 30 DAY
        ORDER BY symbol, ts
    """)

    if df.empty:
        logger.warning("Нет данных для расчёта фичей")
        context["ti"].xcom_push(key="features_rows", value=0)
        return

    features = calculate_features(df)
    if not features.empty:
        loader.insert_features(features)
        context["ti"].xcom_push(key="features_rows", value=len(features))
        logger.info("Загружено %s строк в market_features", len(features))
    else:
        context["ti"].xcom_push(key="features_rows", value=0)


def run_anomaly_detection(**context):
    loader = ClickHouseLoader(host=CH_HOST)
    df = loader.execute_query("""
        SELECT ts, symbol, close, log_return, volume_zscore,
               volatility_14d, price_range_pct
        FROM market_features
        WHERE ts >= now() - INTERVAL 7 DAY
        ORDER BY symbol, ts
    """)

    if df.empty:
        context["ti"].xcom_push(key="anomalies_count", value=0)
        context["ti"].xcom_push(key="total_anomalies", value=0)
        return

    ensemble = AnomalyEnsemble(min_votes=2)
    anomalies = ensemble.detect(df)

    if not anomalies.empty:
        anomalies["detected_at"] = pd.Timestamp.now(tz="UTC")
        loader.insert_anomalies(anomalies)

    critical_count = ensemble.count_critical(anomalies)
    context["ti"].xcom_push(key="anomalies_count", value=critical_count)
    context["ti"].xcom_push(key="total_anomalies", value=len(anomalies))
    logger.info("Аномалий: %s (критических: %s)", len(anomalies), critical_count)


def run_greenplum_analytics(**context):
    loader = ClickHouseLoader(host=CH_HOST)
    df = loader.execute_query("""
        SELECT ts, symbol, asset_type, close, volume, log_return
        FROM market_features
        WHERE ts >= now() - INTERVAL 252 DAY
        ORDER BY symbol, ts
    """)

    if df.empty:
        logger.warning("Нет данных для GP analytics")
        return

    load_returns_to_gp(df, gp_host=GP_HOST)
    calculate_correlation_matrix(df, gp_host=GP_HOST)
    calculate_gp_risk_metrics(df, gp_host=GP_HOST)
    logger.info("Greenplum analytics завершён")


def run_risk_metrics(**context):
    loader = ClickHouseLoader(host=CH_HOST)
    df = loader.execute_query("""
        SELECT ts, symbol, close, log_return
        FROM market_features
        WHERE ts >= now() - INTERVAL 252 DAY
        ORDER BY symbol, ts
    """)

    if df.empty:
        return

    metrics_rows = []
    for symbol in df["symbol"].unique():
        metrics = calculate_risk_metrics(df, symbol)
        if metrics:
            metrics_rows.append(metrics)

    if metrics_rows:
        loader.insert_risk_metrics(pd.DataFrame(metrics_rows))
        logger.info("ClickHouse risk metrics: %s символов", len(metrics_rows))


def run_portfolio_task(**context):
    loader = ClickHouseLoader(host=CH_HOST)
    df = loader.execute_query("""
        SELECT ts, symbol, log_return
        FROM market_features
        WHERE ts >= now() - INTERVAL 90 DAY
        ORDER BY symbol, ts
    """)

    if df.empty:
        return

    result = run_portfolio_optimization(df)
    if result:
        context["ti"].xcom_push(key="portfolio_sharpe", value=result.get("sharpe_ratio", 0))
        context["ti"].xcom_push(key="portfolio_weights", value=str(result.get("weights", {})))
        logger.info("Портфель: Sharpe=%.3f", result.get("sharpe_ratio", 0))


def check_alerts(**context):
    count = context["ti"].xcom_pull(task_ids="run_anomaly_detection", key="anomalies_count")
    if count and count > 0:
        return "send_telegram_alert"
    return "no_alert"


def send_telegram_alert(**context):
    total = context["ti"].xcom_pull(task_ids="run_anomaly_detection", key="total_anomalies") or 0
    critical = context["ti"].xcom_pull(task_ids="run_anomaly_detection", key="anomalies_count") or 0
    moex_rows = context["ti"].xcom_pull(task_ids="ingest_moex", key="moex_rows") or 0
    yahoo_rows = context["ti"].xcom_pull(task_ids="ingest_yahoo_stocks", key="yahoo_rows") or 0
    crypto_rows = context["ti"].xcom_pull(task_ids="ingest_crypto", key="crypto_rows") or 0
    features_rows = context["ti"].xcom_pull(task_ids="calculate_features", key="features_rows") or 0
    portfolio_sharpe = context["ti"].xcom_pull(task_ids="run_portfolio", key="portfolio_sharpe") or 0

    message = (
        f"<b>Financial Analytics Alert</b>\n\n"
        f"Критических аномалий: <b>{critical}</b>\n"
        f"Всего аномалий: {total}\n"
        f"MOEX: {moex_rows} | Yahoo: {yahoo_rows} | Crypto: {crypto_rows}\n"
        f"Features: {features_rows} | Portfolio Sharpe: {portfolio_sharpe:.2f}\n"
        f"Время: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )
    send_telegram_message(message, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)


default_args = {
    "owner": "financial-analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_pipeline",
    default_args=default_args,
    description="Полный pipeline: ingestion → features → anomaly → risk → portfolio",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["market", "analytics", "production"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    no_alert = EmptyOperator(task_id="no_alert")

    t_moex = PythonOperator(task_id="ingest_moex", python_callable=ingest_moex)
    t_yahoo = PythonOperator(task_id="ingest_yahoo_stocks", python_callable=ingest_yahoo_stocks)
    t_forex = PythonOperator(task_id="ingest_yahoo_forex", python_callable=ingest_yahoo_forex)
    t_crypto = PythonOperator(task_id="ingest_crypto", python_callable=ingest_crypto)
    t_features = PythonOperator(task_id="calculate_features", python_callable=calculate_and_load_features)

    # PySpark batch job (опционально, для больших объёмов)
    t_spark = BashOperator(
        task_id="spark_features_batch",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python processing/spark_jobs/calculate_features.py || echo 'Spark skipped (no Java)'"
        ),
    )

    t_anomaly = PythonOperator(task_id="run_anomaly_detection", python_callable=run_anomaly_detection)
    t_gp = PythonOperator(task_id="run_greenplum_analytics", python_callable=run_greenplum_analytics)
    t_risk = PythonOperator(task_id="run_risk_metrics", python_callable=run_risk_metrics)
    t_portfolio = PythonOperator(task_id="run_portfolio", python_callable=run_portfolio_task)
    t_check = BranchPythonOperator(task_id="check_alerts", python_callable=check_alerts)
    t_telegram = PythonOperator(task_id="send_telegram_alert", python_callable=send_telegram_alert)

    ingest_tasks = [t_moex, t_yahoo, t_forex, t_crypto]

    start >> ingest_tasks
    ingest_tasks >> t_features >> t_spark >> t_anomaly
    t_anomaly >> t_check
    t_check >> [t_telegram, no_alert]
    [t_telegram, no_alert] >> t_gp >> t_risk >> t_portfolio >> end
