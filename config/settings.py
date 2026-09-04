"""Централизованная конфигурация платформы."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# --- ClickHouse ---
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "market_db")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analyst")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analyst123")

# --- MinIO / S3 ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "data-lake")

# --- Greenplum / Analytics Postgres ---
GP_HOST = os.getenv("GP_HOST", "localhost")
GP_PORT = int(os.getenv("GP_PORT", "5433"))
GP_DATABASE = os.getenv("GP_DATABASE", "market_analytics")
GP_USER = os.getenv("GP_USER", "gpadmin")
GP_PASSWORD = os.getenv("GP_PASSWORD", "gpadmin123")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# --- Spark ---
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")

# --- Pipeline ---
DEFAULT_RATE_LIMIT_BINANCE = float(os.getenv("RATE_LIMIT_BINANCE", "0.1"))
DEFAULT_RATE_LIMIT_MOEX = float(os.getenv("RATE_LIMIT_MOEX", "0.5"))
DEFAULT_RATE_LIMIT_YAHOO = float(os.getenv("RATE_LIMIT_YAHOO", "0.2"))
