"""Общие функции pipeline для DAG и локального запуска."""

import logging
import os
from typing import Optional, Tuple

import pandas as pd

from ingestion.validators.ohlcv_validator import OHLCVValidator
from ingestion.validators.great_expectations_validator import validate_with_ge
from processing.loaders.clickhouse_loader import ClickHouseLoader
from processing.loaders.s3_writer import S3Writer

logger = logging.getLogger(__name__)


def ingest_dataframe(
    df: pd.DataFrame,
    source: str,
    asset_type: str,
    clickhouse_host: Optional[str] = None,
) -> Tuple[int, Optional[str]]:
    """Валидирует, очищает и загружает DataFrame в S3 + ClickHouse."""
    if df.empty:
        return 0, None

    ge_result = validate_with_ge(df)
    if not ge_result.passed:
        logger.warning("GE validation warnings: %s", ge_result.warnings)

    validator = OHLCVValidator()
    validator.validate(df, hard_fail=True)
    df = validator.clean(df)

    writer = S3Writer()
    s3_key = writer.write_partition(df, source=source, asset_type=asset_type)

    host = clickhouse_host or os.getenv("CLICKHOUSE_HOST", "clickhouse")
    loader = ClickHouseLoader(host=host)
    loader.insert_dataframe(df, "market_ticks")

    return len(df), s3_key
