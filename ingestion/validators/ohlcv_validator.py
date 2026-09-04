"""Валидация качества OHLCV-данных."""

import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from ingestion.schemas import OHLCV_COLUMNS

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    passed: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    rows_before: int = 0
    rows_after: int = 0


class OHLCVValidator:
    """Валидатор OHLCV с hard и soft проверками."""

    REQUIRED_COLUMNS = OHLCV_COLUMNS

    def validate(self, df: pd.DataFrame, hard_fail: bool = False) -> ValidationResult:
        result = ValidationResult(passed=True, rows_before=len(df))

        if df.empty:
            result.warnings.append("DataFrame пустой")
            result.rows_after = 0
            return result

        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            result.errors.append(f"Отсутствуют колонки: {missing}")
            result.passed = False

        null_mask = df[["ts", "symbol", "close"]].isnull().any(axis=1)
        if null_mask.any():
            result.warnings.append(f"NULL в обязательных полях: {null_mask.sum()} строк")

        negative_prices = (df["close"] <= 0) | (df["open"] <= 0)
        if negative_prices.any():
            msg = f"Неположительные цены: {negative_prices.sum()} строк"
            result.errors.append(msg)
            result.passed = False

        invalid_ohlc = (
            (df["high"] < df["low"])
            | (df["high"] < df["close"])
            | (df["high"] < df["open"])
            | (df["low"] > df["close"])
            | (df["low"] > df["open"])
        )
        if invalid_ohlc.any():
            msg = f"Нарушена логика OHLC: {invalid_ohlc.sum()} строк"
            result.errors.append(msg)
            result.passed = False

        if "volume" in df.columns and (df["volume"] < 0).any():
            result.errors.append("Отрицательный объём")

        duplicates = df.duplicated(subset=["ts", "symbol"]).sum()
        if duplicates > 0:
            result.warnings.append(f"Дубликаты (ts, symbol): {duplicates}")

        result.rows_after = len(df)

        if not result.passed:
            logger.error("Валидация не пройдена: %s", result.errors)
            if hard_fail:
                raise ValueError("; ".join(result.errors))
        elif result.warnings:
            logger.warning("Валидация с предупреждениями: %s", result.warnings)

        return result

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Очищает DataFrame от невалидных строк."""
        if df.empty:
            return df

        cleaned = df.dropna(subset=["ts", "symbol", "close"]).copy()
        cleaned = cleaned[cleaned["close"] > 0]
        cleaned = cleaned[cleaned["open"] > 0]
        cleaned = cleaned[cleaned["high"] >= cleaned["low"]]
        cleaned = cleaned[cleaned["volume"] >= 0]
        cleaned = cleaned.drop_duplicates(subset=["ts", "symbol"])
        return cleaned.sort_values("ts").reset_index(drop=True)
