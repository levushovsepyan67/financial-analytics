"""Great Expectations валидация с fallback на OHLCVValidator."""

import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from ingestion.validators.ohlcv_validator import OHLCVValidator

logger = logging.getLogger(__name__)


@dataclass
class GEResult:
    passed: bool
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def validate_with_ge(df: pd.DataFrame) -> GEResult:
    """Проверяет данные через Great Expectations, при ошибке — fallback."""
    if df.empty:
        return GEResult(passed=True, warnings=["Empty DataFrame"])

    try:
        import great_expectations as gx
        from great_expectations.core import ExpectationSuite

        context = gx.get_context(mode="ephemeral")
        validator = context.sources.pandas_default.read_dataframe(df)

        validator.expect_column_values_to_not_be_null("ts")
        validator.expect_column_values_to_not_be_null("symbol")
        validator.expect_column_values_to_not_be_null("close")
        validator.expect_column_values_to_be_between("close", min_value=0, strict_min=True)
        validator.expect_column_values_to_be_between("open", min_value=0, strict_min=True)
        validator.expect_column_values_to_be_between("volume", min_value=0)

        result = validator.validate()
        if result.success:
            return GEResult(passed=True)

        errors = [str(r) for r in result.results if not r.success]
        return GEResult(passed=False, errors=errors)

    except Exception as exc:
        logger.debug("GE недоступен (%s), используем OHLCVValidator", exc)
        ohlcv = OHLCVValidator()
        result = ohlcv.validate(df)
        return GEResult(
            passed=result.passed,
            warnings=result.warnings,
            errors=result.errors,
        )
