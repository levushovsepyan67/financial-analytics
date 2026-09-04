"""Тесты валидаторов."""

import pandas as pd

from ingestion.validators.ohlcv_validator import OHLCVValidator


class TestOHLCVValidator:
    def test_valid_data_passes(self, sample_ohlcv):
        validator = OHLCVValidator()
        result = validator.validate(sample_ohlcv)
        assert result.passed

    def test_negative_prices_fail(self, sample_ohlcv):
        validator = OHLCVValidator()
        df = sample_ohlcv.copy()
        df.loc[0, "close"] = -1
        result = validator.validate(df)
        assert not result.passed

    def test_clean_removes_invalid(self, sample_ohlcv):
        validator = OHLCVValidator()
        df = sample_ohlcv.copy()
        df.loc[0, "close"] = -1
        cleaned = validator.clean(df)
        assert (cleaned["close"] > 0).all()
        assert len(cleaned) < len(df)

    def test_empty_dataframe(self):
        validator = OHLCVValidator()
        result = validator.validate(pd.DataFrame())
        assert result.passed
