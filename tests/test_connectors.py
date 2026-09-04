"""Тесты коннекторов."""

import pandas as pd
import pytest

from ingestion.connectors.binance import BinanceConnector
from ingestion.connectors.moex import MoexConnector
from ingestion.schemas import OHLCV_COLUMNS, normalize_ohlcv


class TestSchemas:
    def test_normalize_ohlcv_columns(self):
        df = pd.DataFrame({
            "ts": [pd.Timestamp("2024-01-01", tz="UTC")],
            "symbol": ["TEST"],
            "asset_type": ["stock"],
            "source": ["test"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000.0],
            "ingested_at": [pd.Timestamp.now(tz="UTC")],
        })
        result = normalize_ohlcv(df)
        assert list(result.columns) == OHLCV_COLUMNS

    def test_normalize_empty(self):
        result = normalize_ohlcv(pd.DataFrame())
        assert list(result.columns) == OHLCV_COLUMNS
        assert len(result) == 0


class TestBinanceConnector:
    def test_valid_intervals(self):
        connector = BinanceConnector()
        assert "5m" in connector.VALID_INTERVALS

    def test_invalid_interval_raises(self):
        connector = BinanceConnector()
        with pytest.raises(ValueError):
            connector.fetch_ohlcv(["BTCUSDT"], interval="invalid")

    def test_parse_klines(self):
        connector = BinanceConnector()
        data = [[1704067200000, "100", "105", "95", "102", "1000", 0, "0", 0, "0", "0", "0"]]
        df = connector._parse_klines_response(data, "BTCUSDT")
        assert len(df) == 1
        assert df["close"].iloc[0] == 102.0
        assert df["symbol"].iloc[0] == "BTCUSDT"


class TestMoexConnector:
    def test_invalid_interval_raises(self):
        connector = MoexConnector()
        with pytest.raises(ValueError):
            connector.fetch_ohlcv(["SBER"], interval="5m")

    def test_parse_moex_response(self):
        connector = MoexConnector()
        data = {
            "candles": {
                "columns": ["begin", "open", "close", "high", "low", "value", "volume"],
                "data": [["2024-06-03 10:00:00", 300, 301, 302, 299, 1000, 500]],
            }
        }
        df = connector._parse_moex_response(data, "SBER")
        assert len(df) == 1
        assert df["symbol"].iloc[0] == "SBER"
        assert df["close"].iloc[0] == 301.0
