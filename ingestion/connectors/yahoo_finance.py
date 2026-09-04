"""Коннектор к Yahoo Finance API через библиотеку yfinance."""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yfinance as yf

from ingestion.schemas import normalize_ohlcv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class YahooFinanceConnector:
    """Коннектор для акций US и forex через Yahoo Finance."""

    VALID_INTERVALS = ["1m", "5m", "15m", "30m", "1h", "1d", "1wk"]

    def __init__(self, rate_limit_delay: float = 0.2):
        self.rate_limit_delay = rate_limit_delay
        logger.info("YahooFinanceConnector инициализирован с delay=%ss", rate_limit_delay)

    def fetch_ohlcv(
        self,
        symbols: List[str],
        interval: str = "5m",
        period: str = "5d",
        asset_type: str = "stock",
    ) -> pd.DataFrame:
        if interval not in self.VALID_INTERVALS:
            raise ValueError(
                f"Невалидный интервал {interval}. Доступные: {self.VALID_INTERVALS}"
            )

        all_data = []

        for symbol in symbols:
            try:
                logger.info("Загрузка Yahoo Finance: %s", symbol)
                ticker = yf.Ticker(symbol)
                history = ticker.history(period=period, interval=interval)

                if history.empty:
                    logger.warning("Пустой ответ для %s, пропускаем", symbol)
                    continue

                df = self._parse_history(history, symbol, asset_type)
                all_data.append(df)
                logger.info("Загружено %s записей для %s", len(df), symbol)
                time.sleep(self.rate_limit_delay)

            except Exception as exc:
                logger.error("Ошибка загрузки %s: %s", symbol, exc)
                continue

        if not all_data:
            logger.warning("Не удалось загрузить данные ни для одного символа Yahoo")
            return pd.DataFrame(columns=normalize_ohlcv(pd.DataFrame()).columns)

        result = pd.concat(all_data, ignore_index=True)
        logger.info("Итого Yahoo: %s записей для %s символов", len(result), len(symbols))
        return result

    def _parse_history(
        self, history: pd.DataFrame, symbol: str, asset_type: str
    ) -> pd.DataFrame:
        df = history.reset_index()

        df = df.rename(
            columns={
                "Datetime": "ts",
                "Date": "ts",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )

        df["symbol"] = symbol.upper().replace("=X", "")
        df["asset_type"] = asset_type
        df["source"] = "yahoo_finance"
        df["ingested_at"] = datetime.now(timezone.utc)

        if df["ts"].dt.tz is None:
            df["ts"] = df["ts"].dt.tz_localize("UTC")
        else:
            df["ts"] = df["ts"].dt.tz_convert("UTC")

        df = self._validate_dataframe(df)
        return normalize_ohlcv(df)

    def _validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["ts", "close"])
        df = df[df["close"] > 0]
        df = df[df["open"] > 0]
        df = df[df["high"] >= df["low"]]
        df = df[df["volume"] >= 0]
        df = df.drop_duplicates(subset=["ts", "symbol"])
        return df.sort_values("ts").reset_index(drop=True)


def main():
    connector = YahooFinanceConnector()
    df = connector.fetch_ohlcv(symbols=["AAPL", "MSFT"], interval="1h", period="5d")

    if not df.empty:
        print(f"Загружено {len(df)} записей")
        print(df.head())
        Path("data").mkdir(exist_ok=True)
        df.to_csv("data/test_yahoo.csv", index=False)
        print("Сохранено в data/test_yahoo.csv")


if __name__ == "__main__":
    main()
