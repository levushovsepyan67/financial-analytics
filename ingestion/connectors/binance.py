import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

from ingestion.schemas import normalize_ohlcv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class BinanceConnector:
    BASE_URL = "https://api.binance.com/api/v3"

    VALID_INTERVALS = [
        "1m", "3m", "5m", "15m", "30m",
        "1h", "2h", "4h", "6h", "8h", "12h",
        "1d", "3d", "1w", "1M",
    ]

    def __init__(self, rate_limit_delay: float = 0.1):
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "FinancialAnalyticsPlatform/1.0",
            "Accept": "application/json",
        })
        logger.info("BinanceConnector инициализирован с delay=%ss", rate_limit_delay)

    def fetch_ohlcv(
        self,
        symbols: List[str],
        interval: str = "5m",
        limit: int = 1000,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> pd.DataFrame:
        if interval not in self.VALID_INTERVALS:
            raise ValueError(
                f"Невалидный интервал {interval}. Доступные: {self.VALID_INTERVALS}"
            )

        if limit > 1000:
            logger.warning("Limit %s превышает максимум 1000, снижаем до 1000", limit)
            limit = 1000

        all_data = []

        for symbol in symbols:
            try:
                logger.info("Загрузка Binance: %s (interval=%s)", symbol, interval)

                params = {
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "limit": limit,
                }
                if start_time:
                    params["startTime"] = start_time
                if end_time:
                    params["endTime"] = end_time

                response = self.session.get(
                    f"{self.BASE_URL}/klines",
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                if not data:
                    logger.warning("Пустой ответ для %s, пропускаем", symbol)
                    continue

                df = self._parse_klines_response(data, symbol)
                all_data.append(df)
                logger.info("Загружено %s записей для %s", len(df), symbol)
                time.sleep(self.rate_limit_delay)

            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 429:
                    logger.error("Rate limit превышен для %s", symbol)
                elif exc.response is not None and exc.response.status_code == 400:
                    logger.error("Невалидный символ %s или параметры запроса", symbol)
                else:
                    logger.error("HTTP ошибка для %s: %s", symbol, exc)
                continue
            except Exception as exc:
                logger.error("Ошибка загрузки %s: %s", symbol, exc)
                continue

        if not all_data:
            logger.warning("Не удалось загрузить данные ни для одного символа Binance")
            return pd.DataFrame(columns=normalize_ohlcv(pd.DataFrame()).columns)

        result = pd.concat(all_data, ignore_index=True)
        logger.info("Итого Binance: %s записей", len(result))
        return result

    def _parse_klines_response(self, data: List[List], symbol: str) -> pd.DataFrame:
        rows = []
        for row in data:
            rows.append({
                "ts": datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
                "symbol": symbol.upper(),
                "asset_type": "crypto",
                "source": "binance",
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "ingested_at": datetime.now(timezone.utc),
            })

        df = pd.DataFrame(rows)
        df = self._validate_dataframe(df)
        return normalize_ohlcv(df)

    def _validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["ts", "close"])
        df = df[df["close"] > 0]
        df = df[df["open"] > 0]
        df = df[df["high"] > 0]
        df = df[df["low"] > 0]
        df = df[df["high"] >= df["low"]]
        df = df[df["high"] >= df["close"]]
        df = df[df["high"] >= df["open"]]
        df = df[df["low"] <= df["close"]]
        df = df[df["low"] <= df["open"]]
        df = df[df["volume"] >= 0]
        df = df.drop_duplicates(subset=["ts", "symbol"])
        return df.sort_values("ts").reset_index(drop=True)

    def get_available_symbols(self) -> List[str]:
        try:
            response = self.session.get(f"{self.BASE_URL}/exchangeInfo", timeout=30)
            response.raise_for_status()
            data = response.json()
            symbols = [
                s["symbol"] for s in data["symbols"]
                if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
            ]
            logger.info("Найдено %s активных USDT пар", len(symbols))
            return symbols
        except Exception as exc:
            logger.error("Ошибка получения списка символов: %s", exc)
            return []


def main():
    connector = BinanceConnector(rate_limit_delay=0.1)
    df = connector.fetch_ohlcv(symbols=["BTCUSDT", "ETHUSDT"], interval="5m", limit=100)

    if not df.empty:
        print(f"Всего записей: {len(df)}")
        print(df.head())
        Path("data").mkdir(exist_ok=True)
        df.to_csv("data/test_binance.csv", index=False)
        print("Сохранено в data/test_binance.csv")


if __name__ == "__main__":
    main()
