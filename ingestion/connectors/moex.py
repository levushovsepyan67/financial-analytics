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


class MoexConnector:
    """Коннектор к MOEX ISS API для данных российского рынка."""

    BASE_URL = "https://iss.moex.com/iss"

    INTERVAL_MAP = {
        "1m": 1,
        "10m": 10,
        "1h": 60,
        "1d": 24,
    }

    def __init__(self, rate_limit_delay: float = 0.5):
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "FinancialAnalyticsPlatform/1.0",
            "Accept": "application/json",
        })
        logger.info("MoexConnector инициализирован с delay=%ss", rate_limit_delay)

    def fetch_ohlcv(
        self,
        symbols: List[str],
        interval: str = "10m",
        start_date: str = "2024-01-01",
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        if interval not in self.INTERVAL_MAP:
            raise ValueError(
                f"Невалидный интервал {interval}. Доступные: {list(self.INTERVAL_MAP.keys())}"
            )

        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        moex_interval = self.INTERVAL_MAP[interval]
        all_data = []

        for symbol in symbols:
            try:
                logger.info("Загрузка MOEX: %s", symbol)
                url = (
                    f"{self.BASE_URL}/engines/stock/markets/shares/"
                    f"boards/TQBR/securities/{symbol}/candles.json"
                )
                params = {
                    "interval": moex_interval,
                    "from": start_date,
                    "till": end_date,
                    "limit": 5000,
                }

                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if "candles" not in data or not data["candles"]["data"]:
                    logger.warning("Нет данных для %s, пропускаем", symbol)
                    continue

                df = self._parse_moex_response(data, symbol)
                all_data.append(df)
                logger.info("Загружено %s записей для %s", len(df), symbol)
                time.sleep(self.rate_limit_delay)

            except requests.exceptions.HTTPError as exc:
                logger.error("HTTP ошибка для %s: %s", symbol, exc)
                continue
            except Exception as exc:
                logger.error("Ошибка загрузки %s: %s", symbol, exc)
                continue

        if not all_data:
            logger.warning("Не удалось загрузить данные ни для одного символа MOEX")
            return pd.DataFrame(columns=normalize_ohlcv(pd.DataFrame()).columns)

        result = pd.concat(all_data, ignore_index=True)
        logger.info("Итого MOEX: %s записей", len(result))
        return result

    def _parse_moex_response(self, data: dict, symbol: str) -> pd.DataFrame:
        candles = data["candles"]
        df = pd.DataFrame(data=candles["data"], columns=candles["columns"])
        df = df.rename(columns={"begin": "ts"})

        df["symbol"] = symbol.upper()
        df["asset_type"] = "stock"
        df["source"] = "moex"
        df["ingested_at"] = datetime.now(timezone.utc)

        df["ts"] = pd.to_datetime(df["ts"])
        df["ts"] = df["ts"].dt.tz_localize("Europe/Moscow").dt.tz_convert("UTC")

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = self._validate_dataframe(df)
        return normalize_ohlcv(df)

    def _validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["ts", "close"])
        df = df[df["close"] > 0]
        df = df[df["high"] >= df["low"]]
        df = df[df["volume"] >= 0]
        df = df.drop_duplicates(subset=["ts", "symbol"])
        return df.sort_values("ts").reset_index(drop=True)


def main():
    connector = MoexConnector(rate_limit_delay=0.5)
    df = connector.fetch_ohlcv(
        symbols=["SBER", "GAZP"],
        interval="10m",
        start_date="2024-06-01",
        end_date="2024-06-03",
    )

    if not df.empty:
        print(f"Всего записей: {len(df)}")
        print(df.head())
        Path("data").mkdir(exist_ok=True)
        df.to_csv("data/test_moex.csv", index=False)
        print("Сохранено в data/test_moex.csv")


if __name__ == "__main__":
    main()
