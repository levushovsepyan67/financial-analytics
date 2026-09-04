import logging
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd
from clickhouse_driver import Client

from config.settings import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """Загрузчик данных в ClickHouse через Native Protocol."""

    def __init__(
        self,
        host: str = CLICKHOUSE_HOST,
        port: int = CLICKHOUSE_PORT,
        database: str = CLICKHOUSE_DATABASE,
        user: str = CLICKHOUSE_USER,
        password: str = CLICKHOUSE_PASSWORD,
    ):
        self.client = Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        self.database = database
        logger.info("ClickHouseLoader подключён к %s:%s/%s", host, port, database)

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_size: int = 10000,
    ) -> int:
        if df.empty:
            logger.warning("Пустой DataFrame, вставка пропущена")
            return 0

        df = df.where(pd.notnull(df), None)
        columns = df.columns.tolist()
        records = df.to_records(index=False).tolist()
        total_inserted = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            cols_str = ", ".join(columns)
            query = f"INSERT INTO {table_name} ({cols_str}) VALUES"

            try:
                self.client.execute(query, batch, settings={"insert_deduplicate": 0})
                total_inserted += len(batch)
                logger.info("Вставлено %s строк (всего: %s)", len(batch), total_inserted)
            except Exception as exc:
                logger.error("Ошибка вставки батча в %s: %s", table_name, exc)
                raise

        logger.info("Успешно вставлено %s строк в %s", total_inserted, table_name)
        return total_inserted

    def execute_query(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        try:
            result = self.client.execute(query, params or {}, with_column_types=True)
            rows = result[0]
            columns = [col[0] for col in result[1]]
            return pd.DataFrame(rows, columns=columns)
        except Exception as exc:
            logger.error("Ошибка выполнения запроса: %s", exc)
            raise

    def get_last_ts(self, symbol: str, table_name: str) -> Optional[datetime]:
        query = f"""
            SELECT max(ts) AS last_ts
            FROM {table_name}
            WHERE symbol = %(symbol)s
        """
        result = self.client.execute(query, {"symbol": symbol})
        if result and result[0][0]:
            return result[0][0]
        return None

    def insert_anomalies(self, df: pd.DataFrame) -> int:
        return self.insert_dataframe(df, "anomalies")

    def insert_features(self, df: pd.DataFrame) -> int:
        return self.insert_dataframe(df, "market_features")

    def insert_risk_metrics(self, df: pd.DataFrame) -> int:
        return self.insert_dataframe(df, "risk_metrics")
