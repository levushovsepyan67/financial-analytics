import io
import logging
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2 import sql

from config.settings import GP_DATABASE, GP_HOST, GP_PASSWORD, GP_PORT, GP_USER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class GreenplumLoader:
    """Загрузчик в PostgreSQL/Greenplum через COPY FROM STDIN."""

    def __init__(
        self,
        host: str = GP_HOST,
        port: int = GP_PORT,
        database: str = GP_DATABASE,
        user: str = GP_USER,
        password: str = GP_PASSWORD,
    ):
        self.conn_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        logger.info("GreenplumLoader настроен для %s:%s/%s", host, port, database)

    def _get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        if df.empty:
            logger.warning("Пустой DataFrame, вставка пропущена")
            return 0

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)

        columns = df.columns.tolist()
        copy_sql = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )

        conn = self._get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.copy_expert(copy_sql.as_string(conn), buffer)
            logger.info("Вставлено %s строк в %s", len(df), table_name)
            return len(df)
        except Exception as exc:
            logger.error("Ошибка COPY в %s: %s", table_name, exc)
            raise
        finally:
            conn.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            return pd.read_sql_query(query, conn, params=params)
        finally:
            conn.close()
