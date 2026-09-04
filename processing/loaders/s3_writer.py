import logging
from typing import Optional

import boto3
import pandas as pd

from config.settings import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class S3Writer:
    """Запись данных в S3-совместимое хранилище (MinIO)."""

    def __init__(
        self,
        endpoint_url: str = MINIO_ENDPOINT,
        bucket: str = MINIO_BUCKET,
        access_key: str = MINIO_ACCESS_KEY,
        secret_key: str = MINIO_SECRET_KEY,
    ):
        self.bucket = bucket
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self._ensure_bucket()
        logger.info("S3Writer инициализирован, bucket=%s", self.bucket)

    def _ensure_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except Exception:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket)
                logger.info("Создан бакет %s", self.bucket)
            except Exception as exc:
                logger.warning("Не удалось создать бакет %s: %s", self.bucket, exc)

    def write_partition(
        self,
        df: pd.DataFrame,
        source: str,
        asset_type: str,
        ts_column: str = "ts",
    ) -> Optional[str]:
        if df.empty:
            logger.warning("Пустой DataFrame, запись пропущена")
            return None

        first_ts = pd.to_datetime(df[ts_column].iloc[0])
        year = first_ts.strftime("%Y")
        month = first_ts.strftime("%m")
        day = first_ts.strftime("%d")
        hour = first_ts.strftime("%H")

        s3_key = (
            f"{source}/{asset_type}/year={year}/month={month}/"
            f"day={day}/hour={hour}/data.jsonl"
        )

        json_lines = df.to_json(orient="records", lines=True, date_format="iso")

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json_lines.encode("utf-8"),
            ContentType="application/jsonlines",
        )

        logger.info("Записано %s записей в s3://%s/%s", len(df), self.bucket, s3_key)
        return s3_key

    def list_partitions(self, prefix: str = "") -> list:
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        contents = response.get("Contents") or []
        objects = [obj["Key"] for obj in contents]
        logger.info("Найдено %s объектов в s3://%s/%s", len(objects), self.bucket, prefix)
        return objects
