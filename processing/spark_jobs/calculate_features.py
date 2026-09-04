"""PySpark job: расчёт технических индикаторов из Data Lake."""

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    SPARK_DRIVER_MEMORY,
    SPARK_MASTER,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Создаёт SparkSession с конфигурацией для MinIO (S3)."""
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

    builder = (
        SparkSession.builder
        .appName("FinancialFeaturesCalculation")
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
    )

    if sys.platform == "win32" and os.path.exists(r"C:\hadoop"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop"

    return builder.getOrCreate()


def calculate_features(spark: SparkSession, input_glob: str = None, output_path: str = None):
    """Основная логика расчёта фичей."""
    bucket = MINIO_BUCKET
    input_path = input_glob or f"s3a://{bucket}/*/*/*/*/*/*.jsonl"
    output = output_path or f"s3a://{bucket}/processed/features"

    logger.info("Чтение данных из %s", input_path)
    df_raw = spark.read.json(input_path)

    if df_raw.rdd.isEmpty():
        logger.warning("Нет данных для обработки")
        return

    df_raw = (
        df_raw
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("open", F.col("open").cast("double"))
        .withColumn("high", F.col("high").cast("double"))
        .withColumn("low", F.col("low").cast("double"))
        .withColumn("volume", F.col("volume").cast("double"))
    )

    w_symbol = Window.partitionBy("symbol").orderBy("ts")
    w_7 = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-6, 0)
    w_30 = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-29, 0)
    w_14 = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-13, 0)
    w_vol = Window.partitionBy("symbol").orderBy("ts").rowsBetween(-13, 0)

    df = (
        df_raw
        .withColumn("prev_close", F.lag("close", 1).over(w_symbol))
        .withColumn("log_return", F.log(F.col("close") / F.col("prev_close")))
        .withColumn("sma_7", F.avg("close").over(w_7))
        .withColumn("sma_30", F.avg("close").over(w_30))
        .withColumn("ema_14", F.avg("close").over(w_14))
        .withColumn("volatility_14d", F.stddev("log_return").over(w_14))
        .withColumn("volatility_30d", F.stddev("log_return").over(w_30))
        .withColumn("avg_volume", F.avg("volume").over(w_symbol))
        .withColumn("std_volume", F.stddev("volume").over(w_symbol))
        .withColumn(
            "volume_zscore",
            (F.col("volume") - F.col("avg_volume")) / F.col("std_volume"),
        )
        .withColumn("price_range_pct", (F.col("high") - F.col("low")) / F.col("close"))
    )

    # RSI(14)
    df = (
        df
        .withColumn("gain", F.when(F.col("log_return") > 0, F.col("log_return")).otherwise(0))
        .withColumn(
            "loss",
            F.when(F.col("log_return") < 0, F.abs(F.col("log_return"))).otherwise(0),
        )
        .withColumn("avg_gain", F.avg("gain").over(w_14))
        .withColumn("avg_loss", F.avg("loss").over(w_14))
        .withColumn(
            "rsi_14",
            F.when(F.col("avg_loss") == 0, 100.0).otherwise(
                100 - 100 / (1 + F.col("avg_gain") / F.col("avg_loss"))
            ),
        )
    )

    df_features = (
        df
        .filter(F.col("prev_close").isNotNull())
        .withColumn("calculated_at", F.current_timestamp())
        .select(
            "ts", "symbol", "asset_type", "close",
            "log_return", "sma_7", "sma_30", "ema_14",
            "volatility_14d", "volatility_30d", "rsi_14",
            "volume_zscore", "price_range_pct", "calculated_at",
        )
    )

    count = df_features.count()
    logger.info("Рассчитано %s строк фичей", count)
    df_features.show(5, truncate=False)

    logger.info("Запись фичей в %s", output)
    df_features.write.mode("overwrite").partitionBy("asset_type", "symbol").parquet(output)
    logger.info("PySpark job завершён успешно")


if __name__ == "__main__":
    spark = create_spark_session()
    try:
        calculate_features(spark)
    finally:
        spark.stop()
