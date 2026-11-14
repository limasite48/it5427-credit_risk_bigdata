"""SparkSession factory. Adds MinIO (s3a), Kafka and Delta settings only when they are enabled."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from common.config import load_config

_SESSION: SparkSession | None = None


def get_spark(app_name: str = "credit-risk", need_kafka: bool = False) -> SparkSession:
    global _SESSION
    if _SESSION is not None and _SESSION.sparkContext._jsc is not None:  # noqa: SLF001
        return _SESSION

    cfg = load_config()
    sc = cfg["spark"]
    lake = cfg["lake"]
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", sc["master"]))
        .config("spark.driver.memory", sc["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(sc["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
    )

    packages: list[str] = []
    if lake["backend"] == "s3a":
        packages.append(sc["packages"]["s3a"])
        m = cfg["minio"]
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", m["endpoint"])
            .config("spark.hadoop.fs.s3a.access.key", m["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", m["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(m["endpoint"].startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    if need_kafka:
        packages.append(sc["packages"]["kafka"])
    if lake["format"] == "delta":
        packages.append(sc["packages"]["delta"])
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    _SESSION = builder.getOrCreate()
    _SESSION.sparkContext.setLogLevel("WARN")
    return _SESSION


def stop_spark() -> None:
    global _SESSION
    if _SESSION is not None:
        _SESSION.stop()
        _SESSION = None
