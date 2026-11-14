"""Data-lake addressing and I/O.

Layout (identical on local disk and on MinIO; on MinIO the first segment is the bucket):

    bronze/original/<source_id>/<table>/ingest_id=<id>/<file>.csv      raw Kaggle files, byte-identical
    bronze/synthetic/<source_id>/<table>/ingest_id=<id>/<file>.csv     generated data, kept apart
    bronze/_manifests/<table>.jsonl                                     ingestion history (append-only)
    silver/<table>/                                                     cleaned, typed parquet
    silver/_quarantine/<table>/                                         rows rejected by validation
    silver/stream_events/source_id=<..>/event_date=<..>/                streaming events (dedup'd)
    gold/<table>/                                                       integrated tables / features
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from common.config import load_config, path_of

LAYERS = ("bronze", "silver", "gold")


def spark_uri(layer: str, *parts: str, cfg: dict | None = None) -> str:
    """URI Spark uses to read/write a lake object."""
    cfg = cfg or load_config()
    if layer not in LAYERS:
        raise ValueError(f"unknown layer {layer}")
    if cfg["lake"]["backend"] == "s3a":
        bucket = cfg["lake"]["buckets"][layer]
        return "s3a://" + "/".join([bucket, *parts])
    return str(path_of(cfg, "local_lake_dir").joinpath(layer, *parts))


def local_layer_dir(layer: str, cfg: dict | None = None) -> Path:
    cfg = cfg or load_config()
    return path_of(cfg, "local_lake_dir") / layer


def write_table(df, layer: str, table: str, mode: str = "overwrite", partition_by: list[str] | None = None,
                num_files: int | None = None, cfg: dict | None = None) -> str:
    """Write a DataFrame to the lake in the configured format (parquet|delta)."""
    cfg = cfg or load_config()
    fmt = cfg["lake"]["format"]
    uri = spark_uri(layer, table, cfg=cfg)
    if num_files:
        df = df.repartition(num_files, *partition_by) if partition_by else df.repartition(num_files)
    writer = df.write.format(fmt).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if fmt == "delta":
        writer = writer.option("overwriteSchema", "true")
    writer.save(uri)
    return uri


def read_table(spark, layer: str, table: str, cfg: dict | None = None):
    cfg = cfg or load_config()
    return spark.read.format(cfg["lake"]["format"]).load(spark_uri(layer, table, cfg=cfg))


def files_for_size(size_bytes: int, cfg: dict | None = None) -> int:
    """Number of output files so that each is ~target_file_mb (min 1)."""
    cfg = cfg or load_config()
    target = cfg["lake"]["target_file_mb"] * 1024 * 1024
    return max(1, int(round(size_bytes / target)))


# --------------------------------------------------------------------------------------
# Python-side object store (used by ingestion, which copies raw files without Spark)
# --------------------------------------------------------------------------------------
class LakeStore:
    """Minimal put/get/list API over local disk or S3 (MinIO)."""

    def __init__(self, cfg: dict | None = None):
        self.cfg = cfg or load_config()
        self.backend = self.cfg["lake"]["backend"]
        self._s3 = None
        if self.backend == "s3a":
            from storage.minio.client import ensure_buckets, make_s3_client

            ensure_buckets(self.cfg)          # docker-compose no longer pre-creates buckets
            self._s3 = make_s3_client(self.cfg)

    # key = "<layer>/<rest/of/path>"
    def _split(self, key: str) -> tuple[str, str]:
        layer, _, rest = key.partition("/")
        return self.cfg["lake"]["buckets"][layer], rest

    def local_path(self, key: str) -> Path:
        return path_of(self.cfg, "local_lake_dir") / key

    def exists(self, key: str) -> bool:
        if self._s3 is None:
            return self.local_path(key).exists()
        bucket, rest = self._split(key)
        resp = self._s3.list_objects_v2(Bucket=bucket, Prefix=rest, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0

    def put_file(self, src: Path, key: str) -> None:
        """Copy a file into the lake atomically (tmp + rename locally; single PUT on S3)."""
        if self._s3 is None:
            dst = self.local_path(key)
            dst.parent.mkdir(parents=True, exist_ok=True)
            tmp = dst.with_suffix(dst.suffix + ".tmp")
            shutil.copyfile(src, tmp)
            tmp.replace(dst)
            return
        bucket, rest = self._split(key)
        self._s3.upload_file(str(src), bucket, rest)

    def put_text(self, text: str, key: str) -> None:
        if self._s3 is None:
            dst = self.local_path(key)
            dst.parent.mkdir(parents=True, exist_ok=True)
            tmp = dst.with_suffix(dst.suffix + ".tmp")
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(dst)
            return
        bucket, rest = self._split(key)
        self._s3.put_object(Bucket=bucket, Key=rest, Body=text.encode("utf-8"))

    def get_text(self, key: str) -> str | None:
        if self._s3 is None:
            p = self.local_path(key)
            return p.read_text(encoding="utf-8") if p.exists() else None
        bucket, rest = self._split(key)
        try:
            return self._s3.get_object(Bucket=bucket, Key=rest)["Body"].read().decode("utf-8")
        except self._s3.exceptions.NoSuchKey:
            return None

    def append_jsonl(self, record: dict, key: str) -> None:
        existing = self.get_text(key) or ""
        self.put_text(existing + json.dumps(record, ensure_ascii=False, default=str) + "\n", key)

    def read_jsonl(self, key: str) -> list[dict]:
        text = self.get_text(key)
        return [json.loads(line) for line in text.splitlines() if line.strip()] if text else []
