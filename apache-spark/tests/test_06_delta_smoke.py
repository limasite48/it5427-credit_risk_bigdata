"""Delta Lake configuration & read/write smoke test.

Always verifies config override wiring (`CRB_LAKE_FORMAT=delta`).
Executes a real Delta Lake read/write round-trip when `CRB_TEST_DELTA=1`
(used in environments with Spark + `delta-spark` jars available, e.g. Colab or Docker).
"""
from __future__ import annotations

import os

import pytest

from common.config import load_config


def test_delta_format_config_override(monkeypatch):
    monkeypatch.setenv("CRB_LAKE_FORMAT", "delta")
    cfg = load_config()
    assert cfg["lake"]["format"] == "delta"
    assert "delta" in cfg["spark"]["packages"]


@pytest.mark.skipif(
    os.environ.get("CRB_TEST_DELTA", "0") != "1",
    reason="Set CRB_TEST_DELTA=1 in a Spark environment with delta-spark jars to run live Delta I/O",
)
def test_delta_table_roundtrip(tmp_path, monkeypatch):
    from common.lake import read_table, write_table
    from common.spark import get_spark, stop_spark

    stop_spark()
    monkeypatch.setenv("CRB_LAKE_FORMAT", "delta")
    monkeypatch.setenv("CRB_LAKE_BACKEND", "local")
    monkeypatch.setenv("CRB_PATHS_LOCAL_LAKE_DIR", str(tmp_path / "delta_lake"))
    cfg = load_config()
    spark = get_spark("delta-smoke")
    try:
        df = spark.createDataFrame([(100001, 0.041), (100002, 0.215)], ["sk_id_curr", "pd"])
        write_table(df, "gold", "delta_smoke_check", mode="overwrite", num_files=1, cfg=cfg)
        loaded = read_table(spark, "gold", "delta_smoke_check", cfg=cfg)
        assert loaded.count() == 2
        assert (tmp_path / "delta_lake" / "gold" / "delta_smoke_check" / "_delta_log").exists()
    finally:
        stop_spark()
