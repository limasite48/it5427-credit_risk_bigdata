"""Test harness: a small TEST FIXTURE (schema-faithful random data) is generated in a temp dir and the whole
batch pipeline is run once per session against an isolated lake/reports/state directory.
Nothing under the project's data/, reports/ or docs/ is touched."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

N_CUSTOMERS = 500

_ISOLATED = ["raw_dir", "synthetic_dir", "local_lake_dir", "reports_dir", "docs_dir", "checkpoint_dir",
             "stream_landing_dir", "state_dir"]


@pytest.fixture(scope="session")
def workdir(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("crb")
    for key in _ISOLATED:
        os.environ[f"CRB_PATHS_{key.upper()}"] = str(root / key)
    os.environ["CRB_LAKE_BACKEND"] = "local"
    os.environ["CRB_LAKE_FORMAT"] = "parquet"
    os.environ["CRB_SPARK_SHUFFLE_PARTITIONS"] = "4"
    os.environ["CRB_SPARK_MASTER"] = "local[2]"
    return root


@pytest.fixture(scope="session")
def fixture_meta(workdir) -> dict:
    from scripts.make_test_fixture import main as make_fixture
    return make_fixture(workdir / "raw_dir", N_CUSTOMERS, seed=7)


@pytest.fixture(scope="session")
def raw_dir(workdir, fixture_meta) -> Path:
    return workdir / "raw_dir"


@pytest.fixture(scope="session")
def ingested(raw_dir, workdir):
    from ingestion import run_ingestion
    from ingestion.synthetic_source.generator import generate
    generate(raw_dir, workdir / "synthetic_dir", seed=11)
    return run_ingestion.run(raw_dir, workdir / "synthetic_dir")


@pytest.fixture(scope="session")
def silver_metrics(ingested):
    from processing.cleaning import clean_silver
    return {m["table"]: m for m in clean_silver.run()}


@pytest.fixture(scope="session")
def gold(silver_metrics, workdir):
    from processing.integration import build_gold
    stats = build_gold.run()
    checks = json.loads((workdir / "reports_dir" / "data_quality" / "gold_checks.json").read_text())
    return {"stats": stats, "checks": checks}


@pytest.fixture(scope="session")
def spark(workdir):
    from common.spark import get_spark
    return get_spark("tests")


@pytest.fixture(scope="session")
def read(workdir):
    """Always resolves the CURRENT SparkSession (the streaming consumer stops its session when it ends)."""
    from common.lake import read_table
    from common.spark import get_spark
    return lambda layer, table: read_table(get_spark("tests"), layer, table)
