"""Requirements 1-2: files are readable, schemas are validated, ingestion is idempotent and detects drift."""
from __future__ import annotations

import pandas as pd

from common.schemas import REFERENCE_FILES, TABLES


def test_all_kaggle_files_present_and_readable(raw_dir):
    expected = [s.file for s in TABLES.values() if s.origin == "ORIGINAL"] + list(REFERENCE_FILES)
    assert len(expected) == 10
    for f in expected:
        df = pd.read_csv(raw_dir / f, nrows=5, encoding="latin-1" if f.startswith("HomeCredit") else "utf-8")
        assert len(df.columns) > 1, f


def test_every_table_ingested_with_correct_row_count(ingested, raw_dir, workdir):
    by_table = {r.table: r for r in ingested}
    for name, spec in TABLES.items():
        r = by_table[name]
        assert r.status == "SUCCESS", (name, r.error)
        src = (workdir / "synthetic_dir" if spec.origin == "SYNTHETIC" else raw_dir) / spec.file
        assert r.rows == len(pd.read_csv(src, dtype=str)), name


def test_bronze_copy_is_byte_identical_and_separated_by_origin(ingested, raw_dir, workdir):
    from ingestion.base import sha256_of
    for r in ingested:
        dst = workdir / "local_lake_dir" / r.bronze_key
        assert dst.exists()
        assert sha256_of(dst) == r.sha256
        origin = TABLES[r.table].origin.lower()
        assert r.bronze_key.startswith(f"bronze/{origin}/{TABLES[r.table].source_id}/")


def test_required_columns_present_in_headers(raw_dir):
    from ingestion.base import read_header
    for spec in TABLES.values():
        if spec.origin == "ORIGINAL":
            header = read_header(raw_dir / spec.file)
            assert set(spec.required) <= set(header), spec.name


def test_silver_types_follow_registry(silver_metrics, read):
    app = dict(read("silver", "application_train").dtypes)
    assert app["sk_id_curr"] == "bigint" and app["amt_credit"] == "double" and app["target"] == "int"
    assert app["flag_own_car"] == "int"                       # Y/N normalised
    bb = dict(read("silver", "bureau_balance").dtypes)
    assert bb["months_balance"] == "int" and bb["status"] == "string"
    assert all(not m["cast_failures"] for m in silver_metrics.values())


def test_reingest_same_file_is_skipped(ingested, raw_dir):
    from ingestion import run_ingestion
    again = run_ingestion.run(raw_dir, sources=["external_credit_bureau"])
    assert {r.status for r in again} == {"SKIPPED_ALREADY_INGESTED"}


def test_schema_drift_detected_and_missing_required_column_rejected(ingested, raw_dir, tmp_path):
    """Runs against a separate scratch lake so the session lake is not modified."""
    from common.config import load_config
    from common.lake import LakeStore
    from ingestion.base import ingest_table
    cfg = load_config()
    cfg["paths"]["local_lake_dir"] = str(tmp_path / "lake")
    store = LakeStore(cfg)
    spec = TABLES["bureau_balance"]
    df = pd.read_csv(raw_dir / spec.file, dtype=str).head(100)
    for name, frame in {"v1": df, "drift": df.assign(NEW_COLUMN="x"), "broken": df.drop(columns=["STATUS"])}.items():
        (tmp_path / name).mkdir()
        frame.to_csv(tmp_path / name / spec.file, index=False)

    assert ingest_table(spec, tmp_path / "v1" / spec.file, store=store).status == "SUCCESS"
    res = ingest_table(spec, tmp_path / "drift" / spec.file, store=store)
    assert res.status == "SUCCESS_WITH_DRIFT"
    assert res.schema_drift["added_columns"] == ["NEW_COLUMN"]

    res = ingest_table(spec, tmp_path / "broken" / spec.file, store=store)
    assert res.status == "FAILED" and "STATUS" in res.error
    statuses = [r["status"] for r in store.read_jsonl("bronze/_manifests/bureau_balance.jsonl")]
    assert statuses == ["SUCCESS", "SUCCESS_WITH_DRIFT", "FAILED"]          # every attempt is logged
