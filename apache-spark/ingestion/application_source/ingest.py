"""SOURCE 1 - Loan Application System: application_train.csv, application_test.csv (key SK_ID_CURR)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from common.schemas import TableSpec
from ingestion.base import IngestResult, ingest_source

SOURCE_ID = "loan_application_system"


def validate_chunk(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    out = {}
    if "TARGET" in chunk.columns:
        out["target_not_0_1"] = int((~chunk["TARGET"].isin(["0", "1"])).sum())
    if spec.name == "application_test" and "TARGET" in chunk.columns:
        out["unexpected_target_in_test"] = len(chunk)
    for c in ("AMT_INCOME_TOTAL", "AMT_CREDIT"):
        v = pd.to_numeric(chunk[c], errors="coerce")
        out[f"non_numeric_{c}"] = int((v.isna() & (chunk[c] != "")).sum())
        out[f"non_positive_{c}"] = int((v <= 0).sum())
    out["days_employed_365243"] = int((chunk["DAYS_EMPLOYED"].isin(["365243", "365243.0"])).sum())
    return out


def run(src_dir: Path, force: bool = False, store=None) -> list[IngestResult]:
    return ingest_source(SOURCE_ID, src_dir, validate_chunk, force, store)
