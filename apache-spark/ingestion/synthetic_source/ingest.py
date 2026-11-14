"""SOURCE 7 - SYNTHETIC customer behaviour (traffic profile). Stored under bronze/synthetic/, never mixed with originals."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from common.schemas import TableSpec
from ingestion.base import IngestResult, ingest_source

SOURCE_ID = "synthetic_customer_behavior"


def validate_chunk(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    total = pd.to_numeric(chunk["TRAFFIC_VIOLATION_COUNT"], errors="coerce")
    major = pd.to_numeric(chunk["MAJOR_VIOLATION_COUNT"], errors="coerce")
    return {"not_labelled_SYNTHETIC": int((chunk["DATA_ORIGIN"] != "SYNTHETIC").sum()),
            "major_gt_total": int((major > total).sum()),
            "duplicate_SK_ID_CURR": int(chunk["SK_ID_CURR"].duplicated().sum())}


def run(src_dir: Path, force: bool = False, store=None) -> list[IngestResult]:
    return ingest_source(SOURCE_ID, src_dir, validate_chunk, force, store)
