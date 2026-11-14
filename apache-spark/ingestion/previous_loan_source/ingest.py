"""SOURCE 3 - Previous Loan System: previous_application.csv (SK_ID_CURR, SK_ID_PREV)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from common.schemas import TableSpec
from ingestion.base import IngestResult, ingest_source

SOURCE_ID = "previous_loan_system"
STATUS = {"Approved", "Canceled", "Refused", "Unused offer"}


def validate_chunk(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    return {"unknown_NAME_CONTRACT_STATUS": int((~chunk["NAME_CONTRACT_STATUS"].isin(STATUS)).sum()),
            "decision_after_application": int((pd.to_numeric(chunk["DAYS_DECISION"], errors="coerce") > 0).sum()),
            "sentinel_365243_values": int(chunk.isin(["365243", "365243.0"]).sum().sum())}


def run(src_dir: Path, force: bool = False, store=None) -> list[IngestResult]:
    return ingest_source(SOURCE_ID, src_dir, validate_chunk, force, store)
