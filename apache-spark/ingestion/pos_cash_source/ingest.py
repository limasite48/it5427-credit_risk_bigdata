"""SOURCE 5 - POS/Cash Loan System: POS_CASH_balance.csv (monthly, linked by SK_ID_PREV)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from common.schemas import TableSpec
from ingestion.base import IngestResult, ingest_source

SOURCE_ID = "pos_cash_system"


def validate_chunk(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    dpd = pd.to_numeric(chunk["SK_DPD"], errors="coerce")
    mb = pd.to_numeric(chunk["MONTHS_BALANCE"], errors="coerce")
    return {"negative_SK_DPD": int((dpd < 0).sum()), "months_balance_after_application": int((mb > 0).sum())}


def run(src_dir: Path, force: bool = False, store=None) -> list[IngestResult]:
    return ingest_source(SOURCE_ID, src_dir, validate_chunk, force, store)
