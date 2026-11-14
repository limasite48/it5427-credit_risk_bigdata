"""SOURCE 6 - Credit Card System: credit_card_balance.csv (monthly statement, linked by SK_ID_PREV)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from common.schemas import TableSpec
from ingestion.base import IngestResult, ingest_source

SOURCE_ID = "credit_card_system"


def validate_chunk(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    lim = pd.to_numeric(chunk["AMT_CREDIT_LIMIT_ACTUAL"], errors="coerce")
    bal = pd.to_numeric(chunk["AMT_BALANCE"], errors="coerce")
    return {"negative_limit": int((lim < 0).sum()), "zero_limit": int((lim == 0).sum()),
            "negative_balance": int((bal < 0).sum()),
            "months_balance_after_application": int((pd.to_numeric(chunk["MONTHS_BALANCE"], errors="coerce") > 0).sum())}


def run(src_dir: Path, force: bool = False, store=None) -> list[IngestResult]:
    return ingest_source(SOURCE_ID, src_dir, validate_chunk, force, store)
