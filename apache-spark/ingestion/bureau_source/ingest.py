"""SOURCE 2 - External Credit Bureau: bureau.csv (SK_ID_CURR, SK_ID_BUREAU), bureau_balance.csv (SK_ID_BUREAU)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from common.schemas import TableSpec
from ingestion.base import IngestResult, ingest_source

SOURCE_ID = "external_credit_bureau"
CREDIT_ACTIVE = {"Active", "Closed", "Sold", "Bad debt"}
STATUS = {"C", "X", "0", "1", "2", "3", "4", "5"}


def validate_chunk(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    if spec.name == "bureau":
        return {"unknown_CREDIT_ACTIVE": int((~chunk["CREDIT_ACTIVE"].isin(CREDIT_ACTIVE)).sum()),
                "days_credit_after_application": int((pd.to_numeric(chunk["DAYS_CREDIT"], errors="coerce") > 0).sum())}
    return {"unknown_STATUS": int((~chunk["STATUS"].isin(STATUS)).sum()),
            "months_balance_after_application": int((pd.to_numeric(chunk["MONTHS_BALANCE"], errors="coerce") > 0).sum())}


def run(src_dir: Path, force: bool = False, store=None) -> list[IngestResult]:
    return ingest_source(SOURCE_ID, src_dir, validate_chunk, force, store)
