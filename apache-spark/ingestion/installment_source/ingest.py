"""SOURCE 4 - Installment Payment System: installments_payments.csv (linked by SK_ID_PREV)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from common.schemas import TableSpec
from ingestion.base import IngestResult, ingest_source

SOURCE_ID = "installment_payment_system"


def validate_chunk(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    inst = pd.to_numeric(chunk["AMT_INSTALMENT"], errors="coerce")
    pay = pd.to_numeric(chunk["AMT_PAYMENT"], errors="coerce")
    entry = pd.to_numeric(chunk["DAYS_ENTRY_PAYMENT"], errors="coerce")
    return {"negative_AMT_INSTALMENT": int((inst < 0).sum()), "negative_AMT_PAYMENT": int((pay < 0).sum()),
            "missing_payment": int((chunk["AMT_PAYMENT"] == "").sum()),
            "payment_after_application": int((entry > 0).sum())}


def run(src_dir: Path, force: bool = False, store=None) -> list[IngestResult]:
    return ingest_source(SOURCE_ID, src_dir, validate_chunk, force, store)
