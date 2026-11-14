"""Requirement 12: the batch pipeline can be re-run without creating duplicated or different data."""
from __future__ import annotations

import pandas as pd


def _snapshot(read):
    p = read("gold", "integrated_credit_profile").toPandas().sort_values("sk_id_curr").reset_index(drop=True)
    return p.drop(columns=[c for c in p.columns if c.startswith("_")])


def test_rerun_is_idempotent(gold, raw_dir, read):
    before = _snapshot(read)
    silver_before = {t: read("silver", t).count() for t in ("bureau", "installments_payments", "POS_CASH_balance")}

    from ingestion import run_ingestion
    from processing.cleaning import clean_silver
    from processing.integration import build_gold
    res = run_ingestion.run(raw_dir)
    assert {r.status for r in res} == {"SKIPPED_ALREADY_INGESTED"}
    clean_silver.run()
    build_gold.run()

    assert {t: read("silver", t).count() for t in silver_before} == silver_before
    after = _snapshot(read)
    pd.testing.assert_frame_equal(before, after, check_like=True)


def test_forced_reingest_does_not_duplicate_silver(gold, raw_dir, read):
    from ingestion import run_ingestion
    from processing.cleaning import clean_silver
    n = read("silver", "bureau").count()
    res = run_ingestion.run(raw_dir, sources=["external_credit_bureau"], force=True)
    assert {r.status for r in res} == {"SUCCESS"}                      # a second Bronze snapshot exists...
    clean_silver.run(["bureau", "bureau_balance"])
    assert read("silver", "bureau").count() == n                      # ...but Silver reads only the latest one
