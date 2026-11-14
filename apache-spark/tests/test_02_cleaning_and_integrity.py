"""Requirements 3-4: primary/foreign keys and cleaning results. Expected numbers come from the fixture's
documented injections (fixture_injections.json), so each assertion checks a known defect is caught."""
from __future__ import annotations

from pyspark.sql import functions as F


def test_reconciliation_rows_in_equals_out_plus_removed(silver_metrics):
    for t, m in silver_metrics.items():
        assert m["reconciliation_ok"], t


def test_exact_duplicates_removed_as_injected(silver_metrics, fixture_meta):
    inj = fixture_meta["injections"]
    assert silver_metrics["application_train"]["exact_duplicates_removed"] == inj["application_train.exact_duplicates"]
    assert silver_metrics["installments_payments"]["exact_duplicates_removed"] == inj["installments_payments.exact_duplicates"]
    assert silver_metrics["POS_CASH_balance"]["exact_duplicates_removed"] == inj["POS_CASH_balance.exact_duplicates"]


def test_primary_keys_unique_in_silver(silver_metrics, read):
    from common.schemas import TABLES, silver_name
    for name, spec in TABLES.items():
        if spec.primary_key:
            df = read("silver", name)
            pk = [silver_name(c) for c in spec.primary_key]
            assert df.count() == df.select(*pk).distinct().count(), name


def test_orphans_flagged_not_dropped(silver_metrics, fixture_meta, read):
    inj = fixture_meta["injections"]
    assert silver_metrics["bureau"]["flags"]["_orphan_application"] == inj["bureau.orphan_sk_id_curr_rows"]
    assert silver_metrics["bureau_balance"]["flags"]["_orphan_bureau"] == inj["bureau_balance.orphan_rows"]
    assert silver_metrics["installments_payments"]["flags"]["_orphan_previous_application"] == \
        inj["installments_payments.orphan_sk_id_prev_rows"]
    orphans = read("silver", "bureau").filter("_orphan_application")
    assert orphans.filter(F.col("sk_id_curr") >= 999990001).count() == inj["bureau.orphan_sk_id_curr_rows"]


def test_missing_required_key_goes_to_quarantine(silver_metrics, read):
    assert silver_metrics["bureau"]["quarantined_required_null"] == 1
    q = read("silver", "_quarantine/bureau")
    assert q.filter(F.col("_reject_reason").contains("required_null:sk_id_curr")).count() == 1


def test_sentinel_365243_becomes_null_with_flag(silver_metrics, read, raw_dir):
    import pandas as pd
    raw = pd.read_csv(raw_dir / "application_train.csv").drop_duplicates()
    n_sentinel = int((raw["DAYS_EMPLOYED"] == 365243).sum())
    s = read("silver", "application_train")
    assert s.filter("days_employed_is_sentinel").count() == n_sentinel
    assert s.filter("days_employed = 365243").count() == 0
    assert s.filter("days_employed_is_sentinel AND days_employed IS NOT NULL").count() == 0


def test_placeholders_and_flags_normalised(silver_metrics, read):
    app = read("silver", "application_train")
    assert app.filter("code_gender = 'XNA'").count() == 0
    assert set(r[0] for r in app.select("flag_own_car").distinct().collect()) <= {0, 1}
    prev = read("silver", "previous_application")
    assert prev.filter("code_reject_reason = 'XAP'").count() > 0      # XAP (not applicable) is kept


def test_outlier_flagged_not_removed(silver_metrics, read, fixture_meta):
    oid = fixture_meta["injections"]["application_train.income_outlier_ids"][0]
    row = read("silver", "application_train").filter(F.col("sk_id_curr") == oid).first()
    assert row is not None and row["amt_income_total"] == 117000000.0 and row["amt_income_total_outlier"]


def test_after_application_rows_flagged(silver_metrics, fixture_meta):
    inj = fixture_meta["injections"]
    assert silver_metrics["bureau"]["flags"]["_after_application"] == inj["bureau.after_application_rows"]
    assert silver_metrics["installments_payments"]["flags"]["_after_application"] == \
        inj["installments_payments.after_application_rows"]


def test_no_column_dropped_in_silver(silver_metrics):
    for t, m in silver_metrics.items():
        assert len(m["missing"]) == m["columns_in"], t
