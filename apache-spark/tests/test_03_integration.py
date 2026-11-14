"""Requirements 5-8: aggregates are correct, joins do not multiply rows, one row per SK_ID_CURR, no TARGET leakage.
Aggregates are recomputed independently with pandas from the RAW fixture files and compared to Gold."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest


@pytest.fixture(scope="module")
def profile(gold, read):
    return read("gold", "integrated_credit_profile").toPandas().set_index("sk_id_curr")


@pytest.fixture(scope="module")
def raw(raw_dir):
    return {n: pd.read_csv(raw_dir / f"{n}.csv") for n in
            ("application_train", "application_test", "bureau", "installments_payments", "previous_application",
             "credit_card_balance")}


def test_all_integration_checks_passed(gold):
    failed = [c for c in gold["checks"]["checks"] if not c["passed"]]
    assert not failed, failed


def test_one_row_per_application_and_unique_key(profile, raw):
    apps = pd.concat([raw["application_train"], raw["application_test"]]).drop_duplicates()
    assert len(profile) == apps["SK_ID_CURR"].nunique()
    assert profile.index.is_unique


def test_loan_level_unique(gold, read):
    loan = read("gold", "loan_credit_features")
    assert loan.count() == loan.select("sk_id_prev").distinct().count()


def test_bureau_aggregates_match_pandas(profile, raw):
    b = raw["bureau"].dropna(subset=["SK_ID_CURR"])
    b = b[b["DAYS_CREDIT"] <= 0]                                      # leakage filter
    exp = b.groupby("SK_ID_CURR").agg(cnt=("SK_ID_BUREAU", "size"), active=("CREDIT_ACTIVE", lambda s: (s == "Active").sum()),
                                      credit=("AMT_CREDIT_SUM", "sum"))
    exp = exp[exp.index.isin(profile.index)]
    got = profile.loc[exp.index]
    assert (got["bureau_loan_count"].values == exp["cnt"].values).all()
    assert (got["bureau_active_loan_count"].values == exp["active"].values).all()
    assert np.allclose(got["bureau_total_credit"].values, exp["credit"].values)


def test_installment_aggregates_match_pandas(profile, raw):
    prev_ids = set(raw["previous_application"]["SK_ID_PREV"])
    i = raw["installments_payments"].drop_duplicates()
    i = i[i["SK_ID_PREV"].isin(prev_ids) & ~(i["DAYS_ENTRY_PAYMENT"] > 0) & ~(i["DAYS_INSTALMENT"] > 0)]
    lvl = i.groupby(["SK_ID_CURR", "SK_ID_PREV", "NUM_INSTALMENT_VERSION", "NUM_INSTALMENT_NUMBER"]).agg(
        due=("DAYS_INSTALMENT", "min"), inst=("AMT_INSTALMENT", "max"), paid=("AMT_PAYMENT", lambda s: s.fillna(0).sum()),
        last=("DAYS_ENTRY_PAYMENT", "max")).reset_index()
    lvl["late"] = (lvl["last"] > lvl["due"]).astype(int)
    lvl["short"] = (lvl["inst"].fillna(0) - lvl["paid"]).clip(lower=0)
    exp = lvl.groupby("SK_ID_CURR").agg(n=("late", "size"), late=("late", "sum"), paid=("paid", "sum"), short=("short", "sum"))
    got = profile.loc[exp.index]
    assert (got["installment_count"].values == exp["n"].values).all()
    assert (got["late_payment_count"].values == exp["late"].values).all()
    assert np.allclose(got["total_payment_amount"].values, exp["paid"].values)
    assert np.allclose(got["payment_shortfall"].values, exp["short"].values)
    assert np.allclose(got["payment_delay_ratio"].values, (exp["late"] / exp["n"]).values)


def test_partial_payments_counted_once_per_installment(gold, read):
    """Several payment rows for one installment must not inflate installment_count (grain fix)."""
    raw_rows = read("silver", "installments_payments").filter("NOT _after_application AND NOT _orphan_previous_application").count()
    n_inst = read("gold", "customer_credit_features").agg({"installment_count": "sum"}).first()[0]
    assert n_inst < raw_rows


def test_customers_without_history_get_zero_counts_and_null_ratios(profile, raw):
    no_bureau = set(profile.index) - set(raw["bureau"]["SK_ID_CURR"].dropna().astype(int))
    sub = profile.loc[sorted(no_bureau)]
    assert (sub["bureau_loan_count"] == 0).all()
    assert sub["bureau_avg_credit"].isna().all()


def test_engineered_features(profile):
    p = profile.dropna(subset=["amt_credit", "amt_income_total"])
    assert np.allclose(p["loan_to_income"], p["amt_credit"] / p["amt_income_total"])
    assert profile.loc[profile["days_employed_is_sentinel"], "employment_years"].isna().all()
    assert set(profile["has_previous_overdue"].unique()) <= {0, 1}


def test_target_not_in_features(profile, gold, workdir, read, raw):
    assert "target" not in [c.lower() for c in profile.columns]
    assert "target" not in [c.lower() for c in read("gold", "customer_credit_features").columns]
    feats = json.loads((workdir / "reports_dir" / "data_quality" / "feature_columns.json").read_text())["features"]
    assert "target" not in [f.lower() for f in feats]
    labels = read("gold", "training_labels")
    assert set(labels.columns) == {"sk_id_curr", "target"}
    label_ids = {r[0] for r in labels.select("sk_id_curr").collect()}
    assert label_ids <= set(raw["application_train"]["SK_ID_CURR"])            # labels only for train
    assert not label_ids & set(raw["application_test"]["SK_ID_CURR"])


def test_rows_after_application_excluded(profile, raw, fixture_meta):
    b = raw["bureau"]
    future_cust = int(b.loc[b["DAYS_CREDIT"] > 0, "SK_ID_CURR"].iloc[0])
    valid = b[(b["SK_ID_CURR"] == future_cust) & (b["DAYS_CREDIT"] <= 0)]
    assert profile.loc[future_cust, "bureau_loan_count"] == len(valid)


def test_synthetic_source_is_labelled_and_linked_to_real_ids(silver_metrics, read, raw):
    s = read("silver", "synthetic_traffic_profile").toPandas()
    ids = set(pd.concat([raw["application_train"], raw["application_test"]])["SK_ID_CURR"])
    assert set(s["sk_id_curr"]) <= ids and s["sk_id_curr"].is_unique
    assert (s["data_origin"] == "SYNTHETIC").all() and (s["_origin"] == "SYNTHETIC").all()
    assert (s["major_violation_count"] <= s["traffic_violation_count"]).all()


def test_synthetic_generator_is_deterministic(raw_dir, tmp_path):
    from ingestion.synthetic_source.generator import generate
    generate(raw_dir, tmp_path / "a", seed=5)
    generate(raw_dir, tmp_path / "b", seed=5)
    a = pd.read_csv(tmp_path / "a" / "synthetic_traffic_profile.csv")
    b = pd.read_csv(tmp_path / "b" / "synthetic_traffic_profile.csv")
    pd.testing.assert_frame_equal(a, b)
