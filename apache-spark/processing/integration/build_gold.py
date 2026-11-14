"""Silver -> Gold integration (Spark). The core of the project.

Step 1  grain of every table is fixed in common/schemas.py and verified by profiling.
Step 2  each child table is aggregated to SK_ID_CURR (customer) or SK_ID_PREV (previous loan) FIRST.
Step 3  aggregates are LEFT JOINed to the application base; row counts are checked before/after each
        join and the pipeline FAILS if a join multiplies rows or a key is duplicated.
Step 4  engineered features (processing/feature_engineering/features.py).
Step 5  outputs: gold/loan_credit_features, gold/customer_credit_features,
        gold/integrated_credit_profile (no TARGET), gold/training_labels (TARGET only),
        gold/data_quality_report (+ reports/data_quality/gold_checks.json).

Leakage guards: rows dated after the application are excluded (`_after_application`), TARGET is never
joined into a feature table, and the feature list is checked for TARGET before writing.

    python -m processing.integration.build_gold
"""
from __future__ import annotations

import json
import time

from pyspark.sql import DataFrame, functions as F

from common.config import load_config, path_of
from common.lake import read_table, write_table
from common.logging_utils import get_logger
from common.spark import get_spark
from processing.aggregation import aggregates as A
from processing.feature_engineering.features import COUNT_FEATURES, add_engineered_features

log = get_logger("gold")
SYNTHETIC_FEATURES = ["traffic_violation_count", "major_violation_count", "total_traffic_fine", "accident_count"]
INTERNAL_PREFIXES = ("_",)


class IntegrationError(RuntimeError):
    pass


class Checks:
    def __init__(self):
        self.items: list[dict] = []

    def add(self, name: str, ok: bool, **details):
        self.items.append({"check": name, "passed": bool(ok), **details})
        (log.info if ok else log.error)("check %-55s %s %s", name, "PASS" if ok else "FAIL", details)

    def assert_unique(self, df: DataFrame, key: str, name: str) -> int:
        n, d = df.count(), df.select(key).distinct().count()
        self.add(f"{name}: one row per {key}", n == d, rows=n, distinct_keys=d)
        return n

    def failed(self):
        return [c for c in self.items if not c["passed"]]


def _left_join_checked(base: DataFrame, other: DataFrame, key: str, name: str, checks: Checks) -> DataFrame:
    before = base.count()
    out = base.join(other, key, "left")
    after = out.count()
    checks.add(f"join {name}: row count unchanged", before == after, before=before, after=after)
    return out


def build(spark, cfg) -> dict:
    t0 = time.time()
    checks = Checks()
    silver = lambda t: read_table(spark, "silver", t, cfg)  # noqa: E731

    # ---------------- base: one row per current application (train + test)
    train = silver("application_train").withColumn("dataset_split", F.lit("train"))
    test = silver("application_test").withColumn("dataset_split", F.lit("test"))
    labels = train.select("sk_id_curr", F.col("target").cast("int").alias("target"))
    app = train.drop("target").unionByName(test, allowMissingColumns=True)
    app = app.drop(*[c for c in app.columns if c.startswith(INTERNAL_PREFIXES)])
    n_app = checks.assert_unique(app, "sk_id_curr", "application base (train+test)")

    # ---------------- step 2: usable child rows (leakage + integrity filters), counted
    raw = {t: silver(t) for t in ("bureau", "bureau_balance", "previous_application", "installments_payments",
                                  "POS_CASH_balance", "credit_card_balance")}
    filters = {
        "bureau": ("_orphan_application",),
        "bureau_balance": ("_orphan_bureau",),
        "previous_application": ("_orphan_application",),
        "installments_payments": ("_orphan_previous_application", "_orphan_application"),
        "POS_CASH_balance": ("_orphan_previous_application", "_orphan_application"),
        "credit_card_balance": ("_orphan_previous_application", "_orphan_application"),
    }
    use, excluded = {}, {}
    for t, df in raw.items():
        use[t] = A.usable(df, filters[t]).cache()
        n_all, n_use = df.count(), use[t].count()
        excluded[t] = {"rows_silver": n_all, "rows_used": n_use,
                       "excluded_after_application": df.filter("_after_application").count(),
                       "excluded_total": n_all - n_use}
    checks.add("leakage: no child row dated after application is used",
               all(use[t].filter("_after_application").count() == 0 for t in use), excluded=excluded)

    # ---------------- loan level (grain SK_ID_PREV)
    inst_lvl = A.installment_level(use["installments_payments"]).cache()
    prev = use["previous_application"]
    loan = prev.select("sk_id_prev", "sk_id_curr", "name_contract_type", "name_contract_status", "amt_application",
                       "amt_credit", "amt_annuity", "cnt_payment", "days_decision", "name_client_type")
    checks.assert_unique(loan, "sk_id_prev", "previous_application base")
    loan_parts = {
        "installments@loan": A.installment_features(inst_lvl, "sk_id_prev"),
        "pos@loan": A.pos_features(use["POS_CASH_balance"], "sk_id_prev"),
        "credit_card@loan": A.credit_card_features(use["credit_card_balance"], "sk_id_prev"),
    }
    for name, part in loan_parts.items():
        checks.assert_unique(part, "sk_id_prev", f"aggregate {name}")
        loan = _left_join_checked(loan, part, "sk_id_prev", name, checks)
    loan_features = loan.withColumn("payment_delay_ratio", F.col("late_payment_ratio"))
    checks.assert_unique(loan_features, "sk_id_prev", "gold.loan_credit_features")

    # ---------------- customer level (grain SK_ID_CURR)
    syn = silver("synthetic_traffic_profile").filter(~F.col("_orphan_application")).select(
        "sk_id_curr", *SYNTHETIC_FEATURES, F.col("data_updated_at").alias("synthetic_data_updated_at"))
    parts = {
        "bureau": A.bureau_features(use["bureau"]),
        "bureau_balance": A.bureau_balance_features(use["bureau_balance"], use["bureau"]),
        "previous_application": A.previous_features(prev),
        "installments": A.installment_features(inst_lvl, "sk_id_curr"),
        "pos_cash": A.pos_features(use["POS_CASH_balance"], "sk_id_curr"),
        "credit_card": A.credit_card_features(use["credit_card_balance"], "sk_id_curr"),
        "synthetic_traffic (SYNTHETIC)": syn,
    }
    customer = app.select("sk_id_curr", "dataset_split")
    for name, part in parts.items():
        checks.assert_unique(part, "sk_id_curr", f"aggregate {name}")
        customer = _left_join_checked(customer, part, "sk_id_curr", name, checks)
    real_counts = [c for c in COUNT_FEATURES if c in customer.columns and c not in SYNTHETIC_FEATURES]
    customer = customer.fillna(0, subset=real_counts)       # "no record in that source" == 0, ratios stay NULL
    customer = customer.withColumn("has_synthetic_profile", F.col("synthetic_data_updated_at").isNotNull())
    n_cust = checks.assert_unique(customer, "sk_id_curr", "gold.customer_credit_features")
    checks.add("customer_credit_features covers every application", n_cust == n_app, customers=n_cust, applications=n_app)

    # ---------------- integrated profile = application attributes + customer features + engineered
    profile = _left_join_checked(app, customer.drop("dataset_split"), "sk_id_curr", "application+customer_features", checks)
    profile = add_engineered_features(profile)
    n_prof = checks.assert_unique(profile, "sk_id_curr", "gold.integrated_credit_profile")
    checks.add("integrated profile has exactly one row per application", n_prof == n_app, profile=n_prof, applications=n_app)
    leaked = [c for c in profile.columns if c.lower() == "target"] + [c for c in customer.columns if c.lower() == "target"]
    checks.add("TARGET not present in any feature table", not leaked, columns=leaked)
    checks.add("training_labels only for train split", labels.join(test.select("sk_id_curr"), "sk_id_curr").count() == 0)

    if checks.failed():
        _save_checks(cfg, checks, excluded, {})
        raise IntegrationError(f"{len(checks.failed())} integration checks failed: {checks.failed()}")

    # ---------------- write
    out = {}
    out["loan_credit_features"] = write_table(loan_features, "gold", "loan_credit_features", num_files=2, cfg=cfg)
    out["customer_credit_features"] = write_table(customer, "gold", "customer_credit_features",
                                                  partition_by=["dataset_split"], num_files=1, cfg=cfg)
    out["integrated_credit_profile"] = write_table(profile, "gold", "integrated_credit_profile",
                                                   partition_by=["dataset_split"], num_files=1, cfg=cfg)
    out["training_labels"] = write_table(labels, "gold", "training_labels", num_files=1, cfg=cfg)
    feature_cols = [c for c in profile.columns if c not in ("sk_id_curr", "dataset_split")]
    stats = {"rows": {"loan_credit_features": loan_features.count(), "customer_credit_features": n_cust,
                      "integrated_credit_profile": n_prof, "training_labels": labels.count()},
             "feature_count": len(feature_cols),
             "customer_feature_count": len([c for c in customer.columns if c not in ("sk_id_curr", "dataset_split")]),
             "seconds": round(time.time() - t0, 2), "outputs": out}
    feat_path = path_of(cfg, "reports_dir") / "data_quality" / "feature_columns.json"
    feat_path.write_text(json.dumps({"id": "sk_id_curr", "split": "dataset_split", "label_table": "gold/training_labels",
                                     "features": feature_cols, "synthetic_features": SYNTHETIC_FEATURES}, indent=2))
    _save_checks(cfg, checks, excluded, stats)
    for df in list(use.values()) + [inst_lvl]:
        df.unpersist()
    log.info("gold done: %s", stats["rows"])
    return stats


def _save_checks(cfg, checks: Checks, excluded: dict, stats: dict):
    p = path_of(cfg, "reports_dir") / "data_quality" / "gold_checks.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"checks": checks.items, "excluded_rows": excluded, "stats": stats}, indent=2, default=str))


def run() -> dict:
    cfg = load_config()
    stats = build(get_spark("gold"), cfg)
    from processing.integration.dq_report import build_dq_report
    build_dq_report(get_spark("gold"), cfg)
    return stats


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
