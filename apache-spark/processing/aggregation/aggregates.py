"""Aggregations from each child table to the grain of its parent (Gold, step 2).

Grain chain (verified in profiling):
    bureau_balance (SK_ID_BUREAU x month) --> bureau (SK_ID_BUREAU) --> customer (SK_ID_CURR)
    installments (payment row) --> installment (SK_ID_PREV, version, number) --> loan (SK_ID_PREV) / customer
    POS_CASH / credit_card (SK_ID_PREV x month) --> loan (SK_ID_PREV) / customer (SK_ID_CURR)
    previous_application (SK_ID_PREV) --> customer (SK_ID_CURR)

Every function returns ONE row per key. Inputs are Silver DataFrames already filtered by
`usable()` (no rows dated after the application, no orphan rows).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

DPD_STATUS = ["1", "2", "3", "4", "5"]   # bureau_balance STATUS meaning DPD > 0 (5 = 120+ / sold / written off)
KNOWN_STATUS = ["C", "0", "1", "2", "3", "4", "5"]   # 'X' = status unknown -> excluded from ratios


def usable(df: DataFrame, orphan_flags: tuple[str, ...] = ()) -> DataFrame:
    """Leakage + integrity filter applied before any aggregation."""
    cond = ~F.col("_after_application")
    for flag in orphan_flags:
        if flag in df.columns:
            cond = cond & ~F.col(flag)
    return df.filter(cond)


def _safe_div(a, b):
    return F.when(b.isNull() | (b == 0), None).otherwise(a / b)


# ---------------------------------------------------------------- bureau
def bureau_features(bureau: DataFrame) -> DataFrame:
    overdue = (F.coalesce(F.col("credit_day_overdue"), F.lit(0)) > 0) | (F.coalesce(F.col("amt_credit_sum_overdue"), F.lit(0)) > 0)
    return bureau.groupBy("sk_id_curr").agg(
        F.count(F.lit(1)).alias("bureau_loan_count"),
        F.sum((F.col("credit_active") == "Active").cast("int")).alias("bureau_active_loan_count"),
        F.sum("amt_credit_sum_debt").alias("bureau_total_debt"),
        F.sum("amt_credit_sum").alias("bureau_total_credit"),
        F.avg("amt_credit_sum").alias("bureau_avg_credit"),
        F.sum(overdue.cast("int")).alias("bureau_overdue_count"),
        F.max("credit_day_overdue").alias("bureau_max_days_overdue"),
    )


def bureau_balance_features(bureau_balance: DataFrame, bureau: DataFrame) -> DataFrame:
    """bureau_balance has no SK_ID_CURR: it is mapped to the customer through bureau (SK_ID_BUREAU)."""
    bb = bureau_balance.join(bureau.select("sk_id_bureau", "sk_id_curr"), "sk_id_bureau", "inner")
    status_num = F.when(F.col("status").isin(DPD_STATUS + ["0"]), F.col("status").cast("int"))
    return bb.groupBy("sk_id_curr").agg(
        F.count(F.lit(1)).alias("bureau_monthly_status_count"),
        _safe_div(F.sum(F.col("status").isin(DPD_STATUS).cast("int")),
                  F.sum(F.col("status").isin(KNOWN_STATUS).cast("int"))).alias("bureau_overdue_ratio"),
        F.max(status_num).alias("bureau_max_overdue_status"),
    )


# ---------------------------------------------------------------- previous applications
def previous_features(prev: DataFrame) -> DataFrame:
    return prev.groupBy("sk_id_curr").agg(
        F.count(F.lit(1)).alias("previous_application_count"),
        F.sum((F.col("name_contract_status") == "Approved").cast("int")).alias("previous_approved_count"),
        F.sum((F.col("name_contract_status") == "Refused").cast("int")).alias("previous_refused_count"),
        _safe_div(F.sum((F.col("name_contract_status") == "Approved").cast("int")), F.count(F.lit(1))).alias("previous_approval_ratio"),
        F.avg("amt_credit").alias("previous_avg_credit"),
    )


# ---------------------------------------------------------------- installments
def installment_level(inst: DataFrame) -> DataFrame:
    """Payment rows -> one row per installment (partial payments are summed, last payment date kept)."""
    lvl = inst.groupBy("sk_id_curr", "sk_id_prev", "num_instalment_version", "num_instalment_number").agg(
        F.min("days_instalment").alias("due_day"),
        F.max("amt_instalment").alias("amt_instalment"),
        F.sum(F.coalesce(F.col("amt_payment"), F.lit(0.0))).alias("amt_paid"),
        F.max("days_entry_payment").alias("last_payment_day"),
        F.count(F.lit(1)).alias("payment_rows"),
    )
    return (lvl.withColumn("is_unpaid", F.col("last_payment_day").isNull())
            .withColumn("is_late", F.coalesce(F.col("last_payment_day") > F.col("due_day"), F.lit(False)))
            .withColumn("days_late", F.when(F.col("is_unpaid"), None)
                        .otherwise(F.greatest(F.col("last_payment_day") - F.col("due_day"), F.lit(0.0))))
            .withColumn("shortfall", F.greatest(F.coalesce(F.col("amt_instalment"), F.lit(0.0)) - F.col("amt_paid"), F.lit(0.0))))


def installment_features(lvl: DataFrame, key: str) -> DataFrame:
    return lvl.groupBy(key).agg(
        F.count(F.lit(1)).alias("installment_count"),
        F.sum(F.col("is_late").cast("int")).alias("late_payment_count"),
        _safe_div(F.sum(F.col("is_late").cast("int")), F.count(F.lit(1))).alias("late_payment_ratio"),
        F.avg("days_late").alias("average_days_late"),
        F.sum("amt_instalment").alias("total_installment_amount"),
        F.sum("amt_paid").alias("total_payment_amount"),
        F.sum("shortfall").alias("payment_shortfall"),
        F.sum(F.col("is_unpaid").cast("int")).alias("unpaid_installment_count"),
    )


# ---------------------------------------------------------------- POS / CASH
def pos_features(pos: DataFrame, key: str) -> DataFrame:
    return pos.groupBy(key).agg(
        F.count(F.lit(1)).alias("pos_month_count"),
        F.sum((F.coalesce(F.col("sk_dpd"), F.lit(0)) > 0).cast("int")).alias("pos_dpd_count"),
        F.max("sk_dpd").alias("pos_max_dpd"),
        F.avg("sk_dpd").alias("pos_avg_dpd"),
    )


# ---------------------------------------------------------------- credit card
def credit_card_features(cc: DataFrame, key: str) -> DataFrame:
    util = _safe_div(F.col("amt_balance"), F.col("amt_credit_limit_actual"))
    base = cc.groupBy(key).agg(
        F.avg("amt_balance").alias("credit_card_avg_balance"),
        F.max("amt_balance").alias("credit_card_max_balance"),
        F.avg(util).alias("credit_card_avg_utilization"),     # months with limit 0 / NULL are excluded
        F.max("sk_dpd").alias("credit_card_max_dpd"),
        F.count(F.lit(1)).alias("credit_card_month_count"),
    )
    # latest statement per card (max MONTHS_BALANCE) -> outstanding balance now
    latest = (cc.groupBy("sk_id_curr", "sk_id_prev").agg(F.max_by("amt_balance", "months_balance").alias("latest_balance")))
    latest = latest.groupBy(key).agg(F.sum("latest_balance").alias("credit_card_latest_balance_total"))
    return base.join(latest, key, "left")
