"""Engineered features on the integrated (one row per SK_ID_CURR) profile + their documentation."""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

DAYS_PER_YEAR = 365.25


def _ratio(a: str, b: str):
    return F.when(F.col(b).isNull() | (F.col(b) == 0), None).otherwise(F.col(a) / F.col(b))


ENGINEERED = {
    "age_years": ("-days_birth / 365.25", "Applicant age at application date."),
    "employment_years": ("-days_employed / 365.25 (NULL when DAYS_EMPLOYED was the 365243 sentinel)",
                         "Length of current employment; NULL for pensioners/unemployed."),
    "loan_to_income": ("amt_credit / amt_income_total", "Size of the requested loan relative to yearly income."),
    "annuity_to_income": ("amt_annuity / amt_income_total", "Share of income consumed by the new loan's annuity (payment burden)."),
    "credit_to_goods_ratio": ("amt_credit / amt_goods_price", "> 1 means the loan finances more than the goods price (fees, insurance)."),
    "debt_to_income": ("bureau_total_debt / amt_income_total", "Debt already owed to other institutions relative to income."),
    "bureau_debt_to_credit": ("bureau_total_debt / bureau_total_credit", "How much of the externally granted credit is still outstanding."),
    "payment_delay_ratio": ("late_payment_count / installment_count", "Share of past Home Credit installments paid after the due date."),
    "credit_utilization": ("credit_card_avg_utilization = avg(amt_balance / amt_credit_limit_actual)",
                           "Average card utilization; > 1 means over-limit."),
    "number_of_previous_applications": ("previous_application_count", "How often the client applied to Home Credit before."),
    "total_outstanding_debt": ("coalesce(bureau_total_debt,0) + coalesce(credit_card_latest_balance_total,0)",
                               "Known outstanding debt: other institutions + latest Home Credit card balances."),
    "has_previous_overdue": ("bureau_overdue_count>0 OR bureau_max_overdue_status>=1 OR late_payment_count>0 OR pos_max_dpd>0 OR credit_card_max_dpd>0",
                             "Any recorded delinquency before the application (1/0)."),
    "income_per_family_member": ("amt_income_total / cnt_fam_members", "Income available per household member."),
}


def add_engineered_features(df: DataFrame) -> DataFrame:
    overdue = (
        (F.coalesce(F.col("bureau_overdue_count"), F.lit(0)) > 0)
        | (F.coalesce(F.col("bureau_max_overdue_status"), F.lit(0)) >= 1)
        | (F.coalesce(F.col("late_payment_count"), F.lit(0)) > 0)
        | (F.coalesce(F.col("pos_max_dpd"), F.lit(0)) > 0)
        | (F.coalesce(F.col("credit_card_max_dpd"), F.lit(0)) > 0)
    )
    return (df
            .withColumn("age_years", -F.col("days_birth") / DAYS_PER_YEAR)
            .withColumn("employment_years", -F.col("days_employed") / DAYS_PER_YEAR)
            .withColumn("loan_to_income", _ratio("amt_credit", "amt_income_total"))
            .withColumn("annuity_to_income", _ratio("amt_annuity", "amt_income_total"))
            .withColumn("credit_to_goods_ratio", _ratio("amt_credit", "amt_goods_price"))
            .withColumn("debt_to_income", _ratio("bureau_total_debt", "amt_income_total"))
            .withColumn("bureau_debt_to_credit", _ratio("bureau_total_debt", "bureau_total_credit"))
            .withColumn("payment_delay_ratio", _ratio("late_payment_count", "installment_count"))
            .withColumn("credit_utilization", F.col("credit_card_avg_utilization"))
            .withColumn("number_of_previous_applications", F.col("previous_application_count"))
            .withColumn("total_outstanding_debt", F.coalesce(F.col("bureau_total_debt"), F.lit(0.0))
                        + F.coalesce(F.col("credit_card_latest_balance_total"), F.lit(0.0)))
            .withColumn("has_previous_overdue", overdue.cast("int"))
            .withColumn("income_per_family_member", _ratio("amt_income_total", "cnt_fam_members")))


# documentation of every aggregated feature (used to generate DATA_DICTIONARY.md)
AGGREGATED = {
    "bureau_loan_count": ("bureau", "count(*) per SK_ID_CURR", "Number of credits reported by other institutions."),
    "bureau_active_loan_count": ("bureau", "count(CREDIT_ACTIVE='Active')", "Credits still open at other institutions."),
    "bureau_total_debt": ("bureau", "sum(AMT_CREDIT_SUM_DEBT)", "Debt currently owed to other institutions (NULL debts ignored)."),
    "bureau_total_credit": ("bureau", "sum(AMT_CREDIT_SUM)", "Total credit amount granted by other institutions."),
    "bureau_avg_credit": ("bureau", "avg(AMT_CREDIT_SUM)", "Average size of external credits."),
    "bureau_overdue_count": ("bureau", "count(CREDIT_DAY_OVERDUE>0 OR AMT_CREDIT_SUM_OVERDUE>0)", "External credits overdue at application time."),
    "bureau_max_days_overdue": ("bureau", "max(CREDIT_DAY_OVERDUE)", "Worst current overdue (days) at another institution."),
    "bureau_monthly_status_count": ("bureau_balance", "count(*) via SK_ID_BUREAU -> SK_ID_CURR", "Months of bureau history available."),
    "bureau_overdue_ratio": ("bureau_balance", "count(STATUS in 1..5) / count(STATUS in C,0..5)", "Share of known months with DPD > 0 ('X' unknown excluded)."),
    "bureau_max_overdue_status": ("bureau_balance", "max(STATUS as int, 0..5)", "Worst DPD bucket ever reported (5 = 120+ DPD or written off)."),
    "previous_application_count": ("previous_application", "count(*)", "Previous Home Credit applications."),
    "previous_approved_count": ("previous_application", "count(NAME_CONTRACT_STATUS='Approved')", "Approved previous applications."),
    "previous_refused_count": ("previous_application", "count(NAME_CONTRACT_STATUS='Refused')", "Refused previous applications."),
    "previous_approval_ratio": ("previous_application", "approved / count", "Historical approval rate."),
    "previous_avg_credit": ("previous_application", "avg(AMT_CREDIT)", "Average credit of previous applications."),
    "installment_count": ("installments_payments", "count(distinct installment = SK_ID_PREV+version+number)", "Installments due before the application."),
    "late_payment_count": ("installments_payments", "count(last payment day > due day)", "Installments paid late."),
    "late_payment_ratio": ("installments_payments", "late / installments", "Share of installments paid late."),
    "average_days_late": ("installments_payments", "avg(max(last_payment_day - due_day, 0)) over paid installments", "Average delay in days (0 when on time)."),
    "total_installment_amount": ("installments_payments", "sum(AMT_INSTALMENT per installment)", "Amount that was due."),
    "total_payment_amount": ("installments_payments", "sum(AMT_PAYMENT), NULL payment = 0", "Amount actually paid."),
    "payment_shortfall": ("installments_payments", "sum(max(due - paid, 0)) per installment", "Unpaid part of due amounts."),
    "unpaid_installment_count": ("installments_payments", "count(installments with no payment row)", "Installments with no recorded payment."),
    "pos_month_count": ("POS_CASH_balance", "count(*)", "Monthly POS/cash snapshots."),
    "pos_dpd_count": ("POS_CASH_balance", "count(SK_DPD>0)", "Months with days past due."),
    "pos_max_dpd": ("POS_CASH_balance", "max(SK_DPD)", "Worst DPD on POS/cash loans."),
    "pos_avg_dpd": ("POS_CASH_balance", "avg(SK_DPD)", "Average DPD on POS/cash loans."),
    "credit_card_avg_balance": ("credit_card_balance", "avg(AMT_BALANCE)", "Average monthly card balance."),
    "credit_card_max_balance": ("credit_card_balance", "max(AMT_BALANCE)", "Highest monthly card balance."),
    "credit_card_avg_utilization": ("credit_card_balance", "avg(AMT_BALANCE / AMT_CREDIT_LIMIT_ACTUAL), limit>0", "Average utilization of the card limit."),
    "credit_card_max_dpd": ("credit_card_balance", "max(SK_DPD)", "Worst DPD on cards."),
    "credit_card_month_count": ("credit_card_balance", "count(*)", "Monthly card statements."),
    "credit_card_latest_balance_total": ("credit_card_balance", "sum over cards of AMT_BALANCE at max(MONTHS_BALANCE)", "Card balance at the latest statement."),
    "traffic_violation_count": ("synthetic_traffic_profile (SYNTHETIC)", "TRAFFIC_VIOLATION_COUNT", "SYNTHETIC - not real data."),
    "major_violation_count": ("synthetic_traffic_profile (SYNTHETIC)", "MAJOR_VIOLATION_COUNT", "SYNTHETIC - not real data."),
    "total_traffic_fine": ("synthetic_traffic_profile (SYNTHETIC)", "TOTAL_TRAFFIC_FINE", "SYNTHETIC - not real data."),
    "accident_count": ("synthetic_traffic_profile (SYNTHETIC)", "ACCIDENT_COUNT", "SYNTHETIC - not real data."),
}
# counts are 0 (not NULL) when the customer simply has no record in that source
COUNT_FEATURES = [k for k in AGGREGATED if k.endswith("_count")]
