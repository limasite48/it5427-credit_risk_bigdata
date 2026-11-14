"""Cleaning rules (single source of truth: the code applies them, docs are generated from them)."""
from __future__ import annotations

# string placeholders -> NULL ("value not available"), per table. 'XAP' (= not applicable) is a real
# business value in previous_application and is KEPT.
XNA_TO_NULL: dict[str, tuple[str, ...]] = {
    "application_train": ("CODE_GENDER",),
    "application_test": ("CODE_GENDER",),
    "previous_application": ("NAME_CONTRACT_TYPE", "NAME_CASH_LOAN_PURPOSE", "NAME_PAYMENT_TYPE", "CODE_REJECT_REASON",
                             "NAME_GOODS_CATEGORY", "NAME_PORTFOLIO", "NAME_PRODUCT_TYPE", "NAME_SELLER_INDUSTRY",
                             "NAME_YIELD_GROUP", "NAME_CLIENT_TYPE"),
}
# Y/N columns -> 1/0 integers
YN_TO_INT: dict[str, tuple[str, ...]] = {
    "application_train": ("FLAG_OWN_CAR", "FLAG_OWN_REALTY"),
    "application_test": ("FLAG_OWN_CAR", "FLAG_OWN_REALTY"),
    "previous_application": ("FLAG_LAST_APPL_PER_CONTRACT",),
}
# quantile above which an amount is FLAGGED as outlier (never removed)
OUTLIER_COLS: dict[str, tuple[str, ...]] = {
    "application_train": ("AMT_INCOME_TOTAL", "AMT_CREDIT"),
    "application_test": ("AMT_INCOME_TOTAL", "AMT_CREDIT"),
    "bureau": ("AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT"),
    "installments_payments": ("AMT_PAYMENT",),
    "credit_card_balance": ("AMT_BALANCE",),
}
# missing-value semantics -> how each case is handled
MISSING_POLICY = [
    ("Required column NULL (keys, mandatory attributes)", "data error",
     "row moved to silver/_quarantine/<table> with reason `required_null:<col>`"),
    ("365243 in DAYS_EMPLOYED / DAYS_FIRST_DRAWING / DAYS_FIRST_DUE / DAYS_LAST_DUE* / DAYS_TERMINATION",
     "meaningful: open-ended / not applicable (e.g. pensioner, contract not terminated)",
     "replaced by NULL + boolean flag `<col>_is_sentinel` so the information is not lost"),
    ("OWN_CAR_AGE NULL while FLAG_OWN_CAR = N", "meaningful (no car)", "kept NULL"),
    ("OWN_CAR_AGE NULL while FLAG_OWN_CAR = Y", "data error", "kept NULL, counted in DQ report"),
    ("EXT_SOURCE_*, housing *_AVG/_MODE/_MEDI, AMT_REQ_CREDIT_BUREAU_*", "optional / not collected",
     "kept NULL (no imputation in Silver; imputation is a modelling choice made per algorithm)"),
    ("installments: DAYS_ENTRY_PAYMENT and AMT_PAYMENT both NULL", "meaningful (no payment recorded)",
     "kept NULL in Silver; aggregated as paid=0 and counted as unpaid installment in Gold"),
    ("bureau AMT_CREDIT_SUM_DEBT / AMT_CREDIT_SUM_LIMIT NULL", "not reported by bureau",
     "kept NULL; sums ignore NULLs, counts of reported values are kept"),
    ("Columns with very high missing rate", "any", "NEVER dropped in bulk; the missing rate is reported"),
]
RULES_TEXT = [
    "Column names: Kaggle UPPER_CASE -> lower_snake_case (`AMT_INCOME_TOTAL` -> `amt_income_total`).",
    "Types: cast to the registry type (common/schemas.py); values that fail the cast become NULL and are counted per column.",
    "Strings: trimmed, empty string -> NULL; `XNA` -> NULL for the columns listed in XNA_TO_NULL; `XAP` kept.",
    "`ORGANIZATION_TYPE = XNA` is KEPT: it marks applicants without employer (pensioners/unemployed) and matches DAYS_EMPLOYED=365243.",
    "Y/N flags -> 1/0 integers.",
    "Exact duplicate rows (all original columns equal) are removed: the copy carries no information.",
    "Primary-key duplicates that are not exact copies: one row is kept deterministically (most non-null values, then row hash); the others are quarantined with reason `pk_duplicate`.",
    "Event tables (installments, POS, credit card) have no PK: rows sharing the business key are FLAGGED (`_dup_business_key`) and kept - several payments for the same installment are legitimate.",
    "Negative values in non-negative amounts / DPD are FLAGGED (`_invalid_negative`), not removed.",
    "Values at or above the configured quantile are FLAGGED (`<col>_outlier`), not removed (no business reason to delete them).",
    "Referential integrity: every FK is checked against the parent in Silver; orphans are FLAGGED (`_orphan_<parent>`) and kept, counts go to the DQ report.",
    "Point-in-time: rows whose relative date is AFTER the current application (>0) are FLAGGED (`_after_application`); Gold excludes them (leakage guard).",
    "Lineage columns: `_source_id`, `_origin` (ORIGINAL/SYNTHETIC), `_ingest_id`, `_ingested_at`, `_silver_processed_at`.",
]
