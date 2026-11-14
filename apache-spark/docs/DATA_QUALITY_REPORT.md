# Data Quality Report

- Generated: 2026-11-13T00:55:52+00:00
- Dataset: **TEST FIXTURE (random values, NOT Home Credit)**

> ⚠️ Numbers below were produced on the dataset named above. Re-run `python -m scripts.run_pipeline` on the real Kaggle files to regenerate this report with real figures.

## 1. Profiling summary (raw files)

| File | Rows | Cols | Size MB | Exact dup rows | PK unique |
|---|---:|---:|---:|---:|---|
| application_train.csv | 2,581 | 49 | 0.8 | 2 | no |
| application_test.csv | 421 | 48 | 0.1 | 0 | yes |
| bureau.csv | 9,694 | 17 | 1.0 | 0 | yes |
| bureau_balance.csv | 295,046 | 3 | 4.1 | 0 | yes |
| previous_application.csv | 8,724 | 37 | 2.1 | 0 | yes |
| installments_payments.csv | 68,368 | 8 | 3.6 | 3 | — |
| POS_CASH_balance.csv | 236,650 | 8 | 9.7 | 2 | — |
| credit_card_balance.csv | 29,824 | 23 | 3.8 | 0 | — |

Full detail: `reports/data_profiling/PROFILING_REPORT.md`.

## 2. Before / after cleaning (Bronze → Silver)

| Table | Rows in | Exact dups removed | PK dups quarantined | Required-null quarantined | Rows out | Reconciles |
|---|---:|---:|---:|---:|---:|---|
| application_train | 2,581 | 2 | 0 | 0 | 2,579 | ✅ |
| application_test | 421 | 0 | 0 | 0 | 421 | ✅ |
| bureau | 9,694 | 0 | 0 | 1 | 9,693 | ✅ |
| bureau_balance | 295,046 | 0 | 0 | 0 | 295,046 | ✅ |
| previous_application | 8,724 | 0 | 0 | 0 | 8,724 | ✅ |
| installments_payments | 68,368 | 3 | 0 | 0 | 68,365 | ✅ |
| POS_CASH_balance | 236,650 | 2 | 0 | 0 | 236,648 | ✅ |
| credit_card_balance | 29,824 | 0 | 0 | 0 | 29,824 | ✅ |
| synthetic_traffic_profile | 3,000 | 0 | 0 | 0 | 3,000 | ✅ |

Duplicate rate = exact duplicates removed / rows in.

## 3. Flags (rows kept, flagged)

| Table | Orphan rows | After application date | Negative values | Outliers | Business-key dups | 365243 sentinels |
|---|---:|---:|---:|---:|---:|---:|
| application_train | 0 | 0 | 0 | 8 | 0 | 462 |
| application_test | 0 | 0 | 0 | 2 | 0 | 83 |
| bureau | 4 | 1 | 0 | 28 | 0 | 0 |
| bureau_balance | 3 | 0 | 0 | 0 | 0 | 0 |
| previous_application | 0 | 0 | 0 | 0 | 0 | 6,949 |
| installments_payments | 5 | 2 | 11 | 102 | 0 | 0 |
| POS_CASH_balance | 0 | 0 | 0 | 0 | 0 | 0 |
| credit_card_balance | 0 | 0 | 0 | 39 | 0 | 0 |
| synthetic_traffic_profile | 0 | 0 | 0 | 0 | 0 | 0 |

Semantic missing-value checks: `application_train` {'own_car_age_null_while_owns_car': 539, 'organization_xna_vs_days_employed_sentinel': 0}; `application_test` {'own_car_age_null_while_owns_car': 94, 'organization_xna_vs_days_employed_sentinel': 0}

Cast failures: none

## 4. Missing values (Silver, top 8 per table)

- **application_train**: years_build_avg 65.6%, own_car_age 64.9%, ext_source_1 56.1%, floorsmax_medi 49.8%, apartments_avg 48.7%, totalarea_mode 48.4%, emergencystate_mode 45.2%, occupation_type 30.6% (raw 30.7%)
- **application_test**: years_build_avg 68.6%, own_car_age 67.9%, apartments_avg 56.8%, ext_source_1 56.3%, floorsmax_medi 52.3%, totalarea_mode 46.6%, emergencystate_mode 44.9%, occupation_type 32.1%
- **bureau**: amt_annuity 69.8%, amt_credit_max_overdue 65.9%, days_enddate_fact 36.5%, amt_credit_sum_limit 33.7%, amt_credit_sum_debt 14.1%
- **previous_application**: days_first_drawing 100.0% (raw 38.3%), rate_interest_privileged 99.7%, rate_interest_primary 99.4%, name_product_type 63.3% (raw 0.0%), rate_down_payment 54.1%, amt_down_payment 52.6%, name_type_suite 49.6%, days_last_due 47.2% (raw 38.3%)
- **installments_payments**: days_entry_payment 1.0%, amt_payment 1.0%
- **credit_card_balance**: amt_payment_current 20.2%

Differences between raw and Silver missing rates come from documented conversions (365243 → NULL, `XNA` → NULL). No column was dropped.

## 5. Gold integration checks

| Check | Result | Details |
|---|---|---|
| application base (train+test): one row per sk_id_curr | ✅ pass | {"rows": 3000, "distinct_keys": 3000} |
| leakage: no child row dated after application is used | ✅ pass | {} |
| previous_application base: one row per sk_id_prev | ✅ pass | {"rows": 8724, "distinct_keys": 8724} |
| aggregate installments@loan: one row per sk_id_prev | ✅ pass | {"rows": 4793, "distinct_keys": 4793} |
| join installments@loan: row count unchanged | ✅ pass | {"before": 8724, "after": 8724} |
| aggregate pos@loan: one row per sk_id_prev | ✅ pass | {"rows": 4795, "distinct_keys": 4795} |
| join pos@loan: row count unchanged | ✅ pass | {"before": 8724, "after": 8724} |
| aggregate credit_card@loan: one row per sk_id_prev | ✅ pass | {"rows": 592, "distinct_keys": 592} |
| join credit_card@loan: row count unchanged | ✅ pass | {"before": 8724, "after": 8724} |
| gold.loan_credit_features: one row per sk_id_prev | ✅ pass | {"rows": 8724, "distinct_keys": 8724} |
| aggregate bureau: one row per sk_id_curr | ✅ pass | {"rows": 2576, "distinct_keys": 2576} |
| join bureau: row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| aggregate bureau_balance: one row per sk_id_curr | ✅ pass | {"rows": 2294, "distinct_keys": 2294} |
| join bureau_balance: row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| aggregate previous_application: one row per sk_id_curr | ✅ pass | {"rows": 2856, "distinct_keys": 2856} |
| join previous_application: row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| aggregate installments: one row per sk_id_curr | ✅ pass | {"rows": 2368, "distinct_keys": 2368} |
| join installments: row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| aggregate pos_cash: one row per sk_id_curr | ✅ pass | {"rows": 2368, "distinct_keys": 2368} |
| join pos_cash: row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| aggregate credit_card: one row per sk_id_curr | ✅ pass | {"rows": 535, "distinct_keys": 535} |
| join credit_card: row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| aggregate synthetic_traffic (SYNTHETIC): one row per sk_id_curr | ✅ pass | {"rows": 3000, "distinct_keys": 3000} |
| join synthetic_traffic (SYNTHETIC): row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| gold.customer_credit_features: one row per sk_id_curr | ✅ pass | {"rows": 3000, "distinct_keys": 3000} |
| customer_credit_features covers every application | ✅ pass | {"customers": 3000, "applications": 3000} |
| join application+customer_features: row count unchanged | ✅ pass | {"before": 3000, "after": 3000} |
| gold.integrated_credit_profile: one row per sk_id_curr | ✅ pass | {"rows": 3000, "distinct_keys": 3000} |
| integrated profile has exactly one row per application | ✅ pass | {"profile": 3000, "applications": 3000} |
| TARGET not present in any feature table | ✅ pass | {"columns": []} |
| training_labels only for train split | ✅ pass | {} |

Rows excluded before aggregation (leakage / integrity):

| Table | Silver rows | Used | Excluded after-application | Excluded total |
|---|---:|---:|---:|---:|
| bureau | 9,693 | 9,688 | 1 | 5 |
| bureau_balance | 295,046 | 295,043 | 0 | 3 |
| previous_application | 8,724 | 8,724 | 0 | 0 |
| installments_payments | 68,365 | 68,358 | 2 | 7 |
| POS_CASH_balance | 236,648 | 236,648 | 0 | 0 |
| credit_card_balance | 29,824 | 29,824 | 0 | 0 |

Outputs: {'loan_credit_features': 8724, 'customer_credit_features': 3000, 'integrated_credit_profile': 3000, 'training_labels': 2579}; features in integrated profile: 102.
