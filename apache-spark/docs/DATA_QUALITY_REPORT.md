# Data Quality Report

- Generated: 2026-11-14T14:04:23+00:00
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
| application_train | 4,316 | 0 | 0 | 0 | 4,316 | ✅ |
| application_test | 684 | 0 | 0 | 0 | 684 | ✅ |
| bureau | 24,618 | 0 | 0 | 0 | 24,618 | ✅ |
| bureau_balance | 352,119 | 0 | 0 | 0 | 352,119 | ✅ |
| previous_application | 23,990 | 0 | 0 | 0 | 23,990 | ✅ |
| installments_payments | 192,599 | 0 | 0 | 0 | 192,599 | ✅ |
| POS_CASH_balance | 142,543 | 0 | 0 | 0 | 142,543 | ✅ |
| credit_card_balance | 53,618 | 0 | 0 | 0 | 53,618 | ✅ |
| synthetic_traffic_profile | 5,000 | 0 | 0 | 0 | 5,000 | ✅ |

Duplicate rate = exact duplicates removed / rows in.

## 3. Flags (rows kept, flagged)

| Table | Orphan rows | After application date | Negative values | Outliers | Business-key dups | 365243 sentinels |
|---|---:|---:|---:|---:|---:|---:|
| application_train | 0 | 0 | 0 | 18 | 0 | 813 |
| application_test | 0 | 0 | 0 | 2 | 0 | 127 |
| bureau | 0 | 0 | 0 | 68 | 0 | 0 |
| bureau_balance | 0 | 0 | 0 | 0 | 0 | 0 |
| previous_application | 0 | 0 | 0 | 0 | 0 | 21,612 |
| installments_payments | 16,702 | 0 | 0 | 286 | 0 | 0 |
| POS_CASH_balance | 4,485 | 0 | 0 | 0 | 0 | 0 |
| credit_card_balance | 14,966 | 0 | 0 | 78 | 0 | 0 |
| synthetic_traffic_profile | 0 | 0 | 0 | 0 | 0 | 0 |

Semantic missing-value checks: `application_train` {'own_car_age_null_while_owns_car': 0, 'organization_xna_vs_days_employed_sentinel': 0}; `application_test` {'own_car_age_null_while_owns_car': 0, 'organization_xna_vs_days_employed_sentinel': 0}

Cast failures: none

## 4. Missing values (Silver, top 8 per table)

- **application_train**: commonarea_avg 70.4%, commonarea_mode 70.4%, commonarea_medi 70.4%, nonlivingapartments_avg 70.3%, nonlivingapartments_mode 70.3%, nonlivingapartments_medi 70.3%, livingapartments_avg 69.0%, livingapartments_mode 69.0%
- **application_test**: own_car_age 68.7%, commonarea_avg 67.5%, commonarea_mode 67.5%, commonarea_medi 67.5%, nonlivingapartments_avg 67.0%, nonlivingapartments_mode 67.0%, nonlivingapartments_medi 67.0%, livingapartments_avg 65.5%
- **bureau**: amt_annuity 70.4%, amt_credit_max_overdue 65.3%, days_enddate_fact 37.0%, amt_credit_sum_limit 34.9%, amt_credit_sum_debt 15.2%, days_credit_enddate 6.5%, amt_credit_sum 0.0%
- **previous_application**: rate_interest_primary 99.7%, rate_interest_privileged 99.7%, days_first_drawing 96.3% (raw 40.7%), name_product_type 63.0% (raw 0.0%), name_goods_category 58.1% (raw 0.0%), amt_down_payment 55.1%, rate_down_payment 55.1%, days_termination 54.3% (raw 40.7%)
- **installments_payments**: days_entry_payment 0.0%, amt_payment 0.0%
- **POS_CASH_balance**: cnt_instalment 0.3%, cnt_instalment_future 0.3%
- **credit_card_balance**: amt_payment_current 19.3%, amt_drawings_atm_current 19.1%, amt_drawings_other_current 19.1%, amt_drawings_pos_current 19.1%, cnt_drawings_atm_current 19.1%, cnt_drawings_other_current 19.1%, cnt_drawings_pos_current 19.1%, amt_inst_min_regularity 7.6%

Differences between raw and Silver missing rates come from documented conversions (365243 → NULL, `XNA` → NULL). No column was dropped.

## 5. Gold integration checks

| Check | Result | Details |
|---|---|---|
| application base (train+test): one row per sk_id_curr | ✅ pass | {"rows": 5000, "distinct_keys": 5000} |
| leakage: no child row dated after application is used | ✅ pass | {} |
| previous_application base: one row per sk_id_prev | ✅ pass | {"rows": 23990, "distinct_keys": 23990} |
| aggregate installments@loan: one row per sk_id_prev | ✅ pass | {"rows": 13690, "distinct_keys": 13690} |
| join installments@loan: row count unchanged | ✅ pass | {"before": 23990, "after": 23990} |
| aggregate pos@loan: one row per sk_id_prev | ✅ pass | {"rows": 12831, "distinct_keys": 12831} |
| join pos@loan: row count unchanged | ✅ pass | {"before": 23990, "after": 23990} |
| aggregate credit_card@loan: one row per sk_id_prev | ✅ pass | {"rows": 1331, "distinct_keys": 1331} |
| join credit_card@loan: row count unchanged | ✅ pass | {"before": 23990, "after": 23990} |
| gold.loan_credit_features: one row per sk_id_prev | ✅ pass | {"rows": 23990, "distinct_keys": 23990} |
| aggregate bureau: one row per sk_id_curr | ✅ pass | {"rows": 4296, "distinct_keys": 4296} |
| join bureau: row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| aggregate bureau_balance: one row per sk_id_curr | ✅ pass | {"rows": 1917, "distinct_keys": 1917} |
| join bureau_balance: row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| aggregate previous_application: one row per sk_id_curr | ✅ pass | {"rows": 4762, "distinct_keys": 4762} |
| join previous_application: row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| aggregate installments: one row per sk_id_curr | ✅ pass | {"rows": 4730, "distinct_keys": 4730} |
| join installments: row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| aggregate pos_cash: one row per sk_id_curr | ✅ pass | {"rows": 4692, "distinct_keys": 4692} |
| join pos_cash: row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| aggregate credit_card: one row per sk_id_curr | ✅ pass | {"rows": 1325, "distinct_keys": 1325} |
| join credit_card: row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| aggregate synthetic_traffic (SYNTHETIC): one row per sk_id_curr | ✅ pass | {"rows": 5000, "distinct_keys": 5000} |
| join synthetic_traffic (SYNTHETIC): row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| gold.customer_credit_features: one row per sk_id_curr | ✅ pass | {"rows": 5000, "distinct_keys": 5000} |
| customer_credit_features covers every application | ✅ pass | {"customers": 5000, "applications": 5000} |
| join application+customer_features: row count unchanged | ✅ pass | {"before": 5000, "after": 5000} |
| gold.integrated_credit_profile: one row per sk_id_curr | ✅ pass | {"rows": 5000, "distinct_keys": 5000} |
| integrated profile has exactly one row per application | ✅ pass | {"profile": 5000, "applications": 5000} |
| TARGET not present in any feature table | ✅ pass | {"columns": []} |
| training_labels only for train split | ✅ pass | {} |

Rows excluded before aggregation (leakage / integrity):

| Table | Silver rows | Used | Excluded after-application | Excluded total |
|---|---:|---:|---:|---:|
| bureau | 24,618 | 24,618 | 0 | 0 |
| bureau_balance | 352,119 | 352,119 | 0 | 0 |
| previous_application | 23,990 | 23,990 | 0 | 0 |
| installments_payments | 192,599 | 175,897 | 0 | 16,702 |
| POS_CASH_balance | 142,543 | 138,058 | 0 | 4,485 |
| credit_card_balance | 53,618 | 38,652 | 0 | 14,966 |

Outputs: {'loan_credit_features': 23990, 'customer_credit_features': 5000, 'integrated_credit_profile': 5000, 'training_labels': 4316}; features in integrated profile: 175.
