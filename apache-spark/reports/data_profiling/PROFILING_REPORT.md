# Data Profiling Report

- Generated: 2026-11-13T00:54:39+00:00
- Input: `/home/claude/credit-risk-bigdata/data/fixture/raw`
- Dataset label: **TEST FIXTURE (random values, NOT Home Credit)**

> ⚠️ These numbers describe the dataset named above, **not** the real Home Credit data.

## 1. Files

| File | Logical source | Size (MB) | Rows | Cols | Exact dup rows | PK | PK unique |
|---|---|---:|---:|---:|---:|---|---|
| application_train.csv | loan_application_system | 0.8 | 2,581 | 49 | 2 | SK_ID_CURR | no |
| application_test.csv | loan_application_system | 0.1 | 421 | 48 | 0 | SK_ID_CURR | yes |
| bureau.csv | external_credit_bureau | 1.0 | 9,694 | 17 | 0 | SK_ID_BUREAU | yes |
| bureau_balance.csv | external_credit_bureau | 4.1 | 295,046 | 3 | 0 | SK_ID_BUREAU,MONTHS_BALANCE | yes |
| previous_application.csv | previous_loan_system | 2.1 | 8,724 | 37 | 0 | SK_ID_PREV | yes |
| installments_payments.csv | installment_payment_system | 3.6 | 68,368 | 8 | 3 | — (event table) | — |
| POS_CASH_balance.csv | pos_cash_system | 9.7 | 236,650 | 8 | 2 | — (event table) | — |
| credit_card_balance.csv | credit_card_system | 3.8 | 29,824 | 23 | 0 | — (event table) | — |

Total: **25.1 MB**, **651,308 rows** across 8 business files.

Reference files (not business sources): `sample_submission.csv` (421 rows)

## 2. TARGET

- `application_train`: distribution {'1': 213, '0': 2368}, positive rate **8.25%**, ≈ 11.1 negatives per positive.
- `application_test` has no TARGET (expected).

## 3. Relationships (verified on data)

| Child.key → Parent.key | Child rows | Orphan rows | Orphan keys | Parent coverage | Children/parent (min/avg/max) | Observed |
|---|---:|---:|---:|---:|---|---|
| bureau.SK_ID_CURR → application.SK_ID_CURR | 9,693 | 4 | 4 | 85.87% | 1/3.76/10 | 1:N |
| bureau_balance.SK_ID_BUREAU → bureau.SK_ID_BUREAU | 295,046 | 3 | 1 | 61.26% | 2/49.68/97 | 1:N |
| previous_application.SK_ID_CURR → application.SK_ID_CURR | 8,724 | 0 | 0 | 95.2% | 1/3.05/7 | 1:N |
| installments_payments.SK_ID_PREV → previous_application.SK_ID_PREV | 68,368 | 5 | 1 | 54.94% | 1/14.26/30 | 1:N |
| installments_payments.SK_ID_CURR → application.SK_ID_CURR | 68,368 | 0 | 0 | 78.93% | 1/28.87/117 | 1:N |
| POS_CASH_balance.SK_ID_PREV → previous_application.SK_ID_PREV | 236,650 | 0 | 0 | 54.96% | 1/49.35/97 | 1:N |
| POS_CASH_balance.SK_ID_CURR → application.SK_ID_CURR | 236,650 | 0 | 0 | 78.93% | 2/99.94/442 | 1:N |
| credit_card_balance.SK_ID_PREV → previous_application.SK_ID_PREV | 29,824 | 0 | 0 | 6.79% | 2/50.38/97 | 1:N |
| credit_card_balance.SK_ID_CURR → application.SK_ID_CURR | 29,824 | 0 | 0 | 17.83% | 2/55.75/179 | 1:N |

- train/test SK_ID_CURR overlap: overlap_ids=0
- installments_payments.SK_ID_CURR consistent with previous_application via SK_ID_PREV: matched_rows=68,363, mismatched_rows=0
- POS_CASH_balance.SK_ID_CURR consistent with previous_application via SK_ID_PREV: matched_rows=236,650, mismatched_rows=0
- credit_card_balance.SK_ID_CURR consistent with previous_application via SK_ID_PREV: matched_rows=29,824, mismatched_rows=0

## 4. Per-table detail

### application_train  (1 row = 1 current loan application (SK_ID_CURR))


Top missing columns:

| Column | Missing % | Description |
|---|---:|---|
| YEARS_BUILD_AVG | 65.56 |  |
| OWN_CAR_AGE | 64.90 |  |
| EXT_SOURCE_1 | 56.06 |  |
| FLOORSMAX_MEDI | 49.75 |  |
| APARTMENTS_AVG | 48.66 |  |
| TOTALAREA_MODE | 48.47 |  |
| EMERGENCYSTATE_MODE | 45.14 |  |
| OCCUPATION_TYPE | 30.69 | What kind of occupation does the client have |
| EXT_SOURCE_3 | 19.53 |  |
| AMT_REQ_CREDIT_BUREAU_YEAR | 13.79 |  |
| NAME_TYPE_SUITE | 0.74 |  |
| DEF_30_CNT_SOCIAL_CIRCLE | 0.35 |  |

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| CNT_CHILDREN | 0 | 0 | 3 | 3 | 0.4146 | 0 | 0 |
| AMT_INCOME_TOTAL | 2.835e+04 | 1.476e+05 | 117,000,000 | 117,000,000 | 2.119e+05 | 0 | 0 |
| AMT_CREDIT | 4.5e+04 | 4.77e+05 | 3,613,500 | 3,613,500 | 5.862e+05 | 0 | 0 |
| AMT_ANNUITY | 1,078 | 1.865e+04 | 2.747e+05 | 2.747e+05 | 2.775e+04 | 0 | 0 |
| AMT_GOODS_PRICE | 4.5e+04 | 4.185e+05 | 3,370,500 | 3,370,500 | 5.26e+05 | 0 | 0 |
| DAYS_BIRTH | -2.52e+04 | -1.612e+04 | -7,500 | -7,500 | -1.616e+04 | 2,581 | 0 |
| DAYS_EMPLOYED | -1.2e+04 | -4,427 | 3.652e+05 | 3.652e+05 | 6.068e+04 | 2,118 | 463 |
| DAYS_REGISTRATION | -2e+04 | -9,729 | -3 | -3 | -9,872 | 2,581 | 0 |
| DAYS_ID_PUBLISH | -6,997 | -3,548 | -1 | -1 | -3,502 | 2,581 | 0 |
| CNT_FAM_MEMBERS | 1 | 2 | 5 | 5 | 2.172 | 0 | 0 |
| DAYS_LAST_PHONE_CHANGE | -3,999 | -2,028 | -1 | -1 | -2,019 | 2,581 | 0 |
| AMT_REQ_CREDIT_BUREAU_YEAR | 0 | 2 | 8 | 8 | 1.898 | 0 | 0 |

- Anomaly `DAYS_EMPLOYED`: 2,118 negative, 463 equal to 365243
- Placeholder categories: `CODE_GENDER` XNA=2; `ORGANIZATION_TYPE` XNA=463

### application_test  (1 row = 1 current loan application (SK_ID_CURR), no TARGET)


Top missing columns:

| Column | Missing % | Description |
|---|---:|---|
| YEARS_BUILD_AVG | 68.65 |  |
| OWN_CAR_AGE | 67.93 |  |
| APARTMENTS_AVG | 56.77 |  |
| EXT_SOURCE_1 | 56.30 |  |
| FLOORSMAX_MEDI | 52.26 |  |
| TOTALAREA_MODE | 46.56 |  |
| EMERGENCYSTATE_MODE | 44.89 |  |
| OCCUPATION_TYPE | 32.07 | What kind of occupation does the client have |
| EXT_SOURCE_3 | 20.90 |  |
| AMT_REQ_CREDIT_BUREAU_YEAR | 14.96 |  |
| OBS_30_CNT_SOCIAL_CIRCLE | 0.95 |  |
| EXT_SOURCE_2 | 0.47 |  |

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| CNT_CHILDREN | 0 | 0 | 3 | 3 | 0.3848 | 0 | 0 |
| AMT_INCOME_TOTAL | 3.6e+04 | 1.552e+05 | 7.052e+05 | 7.052e+05 | 1.787e+05 | 0 | 0 |
| AMT_CREDIT | 6.75e+04 | 5.22e+05 | 2,920,500 | 2,920,500 | 6.311e+05 | 0 | 0 |
| AMT_ANNUITY | 2,415 | 1.969e+04 | 2.799e+05 | 2.799e+05 | 3.044e+04 | 0 | 0 |
| AMT_GOODS_PRICE | 5.85e+04 | 4.635e+05 | 2,718,000 | 2,718,000 | 5.688e+05 | 0 | 0 |
| DAYS_BIRTH | -2.514e+04 | -1.649e+04 | -7,501 | -7,501 | -1.631e+04 | 421 | 0 |
| DAYS_EMPLOYED | -1.197e+04 | -4,676 | 3.652e+05 | 3.652e+05 | 6.725e+04 | 338 | 83 |
| DAYS_REGISTRATION | -1.997e+04 | -1.107e+04 | -133 | -133 | -1.051e+04 | 421 | 0 |
| DAYS_ID_PUBLISH | -6,952 | -3,627 | -7 | -7 | -3,575 | 421 | 0 |
| CNT_FAM_MEMBERS | 1 | 2 | 5 | 5 | 2.093 | 0 | 0 |
| DAYS_LAST_PHONE_CHANGE | -3,996 | -2,030 | -25 | -25 | -1,982 | 421 | 0 |
| AMT_REQ_CREDIT_BUREAU_YEAR | 0 | 2 | 7 | 7 | 1.902 | 0 | 0 |

- Anomaly `DAYS_EMPLOYED`: 338 negative, 83 equal to 365243
- Placeholder categories: `ORGANIZATION_TYPE` XNA=83

### bureau  (1 row = 1 credit reported by another institution (SK_ID_BUREAU))

- Rows dated AFTER the current application (leakage risk): {'DAYS_CREDIT': 1}

Top missing columns:

| Column | Missing % | Description |
|---|---:|---|
| AMT_ANNUITY | 69.83 |  |
| AMT_CREDIT_MAX_OVERDUE | 65.88 |  |
| DAYS_ENDDATE_FACT | 36.54 |  |
| AMT_CREDIT_SUM_LIMIT | 33.68 |  |
| AMT_CREDIT_SUM_DEBT | 14.09 | Current debt on Credit Bureau credit |
| SK_ID_CURR | 0.01 |  |

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| DAYS_CREDIT | -2,899 | -1,452 | 5 | 5 | -1,453 | 9,693 | 0 |
| DAYS_CREDIT_ENDDATE | -2,782 | 101 | 2,967 | 2,967 | 103.3 | 4,517 | 0 |
| DAYS_ENDDATE_FACT | -2,842 | -692 | -1 | -1 | -813.4 | 6,152 | 0 |
| AMT_CREDIT_MAX_OVERDUE | 0 | 2,483 | 4,999 | 4,999 | 2,532 | 0 | 0 |
| CNT_CREDIT_PROLONG | 0 | 0 | 1 | 1 | 0.005983 | 0 | 0 |
| AMT_CREDIT_SUM | 3,200 | 1.613e+05 | 9,595,700 | 9,595,700 | 2.681e+05 | 0 | 0 |
| AMT_CREDIT_SUM_DEBT | 0 | 0 | 3,268,791 | 3,268,791 | 4.979e+04 | 0 | 0 |
| AMT_CREDIT_SUM_LIMIT | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| AMT_CREDIT_SUM_OVERDUE | 0 | 0 | 2,000 | 2,000 | 82.37 | 0 | 0 |
| DAYS_CREDIT_UPDATE | -2,864 | -1 | -1 | -1 | -472.9 | 9,694 | 0 |
| AMT_ANNUITY | 42 | 1.418e+04 | 2.999e+04 | 2.999e+04 | 1.476e+04 | 0 | 0 |

### bureau_balance  (1 row = 1 bureau credit x 1 month (SK_ID_BUREAU, MONTHS_BALANCE))

- Rows dated AFTER the current application (leakage risk): {'MONTHS_BALANCE': 0}

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| MONTHS_BALANCE | -96 | -28 | 0 | 0 | -32.32 | 289,106 | 0 |

### previous_application  (1 row = 1 previous Home Credit application (SK_ID_PREV))

- Rows dated AFTER the current application (leakage risk): {'DAYS_DECISION': 0}

Top missing columns:

| Column | Missing % | Description |
|---|---:|---|
| RATE_INTEREST_PRIVILEGED | 99.70 |  |
| RATE_INTEREST_PRIMARY | 99.44 |  |
| RATE_DOWN_PAYMENT | 54.13 |  |
| AMT_DOWN_PAYMENT | 52.60 |  |
| NAME_TYPE_SUITE | 49.62 |  |
| AMT_ANNUITY | 38.25 |  |
| CNT_PAYMENT | 38.25 |  |
| DAYS_FIRST_DRAWING | 38.25 |  |
| DAYS_FIRST_DUE | 38.25 |  |
| DAYS_LAST_DUE_1ST_VERSION | 38.25 |  |
| DAYS_LAST_DUE | 38.25 |  |
| DAYS_TERMINATION | 38.25 |  |

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| AMT_ANNUITY | 135.7 | 9,199 | 4.479e+05 | 4.479e+05 | 1.716e+04 | 0 | 0 |
| AMT_APPLICATION | 2,300 | 9.87e+04 | 2,772,700 | 2,772,700 | 1.63e+05 | 0 | 0 |
| AMT_CREDIT | 0 | 9.93e+04 | 2,978,958 | 2,978,958 | 1.644e+05 | 0 | 0 |
| AMT_DOWN_PAYMENT | 12 | 1.008e+04 | 2e+04 | 2e+04 | 1.007e+04 | 0 | 0 |
| AMT_GOODS_PRICE | 2,300 | 9.97e+04 | 2,772,700 | 2,772,700 | 1.635e+05 | 0 | 0 |
| DAYS_DECISION | -2,899 | -1,484 | -30 | -30 | -1,469 | 8,724 | 0 |
| CNT_PAYMENT | 6 | 12 | 24 | 24 | 15.05 | 0 | 0 |
| DAYS_FIRST_DRAWING | 3.652e+05 | 3.652e+05 | 3.652e+05 | 3.652e+05 | 3.652e+05 | 0 | 5,387 |
| DAYS_FIRST_DUE | -2,868 | -1,436 | 0 | 0 | -1,439 | 5,385 | 0 |
| DAYS_LAST_DUE_1ST_VERSION | -2,715 | -1,025 | 690 | 690 | -1,018 | 4,608 | 0 |
| DAYS_LAST_DUE | -2,715 | -1,025 | 3.652e+05 | 3.652e+05 | 5.189e+04 | 4,606 | 781 |
| DAYS_TERMINATION | -2,712 | -1,022 | 3.652e+05 | 3.652e+05 | 5.19e+04 | 4,603 | 781 |

- Anomaly `DAYS_FIRST_DRAWING`: 0 negative, 5,387 equal to 365243
- Anomaly `DAYS_LAST_DUE`: 4,606 negative, 781 equal to 365243
- Anomaly `DAYS_TERMINATION`: 4,603 negative, 781 equal to 365243
- Placeholder categories: `NAME_CONTRACT_TYPE` XNA=7; `NAME_CASH_LOAN_PURPOSE` XAP=4,802, XNA=1,286; `NAME_PAYMENT_TYPE` XNA=3,370; `CODE_REJECT_REASON` XAP=7,241, XNA=385; `NAME_GOODS_CATEGORY` XNA=2,138; `NAME_PORTFOLIO` XNA=7; `NAME_PRODUCT_TYPE` XNA=5,522; `NAME_SELLER_INDUSTRY` XNA=2,920; `NAME_YIELD_GROUP` XNA=2,124

### installments_payments  (1 row = 1 payment record of 1 installment of a previous loan (an installment can have several partial-payment rows))

- Business key ['SK_ID_PREV', 'NUM_INSTALMENT_VERSION', 'NUM_INSTALMENT_NUMBER', 'DAYS_ENTRY_PAYMENT', 'AMT_PAYMENT']: 3 rows share a key with another row
- Rows dated AFTER the current application (leakage risk): {'DAYS_INSTALMENT': 0, 'DAYS_ENTRY_PAYMENT': 2}

Top missing columns:

| Column | Missing % | Description |
|---|---:|---|
| DAYS_ENTRY_PAYMENT | 0.98 | When was the installments of previous credit paid actually (relative to application date o |
| AMT_PAYMENT | 0.98 | What the client actually paid on previous credit on this installment |

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| DAYS_INSTALMENT | -2,868 | -1,324 | -1 | -1 | -1,326 | 68,368 | 0 |
| DAYS_ENTRY_PAYMENT | -2,880 | -1,352 | 7 | 7 | -1,331 | 67,697 | 0 |
| AMT_INSTALMENT | 135.7 | 7,381 | 4.479e+05 | 4.479e+05 | 1.379e+04 | 0 | 0 |
| AMT_PAYMENT | -439.6 | 7,072 | 4.479e+05 | 4.479e+05 | 1.31e+04 | 11 | 0 |

- Anomaly `AMT_PAYMENT`: 11 negative, 0 equal to 365243

### POS_CASH_balance  (1 row = 1 previous POS/cash loan x 1 month (SK_ID_PREV, MONTHS_BALANCE))

- Business key ['SK_ID_PREV', 'MONTHS_BALANCE']: 2 rows share a key with another row
- Rows dated AFTER the current application (leakage risk): {'MONTHS_BALANCE': 0}

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| MONTHS_BALANCE | -97 | -29 | -1 | -1 | -33 | 236,650 | 0 |
| CNT_INSTALMENT | 6 | 12 | 24 | 24 | 15 | 0 | 0 |
| CNT_INSTALMENT_FUTURE | 0 | 0 | 24 | 24 | 2.696 | 0 | 0 |
| SK_DPD | 0 | 0 | 89 | 89 | 1.465 | 0 | 0 |
| SK_DPD_DEF | 0 | 0 | 89 | 89 | 1.3 | 0 | 0 |

### credit_card_balance  (1 row = 1 previous credit card x 1 month (SK_ID_PREV, MONTHS_BALANCE))

- Business key ['SK_ID_PREV', 'MONTHS_BALANCE']: 0 rows share a key with another row
- Rows dated AFTER the current application (leakage risk): {'MONTHS_BALANCE': 0}

Top missing columns:

| Column | Missing % | Description |
|---|---:|---|
| AMT_PAYMENT_CURRENT | 20.21 |  |

| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |
|---|---:|---:|---:|---:|---:|---:|---:|
| MONTHS_BALANCE | -97 | -30 | -1 | -1 | -33.35 | 29,824 | 0 |
| AMT_BALANCE | 0 | 2.877e+04 | 2.835e+05 | 2.835e+05 | 5.113e+04 | 0 | 0 |
| AMT_CREDIT_LIMIT_ACTUAL | 0 | 1.35e+05 | 2.7e+05 | 2.7e+05 | 1.373e+05 | 0 | 0 |
| AMT_DRAWINGS_ATM_CURRENT | 0 | 0 | 1.5e+04 | 1.5e+04 | 2,261 | 0 | 0 |
| AMT_DRAWINGS_CURRENT | 0 | 0 | 3e+04 | 3e+04 | 4,522 | 0 | 0 |
| AMT_DRAWINGS_OTHER_CURRENT | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| AMT_DRAWINGS_POS_CURRENT | 0 | 0 | 1.5e+04 | 1.5e+04 | 2,261 | 0 | 0 |
| AMT_INST_MIN_REGULARITY | 0 | 1,438 | 1.417e+04 | 1.417e+04 | 2,557 | 0 | 0 |
| AMT_PAYMENT_CURRENT | 0 | 2,811 | 2.835e+04 | 2.835e+04 | 5,113 | 0 | 0 |
| AMT_PAYMENT_TOTAL_CURRENT | 0 | 2,877 | 2.835e+04 | 2.835e+04 | 5,113 | 0 | 0 |
| AMT_RECEIVABLE_PRINCIPAL | 0 | 2.733e+04 | 2.693e+05 | 2.693e+05 | 4.857e+04 | 0 | 0 |
| AMT_RECIVABLE | 0 | 2.877e+04 | 2.835e+05 | 2.835e+05 | 5.113e+04 | 0 | 0 |
| AMT_TOTAL_RECEIVABLE | 0 | 2.877e+04 | 2.835e+05 | 2.835e+05 | 5.113e+04 | 0 | 0 |
| CNT_DRAWINGS_ATM_CURRENT | 0 | 0 | 1 | 1 | 0.2997 | 0 | 0 |
