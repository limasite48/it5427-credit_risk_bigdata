# Pipeline statistics

- Dataset: **SUBSET of real Home Credit files** (`/Users/huylq/it5427-credit_risk_bigdata/apache-spark/data/dev_subset/raw`)
- Run: 2026-11-14T14:03:44+00:00 → 2026-11-14T14:04:23+00:00

> ⚠️ Figures below come from the dataset named above, not from the full Home Credit data.

## Volume and ingestion speed (latest successful Bronze snapshot per table)

| Table | Origin | Rows | Size (MB) | Ingest s | Rows/s | MB/s |
|---|---|---:|---:|---:|---:|---:|
| POS_CASH_balance | ORIGINAL | 142,543 | 5.60 | 0.1 | 1425430 | 55.97 |
| application_test | ORIGINAL | 684 | 0.38 | 0.009 | 76000 | 42.28 |
| application_train | ORIGINAL | 4,316 | 2.33 | 0.04 | 107900 | 58.22 |
| bureau | ORIGINAL | 24,618 | 2.44 | 0.032 | 769312 | 76.19 |
| bureau_balance | ORIGINAL | 352,119 | 4.84 | 0.114 | 3088763 | 42.43 |
| credit_card_balance | ORIGINAL | 53,618 | 5.93 | 0.091 | 589209 | 65.19 |
| installments_payments | ORIGINAL | 192,599 | 10.24 | 0.18 | 1069994 | 56.9 |
| previous_application | ORIGINAL | 23,990 | 5.80 | 0.071 | 337887 | 81.67 |
| synthetic_traffic_profile | SYNTHETIC | 5,000 | 0.32 | 0.009 | 555556 | 35.87 |

Total: **799,487 rows, 37.9 MB**.

## Processing time per stage

| Stage | Seconds |
|---|---:|
| synthetic_generation | 0.11 |
| ingestion_bronze | 0.03 |
| cleaning_silver | 31.64 |
| integration_gold | 6.94 |
| data_dictionary | 0.17 |

Total: **38.9 s**.

## Rows before / after cleaning

| Table | Bronze rows | Silver rows | Quarantined | Exact dups removed |
|---|---:|---:|---:|---:|
| application_train | 4,316 | 4,316 | 0 | 0 |
| application_test | 684 | 684 | 0 | 0 |
| bureau | 24,618 | 24,618 | 0 | 0 |
| bureau_balance | 352,119 | 352,119 | 0 | 0 |
| previous_application | 23,990 | 23,990 | 0 | 0 |
| installments_payments | 192,599 | 192,599 | 0 | 0 |
| POS_CASH_balance | 142,543 | 142,543 | 0 | 0 |
| credit_card_balance | 53,618 | 53,618 | 0 | 0 |
| synthetic_traffic_profile | 5,000 | 5,000 | 0 | 0 |

## Gold outputs

| Table | Rows |
|---|---:|
| loan_credit_features | 23,990 |
| customer_credit_features | 5,000 |
| integrated_credit_profile | 5,000 |
| training_labels | 4,316 |

Features after integration: **175** columns in `integrated_credit_profile` (39 aggregated customer features + application attributes + engineered).
