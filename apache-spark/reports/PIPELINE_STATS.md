# Pipeline statistics

- Dataset: **TEST FIXTURE (random values, NOT Home Credit)** (`/home/claude/credit-risk-bigdata/data/fixture/raw`)
- Run: 2026-11-13T00:53:56+00:00 → 2026-11-13T00:56:10+00:00

> ⚠️ Figures below come from the dataset named above, not from the full Home Credit data.

## Volume and ingestion speed (latest successful Bronze snapshot per table)

| Table | Origin | Rows | Size (MB) | Ingest s | Rows/s | MB/s |
|---|---|---:|---:|---:|---:|---:|
| POS_CASH_balance | ORIGINAL | 236,650 | 9.69 | 0.346 | 683960 | 27.99 |
| application_test | ORIGINAL | 421 | 0.12 | 0.01 | 42100 | 12.29 |
| application_train | ORIGINAL | 2,581 | 0.76 | 0.036 | 71694 | 21.02 |
| bureau | ORIGINAL | 9,694 | 0.97 | 0.043 | 225442 | 22.59 |
| bureau_balance | ORIGINAL | 295,046 | 4.07 | 0.242 | 1219198 | 16.81 |
| credit_card_balance | ORIGINAL | 29,824 | 3.78 | 0.187 | 159487 | 20.21 |
| installments_payments | ORIGINAL | 68,368 | 3.63 | 0.127 | 538331 | 28.56 |
| previous_application | ORIGINAL | 8,724 | 2.11 | 0.07 | 124629 | 30.21 |
| synthetic_traffic_profile | SYNTHETIC | 3,000 | 0.19 | 0.016 | 187500 | 12.11 |

Total: **654,308 rows, 25.3 MB**.

## Processing time per stage

| Stage | Seconds |
|---|---:|
| profiling | 49.95 |
| synthetic_generation | 0.15 |
| ingestion_bronze | 1.08 |
| cleaning_silver | 50.27 |
| integration_gold | 15.49 |
| minio_sync | 0.79 |
| stream_producer | 1.69 |
| stream_processing | 11.63 |
| stream_snapshot | 1.04 |
| data_dictionary | 0.5 |

Total: **132.6 s**.

## Rows before / after cleaning

| Table | Bronze rows | Silver rows | Quarantined | Exact dups removed |
|---|---:|---:|---:|---:|
| application_train | 2,581 | 2,579 | 0 | 2 |
| application_test | 421 | 421 | 0 | 0 |
| bureau | 9,694 | 9,693 | 1 | 0 |
| bureau_balance | 295,046 | 295,046 | 0 | 0 |
| previous_application | 8,724 | 8,724 | 0 | 0 |
| installments_payments | 68,368 | 68,365 | 0 | 3 |
| POS_CASH_balance | 236,650 | 236,648 | 0 | 2 |
| credit_card_balance | 29,824 | 29,824 | 0 | 0 |
| synthetic_traffic_profile | 3,000 | 3,000 | 0 | 0 |

## Gold outputs

| Table | Rows |
|---|---:|
| loan_credit_features | 8,724 |
| customer_credit_features | 3,000 |
| integrated_credit_profile | 3,000 |
| training_labels | 2,579 |

Features after integration: **102** columns in `integrated_credit_profile` (39 aggregated customer features + application attributes + engineered).

## Streaming demo (file replay)

- Producer: 2,000 events in 1.66 s (1202.7 ev/s); injected per source: installment_payment_system: dup 23, late 9, malformed 8; credit_card_system: dup 29, late 15, malformed 5; pos_cash_system: dup 23, late 15, malformed 6; loan_application_system: dup 21, late 6, malformed 4
- Consumer: 3 micro-batches, 2,119 input rows, 23 quarantined, 96 duplicates dropped, 18 late rows dropped, 1,982 events written, 10.72 s.
- Snapshot: {'customers': 441, 'events_total': 1982, 'batches': 3}

## MinIO sync

- bronze: uploaded 27, unchanged 0, 25.3 MB
- silver: uploaded 36, unchanged 0, 4.1 MB
- gold: uploaded 11, unchanged 0, 1.4 MB
