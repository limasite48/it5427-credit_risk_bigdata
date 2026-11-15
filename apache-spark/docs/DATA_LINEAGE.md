# DATA LINEAGE

## 1. From Raw Sources to Gold

| # | From | To | Transformation | Code |
|---|---|---|---|---|
| 1 | `data/raw/*.csv` (Kaggle) | `bronze/original/<source>/<table>/ingest_id=…/` | Byte-identical copy. Records sha256, row count, header, and chunked validation results. Skips if sha256 already exists. | `ingestion/<source>/ingest.py` → `ingestion/base.py` |
| 2 | `SK_ID_CURR` from application | `data/synthetic/synthetic_traffic_profile.csv` | Deterministic seeded SYNTHETIC generation, one row per real `SK_ID_CURR` | `ingestion/synthetic_source/generator.py` |
| 3 | `data/synthetic/*` | `bronze/synthetic/…` | Same as step 1 | `ingestion/synthetic_source/ingest.py` |
| 4 | Bronze (latest snapshot per manifest) | `silver/<table>` + `silver/_quarantine/<table>` | Type casting, column/value normalization (`365243` → NULL + flag, `XNA` → NULL, `Y/N` → `1/0`), exact dedup removal, PK dedup, negative/outlier/orphan/after-application flags, lineage columns | `processing/cleaning/clean_silver.py` |
| 5 | `silver/*` | `gold/loan_credit_features` | Aggregate installments (via installment level), POS, and credit card to `SK_ID_PREV`, then LEFT JOIN to `previous_application` | `processing/integration/build_gold.py`, `processing/aggregation/aggregates.py` |
| 6 | `silver/*` | `gold/customer_credit_features` | Aggregate each source to `SK_ID_CURR`, then LEFT JOIN onto application base. Missing child records get `count = 0`, `ratio = NULL`. | Same as above |
| 7 | application + customer features | `gold/integrated_credit_profile` | LEFT JOIN + feature engineering, drop `TARGET`, partition by `dataset_split` | `processing/feature_engineering/features.py` |
| 8 | `silver/application_train` | `gold/training_labels` | `(sk_id_curr, target)` only | `build_gold.py` |
| 9 | Silver metrics + Gold checks + profiling | `gold/data_quality_report`, `docs/DATA_QUALITY_REPORT.md` | Combined in long format | `processing/integration/dq_report.py` |
| 10 | Bronze (latest snapshot) | Kafka topic / `data/_stream_landing/*.jsonl` | Each row becomes one event (envelope + payload), dropping `TARGET` | `streaming/producers/replay_producer.py` |
| 11 | event | `silver/stream_events`, `silver/stream_quarantine`, `gold/stream_customer_activity_delta`, `gold/stream_batch_metrics` | Parse, quarantine, watermark, deduplicate by `event_id`, aggregate by `SK_ID_CURR` per micro-batch | `streaming/consumers/stream_processor.py` |
| 12 | `gold/stream_customer_activity_delta` | `gold/stream_customer_snapshot` | Roll up (`sum`/`max`) by `SK_ID_CURR` | `streaming/consumers/snapshot.py` |
| 13 | `data/{bronze,silver,gold}` | MinIO buckets `bronze`, `silver`, `gold` | Incremental sync (size + mtime) | `storage/minio/sync_lake.py` |

Every Silver row carries `_source_id`, `_origin`, `_ingest_id`, and `_ingested_at`, allowing full traceability back to the exact Bronze snapshot, original Kaggle file, and sha256 checksum.

## 2. Logical Source Relationships

| Logical source | Table | Owning key | Links to | Event frequency (simulated) |
|---|---|---|---|---|
| `loan_application_system` | `application_train`, `application_test` | `SK_ID_CURR` | — (root) | Per loan application event |
| `external_credit_bureau` | `bureau`, `bureau_balance` | `SK_ID_BUREAU` | `bureau.SK_ID_CURR` → application; `bureau_balance.SK_ID_BUREAU` → bureau | On inquiry + monthly snapshot |
| `previous_loan_system` | `previous_application` | `SK_ID_PREV` | `SK_ID_CURR` → application | Per past application decision |
| `installment_payment_system` | `installments_payments` | — (event) | `SK_ID_PREV` → `previous_application` | Per installment |
| `pos_cash_system` | `POS_CASH_balance` | — (`SK_ID_PREV` × month) | `SK_ID_PREV` → `previous_application` | Monthly |
| `credit_card_system` | `credit_card_balance` | — (`SK_ID_PREV` × month) | `SK_ID_PREV` → `previous_application` | Monthly |
| `synthetic_customer_behavior` | `synthetic_traffic_profile` | `SK_ID_CURR` | `SK_ID_CURR` → application | Periodic refresh (`DATA_UPDATED_AT`) |

Only original identifier keys are used. No random customers are ever generated and joined into Home Credit data.

## 3. Gold Table Build Steps (Execution Order)

1. Read `silver/application_train` and `application_test`, add `dataset_split`, split `TARGET` into `training_labels`, and verify `SK_ID_CURR` uniqueness.
2. For each child table, apply `usable()`: exclude `_after_application` and orphan rows. Excluded counts are recorded in `gold_checks.json`.
3. `installments`: aggregate payment lines into **installment level** (one installment may have multiple partial payments).
4. Aggregate to `SK_ID_PREV`, verify uniqueness, LEFT JOIN onto `previous_application`, verify row count → `loan_credit_features`.
5. Aggregate each source to `SK_ID_CURR`, verify uniqueness, sequentially LEFT JOIN onto the application base, verifying row counts after each join → `customer_credit_features`.
6. LEFT JOIN application + customer features, add engineered features, verify 1 row per `SK_ID_CURR` and absence of `TARGET` → `integrated_credit_profile`.
7. Write Gold outputs only after all checks pass. Then generate `data_quality_report`.
