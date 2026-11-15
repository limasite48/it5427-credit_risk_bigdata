# Unified Data & Feature Store Contract

This document defines the canonical schemas, lake layers, streaming topics, and feature conventions across the **Credit Risk Intelligence** platform.

## 1. Raw Ingestion, Lakehouse & Streaming (`ingestion-minio-kafka/`)

### 1.1. Kafka Event Topics (`hc.*.v1`)
Live streaming events use a JSON envelope (`event_id`, `event_type`, `schema_version`, `source_system`, `source_table`, `event_time`, `produced_at`, `customer_id`, `payload`) validated against Draft 2020-12 JSON Schemas under `ingestion-minio-kafka/schemas/`:
- `hc.los.loan-application.v1` (`SK_ID_CURR`)
- `hc.bureau.credit-record.v1` (`SK_ID_BUREAU`)
- `hc.bureau.credit-balance-monthly.v1` (`SK_ID_BUREAU`, `MONTHS_BALANCE`)
- `hc.history.previous-application.v1` (`SK_ID_PREV`)
- `hc.pos.cash-balance.v1` (`SK_ID_PREV`, `MONTHS_BALANCE`)
- `hc.credit-card.balance.v1` (`SK_ID_PREV`, `MONTHS_BALANCE`)
- `hc.payments.installment.v1` (`SK_ID_PREV`, `NUM_INSTALMENT_VERSION`, `NUM_INSTALMENT_NUMBER`)
- `hc.dlq.v1` (Dead-Letter Queue for malformed JSON / schema failures)

### 1.2. Supplementary Regional Macro & Salary Benchmarks (`ingestion-minio-kafka/data/supplementary/`)
Generated via `python ingestion-minio-kafka/ingestion/gen_macro.py`:
- `region_dim.csv` (82 regions keyed by `region_code` and `region_population_relative`)
- `regional_gdp_growth_quarterly.csv` (`release_date`, `gdp_growth_yoy_pct`)
- `regional_unemployment_quarterly.csv` (`release_date`, `unemployment_rate_pct`)
- `regional_cpi_monthly.csv` (`release_date`, `cpi_index`, `cpi_yoy_pct`)
- `occupation_benchmarks.csv` (`OCCUPATION_TYPE`, `occ_median_salary`, `occ_mean_salary`, `occ_p25_salary`, `occ_p75_salary`)

`src/data/macro_loader.py` (`MacroFeatureJoiner`) enriches applications via point-in-time `merge_asof(..., direction="backward")` on `release_date <= application_date` when `application_dates` is provided (preventing future lookahead bias), and appends `occ_median_salary` and `income_to_occ_median_ratio`.

---

## 2. Spark Medallion Lake & Gold Feature Store (`apache-spark/`)

### 2.1. Storage Layout (`parquet` default, `CRB_LAKE_FORMAT=delta` Delta Lake supported)
- **Bronze (`bronze/`):** Raw immutable ingestion snapshots (`original/` and `synthetic/`) with append-only `_manifests/*.jsonl`.
- **Silver (`silver/`):** Typed, standardized tables (lower_snake_case, `365243` and `XNA` sentinel normalization, quarantine in `_quarantine/`), deduplicated by primary/business key.
- **Gold (`gold/`):**
  - `gold/loan_credit_features`: 1 row per `sk_id_prev` (previous loan + installments + POS + credit card).
  - `gold/customer_credit_features`: 1 row per `sk_id_curr` across child sources + synthetic traffic profile.
  - `gold/integrated_credit_profile`: 1 row per `sk_id_curr` (`application` + `customer_credit_features` + domain ratios; **strictly excludes `TARGET`**).
  - `gold/training_labels`: 1 row per labeled `sk_id_curr` (`target`).
  - `gold/data_quality_report` and `gold/stream_*`: Automated DQ gates and Structured Streaming activity state.

### 2.2. Standardized Gold Aggregated Feature Groups
- **Bureau (`bureau_*`):** Prior external credit counts, active ratios, debt totals, overdue sums/maxima, monthly DPD rates (`bureau_bb_*`).
- **Previous Applications (`prev_*`):** Approved/refused counts, refusal rate, min/max/mean credit & annuity ratios, days since last decision.
- **Installments (`inst_*` / `installments_*`):** Total installments paid, share paid late, max DPD, payment-to-installment underpayment ratios.
- **POS Cash (`pos_*`) & Credit Card (`cc_*`):** Contract status counts, utilization balance-to-limit ratios, overdue counts, drawing counts.
- **Data Leakage Guards:** All historical child records with relative timestamps after the application date (`_after_application`) are excluded prior to Gold aggregation and verified by automated checks in `apache-spark/reports/data_quality/gold_checks.json`.
