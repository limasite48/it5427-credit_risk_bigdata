# Credit Risk Intelligence – Big Data Data Pipeline

Course project for **Big Data Integration and Processing (IT5427)**.  
Topic: *Multi-Source Big Data Integration and Analytics for Credit Risk Assessment*, using the **Home Credit Default Risk** dataset (Kaggle).  
Scope: **Data Collection & Storage** and **Data Integration & Processing** modules.

---

## 1. What This Module Implements

From the 10 Home Credit CSV files, this module builds a system simulating a realistic financial data environment across multiple independent operational sources, a 3-layer Data Lake, and both batch and streaming pipelines.

```
 7 logical sources ──► Ingestion ──► BRONZE ──► Cleaning/DQ ──► SILVER ──► Integration ──► GOLD
 (6 original + 1 synthetic) (Python) (raw CSV)   (Spark)        (Parquet)   (Spark)       (1 row/customer features)
                                         │
                                         └──► Replay producer ──► Kafka ──► Spark Structured Streaming ──► SILVER/GOLD
                         Storage: MinIO (buckets: bronze / silver / gold)
```

| # | Component | Summary | Key Files |
|---|---|---|---|
| 1 | **Data profiling** | Measures row counts, types, missingness, duplicates, outliers, `TARGET` rate. **Empirically tests** every key relationship (orphan rows, cardinality) on real data rather than joining on column names alone. Generates reports and a measured ERD. | `processing/profiling/profile_raw.py` |
| 2 | **Multi-source simulation** | Splits 8 source tables into 6 independent operational systems (Loan Application, Credit Bureau, Previous Loan, Installment, POS/Cash, Credit Card), each with its own ingestion package, schema, metadata, cadence, and validation rules. | `configs/sources.yaml`, `common/schemas.py`, `ingestion/*_source/` |
| 3 | **SYNTHETIC source** | Simulated traffic behavior profile (violation counts, major violations, fines, accidents). Generated only for real `SK_ID_CURR` keys, seeded, independent of `TARGET`, labeled `SYNTHETIC`, stored separately from original data. | `ingestion/synthetic_source/generator.py`, `docs/SYNTHETIC_DATA.md` |
| 4 | **Ingestion → Bronze** | Byte-identical file copy (`sha256`), chunked scanning, per-run manifest logging. **Idempotent:** re-ingesting an unchanged file is automatically skipped. Detects schema drift; failure in one source does not halt others. | `ingestion/base.py`, `ingestion/run_ingestion.py` |
| 5 | **Cleaning & Data Quality → Silver** | Type casting, column/value normalization (`365243`→NULL + flag, `XNA`→NULL, `Y/N`→`1/0`). Drops exact duplicates; quarantines primary-key duplicates. **Flags without dropping:** orphan rows, outliers, negatives, post-application dates. Reconciles: `rows_in = out + quarantined + dups`. | `processing/cleaning/` |
| 6 | **Integration → Gold** | **Aggregates every child table to the target grain first** (`bureau_balance`→`bureau`→customer; payment lines→installment→loan→customer), **then LEFT JOINs**. Asserts row counts after **every** join; halts pipeline on mismatch. | `processing/aggregation/`, `processing/integration/build_gold.py` |
| 7 | **Feature engineering** | 37 aggregated cross-source features plus 13 engineered financial features (`debt_to_income`, `loan_to_income`, `payment_delay_ratio`, `credit_utilization`, `total_outstanding_debt`, `has_previous_overdue`, …) with documented business formulas. | `processing/feature_engineering/features.py` |
| 8 | **Leakage prevention** | `TARGET` is isolated to `gold/training_labels`. Excludes events dated after the application date. Replay producer strips `TARGET`; streaming consumer quarantines any payload containing `TARGET`. | `build_gold.py`, `stream_processor.py` |
| 9 | **Data Lake on MinIO** | 3 buckets (`bronze`/`silver`/`gold`); Parquet with appropriate partitioning; Delta Lake optional (`CRB_LAKE_FORMAT=delta`). | `common/lake.py`, `storage/minio/`, `docker-compose.yml` |
| 10 | **Streaming** | Replay producer emits historical records as events (configurable rate, pause/resume/stop, replay, fault injection). Spark Structured Streaming handles malformed quarantine, **`event_id` deduplication**, **watermarking** for late events, **checkpoint** recovery, and per-customer aggregation. | `streaming/` |
| 11 | **Tests & documentation** | 39 pytest tests covering 12 pipeline requirements. Includes README, architecture, lineage, auto-generated data dictionary, DQ report, demo guide, and phase log. | `tests/`, `docs/`, `reports/` |

**Gold Outputs**

| Table | Grain | Contents |
|---|---|---|
| `integrated_credit_profile` | 1 row / `SK_ID_CURR` | Application attributes + aggregated features + engineered features, **no `TARGET`** |
| `customer_credit_features` | 1 row / `SK_ID_CURR` | Aggregated features across 7 sources |
| `loan_credit_features` | 1 row / `SK_ID_PREV` | Repayment behavior per prior Home Credit loan |
| `training_labels` | 1 row / `SK_ID_CURR` (train only) | `TARGET` label only |
| `data_quality_report` | 1 row / metric | Data quality check results |

### Data Classification

| Type | Location | Notes |
|---|---|---|
| Original historical data | `data/raw/` → `bronze/original/` | 10 Kaggle CSVs, never modified |
| Synthetic data | `data/synthetic/` → `bronze/synthetic/` | `DATA_ORIGIN = SYNTHETIC` |
| Simulated streaming data | Kafka topic or `data/_stream_landing/` | `event_time` is **replay** time (`time_semantics = SYNTHETIC_REPLAY`), not real transaction timestamps |
| Test fixture | Generated in temp dir during tests | Random values matching Home Credit schema, **not real data** |

### Course Concept Mapping

| Step | Lecture | Concept |
|---|---|---|
| Key-preserving subsetting | 4 | AQP: sampling, join-sampling pitfalls |
| Profiling, key relationship checks | 5, 2 | Source profiling (Bellman), instance-based matching |
| Per-source ingestion | 1, 5, 2 | Wrappers, source descriptions, autonomous sources |
| Adding synthetic source | 2 | GAV: mapping maintenance cost when adding a source |
| Silver cleaning | 1 | Syntactic / semantic / data heterogeneity, Veracity |
| Dedup, FK checks | 3 | Entity Resolution (exact match on shared `SK_ID_*` keys) |
| Gold | 1, 2, 4 | Mediated schema, materialized GAV (warehouse), data fusion, join algorithms, pre-join aggregation |
| DQ, leakage | 5, 1 | Source quality, temporal heterogeneity |
| Streaming | 4 | Adaptive processing, time-to-first-tuple, partial results, fault tolerance |

---

## 2. Setup

Requirements: Python 3.11+ (tested on 3.13), **Java 17 or 21**, Docker. Recommended RAM ≥ 8 GB for full dataset execution.

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d          # MinIO :9000/:9001 · Kafka :9092 · Kafka UI :8081
docker compose ps             # verify minio, kafka, kafka-ui are running
```

Download the dataset from https://www.kaggle.com/competitions/home-credit-default-risk/data and extract all **10 CSV files into `data/raw/`**. The pipeline reads from this directory only and never modifies or re-downloads it.

---

## 3. Running the Pipeline

Run all commands from the `apache-spark/` directory.

### 3.1. Quick Run on a Real Subset (Recommended First)
```bash
python -m scripts.make_dev_subset --customers 5000        # 5,000 real customers with all linked records
export CRB_PATHS_RAW_DIR=data/dev_subset/raw              # Windows PowerShell: $env:CRB_PATHS_RAW_DIR="data/dev_subset/raw"
python -m scripts.run_pipeline --sync-minio
```

### 3.2. Run on the Full Dataset
```bash
unset CRB_PATHS_RAW_DIR
CRB_SPARK_DRIVER_MEMORY=6g python -m scripts.run_pipeline --sync-minio
```
`run_pipeline` executes sequentially: profiling → synthetic generation → ingestion (Bronze) → cleaning (Silver) → integration (Gold + DQ) → MinIO sync → data dictionary. Each stage is timed; the pipeline halts if any stage fails.  
Add `--streaming-demo` to run a file-mode streaming pass; add `--skip-profiling` to skip profiling.

### 3.3. Step-by-Step Execution

| # | Command | Purpose |
|---|---|---|
| 1 | `python -m processing.profiling.profile_raw` | Profile raw files and verify key relationships |
| 2 | `python -m ingestion.synthetic_source.generator` | Generate `SYNTHETIC` source |
| 3 | `python -m ingestion.run_ingestion` | Ingest 7 sources into Bronze (re-running reports `skipped`) |
| 4 | `python -m processing.cleaning.clean_silver` | Clean and validate data quality → Silver |
| 5 | `python -m processing.integration.build_gold` | Aggregate, join, engineer features → Gold + DQ report |
| 6 | `python -m storage.minio.sync_lake` | Sync all 3 layers to MinIO |
| 7 | `python -m scripts.generate_data_dictionary` | Generate `docs/DATA_DICTIONARY.md` from actual lake schemas |

You can run a single source (`python -m ingestion.run_ingestion --sources external_credit_bureau [--force]`) or specific tables (`python -m processing.cleaning.clean_silver --tables bureau bureau_balance`).

### 3.4. Streaming (3 Terminals; Requires Batch Run First for Bronze Data)
```bash
# Terminal A – consumer: Spark Structured Streaming, 5-second micro-batch trigger
python -m streaming.consumers.stream_processor --source kafka --trigger 5 --reset-checkpoint

# Terminal B – producer: replays historical data at 10 events/s with fault injection
python -m streaming.producers.replay_producer --sink kafka --rate 10 \
    --dup-rate 0.05 --late-rate 0.02 --malformed-rate 0.01 --from-start

# Terminal C – control & inspect state
python -m streaming.producers.control pause | resume | stop | status
python -m streaming.consumers.snapshot          # current aggregated state per SK_ID_CURR
```
Without Kafka: replace `kafka` with `file` in both Terminal A and Terminal B (processing logic is identical).  
Fault recovery test: Ctrl-C Terminal A, then restart Terminal A **without** `--reset-checkpoint`. The consumer resumes without reprocessing previously committed events.

### 3.5. Tests
```bash
python -m pytest        # 39 tests, ~2 minutes; runs on temp-dir fixtures without touching data/
```

### 3.6. Configuration
All parameters are in `configs/pipeline.yaml` and can be overridden via `CRB_<SECTION>_<KEY>` environment variables. Examples:
- `CRB_PATHS_RAW_DIR=...`: override raw input directory.
- `CRB_SPARK_DRIVER_MEMORY=6g`: increase Spark driver memory.
- `CRB_LAKE_BACKEND=s3a`: write directly to MinIO.
- `CRB_LAKE_FORMAT=delta`: enable Delta Lake format.

---

## 4. Monitoring & Observability

| What to Inspect | Where |
|---|---|
| Execution logs | Terminal, `reports/logs/pipeline.log` |
| Profiling results & measured ERD | `reports/data_profiling/PROFILING_REPORT.md`, `reports/architecture/ERD.md` |
| Ingestion history | `data/bronze/_manifests/*.jsonl` |
| Quarantined rows & reasons | `data/silver/_quarantine/<table>/` |
| Before/after data quality | `docs/DATA_QUALITY_REPORT.md` |
| Pipeline stats (size, rows, throughput, timings, features) | `reports/PIPELINE_STATS.md` |
| Spark execution plan (pre-join aggregation visible) | http://localhost:4040 → **SQL / DataFrame** tab (while job is running) |
| Streaming rate charts | http://localhost:4040 → **Structured Streaming** tab |
| Kafka events | Kafka UI http://localhost:8081 → Topics → Messages |
| Lake objects | MinIO http://localhost:9001 (`minioadmin` / `minioadmin`) |
| Per-micro-batch streaming metrics | `reports/streaming/batch_metrics.jsonl`, `query_progress.jsonl` (dropped duplicates & late events) |

**Healthy Run Signals:**
- `silver` logs show `reconcile=True` for every table.
- `gold` logs show `check … PASS` for all checks.
- Re-running ingestion reports `skipped` for unchanged tables.
- Streaming `duplicates_dropped` matches injected duplicate count from producer.

Trace one customer across layers:
```bash
python -c "
import pandas as pd; ID=100002   # replace with an SK_ID_CURR present in your dataset
print(len(pd.read_parquet('data/silver/installments_payments', filters=[('sk_id_curr','=',ID)])), 'installment rows in Silver')
print(pd.read_parquet('data/gold/customer_credit_features', filters=[('sk_id_curr','=',ID)]).T)   # 1 aggregated row in Gold
"
```

---

## 5. Directory Structure

```
apache-spark/
├── common/            config, Spark session, lake I/O, schema registry (keys, grain, types, relations)
├── configs/           pipeline.yaml (settings), sources.yaml (7 logical sources)
├── data/              raw | synthetic | bronze | silver | gold   (gitignored)
├── ingestion/         base.py + 1 package per source + run_ingestion.py
├── processing/        profiling | cleaning | aggregation | feature_engineering | integration
├── streaming/         schemas (event contract) | producers | consumers
├── storage/minio/     client.py, sync_lake.py
├── scripts/           run_pipeline, make_dev_subset, make_test_fixture, generate_data_dictionary
├── tests/             39 pytest tests
├── docs/              ARCHITECTURE, DATA_LINEAGE, DATA_DICTIONARY, DATA_QUALITY_REPORT, DEMO_GUIDE, SYNTHETIC_DATA
├── reports/           profiling, data_quality, architecture (ERD), streaming, PIPELINE_STATS, PHASE_LOG
├── docker-compose.yml
└── requirements.txt
```

---

## 6. Verification Status & Known Limitations

The original build environment lacked access to Kaggle, Docker Hub, and Maven Central, so initial verification ran on the **test fixture** (later updated with a 5,000-customer real Home Credit subset in `reports/` and `docs/`). Re-run `python -m scripts.run_pipeline` on the full Kaggle dataset to refresh all metrics for the final report.

| Component | Status |
|---|---|
| Profiling, ingestion, Silver, Gold, synthetic, features, DQ | ✅ Working, tested |
| File-mode streaming (dedup, watermark, quarantine, checkpoint, recovery, replay) | ✅ Working, tested |
| Python S3 / MinIO upload (sync, direct ingestion) | ✅ Tested against S3-compatible mock (`moto`); needs live MinIO verification |
| `docker-compose.yml`, live Kafka producer/consumer | 🟡 Code present; requires Docker verification |
| Spark direct `s3a://` writes, Delta Lake | 🟡 Code & config present; first run requires internet for JAR download |
| Full Home Credit dataset run | ⏳ Run per Section 3.2 |

**Known Limitations:**
- Streaming `event_time` represents replay time. Home Credit `DAYS_*` columns are relative to each application date, so records cannot be placed on an absolute calendar timeline without fabricating dates.
- Replay producer emits rows in file order rather than globally sorted by timestamp.
- Streaming deduplication is bounded by the watermark window (10 minutes).
- `bureau_total_debt` sums debt across all currencies without FX conversion.
- Fuzzy Entity Resolution is omitted because sources share exact `SK_ID_*` keys; virtual integration and AQP are also out of scope (see Course Concept Mapping in Section 1).

## 7. Additional Documentation
[Architecture](docs/ARCHITECTURE.md) · [Lineage](docs/DATA_LINEAGE.md) · [Data Dictionary](docs/DATA_DICTIONARY.md) · [Data Quality](docs/DATA_QUALITY_REPORT.md) · [Demo Guide](docs/DEMO_GUIDE.md) · [Synthetic Data](docs/SYNTHETIC_DATA.md) · [Phase Log](reports/PHASE_LOG.md)
