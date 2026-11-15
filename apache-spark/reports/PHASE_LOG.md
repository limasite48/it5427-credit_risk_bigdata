# PHASE LOG – Objectives, Files, Design, Test Results, Issues & Fixes

Per requirement 16 of the project specification, each phase is logged below. Initial verification runs used the **test fixture** (3,000 simulated customers matching the Home Credit schema; see `scripts/make_test_fixture.py`) because the initial build environment had no network access to Kaggle.

## Phase 1 – Environment Survey, Fixture, Profiling

- **Objective:** verify environment; prepare runnable data; empirically validate profiling and ERD.
- **Files:** `common/{config,schemas,spark,lake,logging_utils}.py`, `configs/*.yaml`, `scripts/make_test_fixture.py`, `scripts/make_dev_subset.py`, `processing/profiling/profile_raw.py`.
- **Design:**
  - Single schema registry (`common/schemas.py`) declaring grain, PK, FK, data types, relative time columns, and sentinel values for every table, shared across all modules.
  - Profiling reads all columns as strings first before casting so cast failures are visible.
  - All relationships are **measured** (orphan count, parent coverage, min/avg/max children per parent) rather than inferred from shared column names.
- **Environment:** Python 3.13, OpenJDK 21, PySpark 3.5.3 running `local[2]`, 7 GB RAM. Kaggle, Docker Hub, and Maven Central blocked (HTTP 403).
- **Results:** profiled 8 files in ~50s. Detected all intentionally injected anomalies: 2 duplicate rows in `application_train` (non-unique PK), 4 orphans in `bureau`, 3 in `bureau_balance`, 5 in `installments`; post-application rows; disjoint train/test IDs; child `SK_ID_CURR` consistent with `previous_application` (0 mismatches).
- **Issues & fixes:** operator precedence bug when looking up application column descriptions caused the wrong dictionary to be selected; fixed by using explicit key selection.

## Phases 2–3 – Logical Sources, Bronze Ingestion, MinIO

- **Objective:** ingest each source independently, preserve raw data, support idempotent re-runs with metadata, and store on MinIO.
- **Files:** `ingestion/base.py`, `ingestion/*_source/ingest.py` (7 sources), `ingestion/run_ingestion.py`, `ingestion/synthetic_source/generator.py`, `storage/minio/{client,sync_lake}.py`, `docker-compose.yml`.
- **Design:**
  - Byte-identical copy with `sha256` checksum. Re-ingesting an identical `sha256` is skipped.
  - Chunked validation in 500k-row chunks (pandas). Every attempt (including failures) is logged to `bronze/_manifests/<table>.jsonl`.
  - Schema drift detection by comparing headers against the previous successful ingestion.
  - Failure in one file does not halt remaining sources.
- **Results:** 9 tables `SUCCESS` (654k rows, 25 MB) in 1s. Re-run: 9/9 `SKIPPED_ALREADY_INGESTED`. Synced to S3-compatible server (`moto`): 27 objects; re-run: 0 uploads, 27 unchanged. Direct S3 ingestion (`CRB_LAKE_BACKEND=s3a`) also verified. `docker compose config` valid.
- **Limitation:** live MinIO/Kafka containers were not started in that environment because images could not be pulled.

## Phase 4 – Cleaning (Silver), Integration & Features (Gold)

- **Objective:** rule-based cleaning, full row reconciliation, integrity checks, grain-safe integration, feature engineering, DQ reporting.
- **Files:** `processing/cleaning/{rules,clean_silver}.py`, `processing/aggregation/aggregates.py`, `processing/feature_engineering/features.py`, `processing/integration/{build_gold,dq_report}.py`.
- **Design:**
  - Remove exact duplicates only. Primary-key duplicates with differing payloads are quarantined. Orphan, outlier, negative, and post-application rows are **flagged and retained**.
  - Gold aggregates first, joins second, and asserts row counts after every join; any failed check halts the pipeline.
- **Results:**
  - Silver: all 9 tables reconciled (`rows_in = out + quarantined + dups`). Caught all injected duplicates (2 + 3 + 2), 1 missing-key row (quarantined), and all injected orphans and post-application rows.
  - Gold: 31/31 checks passed; `integrated_credit_profile` produced 3,000 rows = 3,000 applications, 102 columns (175 columns on the later 5,000-customer real Home Credit subset).
- **Issues & fixes:**
  1. *First run quarantined 1,107/2,581 application rows.* Cause: the "required header columns" list was reused as "non-null columns", causing missing `OCCUPATION_TYPE` (~31%) and `NAME_CONTRACT_TYPE='XNA'` to drop valid rows. Fix: split into `required` (header check) and `not_null` (keys and essential fields only). After fix: 0 application rows quarantined.
  2. *Outliers were never flagged on small samples.* Cause: the 0.999 quantile equaled the max value while the check used `>`. Fixed to `>=`.
- **Note:** 11 `installments` rows had negative `AMT_PAYMENT` due to fixture generation of partial payments; the pipeline flagged `_invalid_negative` as designed.

## Phases 5–6 – Synthetic Source, Replay Producer, Structured Streaming

- **Objective:** simulate continuous event ingestion; handle malformed, duplicate, and late events; support checkpoint recovery.
- **Files:** `streaming/schemas/event_schema.py`, `streaming/producers/{replay_producer,sinks,control}.py`, `streaming/consumers/{stream_processor,snapshot}.py`.
- **Design:**
  - Deterministic `event_id` (`evt_<table>_<offset>`), making replays automatically deduplicated.
  - Two streaming queries: quarantine and main. Main query uses watermark → `dropDuplicatesWithinWatermark` → `foreachBatch`, overwriting by `batch_id` partition (idempotent micro-batch writes).
  - Producer supports rate control, pause/resume/stop, offset persistence for resume, and seeded fault injection.
- **Results (3,000 events + 141 duplicates + 38 malformed + 64 late events):** 5 micro-batches; 38 quarantined; 141 duplicates dropped; 37 late events dropped by watermark; 2,963 events written. Restarting with no new data: 0 rows. Replaying 300 old rows: 296 dropped as duplicates; 4 written were the exact 4 events previously dropped as late (never committed to state).
- **Issues & fixes:**
  1. *All 18 files landed in 1 micro-batch, so late events were not dropped.* Watermark is only initialized after the first batch per Spark's execution model. Fix: added `files_per_trigger` (default 4) to create multiple micro-batches and documented this behavior.
  2. *Py4J exit error.* Cause: Python listener remained registered when JVM stopped. Fix: call `removeListener` before `stop_spark()`.
  3. *Quarantine count reported `-5`.* File sink returns `numOutputRows = -1`. Fix: count quarantine table rows before and after the run.
  4. Initial summary reported `duplicates_dropped = 0` because the metric lives under `stateOperators[].customMetrics`. Fixed listener and summary extraction.

## Phase 7 – Tests, Documentation, Statistics

- **Files:** `tests/*` (39 tests), `scripts/run_pipeline.py`, `scripts/generate_data_dictionary.py`, `docs/*`, `README.md`.
- **Results:** `pytest`: **39 passed** (~2 minutes) on 500-customer temp-dir fixtures. Full pipeline on 3,000-customer fixture including MinIO sync and streaming demo: ~135 seconds.
- **Issues & fixes:**
  - Test `read` fixture held a `SparkSession` already stopped by the streaming consumer. Fix: always fetch the active session.
  - Missing `tzdata` package for `toPandas()` with `Asia/Saigon` timezone. Added to `requirements.txt`.
  - Some tests lacked fixture dependencies on the pipeline run and failed in isolation. Added explicit fixture dependencies.

## Remaining User Actions

1. Place all 10 Kaggle CSVs into `data/raw/`, run `python -m scripts.run_pipeline --sync-minio`, and include full-dataset metrics from `reports/` and `docs/DATA_QUALITY_REPORT.md` in the final report.
2. Run `docker compose up -d` and execute the Kafka demo per `docs/DEMO_GUIDE.md` Section 3 (first Spark run needs internet to fetch `spark-sql-kafka` and `hadoop-aws` JARs if using `s3a`).
3. Update the verification status table in `README.md` once live MinIO, Kafka, and `s3a` are verified.
