# Ingestion Layer: MinIO, Kafka, and Supplementary Data

**Responsible Engineer:** Khai. Covers the 3 assigned areas:
1. **Ingestion & MinIO Bucket Architecture:** Sections 2, 3, and 6
2. **Supplementary Macroeconomic Data** (Regional GDP growth, unemployment rate, CPI): Section 5
3. **Kafka Streaming Event Schema & Enforcement:** Section 4

All components have been verified on active Kafka and MinIO instances (Section 7).

---

## 0. Quick Summary for Integration Engineers

| Service | From Host Machine | From Containers (`hc-net` network) |
|---|---|---|
| **Kafka Broker** | `localhost:9092` | `kafka:19092` |
| **MinIO S3 API** | `http://localhost:9000` | `http://minio:9000` |
| **MinIO Web Console** | `http://localhost:9001` | N/A |

* **Kafka Topics:** 7 source topics (`hc.<system>.<entity>.v1`) + 1 dead-letter queue topic (`hc.dlq.v1`). Message Key = `customer_id` (`SK_ID_CURR`). Message Value = JSON.
* **MinIO Buckets:** 8 buckets (`landing` → `bronze` → `silver` → `gold`, plus `quarantine`, `reference`, `checkpoints`, and `models`).
* **Ingestion Scope:** The ingestion layer **only writes to `landing`, `bronze`, `quarantine`, and `reference`**. The `silver` and `gold` layers are owned by the processing (Spark) and ML teams.
* **Batch vs. Stream Boundary:** Historical data before **`2025-12-01`** is pre-loaded into `bronze` via batch backfill. Data from `2025-12-01` to `2025-12-28` is streamed in real time via Kafka.
* **Timeline Consistency:** All downstream modules requiring timestamps must reference **`reference/timeline/application_timeline.parquet`** rather than generating independent dates.

---

## 1. End-to-End Pipeline Architecture

```
 Simulated Sources                Kafka Broker                           MinIO Lakehouse
 (Kaggle CSVs, data/staged)
 LOS (Loan Applications)  ─┐                                           ┌─ bronze ──▶ silver ──▶ gold
 CREDIT_BUREAU            ─┤  producer.py   hc.los.loan-application.v1│   (Parquet,   (Spark: clean,
 LOAN_HISTORY_DB          ─┼────────────▶  hc.bureau.* / history.* ───┤    append)     feature store, score)
 POS_PARTNER              ─┤ (ordered by    hc.core.installment.*     │
 CARD_SYSTEM              ─┤  event_time)   kafka_to_bronze.py ───────┤
 CORE_BANKING             ─┘                (schema validation)        │
                                            errors ──▶ hc.dlq.v1 ──────┴─ quarantine
 
 History (< 2025-12-01) ── backfill.py ───────────────────────────────▶ landing (raw CSV) + bronze
 STATS_OFFICE (Macro CSV)─ backfill.py ───────────────────────────────▶ landing + bronze
 Reference Tables ──────── backfill.py ───────────────────────────────▶ reference
```

**Recommendation for Downstream Modules:**
* **Real-time Scoring:** Read live applications directly from Kafka using Spark Structured Streaming or Python consumer with a dedicated consumer group.
* **Training & Batch Analytics:** Read `bronze` / `silver` Parquet tables. The bronze sink uses consumer group `hc-bronze-sink`, ensuring downstream consumers maintain independent offsets.

---

## 2. Simulated Timeline Conventions (Critical)

The original Home Credit dataset **contains no calendar dates**; all temporal metrics are relative day/month offsets counted backwards from the application date. The ingestion layer assigns deterministic timestamps using a fixed seed (`SEED = 2026`), ensuring exact reproducibility:

* **Application Submission Time (`application_time`):** Randomly assigned across 104 weeks between `2024-01-01` (Monday) and `2025-12-28`. The day of the week matches `WEEKDAY_APPR_PROCESS_START` and the hour matches `HOUR_APPR_PROCESS_START`. Timezone: `+07:00`.
* **Derivation of `event_time` by Source:**

| Source Table | Derivation Rule for `event_time` |
|---|---|
| `application` | `application_time` |
| `bureau`, `bureau_balance` | `application_time` − 5 minutes (credit bureau report retrieved during underwriting) |
| `previous_application` | Application date + `DAYS_DECISION`, hour = `HOUR_APPR_PROCESS_START` of past application |
| `POS_CASH_balance`, `credit_card_balance` | 02:00 on the 1st day of the month following `MONTHS_BALANCE` |
| `installments_payments` | Application date + `DAYS_ENTRY_PAYMENT` (or `DAYS_INSTALMENT` if missing), during business hours |

* **Causal Sequencing Constraints:** All historical events occur at least 10 minutes prior to the application (and 5 minutes for credit bureau reports). Because the Kafka producer partitions by `customer_id` and publishes in `event_time` order, **a customer's history is guaranteed to arrive before their loan application**.
* **Real-Time Streaming Cutoff (`LIVE_START = 2025-12-01`):** Events before this cutoff are batch-loaded into `bronze`. Events from this cutoff onward are published to Kafka. The real-time window contains **13,560 loan applications and ~979,000 total events**.
* **Recommended Out-of-Time (OOT) Splits (`reference/loan_outcomes/`):**
  * `split = train`: 295,794 labeled historical applications (Default Rate: 8.07%).
  * `split = live_holdout`: 11,717 labeled applications in the real-time window (Default Rate: 8.11%).
  * This matches realistic credit risk model evaluation out-of-time. The target label `TARGET` is **excluded** from application events because default status is only known months after loan disbursement.
* Payloads preserve the raw `DAYS_*` and `MONTHS_*` columns. The silver layer converts these to true dates: `application_time + DAYS_*`.

---

## 3. MinIO Lakehouse Bucket Architecture

| Bucket | Purpose | Write Access | Read Access | Storage Format | Lifecycle / Notes |
|---|---|---|---|---|---|
| `landing` | Immutable raw source files as ingested | `svc-ingest` | `svc-spark` | CSV | Versioning enabled |
| `bronze` | Raw data, append-only; uniform schema for batch and stream | `svc-ingest` | `svc-spark` | Parquet (snappy) | Deletion prohibited |
| `silver` | Cleaned: deduplicated by `event_id`, typed, anomalies handled, dates parsed | `svc-spark` | `svc-spark`, `svc-ml` | Parquet | Owned by Spark team |
| `gold` | Curated feature store, risk scores, aggregated BI tables | `svc-spark`, `svc-ml` | `svc-ml`, `svc-dashboard` | Parquet | Owned by ML & BI |
| `quarantine` | Corrupted, unparseable, or orphan records with failure reason | `svc-ingest`, `svc-spark` | None | Parquet | Auto-expires after 30 days |
| `reference` | Timelines, labels, region mappings, data dictionaries, JSON Schemas | `svc-ingest` | All services | Parquet / CSV / JSON | Versioning enabled |
| `checkpoints` | Spark Structured Streaming offsets and state store | `svc-spark` | `svc-spark` | Binary / JSON | |
| `models` | Serialized trained ML model artifacts and metrics | `svc-ml` | `svc-ml` | Joblib / ONNX | Versioning enabled |

**S3 Path Structure:**
```
landing/<source_system>/<table>/ingest_date=YYYY-MM-DD/<raw_file>
bronze/<source_system>/<table>/ingest_date=YYYY-MM-DD/part-*.parquet
    source_system: los | credit_bureau | loan_history_db | pos_partner | card_system | core_banking | stats_office
quarantine/credit_bureau/bureau_balance/reason=orphan_sk_id_bureau/ingest_date=.../   (from backfill)
quarantine/stream/ingest_date=.../                                                     (from sink, copy of DLQ)
reference/timeline/  loan_outcomes/  region_dim/  column_descriptions/  schemas/
```

**Bronze Column Structure:**
Batch and streaming files share an identical schema:
* `Envelope:` `event_id`, `event_type`, `schema_version`, `source_system`, `source_table`, `event_time`, `customer_id`, `produced_at`
* `Payload:` Original columns and values preserved verbatim.
* `Metadata:` `kafka_topic`, `kafka_partition`, `kafka_offset` (null for batch backfill), `ingest_time`, `ingest_mode` (`batch_backfill` | `stream`). Timestamps use `timestamp[us, +07:00]`.

**Least-Privilege IAM Service Accounts** (initialized by `infra/minio/init-buckets.sh`):

| User | Policy Permissions |
|---|---|
| `svc-ingest` | Write `landing`, `bronze`, `quarantine` (no delete); Read/Write/Delete `reference` |
| `svc-spark` | Read `landing`, `bronze`, `reference`; Read/Write/Delete `silver`, `gold`, `checkpoints`; Write `quarantine` |
| `svc-ml` | Read `reference`, `silver`; Write `gold`; Read/Write/Delete `models` |
| `svc-dashboard` | Read-only `gold`, `reference` |

Root account `minioadmin` is reserved strictly for administrative infrastructure tasks.

**Spark MinIO & Kafka Connectivity:**
```python
spark = (SparkSession.builder
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
  .config("spark.hadoop.fs.s3a.access.key", "svc-spark")
  .config("spark.hadoop.fs.s3a.secret.key", "spark-secret-123")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
  .getOrCreate())

apps = spark.read.parquet("s3a://bronze/los/application/")

stream = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "kafka:19092")
  .option("subscribe", "hc.los.loan-application.v1")
  .load())
```

> ⚠️ **MinIO Container Image Note:** Official `minio/minio` community builds were retired in October 2025. The Docker Compose environment uses Bitnami's archived image (`bitnamilegacy/minio:2025.7.23-debian-12-r5`). It runs reliably for course projects and demo environments. User and policy management is performed via the MinIO Client (`mc`).

---

## 4. Kafka Streaming Event Schema & Enforcement

**Kafka Topic Registry:**

| Topic | Source System | Raw Table | Event Type | Real-Time Events* |
|---|---|---|---|---|
| `hc.los.loan-application.v1` | LOS | `application_train` + `test` | `loan_application.submitted` | 13,560 |
| `hc.bureau.credit-record.v1` | CREDIT_BUREAU | `bureau` | `bureau.credit_record.reported` | 64,700 |
| `hc.bureau.credit-balance-monthly.v1` | CREDIT_BUREAU | `bureau_balance` | `bureau.credit_balance.reported` | 891,832 |
| `hc.history.previous-application.v1` | LOAN_HISTORY_DB | `previous_application` | `previous_application.decided` | 586 |
| `hc.pos.cash-balance-monthly.v1` | POS_PARTNER | `POS_CASH_balance` | `pos_cash.statement_issued` | 3,674 |
| `hc.card.balance-monthly.v1` | CARD_SYSTEM | `credit_card_balance` | `card.statement_issued` | 2,382 |
| `hc.core.installment-payment.v1` | CORE_BANKING | `installments_payments` | `installment.payment_recorded` | 2,035 |
| `hc.dlq.v1` | Stream Sink | (Malformed / invalid events) | `dlq.event` | Dynamic |

\* Total event counts in the real-time window (`2025-12-01` to `2025-12-28`). Source topics are configured with 3 partitions and 7-day retention; DLQ has 1 partition and 30-day retention. Kafka topic auto-creation is disabled; topics are provisioned via `infra/kafka/create-topics.sh`.

**Message Structure:**
* **Key:** `customer_id` (`SK_ID_CURR`) as a UTF-8 string. All events for a customer map to the same partition, guaranteeing chronological delivery.
* **Value:** UTF-8 JSON object containing envelope metadata and raw `payload`.
* **Headers:** `event_type`, `schema_version`.

| Envelope Field | Type | Description |
|---|---|---|
| `event_id` | String (32 hex) | BLAKE2b-128 hash of `source_table \| natural_key`. Deterministic across runs for de-duplication. |
| `event_type` | String | Event classification (e.g., `loan_application.submitted`). |
| `schema_version` | String | Semantic version (`1.0.0`). |
| `source_system` | String | Emitting enterprise source system. |
| `source_table` | String | Underlying table name. |
| `event_time` | String (ISO-8601) | Business event timestamp (simulated, `+07:00`). |
| `produced_at` | String (ISO-8601) | Wall-clock timestamp when published by producer. |
| `customer_id` | Integer | `SK_ID_CURR`. |
| `payload` | Object | Verbatim row data: original column names and raw values preserved. |

**JSON Schema Contract (`schemas/<topic>.schema.json`):**
* Adheres to JSON Schema Draft 2020-12.
* Types and nullability derived from the complete staged dataset.
* Categorical columns include `x-observed-values` to guide silver-layer encoders.
* Verified against 2,000 sample events per topic with **0 validation errors**.

**Dead-Letter Queue (DLQ):**
Malformed JSON or records violating the schema are redirected to `hc.dlq.v1` (schema: `schemas/hc.dlq.v1.schema.json`) and persisted to `quarantine/stream/`. The sink process logs the error and continues streaming without interruption.

---

## 5. Supplementary Macroeconomic Data (Simulated)

### Why Simulation Was Necessary
The Home Credit dataset anonymizes all geographic and temporal indicators. Joining external statistics (such as World Bank or OECD indicators) to anonymized regions without authentic location metadata introduces arbitrary spurious correlations. Therefore, regional macroeconomic indicators were **simulated under controlled statistical assumptions** and must be documented as simulated data in final reports.

### Region Identification
The feature `REGION_POPULATION_RELATIVE` contains exactly 82 discrete values, each having a strict 1-to-1 relationship with `REGION_RATING_CLIENT`. These represent 82 anonymous regions cataloged in `region_dim.csv` (`R01` to `R82`), where `R01` is the most populous:
* 5 regions in Rating 1 (highest socio-economic status)
* 58 regions in Rating 2 (medium socio-economic status)
* 19 regions in Rating 3 (lower socio-economic status)

### Publication Formats & Files (`data/supplementary/`)
To reflect real-world multi-source integration challenges, the 3 macroeconomic datasets are deliberately formatted across **3 different reporting cadences**:

| File | Structure / Columns | Total Rows |
|---|---|---|
| `region_dim.csv` | `region_code`, `region_population_relative`, `region_rating`, `n_applications` | 82 |
| `regional_gdp_growth_quarterly.csv` | `region_code`, **period** (`2025-Q3`), `gdp_growth_yoy_pct`, `release_date` | 3,280 |
| `regional_unemployment_quarterly.csv` | `region_code`, **year, quarter**, `unemployment_rate_pct`, `release_date` | 3,280 |
| `regional_cpi_monthly.csv` | `region_code`, **month** (`2025-09`), `cpi_index` (2015-01=100), `cpi_yoy_pct`, `release_date` | 9,840 |

Covers 2016–2025, spanning the complete historical timeline of applicant loans.

### Generation Methodology (`ingestion/gen_macro.py`)
Generated using: National baseline (GDP 5.5%, Unemployment 3.0%, Inflation 3.2%) + macroeconomic cycle + pandemic shock (2020–2021) + regional rating fixed effects + region-specific random effects + AR(1) autoregressive noise (`SEED = 2026`). **Target variable `TARGET` is not used**, ensuring zero synthetic target leakage.

**2025 Regional Averages by Socio-Economic Rating:**

| Region Rating | GDP Growth (%) | Unemployment (%) | CPI YoY (%) |
|---|---|---|---|
| **1 (Best)** | 7.59% | 1.65% | 3.48% |
| **2 (Medium)** | 5.75% | 2.78% | 3.16% |
| **3 (Lower)** | 4.61% | 3.95% | 3.08% |

### Point-in-Time Join Logic (Avoiding Future Lookahead Bias)
1. Match `region_code` via `round(REGION_POPULATION_RELATIVE, 6) == region_population_relative`.
2. For each indicator, filter for the latest record where `release_date <= application_date`.

*Example:* An application submitted on `2025-12-10` joins November 2025 CPI (released 2025-12-06), Q3 2025 GDP (released 2025-10-30), and Q3 2025 Unemployment (released 2025-11-14).

---

## 6. Execution Instructions

```bash
# 1. Setup virtual environment & dependencies
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt

# 2. Start container infrastructure (Kafka + MinIO + Init jobs)
docker compose -f infra/docker-compose.yml up -d

# 3. Generate supplementary macro data
python ingestion/gen_macro.py

# 4. Stage raw events with simulated timestamps (~3 min)
python ingestion/stage.py

# 5. Generate JSON Schemas and validate samples
python ingestion/gen_schemas.py

# 6. Batch backfill historical records into MinIO (~1.5 min)
LAKE_MODE=minio python ingestion/backfill.py --landing

# 7. Start real-time streaming pipeline (two terminal windows):
# Terminal 1: Kafka-to-Bronze consumer sink
LAKE_MODE=minio python ingestion/kafka_to_bronze.py

# Terminal 2: Source streaming event producer (~11 min for entire live window)
python ingestion/producer.py
```

*PowerShell Environment Setup:* `$env:LAKE_MODE="minio"`  
*Teardown Infrastructure:* `docker compose -f infra/docker-compose.yml down -v`

| Configuration Key | Default Value | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker bootstrap address |
| `LAKE_MODE` | `minio` (or `local` for local `lake/` folder) | Storage target mode |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` | `svc-ingest` | MinIO ingestion user |
| `MINIO_SECRET_KEY` | `ingest-secret-123` | MinIO ingestion password |
| `HC_LIVE_START` | `2025-12-01` | Real-time streaming cutoff date |
| `HC_RAW_DIR` | Auto-detects `../_data` | Path to raw Kaggle CSV files |

---

## 7. Verification & Benchmark Results (Verified 2026-11-11)

* **Staging Phase:** 55.37 million total events processed; **0 duplicate `event_id` occurrences**. Exactly 3.12 million orphan records in `bureau_balance` (missing `SK_ID_BUREAU` in `bureau.csv`) successfully isolated into `quarantine/`.
* **MinIO Backfill Throughput:** Loaded in 94 seconds. `landing` bucket contains 11 files (2.68 GB); `bronze` bucket contains 51 files (2.60 GB). Across all 7 entities, `batch rows + stream rows == total staged records`.
* **Kafka Producer Speed:** Real-time day (46,411 events) published in 1.5 seconds at peak speed (`--speed 0`). Simulated 2-hour window at 3,600x speed completed in 2.2 seconds.
* **Stream Sink Integrity:** Exactly 56,588 / 56,588 test events ingested into bronze with 100% receipt. Synthetic bad messages (malformed JSON, invalid string in numeric amount) routed to `hc.dlq.v1` and `quarantine/stream/` without process interruption.
* **Schema Parity:** For all 7 source tables, files produced by batch backfill and files produced by the streaming sink exhibit **identical Parquet schemas and types**.
* **Security Validation:** `svc-dashboard` write access to bronze blocked (403); `svc-ingest` delete access to bronze blocked (403); `svc-spark` write access to bronze blocked (403); `svc-spark` write access to silver permitted (200).

---

## 8. Requirements for Downstream Silver Layer (Processing Team)

The ingestion layer intentionally preserves raw source fidelity in `bronze`. The **Silver Layer** (Spark ETL) is responsible for:
1. **De-duplication:** Deduplicate streaming micro-batches using `event_id`.
2. **Sentinel Value Handling:** Transform placeholder `365243` in `DAYS_EMPLOYED` and `XNA`/`XAP` categorical values into standardized nulls.
3. **Installment Aggregation:** Group split payment records by `(SK_ID_PREV, NUM_INSTALMENT_VERSION, NUM_INSTALMENT_NUMBER)`.
4. **Delinquency Standardization:** Harmonize heterogeneous delinquency metrics (Credit Bureau status codes `0-5/C/X`, POS/Card `SK_DPD`, and installment late day calculations).
5. **Macroeconomic Point-in-Time Join:** Integrate regional indicators from `bronze/stats_office/` using the release date rules outlined in Section 5.
