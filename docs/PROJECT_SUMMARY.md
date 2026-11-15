# Project Summary — Credit Risk Intelligence (IT5427)

Course: **IT5427 – Big Data**. Topic: Multi-source big data integration to assess and predict credit risk on the **Home Credit Default Risk** dataset.

Business objective: when a customer applies for a loan, estimate the **Probability of Default (PD)**, assign a risk tier, and recommend an underwriting decision for credit officers.

Design flow:

```text
CSV / macro / event sources
        →  storage (MinIO)
        →  processing (Apache Spark: Bronze → Silver → Gold)
        →  scoring (Logistic Regression + LightGBM)
        →  decisions on the dashboard
```

The platform reads synchronized evaluation, portfolio, and pipeline artifacts committed under `outputs/` and `apache-spark/reports/` (upgradable to the complete **307,511-row** Home Credit dataset via `notebooks/colab_full_pipeline.ipynb` / `scripts/run_colab_full_pipeline.py`):

- Pre-computed model & portfolio artifacts in `outputs/` (interactive scorer model `credit_risk_model.joblib`, Gold model `gold_credit_risk_model.joblib`, evaluation metrics, ROC/PR & SHAP summary figures, and Gold lookup scores).
- Spark pipeline reports in `apache-spark/reports/` (updated automatically alongside `DATA_QUALITY_REPORT.md` and `PROFILING_REPORT.md` during full Colab/Spark execution).
- Gold lookup scores in `outputs/gold_scored_applications.csv` (lookup by `SK_ID_CURR` on Underwriting).

Demo credentials (defaults in `.env.example`; if `.env` was edited, use the values in `.env`):

| Username | Password | Role |
| --- | --- | --- |
| `analyst` | `AnalystDemo!234` | `RISK_ANALYST` |
| `underwriter` | `UnderwriterDemo!234` | `UNDERWRITER` |
| `admin` | `AdminDemo!234` | `ADMIN` |

Local: http://127.0.0.1:5173. Docker: http://localhost:8088. To both view analytics reports and score applications, sign in as `underwriter` / `UnderwriterDemo!234`.

---

## 1. Repository Modules

| Component | Directory | Role |
| --- | --- | --- |
| Ingestion & storage | `ingestion-minio-kafka/` | Ingest CSVs into MinIO lake, publish Kafka events, generate regional macro data (GDP, unemployment, CPI across 82 regions). |
| Spark processing | `apache-spark/` | Profiling, Bronze ingestion, Silver cleaning, Gold aggregation & join, feature engineering, streaming, MinIO sync. |
| Analytics & modeling | `src/`, `outputs/`, `run_track_a.py` | Preprocessing, Logistic Regression, LightGBM, cost-sensitive threshold, SHAP, scoring service. |
| Dashboard | `frontend/`, `backend/`, `ml-service/` | React UI, Spring Boot API (JWT), FastAPI scoring adapter. |
| Documentation | `docs/`, `README.md` | Architecture, demo guide, security notes, project summary, cross-team contracts. |

### 1.1. Ingestion (MinIO + Kafka)

- Raw dataset resides in `_data/` (not committed to git).
- Staging simulates a 104-week timeline. The configured live cutoff date is `2025-12-01`.
- Data Lake includes `landing`, `bronze`, `reference`, and `quarantine` buckets (plus `silver`, `gold`, `checkpoints`, `models` in the 8-bucket layout).
- Kafka defines per-source topics plus a dead-letter queue (`hc.dlq.v1`).
- Supplementary macro CSVs live in `ingestion-minio-kafka/data/supplementary/` and power the **Macro & Regions** dashboard page.

### 1.2. Apache Spark Medallion Layer (`apache-spark/`)

Seven logical sources: loan applications, credit bureau, previous applications, installments, POS/cash, credit cards, and a **synthetic** traffic behavior profile (violation counts, major violations, fines, accidents). The synthetic source is keyed only to real `SK_ID_CURR` IDs, seeded, and flagged as `SYNTHETIC`.

```text
7 sources → Bronze (raw CSV) → Silver (cleaned Parquet) → Gold (1 row / customer)
                └→ replayed as events → Kafka or file → Spark Structured Streaming
```

Join rule: never join a multi-row child table directly onto applications. Each child table is aggregated to its parent key first, then `LEFT JOIN`ed. Row counts are verified after every join. `TARGET` is stored solely in `gold/training_labels`, never in feature tables.

Gold tables:

| Table | Grain |
| --- | --- |
| `integrated_credit_profile` | 1 row / `SK_ID_CURR`, no `TARGET` |
| `customer_credit_features` | Aggregated features across 7 sources |
| `loan_credit_features` | 1 row / previous loan (`SK_ID_PREV`) |
| `training_labels` | `TARGET` labels, train split only |
| `data_quality_report` | Data quality check results |

Committed reports are from a **real Home Credit subset** (run on 2026-11-14), not the synthetic test fixture and not yet the full dataset:

| Metric | Value |
| --- | ---: |
| Bronze rows | 799,487 |
| Bronze size | 37.9 MB |
| Silver rows retained | 799,487 |
| Gold profiles (`integrated_credit_profile`) | 5,000 |
| Prior loans (`loan_credit_features`) | 23,990 |
| Train labels | 4,316 |
| Feature columns | 175 |
| Gold checks | 31/31 passed |

Gold features include application attributes plus aggregated bureau metrics (counts, debt, overdue), prior applications, late installments, POS DPD, credit card utilization, traffic violations, and engineered ratios such as `debt_to_income` and `loan_to_income`.

### 1.3. ML Models & Explainability

The unified platform uses two complementary LightGBM models plus a Logistic Regression benchmark (`src/models/baseline.py`):
- **Full Gold Integrated Model (`scripts/train_gold_model.py`):** Trained on the Spark Gold 175-column multi-source profile (`outputs/models/gold_credit_risk_model.joblib`), producing full ROC-AUC, PR-AUC, F1, 5:1 cost optimization, Gold `roc_curve.png`, TreeSHAP `shap_summary.png`, and `outputs/gold_scored_applications.csv` for ID lookup.
- **Interactive Real-Time Application Model (`run_track_a.py`):** Trained directly on application + macro features (`outputs/models/credit_risk_model.joblib`) for low-latency manual underwriting form scoring via FastAPI `/score`.

- Cost-sensitive matrix: approving a defaulter is penalized 5x more than rejecting a good customer (**FN:FP = 5:1**).
- Risk tiers: **LOW** < 5%, **MEDIUM** 5–15%, **HIGH** ≥ 15%.
- `scripts/standardize_dashboard.py` synchronizes Overview KPIs, segment rates, evaluation JSON, ROC/PR curve, SHAP summary figure, and validation sample scores cleanly onto the Gold dataset.

### 1.4. Dashboard

```text
React (Vite) → Spring Boot API (JWT) → FastAPI (internal) → joblib model
                    ├─ reads outputs/                  (KPIs, segments, scored CSV, ROC)
                    ├─ reads supplementary/            (regional macro data)
                    └─ reads apache-spark/reports/     (Data Pipeline page)
```

No application database is required. Demo accounts and refresh tokens are held in memory.

Pages:

| Page | Route | Contents |
| --- | --- | --- |
| Overview | `/` | NPL, application count, LOW/MEDIUM/HIGH distribution, ROC curve, and Spark pipeline highlight card |
| Segment Risk | `/segments` | Risk breakdown by occupation, income type, education, and `EXT_SOURCE_2` quintiles |
| Macro & Regions | `/macro` | Regional slices, unemployment, GDP; interactive unemployment and CPI stress simulation |
| Data Pipeline | `/pipeline` | Bronze / Silver / Gold stats, stage timings, features, streaming metrics, 31 DQ checks |
| Underwriting | `/underwriting` | Score a new applicant or look up `SK_ID_CURR` in scored datasets |

Pipeline API endpoint: `GET /api/insights/pipeline` (requires authentication).

---

## 2. Setup

Two supported deployment modes: Docker Compose or local toolchain.

### 2.1. Software Prerequisites

Local run:

- JDK **25**
- Python **≥ 3.10** (3.12 recommended)
- Node.js **20+**
- macOS Apple Silicon: `brew install libomp` (required by LightGBM)

Docker run: Docker Desktop. The backend image uses Temurin 25, so a local JDK is not needed.

The full Kaggle dataset (`_data/`) is **not required** to launch the dashboard. Pre-trained model artifacts are in `outputs/` and Spark reports are in `apache-spark/reports/`.

### 2.2. Environment Configuration

From the repository root:

```bash
cp -n .env.example .env
```

`./scripts/setup.sh` also copies this file if missing. `.env` is gitignored.

Key variables:

| Variable | Purpose | Default in `.env.example` |
| --- | --- | --- |
| `JWT_SECRET` | Signs access tokens (min 32 chars) | `change-me-jwt-secret-must-be-32-chars-min` |
| `ML_SERVICE_TOKEN` | Shared token for Spring → FastAPI calls | `change-me-ml-internal-token-32chars` |
| `CORS_ALLOWED_ORIGINS` | Allowed frontend origins | `localhost` and `127.0.0.1` on ports `5173` and `8088` |
| `BACKEND_PORT` | Backend API port | `8080` |
| `FRONTEND_PORT` | Docker UI port | `8088` |
| `VITE_DEV_PORT` | Local dev UI port | `5173` |
| `ML_SERVICE_PORT` | Local ML service port | `8000` |

Rotate `JWT_SECRET` and demo passwords before sharing or deploying beyond local use.

### 2.3. Local Setup (One-Time)

```bash
./scripts/setup.sh
```

Verifies Java 25, Python, and Node, creates `.env` if absent, creates `.venv`, installs `requirements.txt`, runs `npm install` in `frontend/`, and resolves Maven dependencies. Exits with an error if `outputs/models/credit_risk_model.joblib` or KPI files are missing.

---

## 3. Starting the Dashboard

### 3.1. Docker (Recommended for Demos)

From the repository root:

```bash
cp -n .env.example .env
docker compose up --build -d
docker compose ps
```

Open [http://localhost:8088](http://localhost:8088).

- UI: port `8088`
- API: port `8080`
- ML service is **not** exposed on host ports; only `backend` on the internal Docker network can reach it.

Logs and shutdown:

```bash
docker compose logs -f
docker compose down
```

The backend image bundles `outputs/`, supplementary macro CSVs, and `apache-spark/reports/` so the Data Pipeline page works immediately on startup.

### 3.2. Local Mode (During Active Development)

```bash
./scripts/setup.sh
./scripts/start.sh
./scripts/status.sh
```

URLs:

| Service | URL |
| --- | --- |
| Frontend | http://127.0.0.1:5173 |
| Backend API | http://127.0.0.1:8080 |
| ML service (local only) | http://127.0.0.1:8000 |
| Swagger (`local` profile) | http://127.0.0.1:8080/swagger-ui.html |

Logs are written to `.run/logs/`. Stop all three services (including Maven/npm child processes and port holders):

```bash
./scripts/stop.sh
./scripts/terminate.sh   # immediate SIGKILL
```

`start.sh` automatically points `SPARK_REPORTS_DIR` to `apache-spark/reports`.

Corresponding Windows PowerShell scripts: `scripts/setup.ps1`, `scripts/start.ps1`, `scripts/stop.ps1`, `scripts/status.ps1`.

---

## 4. Signing In

Passwords below are defaults from `.env.example` (use `.env` values if modified):

| Username | Role | Default Password | Capabilities |
| --- | --- | --- | --- |
| `analyst` | `RISK_ANALYST` | `AnalystDemo!234` | Overview, Segment Risk, Macro & Regions, Data Pipeline, Underwriting |
| `underwriter` | `UNDERWRITER` | `UnderwriterDemo!234` | Same as above + full underwriting workflow |
| `admin` | `ADMIN` | `AdminDemo!234` | Adds `/api/admin/users` and `/api/health` |

Steps:

1. Open http://127.0.0.1:5173 (local) or http://localhost:8088 (Docker).
2. Enter username and password.
3. Click **Sign in**.

Access tokens live in browser memory (15-minute TTL). Refresh tokens use an `HttpOnly` cookie (7-day TTL). Five failed logins trigger a 60-second lockout.

Use **Logout** in the top-right corner to revoke the session.

All three roles can view all dashboard pages including Data Pipeline and Underwriting (`/underwriting`); admin endpoints are restricted to `ADMIN`.

---

## 5. Dashboard Walkthrough

Sign in as `underwriter` to access both analytics and scoring views.

1. **Overview:** NPL **8.11%** across **4,316** labeled Gold subset applications. Risk tier chart shows **1,079** Gold validation applications (in-sample scores excluded). ROC curve reflects Track A (validation AUC **0.7361**).
2. **Segment Risk:** Compare default rates by occupation, income type, education, and `EXT_SOURCE_2` quintiles.
3. **Macro & Regions:** Scored applications are joined to regional macro indicators. Simulate unemployment (percentage points) and CPI shocks to observe `HIGH` risk tier shifts via deterministic re-scaling (no model retraining).
4. **Data Pipeline:** **799,487** Bronze rows, **5,000** Gold customers, **175** columns, **31/31** quality checks passing. Clearly labeled as a real Home Credit subset, not the full dataset.
5. **Underwriting:**
   - **Load Full Applicant** → **Assess Applicant**: expects `LOW` risk, `Auto-Approve`.
   - **Load Subprime Applicant** → **Assess Applicant**: expects `HIGH` risk, `Decline`.
   - Lookup Gold validation IDs `277603` (LOW) or `298157` (HIGH): returns stored scored prediction (note: legacy Track A ID `101676` is absent from Gold outputs and returns 404).

PD numbers may shift slightly if models or payloads are updated; always verify live UI output before copying numbers into presentation slides.

### What the Numbers on Each Page Mean

The five pages answer five underwriting questions: how risky is the loan book, where is risk concentrated, how sensitive is it to regional macro stress, is the multi-source pipeline trustworthy, and should a given applicant be approved or declined.

**Overview — Gold subset loan book.** Across 4,316 labeled training rows, **350** defaulted (**8.11%** NPL). Average loan amount is ~**598,000**, median ~**517,000**, average income ~**171,000**, with average credit-to-income ratio ~**3.9x**. The risk tier distribution uses **1,079** validation rows only (**640** HIGH, **407** MEDIUM, **32** LOW). The Gold model validation ROC-AUC is **0.7331**; in-sample predictions are excluded from the chart because training AUC approaches 1.0.

**Segment Risk — same 4,316 applications.** Against the 8.11% baseline: lowest `EXT_SOURCE_2` quintile defaults at **15.66%** vs **2.55%** in the highest quintile. Cleaning staff defaults at **16.90%** (71 applicants), laborers **11.18%**, sales staff **11.00%**. Working applicants **10.18%**, commercial associates **7.74%**, pensioners **4.43%**, state servants **3.75%**. Secondary education **9.18%** vs higher education **4.86%**. Very small categories (unemployed, businessman, academic degree) show 0% or 100% due to tiny sample sizes.

**Macro & Regions — regional stress scenarios.** Scored applications are joined with regional unemployment, GDP, and CPI. Stress testing applies a fixed multiplier: `PD * (1 + 0.06 * Δ_unemployment_pp + 0.04 * Δ_cpi_pp)`. This is a hypothetical sensitivity scenario, not realized defaults or a retrained model.

**Data Pipeline — integration quality.** 799,487 Bronze rows aggregated to 5,000 Gold customer profiles, 175 columns, 31/31 checks passed, with `TARGET` excluded from feature tables. Represents a 5,000-customer real Home Credit subset. Gold scores appear during Gold ID lookup on Underwriting, whereas manual form entry uses the Track A model.

**Underwriting — individual decisions.** Manual entry scores with Track A: full applicant yields PD ~**4.2%** (`LOW`, `Auto-Approve`); subprime applicant yields PD ~**54.7%** (`HIGH`, `Decline`). Gold subset lookup IDs: `277603` (`LOW`) and `298157` (`HIGH`).

---

## 6. Running the Spark Pipeline (Optional)

The dashboard does **not** require re-running Spark; the Data Pipeline page reads committed reports.

To re-run locally (Java 17 or 21, Python 3.11+, Docker):

```bash
cd apache-spark
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Pipeline infrastructure (MinIO and Kafka) is defined in `apache-spark/docker-compose.yml`:

- MinIO: http://localhost:9001 (`minioadmin` / `minioadmin`), S3 API on port `9000`
- Kafka: `localhost:9092`
- Kafka UI: http://localhost:8081 (remapped from `8080` to prevent conflicting with the Spring Boot backend on `8080`)

Run quickly on a 5,000-customer subset (requires real CSVs in `apache-spark/data/raw/` or test fixture):

```bash
cd apache-spark
python -m scripts.make_dev_subset --customers 5000
export CRB_PATHS_RAW_DIR=data/dev_subset/raw
python -m scripts.run_pipeline --sync-minio
```

Afterwards, commit updated files under `apache-spark/reports/` (especially `pipeline_stats.json` and `data_quality/`) and restart the backend to reload them.

Run Spark pipeline tests without touching real data:

```bash
cd apache-spark
python -m pytest
```

---

## 7. Full-Scale Execution (Google Colab) & Gold Integration

Because processing all **307,511** training applications and ~55M child rows across Spark Bronze/Silver/Gold requires high JVM driver memory (`8–10 GB`), full-scale execution is automated for Google Colab:

- **Notebook:** `notebooks/colab_full_pipeline.ipynb`
- **Unified Runner:** `python scripts/run_colab_full_pipeline.py --raw-dir _data --driver-memory 10g --verify-delta`

This executes:
1. Supplementary macro + occupation salary benchmark generation (`gen_macro.py`) & raw EDA.
2. Spark profiling + synthetic generation + Bronze -> Silver -> Gold + Structured Streaming demo + Delta Lake round-trip check.
3. Full-dataset interactive application scorer (`run_track_a.py --include-macro`) + Gold Medallion model + PR-AUC/ROC/5:1 cost matrix + TreeSHAP global plot (`scripts/train_gold_model.py`).
4. Unified artifact standardization (`scripts/standardize_dashboard.py`) and test verification (`pytest`).

---

## 8. Testing the Dashboard

From the repository root after setup:

```bash
./scripts/test.sh
```

Runs Python `pytest`, backend Maven tests, and frontend `vitest`.

While the dashboard stack is running:

```bash
./scripts/smoke.sh
```

---

## 9. Deployment & Execution Notes

- **Colab Full-Scale Execution:** Committed `outputs/` and `apache-spark/reports/` can be regenerated across all 307,511 applicants using `notebooks/colab_full_pipeline.ipynb` and re-synced locally before building Docker images.
- **Unified Model Architecture:** The interactive manual Underwriting form calls `credit_risk_model.joblib` via FastAPI (`ml-service/`) for instant scoring from application fields, while pre-computed `SK_ID_CURR` lookups and portfolio analytics use the 175-feature Gold LightGBM model (`gold_credit_risk_model.joblib`).
- **Decoupled Streaming:** Live Kafka/MinIO brokers are optional when running the dashboard; committed Structured Streaming replay metrics appear in `apache-spark/reports/pipeline_stats.json`.
- **Storage Format:** Both Parquet (`default`) and Delta Lake (`CRB_LAKE_FORMAT=delta`, `delta-spark==3.2.1`) are supported cleanly.

---

## 10. Troubleshooting

| Symptom | Resolution |
| --- | --- |
| Backend fails to start | Verify `JWT_SECRET` and all three demo user/password pairs in `.env`. Local runs require JDK 25 (`JAVA_HOME`). |
| Scoring returns `503` | Check `curl http://127.0.0.1:8000/health`. Ensure `ML_SERVICE_TOKEN` matches between backend and ML service. On macOS, install `libomp` (`brew install libomp`). |
| Data Pipeline page empty or `503` | Ensure `apache-spark/reports/pipeline_stats.json` exists. Rebuild the Docker image after updating reports. |
| CORS errors | `CORS_ALLOWED_ORIGINS` must include exact origins with `http://` and port (no `*` wildcard). |
| Overview empty | Ensure `outputs/metrics/risk_summary.json` and scored CSV exist. |
| Port `8080` conflict | Resolved: Kafka UI in `apache-spark/docker-compose.yml` now binds host port `8081` so Backend (`8080`) and Kafka UI (`8081`) can run simultaneously. |
| Docker ML service unhealthy | Run `docker compose logs ml-service`. In Compose, backend calls `http://ml-service:8000`. |
| Raw lookup test fails | Expected if `_data/` is not present locally; core scoring and API tests still pass. |
