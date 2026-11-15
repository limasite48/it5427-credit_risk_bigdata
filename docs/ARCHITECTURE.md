# Architecture — Credit Risk Intelligence Platform

## Overview

```text
Raw CSVs (_data/) -> Kafka / MinIO Ingestion -> Apache Spark Medallion (Bronze -> Silver -> Gold)
                                                         ↓
                 ML Training & Evaluation (Gold Profile + Application Model + Cost Opt + SHAP)
                                                         ↓
React (Vite :5173/:8088) -> Spring Boot 4 API (:8080) -> FastAPI ML Adapter (:8000 internal)
                                 ↓
                 Local outputs/ + apache-spark/reports/ + supplementary CSVs
```

The operational dashboard runs standalone without requiring a live RDBMS: portfolio KPIs, Spark quality metrics, regional macro stress analytics, and application lookups read directly from the generated `outputs/`, `apache-spark/reports/`, and `ingestion-minio-kafka/data/supplementary/` artifacts, while live form scoring delegates from Spring Boot to the FastAPI scoring service.

## Components

### 1. Data Ingestion & Supplementary Enrichment (`ingestion-minio-kafka/`)
- JSON Schema Draft 2020-12 event contracts across 7 source topics + `hc.dlq.v1`.
- MinIO S3-compatible buckets (`landing`, `bronze`, `reference`, `quarantine`).
- Supplementary generators (`gen_macro.py`) producing 82-region GDP, unemployment, CPI indicators (`2016–2025`) and per-occupation salary benchmarks (`occupation_benchmarks.csv`).

### 2. Spark Medallion Lakehouse (`apache-spark/`)
- PySpark 3.5.3 ETL across Bronze (raw + synthetic traffic), Silver (typed, deduplicated, sentinel normalization, quarantine), and Gold (customer & prior-loan aggregations with zero post-application leakage).
- Supports both Parquet (`default`) and Delta Lake (`CRB_LAKE_FORMAT=delta`), plus Spark Structured Streaming replay with duplicate & late-watermark filtering.
- Execution on the full 307,511-row dataset is supported via Google Colab (`notebooks/colab_full_pipeline.ipynb` / `scripts/run_colab_full_pipeline.py`) or high-RAM Spark hosts.

### 3. ML Core & Scoring (`src/`, `scripts/`, `ml-service/`)
- Dual-model pipeline: L2 Logistic Regression benchmark + regularized LightGBM with `scale_pos_weight`, 5:1 FN:FP cost-matrix threshold optimization (`src/models/evaluation.py`), and TreeSHAP explainability (`src/models/explainability.py`).
- `scripts/train_gold_model.py` trains the full Gold LightGBM + evaluation curves + SHAP summary; `run_track_a.py` trains the lightweight interactive model (`outputs/models/credit_risk_model.joblib`) wrapped by `ml-service/app.py`.

### 4. Backend & Frontend (`backend/`, `frontend/`)
- **Backend:** Java 25, Spring Boot 4.1, Spring Security JWT + refresh cookie rotation, serving `/api/dashboard/{overview,segments,figures/*}`, `/api/applications/{id}`, `/api/scoring/score`, `/api/insights/{regions,macro-stress,pipeline}`, and `/api/health`.
- **Frontend:** React 19 + TypeScript + Vite + Tailwind CSS + Recharts + TanStack Query + React Hook Form/Zod across Overview, Segment Risk, Macro & Regions, Data Pipeline, and Underwriting views.

## Networking & Ports (Docker Compose)
- Root `docker-compose.yml`: Frontend (`8088`), Spring Boot Backend (`8080`), FastAPI ML Service (`8000`, internal network only).
- Spark infra `apache-spark/docker-compose.yml`: MinIO (`9000` API / `9001` Console), Kafka (`9092`), Kafka UI (`8081`, moved from `8080` to avoid colliding with the backend).
