# Architecture — Credit Risk Underwriting Platform

## Overview

```text
React (Vite)  →  Spring Boot 4 API  →  FastAPI ML adapter  →  CreditRiskScoringService  →  joblib model
                      ↓
              Local outputs/ JSON + CSV
                      ↓
              Supplementary regional macro CSVs (join + stress)
```

No application database is required. Portfolio KPIs and application lookup read existing Track A artifacts. Macro & Regions joins scored applicants to `ingestion-minio-kafka/data/supplementary` via `REGION_POPULATION_RELATIVE`. User accounts and refresh tokens are held in memory for the demo.

## Components

### Frontend (`frontend/`)

- React + TypeScript + Vite
- Tailwind CSS + lightweight shadcn-style primitives
- TanStack Query for server state
- React Hook Form + Zod for underwriting validation
- Recharts for portfolio and segment charts
- Access JWT kept in memory; refresh token in `HttpOnly` cookie

### Backend (`backend/`)

- Java 25, Spring Boot 4.1, Maven Wrapper
- Spring Security + OAuth2 resource server (JWT access tokens)
- Services:
  - `PortfolioService` — `risk_summary.json` + scored CSV tier counts
  - `SegmentService` — `demographic_profiles.json`
  - `ApplicationLookupService` — scored CSV index by `SK_ID_CURR`
  - `ScoringService` / `MlScoringClient` — HTTP to Python with `X-Internal-Token`
  - `MacroInsightService` — regional slices + deterministic PD stress (`GET /api/insights/regions`, `POST /api/insights/macro-stress`)
  - `AuthService` — login, refresh rotation, logout

### ML service (`ml-service/`)

- FastAPI adapter around existing `CreditRiskScoringService.score_applicant`
- Loads the joblib model once at startup
- Endpoints: `POST /score`, `GET /health`
- Not published outside the Docker internal network

### Existing ML core (`src/`)

Unchanged training, preprocessing, explainability, and scoring code.

## Data flow

1. Officer signs in → backend issues access JWT + refresh cookie.
2. Overview / Segments pages call authenticated REST endpoints; backend reads local files.
3. Macro & Regions joins the scored sample to supplementary GDP / unemployment / CPI tables and applies deterministic PD stress (no retrain).
4. Underwriting form posts applicant JSON → Spring validates required fields → Python scores with SHAP → response mapped to DTOs → UI renders PD, tier, action, drivers.
5. Application lookup reads the scored CSV only (no live re-score).

## Portfolio sample sizes

| Source | Rows | Used for |
| --- | ---: | --- |
| `outputs/metrics/risk_summary.json` | 5,000 | NPL, defaults, avg loan/income |
| `outputs/scored_applications_sample.csv` | 500 | Risk tier chart + application lookup |

The API returns both `totalApplications` (5,000) and `scoredSampleSize` (500) so the UI can label them correctly.

## Networking (Docker Compose)

- `internal` network: `ml-service` + `backend` (ML not published)
- `public` network: `frontend` + `backend`
- Published ports: frontend (`8088`), backend (`8080`)

## Optional integrations

Kafka dry-run and MinIO/Spark pipelines remain available under `src/integration` and `ingestion-minio-kafka/` but are **not** required to start the dashboard.
