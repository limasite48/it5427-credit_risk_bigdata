# Demo Guide — Credit Risk Underwriting Platform

## Prerequisites

- Docker Desktop **or** local toolchain:
  - JDK 25
  - Python ≥ 3.10 (3.12 recommended: `brew install python@3.12`)
  - Node.js 20+
  - On macOS Apple Silicon: `brew install libomp` (required by LightGBM)
- Existing ML artifacts under `outputs/` (already in the repo)

## One-command start (recommended)

```bash
cp -n .env.example .env
docker compose up --build -d
docker compose ps
docker compose logs -f
```

Open: [http://localhost:8088](http://localhost:8088)

Stop:

```bash
docker compose down
```

## Local development start

```bash
./scripts/setup.sh
./scripts/start.sh
./scripts/status.sh
```

URLs:

- Frontend: http://127.0.0.1:5173
- Backend: http://127.0.0.1:8080
- ML (local only): http://127.0.0.1:8000

Stop:

```bash
./scripts/stop.sh
```

## Demo accounts

Configured via `.env` (defaults from `.env.example`):

| Username | Role | Password |
| --- | --- | --- |
| `admin` | ADMIN | `AdminDemo!234` |
| `analyst` | RISK_ANALYST | `AnalystDemo!234` |
| `underwriter` | UNDERWRITER | `UnderwriterDemo!234` |

Change these before any shared environment.

## Using the dashboard

1. Sign in as `underwriter`.
2. **Overview** — inspect portfolio NPL, application count, risk-tier chart, LightGBM vs. baseline ROC/PR curves, 5:1 cost optimization stats, and the global TreeSHAP feature importance beeswarm plot.
3. **Segment Risk** — occupation / income / education / EXT_SOURCE_2 charts against portfolio NPL benchmark.
4. **Macro & Regions** — regional rating / unemployment / GDP slices from integrated supplementary macro tables; run unemployment + CPI stress on scored PDs.
5. **Data Pipeline** — inspect Bronze/Silver/Gold volumes, quality gates, feature groups, and Structured Streaming replay metrics.
6. **Underwriting**
   - Click **Load Full Applicant** → **Assess Applicant**.
   - Click **Load Subprime Applicant** → **Assess Applicant**.
   - Lookup an application ID from the pre-scored Gold profile (defaults to `high_example` from `outputs/gold_demo_ids.json`, e.g., `298157`).

### Verified sample scoring (actual model)

Measured via `CreditRiskScoringService.score_applicant` / ML adapter:

| Profile | PD | Tier | Action |
| --- | ---: | --- | --- |
| `sample_full_applicant.json` | 0.0417 | **LOW** | **Auto-Approve** |
| `sample_subprime_applicant.json` | 0.5470 | **HIGH** | **Decline** |

Do not hardcode these in the UI; always call the live scoring path.

## Tests

```bash
./scripts/test.sh
```

Runs pytest, Maven tests, and Vitest.

Live smoke (services must already be up):

```bash
./scripts/smoke.sh
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Backend won’t start | `JWT_SECRET` / demo user env vars set; Java 25 on `PATH`/`JAVA_HOME` |
| Scoring returns 503 | ML health: `curl http://127.0.0.1:8000/health`; token match; on macOS ensure `libomp` |
| CORS errors | `CORS_ALLOWED_ORIGINS` includes the exact frontend origin |
| Empty overview | Ensure `outputs/metrics/risk_summary.json` and scored CSV exist |
| Docker ML unreachable | `docker compose logs ml-service`; backend uses `http://ml-service:8000` |
| `test_lookup_existing_applicant` / ingestion raw tests fail | Expected when `_data/` is absent (gitignored). Core scoring/adapter tests still pass. |

## Optional Kafka dry-run

Not required for the dashboard:

```bash
python src/integration/kafka_consumer_scorer.py --dry-run --limit 5
```

## Presentation & Report Demo Scenarios

Use this flow when presenting the platform or gathering report metrics:

1. **Executive Portfolio Overview (`Overview`):**
   - Point to total applications (`307,511`), baseline NPL rate (`8.07%`), risk-tier breakdown (`LOW < 5%`, `MEDIUM 5-15%`, `HIGH >= 15%`), LightGBM vs. Logistic Regression validation ROC-AUC & PR-AUC, 5:1 cost-matrix optimal threshold `p*`, and global TreeSHAP feature importance.
2. **Segment Risk & Macroeconomic Stress Testing (`Segment Risk`, `Macro & Regions`):**
   - Walk through occupation, income, education, and `EXT_SOURCE_2` default rates relative to the `8.07%` portfolio benchmark.
   - Apply unemployment and CPI stress shocks across regional tiers.
3. **Big Data Lakehouse & Streaming (`Data Pipeline` + CLI Kafka Dry-Run):**
   - Review Bronze -> Silver -> Gold table counts, DQ reconciliation (`rows_in = rows_out + quarantined + exact_dups`), leakage checks (`TARGET` excluded from Gold features; post-application rows excluded), and Structured Streaming replay metrics.
   - *(Interim note: While Google Colab finishes the 58M-row Spark batch, Spark pipeline reports reflect the 5,000-customer real dev subset while ML/Overview reflect all 307,511 rows.)*
   - Run `python src/integration/kafka_consumer_scorer.py --dry-run --limit 5` to show real-time event validation + scoring latency.
4. **Live Underwriting & Individual SHAP (`Underwriting`):**
   - Load `sample_full_applicant.json` -> **Assess Applicant** (`LOW` / `Auto-Approve`).
   - Load `sample_subprime_applicant.json` -> **Assess Applicant** (`HIGH` / `Decline`).
   - Look up pre-scored ID `298157` from `outputs/gold_demo_ids.json`.

## Upgrading After Google Colab Completes

Once Colab finishes generating `credit_risk_full_artifacts.zip`, merge all 307,511-row Spark Medallion + Gold outputs with:

```bash
python scripts/apply_colab_artifacts.py --zip credit_risk_full_artifacts.zip --run-tests
docker compose up --build -d backend ml-service
./scripts/smoke.sh
```
