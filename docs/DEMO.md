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
2. **Overview** — confirm NPL **7.72%**, 5,000 applications, and the scored-sample tier chart (n=500).
3. **Segment Risk** — occupation / income / education / EXT_SOURCE_2 charts.
4. **Macro & Regions** — regional rating / unemployment / GDP slices from integrated supplementary macro tables; run unemployment + CPI stress on scored PDs.
5. **Underwriting**
   - Click **Load Full Applicant** → **Assess Applicant**.
   - Click **Load Subprime Applicant** → **Assess Applicant**.
   - Lookup application `101676` from the scored CSV.

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
