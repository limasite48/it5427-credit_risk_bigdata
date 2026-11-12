# Credit Risk Intelligence: Multi-Source Big Data for Credit Risk Assessment and Prediction

**Course:** IT5427 - Big Data  
**Module:** Analytics, Ingestion Bridge & Machine Learning Pipeline  
**Dataset:** Home Credit Default Risk (`_data/`)

---

## Architecture Overview

```
                      ┌────────────────────────────────────────┐
                      │ Member 1: Ingestion & MinIO Lakehouse  │
                      │ (ingestion-minio-kafka: Kafka + MinIO) │
                      └──────────────────┬─────────────────────┘
                                         ▼
                      ┌────────────────────────────────────────┐
                      │ Member 2: Apache Spark & Delta Lake    │
                      └──────────────────┬─────────────────────┘
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      OUR ROLE: Analytics & Machine Learning                     │
├────────────────────────────────────────┬────────────────────────────────────────┤
│  Track A (Immediate Deployment)        │  Track B & Ingestion Bridge            │
│  • EDA & Portfolio NPL Analysis        │  • Regional macro indicator joiner     │
│  • Demographic & Occupation Profiling  │  • Real-time Kafka scoring consumer    │
│  • Preprocessing & Domain Ratios       │  • Simulated stream scoring (<35ms)    │
│  • Baseline Logistic Regression        │  • Out-of-time (OOT) holdout testing   │
│  • LightGBM Default Classifier         │  • Multi-table feature retraining      │
│  • Cost-Sensitive Threshold Optimizer  │                                        │
│  • SHAP Explainability Engine          │                                        │
│  • Customer Risk Tier Segmentation     │                                        │
└────────────────────────────────────────┴────────────────────────────────────────┘
                                         │
                                         ▼
                      ┌────────────────────────────────────────┐
                      │ Member 3: Decision Support Dashboard   │
                      └────────────────────────────────────────┘
```

---

## Directory Structure

```
it5427-credit_risk_bigdata/
├── _data/                               # Raw Kaggle CSV files (git-ignored)
├── docs/
│   ├── track_b_requirements.md          # Upstream data contract for Members 1 & 2
│   └── dashboard_interface_spec.md      # Downstream API spec for Member 3
├── ingestion-minio-kafka/               # Ingestion Layer (Member 1)
│   ├── data/supplementary/              # Regional GDP, unemployment, CPI (simulated)
│   ├── docs/INGESTION_MINIO_KAFKA.md    # English technical documentation
│   ├── infra/                           # Docker Compose (Kafka KRaft + Bitnami MinIO)
│   ├── ingestion/                       # stage.py, gen_macro.py, backfill.py, producer.py
│   └── schemas/                         # JSON Schema Draft 2020-12 per Kafka topic
├── outputs/
│   ├── figures/                         # Diagnostic curves & SHAP summary plots
│   ├── metrics/                         # Statistical summaries & evaluation results
│   ├── models/                          # Serialized credit risk models
│   └── scored_applications_sample.csv   # Scored sample dataset for Dashboard
├── src/
│   ├── analytics/
│   │   ├── eda.py                       # Portfolio NPL ratio & risk metrics
│   │   └── risk_profiling.py            # Occupation & demographic default rates
│   ├── data/
│   │   ├── loader.py                    # Memory-optimized data loader
│   │   ├── macro_loader.py              # Point-in-time regional macro joiner
│   │   └── preprocessor.py              # Anomaly handling & banking domain ratios
│   ├── integration/
│   │   └── kafka_consumer_scorer.py     # Real-time streaming scoring engine
│   ├── models/
│   │   ├── baseline.py                  # Regularized Logistic Regression scorecard
│   │   ├── evaluation.py                # ROC-AUC, PR-AUC & cost-sensitive matrix
│   │   ├── explainability.py            # SHAP global & local explainers
│   │   └── tree_models.py               # LightGBM Classifier with class weighting
│   ├── service/
│   │   └── scoring_service.py           # Unified inference & risk tiering service
│   └── config.py                        # Centralized paths and hyperparameters
├── tests/
│   ├── test_ingestion_integration.py    # Integration tests (macro, schema, real-time scorer)
│   ├── test_preprocessor.py             # Preprocessing & ratio unit tests
│   └── test_scoring_service.py          # Scoring and risk tiering unit tests
├── requirements.txt                     # Dependencies
└── run_track_a.py                       # Master execution script
```

---

## Getting Started

### 1. Installation
```powershell
pip install -r requirements.txt
```

### 2. Run All Automated Tests
```powershell
pytest tests/ -v
```

### 3. Execute Track A Pipeline
Run on a fast sample (50,000 rows):
```powershell
python run_track_a.py --sample-size 50000 --fast
```

Run on the full 307,511 application training dataset:
```powershell
python run_track_a.py
```

### 4. Run Real-Time Streaming Scorer
Run the offline stream simulator (processes 10 applications with instant SHAP explanations):
```powershell
python src/integration/kafka_consumer_scorer.py --dry-run --limit 10
```

Run live with active Kafka broker (`localhost:9092`):
```powershell
python src/integration/kafka_consumer_scorer.py --bootstrap localhost:9092 --topic hc.los.loan-application.v1
```

### 5. Generate Supplementary Macro Data
```powershell
python ingestion-minio-kafka/ingestion/gen_macro.py
```

---

## Risk Policy & Tiers
| Tier | Default Probability ($PD$) | Recommended Action | Operational Description |
| :--- | :---: | :---: | :--- |
| **LOW** | $PD < 5\%$ | **Auto-Approve** | Instant approval, prime interest rates |
| **MEDIUM** | $5\% \le PD < 15\%$ | **Manual Review** | Underwriting review, conservative limits |
| **HIGH** | $PD \ge 15\%$ | **Decline** | Decline or strict collateral requirements |
