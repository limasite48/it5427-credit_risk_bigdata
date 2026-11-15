# Dashboard & API Interface Specification

This document specifies the serialized artifacts, HTTP endpoints, and DTO schemas shared across the ML pipeline, Spring Boot backend gateway, and React underwriting frontend.

## 1. Output Artifacts (`outputs/`)

1. **`outputs/models/credit_risk_model.joblib`** & **`outputs/models/gold_credit_risk_model.joblib`**:
   - Interactive real-time model (`credit_risk_model.joblib`) loaded by `ml-service/` for manual underwriting form scoring.
   - Full Spark Gold integrated model (`gold_credit_risk_model.joblib`) trained across 175 Gold profile features.
2. **`outputs/metrics/risk_summary.json`**:
   - Portfolio KPIs (`portfolio_summary`, `financial_exposure`, `delinquency_indicators`) synchronized to the integrated Gold dataset via `scripts/standardize_dashboard.py`.
3. **`outputs/metrics/demographic_profiles.json`**:
   - Default-rate breakdowns by `risk_by_occupation`, `risk_by_income_type`, `risk_by_education`, and `risk_by_ext_source_2_quintile`.
4. **`outputs/metrics/model_evaluation.json`** (and `gold_model_evaluation.json`):
   - Baseline Logistic Regression & LightGBM ROC-AUC, PR-AUC, Precision, Recall, F1, confusion matrix, and 5:1 cost-matrix optimization (`optimal_threshold`, `cost_reduction_percent`).
5. **`outputs/figures/roc_curve.png`** & **`outputs/figures/shap_summary.png`**:
   - Combined ROC/PR curves and TreeSHAP global feature importance beeswarm plots served via `/api/dashboard/figures/roc-curve` and `/api/dashboard/figures/shap-summary`.
6. **`outputs/scored_applications_sample.csv`** & **`outputs/gold_scored_applications.csv`**:
   - Pre-scored risk tier (`LOW` < 5%, `MEDIUM` 5–15%, `HIGH` >= 15%), action recommendation (`Auto-Approve`, `Manual Review`, `Decline`), `PREDICTED_PD`, and `SCORE_SOURCE`.
   - `outputs/gold_demo_ids.json` records verified `low_example` and `high_example` `SK_ID_CURR` keys from the validation fold.

---

## 2. Internal ML Scoring Contract (`ml-service` -> `backend`)

`POST /score` (authenticated via `X-Internal-Token`):

```json
{
  "SK_ID_CURR": 100001,
  "default_probability": 0.0417,
  "risk_tier": "LOW",
  "action": "Auto-Approve",
  "tier_description": "Low Risk (<5% PD)",
  "top_risk_drivers": [
    {
      "feature": "EXT_SOURCE_2",
      "feature_value": 0.72,
      "shap_value": -0.34,
      "effect": "Lowers Risk",
      "magnitude": 0.34
    }
  ]
}
```

---

## 3. Public Backend REST DTOs (`backend` -> `frontend`)

- **`GET /api/dashboard/overview`**: `OverviewResponse` (`totalApplications`, `nplRate`, `modelValAuc`, `baselineValAuc`, `modelPrAuc`, `modelF1`, `optimalThreshold`, `costReductionPercent`, `riskDistribution`, `scoredSampleSize`).
- **`GET /api/dashboard/segments`**: `SegmentsResponse` (`byOccupation`, `byIncomeType`, `byEducation`, `byExtSource2`).
- **`GET /api/dashboard/figures/roc-curve`** & **`GET /api/dashboard/figures/shap-summary`**: Authenticated `image/png` streams.
- **`GET /api/applications/{id}`**: `ApplicationResponse` (looks up Gold pre-scored records first, then validation CSV fallback).
- **`POST /api/scoring/score`**: `ScoreResponse` with camelCase `topRiskDrivers` (`feature`, `featureValue`, `shapValue`, `effect`, `raisesRisk`).
- **`GET /api/insights/regions`** & **`POST /api/insights/macro-stress`**: Regional macro slices + deterministic unemployment/CPI shock simulation.
- **`GET /api/insights/pipeline`**: `PipelineResponse` reading `apache-spark/reports/pipeline_stats.json`, `gold_checks.json`, and `feature_columns.json`.
