# Downstream Interface Specification: Member 3 Dashboard Integration

This document defines the artifacts, data structures, and APIs that our Analytics & ML module provides to **Member 3 (Dashboard, Integration & Deployment Validate)**.

---

## 1. Provided Artifacts

### 1.1 Trained Model Artifact
* **Location:** `outputs/models/credit_risk_model.joblib`
* **Contents:** Serialized scikit-learn / LightGBM pipeline including imputer, scaler, feature encoders, and calibrated classifier.

### 1.2 Portfolio Metrics & Summary Data
* **Location:** `outputs/metrics/risk_summary.json`
  * Contains overall portfolio statistics: total loans, default count, Non-Performing Loan ($NPL$) ratio, average loan credit, average borrower income, and credit-to-income distribution.
* **Location:** `outputs/metrics/demographic_profiles.json`
  * Contains breakdown of default rates across occupation types, income types, education levels, and credit score bins for interactive filtering in the dashboard.

### 1.3 Pre-computed Inference Dataset
* **Location:** `outputs/scored_applications_sample.csv` (or `.parquet`)
  * Columns:
    * `SK_ID_CURR`: Applicant ID.
    * `AMT_INCOME_TOTAL`, `AMT_CREDIT`, `OCCUPATION_TYPE`, `NAME_INCOME_TYPE`: Demographics.
    * `PREDICTED_PD`: Continuous default probability between 0.0 and 1.0.
    * `RISK_TIER`: `LOW`, `MEDIUM`, or `HIGH`.
    * `ACTION_RECOMMENDATION`: `Auto-Approve`, `Manual Review`, or `Decline`.
    * `TOP_RISK_FACTOR_1`, `TOP_RISK_FACTOR_2`: Key features driving risk upwards.

### 1.4 Visual Assets
* `outputs/figures/shap_summary.png`: Global feature importance beeswarm chart.
* `outputs/figures/roc_curve.png`: Model discrimination curve (ROC and Precision-Recall).

---

## 2. Interactive Python Scoring Function

Member 3 can import and execute the inference engine directly inside their Streamlit / Dash application:

```python
from src.service.scoring_service import CreditRiskScoringService

# Initialize scoring service (loads model and explainer)
service = CreditRiskScoringService()

# Evaluate an incoming applicant profile
applicant = {
    "SK_ID_CURR": 100002,
    "AMT_INCOME_TOTAL": 202500.0,
    "AMT_CREDIT": 406597.5,
    "AMT_ANNUITY": 24700.5,
    "NAME_INCOME_TYPE": "Working",
    "NAME_EDUCATION_TYPE": "Secondary / secondary special",
    "OCCUPATION_TYPE": "Laborers",
    "DAYS_BIRTH": -9461,
    "DAYS_EMPLOYED": -637,
    "EXT_SOURCE_1": 0.083,
    "EXT_SOURCE_2": 0.263,
    "EXT_SOURCE_3": 0.139,
    # Additional raw application fields...
}

decision = service.score(applicant)
print(decision)
```

### Output Schema:
```json
{
  "SK_ID_CURR": 100002,
  "default_probability": 0.214,
  "risk_tier": "HIGH",
  "action": "Decline",
  "tier_description": "High Risk (Requires Collateral / Adverse Action)",
  "top_risk_drivers": [
    {
      "feature": "EXT_SOURCE_2",
      "value": 0.263,
      "shap_impact": 0.45,
      "effect": "Increases Default Risk"
    },
    {
      "feature": "CREDIT_INCOME_RATIO",
      "value": 2.01,
      "shap_impact": 0.18,
      "effect": "Increases Default Risk"
    }
  ]
}
```

---

## 3. Recommended Dashboard Views for Member 3
1. **Executive Portfolio Overview:** Gauge charts for NPL ratio (8.07%), Total Exposure, and Portfolio Risk Distribution (pie/bar chart of Low vs Medium vs High risk).
2. **Segment Risk Analysis:** Heatmaps/bar charts showing default rate across occupation types and income brackets.
3. **Interactive Underwriting Simulator:** Input form where a loan officer enters applicant details and receives instant PD score, assigned risk tier, and SHAP waterfall chart explaining the credit decision.
