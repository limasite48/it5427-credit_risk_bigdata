"""
Centralized Configuration Module for Credit Risk Intelligence
"""
import os
import tempfile
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "_data"
OUTPUTS_DIR = BASE_DIR / "outputs"
MODELS_DIR = OUTPUTS_DIR / "models"
METRICS_DIR = OUTPUTS_DIR / "metrics"
FIGURES_DIR = OUTPUTS_DIR / "figures"

# Configure writable cache directories for Matplotlib
os.environ["MPLCONFIGDIR"] = str(OUTPUTS_DIR / ".matplotlib")

# Ensure output directories exist
for directory in [OUTPUTS_DIR, MODELS_DIR, METRICS_DIR, FIGURES_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# File Paths
TRAIN_DATA_PATH = DATA_DIR / "application_train.csv"
TEST_DATA_PATH = DATA_DIR / "application_test.csv"
MODEL_SAVE_PATH = MODELS_DIR / "credit_risk_model.joblib"
METRICS_SUMMARY_PATH = METRICS_DIR / "risk_summary.json"
DEMOGRAPHICS_PATH = METRICS_DIR / "demographic_profiles.json"
EVALUATION_PATH = METRICS_DIR / "model_evaluation.json"
SHAP_SUMMARY_PLOT_PATH = FIGURES_DIR / "shap_summary.png"
ROC_CURVE_PLOT_PATH = FIGURES_DIR / "roc_curve.png"
SCORED_SAMPLE_PATH = OUTPUTS_DIR / "scored_applications_sample.csv"

# Risk Tier Definition (Approved policy)
RISK_TIERS = {
    "LOW": {
        "max_pd": 0.05,
        "action": "Auto-Approve",
        "description": "Low Risk (Instant Approval / Prime Terms)",
    },
    "MEDIUM": {
        "max_pd": 0.15,
        "action": "Manual Review",
        "description": "Medium Risk (Manual Underwriting & Strict Limits)",
    },
    "HIGH": {
        "max_pd": 1.00,
        "action": "Decline",
        "description": "High Risk (Decline / Requires Collateral)",
    },
}

# Cost-Sensitive Decision Matrix (FN penalty : FP penalty)
COST_FALSE_NEGATIVE = 5.0  # Approving a defaulter
COST_FALSE_POSITIVE = 1.0  # Rejecting a good borrower

# Data Cleaning constants
DAYS_EMPLOYED_ANOMALY = 365243

# Random seed for reproducibility
RANDOM_STATE = 42

# LightGBM Default Hyperparameters
LGBM_PARAMS = {
    "objective": "binary",
    "metric": "auc",
    "boosting_type": "gbdt",
    "n_estimators": 500,
    "learning_rate": 0.05,
    "num_leaves": 31,
    "max_depth": -1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": RANDOM_STATE,
    "n_jobs": -1,
    "verbose": -1,
}
