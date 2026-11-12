"""
Master Execution Driver for Track A: Credit Risk Analytics & ML Pipeline
"""
import argparse
import json
import time
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from src.config import (
    TRAIN_DATA_PATH,
    MODEL_SAVE_PATH,
    METRICS_SUMMARY_PATH,
    DEMOGRAPHICS_PATH,
    EVALUATION_PATH,
    SHAP_SUMMARY_PLOT_PATH,
    ROC_CURVE_PLOT_PATH,
    SCORED_SAMPLE_PATH,
    RANDOM_STATE,
)
from src.data.loader import load_application_data
from src.data.preprocessor import CreditRiskPreprocessor
from src.analytics.eda import compute_portfolio_risk_metrics
from src.analytics.risk_profiling import compute_demographic_risk_profiles
from src.models.baseline import train_logistic_regression_baseline
from src.models.tree_models import train_lightgbm_model
from src.models.evaluation import evaluate_predictions
from src.models.explainability import CreditRiskExplainer
from src.service.scoring_service import CreditRiskScoringService


def run_pipeline(sample_size: int = None, fast_mode: bool = False):
    start_time = time.time()
    print("=" * 70)
    print(" IT5427 Big Data: Track A Credit Risk Analytics & ML Pipeline")
    print("=" * 70)

    # 1. Ingestion
    print(f"\n[1/7] Loading application data from {TRAIN_DATA_PATH.name}...")
    df = load_application_data(TRAIN_DATA_PATH, nrows=sample_size)
    print(f"      Loaded {len(df):,} records with {df.shape[1]} columns.")

    # 2. Statistical EDA & NPL Metrics
    print("\n[2/7] Computing portfolio-level risk metrics & baseline NPL...")
    summary_metrics = compute_portfolio_risk_metrics(df, save_path=METRICS_SUMMARY_PATH)
    npl_pct = summary_metrics["portfolio_summary"]["npl_default_percent"]
    avg_credit = summary_metrics["financial_exposure"]["average_loan_amount"]
    print(f"      Baseline Portfolio NPL / Default Rate: {npl_pct}")
    print(f"      Average Loan Credit: {avg_credit:,.2f}")
    print(f"      Saved metrics to: {METRICS_SUMMARY_PATH.name}")

    # 3. Demographic & Segment Profiling
    print("\n[3/7] Generating demographic and occupation risk profiles...")
    demographics = compute_demographic_risk_profiles(df, save_path=DEMOGRAPHICS_PATH)
    top_occ = list(demographics["risk_by_occupation"].keys())[:3]
    print(f"      Analyzed risk by occupation (e.g., {', '.join(top_occ)}) and income type.")
    print(f"      Saved profiles to: {DEMOGRAPHICS_PATH.name}")

    # 4. Feature Preprocessing & Banking Domain Ratios
    print("\n[4/7] Engineering domain features and preprocessing data...")
    preprocessor = CreditRiskPreprocessor()
    
    y = df["TARGET"]
    X = df.drop(columns=["TARGET"])

    X_train_raw, X_val_raw, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
    )

    X_train = preprocessor.fit_transform(X_train_raw)
    X_val = preprocessor.transform(X_val_raw)
    print(f"      Engineered features: {len(preprocessor.feature_names_)} columns.")
    print(f"      Train shape: {X_train.shape}, Validation shape: {X_val.shape}")

    # 5. Model Training (Baseline vs LightGBM)
    print("\n[5/7] Training models...")
    print("      -> Training Baseline Scorecard (Regularized Logistic Regression)...")
    lr_model, lr_metrics = train_logistic_regression_baseline(
        X_train, y_train, X_val, y_val
    )
    print(f"         Baseline LR Validation ROC-AUC: {lr_metrics['val_roc_auc']:.4f}")

    print("      -> Training LightGBM Classifier...")
    n_estimators = 150 if fast_mode else 500
    lgbm_model, lgbm_metrics = train_lightgbm_model(
        X_train, y_train, X_val, y_val, custom_params={"n_estimators": n_estimators}
    )
    print(f"         LightGBM Validation ROC-AUC: {lgbm_metrics['val_roc_auc']:.4f}")
    print(f"         AUC Improvement over Baseline: +{(lgbm_metrics['val_roc_auc'] - lr_metrics['val_roc_auc']):.4f}")

    # Comprehensive Evaluation & Cost Optimization
    val_probs = lgbm_model.predict_proba(X_val)[:, 1]
    eval_results = evaluate_predictions(y_val.values, val_probs, plot_path=ROC_CURVE_PLOT_PATH)
    
    combined_evaluation = {
        "baseline_logistic_regression": lr_metrics,
        "lightgbm_classifier": lgbm_metrics,
        "detailed_lightgbm_evaluation": eval_results,
    }
    with open(EVALUATION_PATH, "w", encoding="utf-8") as f:
        json.dump(combined_evaluation, f, indent=2)

    cost_opt = eval_results["cost_optimization"]
    print(f"      Optimal Decision Threshold: p* = {cost_opt['optimal_threshold']:.2f}")
    print(f"      Cost Reduction vs standard 0.5 cutoff: {cost_opt['cost_reduction_percent']}")
    print(f"      Saved diagnostic ROC/PR curve to: {ROC_CURVE_PLOT_PATH.name}")

    # 6. Explainable AI (SHAP)
    print("\n[6/7] Computing Explainable AI (SHAP) feature importance...")
    explainer = CreditRiskExplainer(lgbm_model, preprocessor.feature_names_)
    shap_sample = X_val.iloc[:300]
    explainer.explain_global(shap_sample, save_path=SHAP_SUMMARY_PLOT_PATH)
    print(f"      Saved global SHAP beeswarm plot to: {SHAP_SUMMARY_PLOT_PATH.name}")

    # 7. Model Serialization & Handoff to Member 3
    print("\n[7/7] Serializing model artifact & generating dashboard sample dataset...")
    artifact = {
        "model": lgbm_model,
        "preprocessor": preprocessor,
        "baseline_metrics": lr_metrics,
        "best_metrics": lgbm_metrics,
        "evaluation": eval_results,
    }
    joblib.dump(artifact, MODEL_SAVE_PATH)
    print(f"      Saved model artifact to: {MODEL_SAVE_PATH.name}")

    # Initialize scoring service and score sample batch
    service = CreditRiskScoringService(
        model=lgbm_model, preprocessor=preprocessor, explainer=explainer
    )
    sample_to_score = X_val_raw.iloc[:500].copy()
    scored_sample = service.score_batch(sample_to_score, save_path=SCORED_SAMPLE_PATH)
    
    # Example individual applicant inference
    sample_applicant = X_val_raw.iloc[0].to_dict()
    individual_decision = service.score_applicant(sample_applicant)
    print("\n" + "-" * 70)
    print(" Sample Applicant Decision (Inference Service Output for Member 3):")
    print(json.dumps(individual_decision, indent=2))
    print("-" * 70)

    elapsed = time.time() - start_time
    print(f"\nPipeline successfully executed in {elapsed:.1f} seconds.")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Track A Credit Risk Pipeline")
    parser.add_argument("--sample-size", type=int, default=None, help="Limit number of rows for testing")
    parser.add_argument("--fast", action="store_true", help="Run with reduced trees for faster execution")
    args = parser.parse_args()

    run_pipeline(sample_size=args.sample_size, fast_mode=args.fast)
