#!/usr/bin/env python3
"""Train Logistic Regression baseline + LightGBM on the Spark Gold integrated credit profile.

Reads ``apache-spark/data/gold/integrated_credit_profile`` joined to ``training_labels``,
performs full evaluation (ROC-AUC, PR-AUC, Precision, Recall, F1, 5:1 cost-matrix optimization),
generates Gold diagnostic curves + TreeSHAP global summary plots, and writes lookup scores.

Outputs:
  outputs/models/gold_credit_risk_model.joblib
  outputs/metrics/gold_model_evaluation.json
  outputs/figures/gold_roc_curve.png
  outputs/figures/gold_shap_summary.png
  outputs/gold_scored_applications.csv
  outputs/gold_demo_ids.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Tuple

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, early_stopping, log_evaluation
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.models.baseline import train_logistic_regression_baseline  # noqa: E402
from src.models.evaluation import evaluate_predictions  # noqa: E402
from src.models.explainability import CreditRiskExplainer  # noqa: E402

DEFAULT_GOLD = ROOT / "apache-spark" / "data" / "gold"
OUT_MODEL = ROOT / "outputs" / "models" / "gold_credit_risk_model.joblib"
OUT_METRICS = ROOT / "outputs" / "metrics" / "gold_model_evaluation.json"
OUT_ROC = ROOT / "outputs" / "figures" / "gold_roc_curve.png"
OUT_SHAP = ROOT / "outputs" / "figures" / "gold_shap_summary.png"
OUT_SCORED = ROOT / "outputs" / "gold_scored_applications.csv"
OUT_IDS = ROOT / "outputs" / "gold_demo_ids.json"

DROP = {
    "sk_id_curr",
    "dataset_split",
    "target",
    "synthetic_data_updated_at",
}


def tier_of(pd_val: float) -> Tuple[str, str]:
    if pd_val < 0.05:
        return "LOW", "Auto-Approve"
    if pd_val < 0.15:
        return "MEDIUM", "Manual Review"
    return "HIGH", "Decline"


def load_frame(gold_dir: Path, sample_size: int | None = None) -> pd.DataFrame:
    profile = pd.read_parquet(gold_dir / "integrated_credit_profile")
    labels = pd.read_parquet(gold_dir / "training_labels")
    profile["sk_id_curr"] = pd.to_numeric(profile["sk_id_curr"], errors="coerce").astype("int64")
    labels["sk_id_curr"] = pd.to_numeric(labels["sk_id_curr"], errors="coerce").astype("int64")
    label_col = "target" if "target" in labels.columns else "TARGET"
    labels = labels.rename(columns={label_col: "target"})
    frame = profile.merge(labels[["sk_id_curr", "target"]], on="sk_id_curr", how="inner")
    frame = frame.dropna(subset=["sk_id_curr", "target"]).copy()
    frame["target"] = frame["target"].astype(int)
    if sample_size is not None and sample_size < len(frame):
        frame = frame.sample(n=sample_size, random_state=42).sort_index()
    return frame


def fit_and_encode(
    train_frame: pd.DataFrame,
    val_frame: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, list[str], Dict[str, float], Dict[str, Dict[str, int]]]:
    """Fit medians and categorical vocabularies strictly on the training fold (no leakage)."""
    work_cols = [c for c in train_frame.columns if c not in DROP]
    features: list[str] = []
    medians: Dict[str, float] = {}
    categories: Dict[str, Dict[str, int]] = {}
    tr_cols: Dict[str, pd.Series] = {}
    va_cols: Dict[str, pd.Series] = {}

    for column in work_cols:
        series_tr = train_frame[column]
        series_va = val_frame[column]
        if pd.api.types.is_datetime64_any_dtype(series_tr):
            continue
        num_tr = pd.to_numeric(series_tr, errors="coerce")
        if num_tr.notna().mean() >= 0.8:
            fill = float(num_tr.median()) if num_tr.notna().any() else 0.0
            medians[column] = fill
            tr_cols[column] = num_tr.fillna(fill)
            va_cols[column] = pd.to_numeric(series_va, errors="coerce").fillna(fill)
            features.append(column)
            continue

        txt_tr = series_tr.astype("string").fillna("MISSING")
        uniq = sorted(txt_tr.unique().tolist())
        if len(uniq) > 40:
            continue
        mapping = {val: idx for idx, val in enumerate(uniq)}
        categories[column] = mapping
        tr_cols[column] = txt_tr.map(mapping).fillna(-1).astype(float)
        txt_va = series_va.astype("string").fillna("MISSING")
        va_cols[column] = txt_va.map(mapping).fillna(-1).astype(float)
        features.append(column)

    return (
        pd.DataFrame(tr_cols, index=train_frame.index)[features],
        pd.DataFrame(va_cols, index=val_frame.index)[features],
        features,
        medians,
        categories,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Gold integrated profile LightGBM + SHAP + evaluation")
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD)
    parser.add_argument("--sample-size", type=int, default=None, help="Optional row cap for quick verification")
    parser.add_argument("--skip-shap", action="store_true", help="Skip TreeSHAP global beeswarm plot generation")
    args = parser.parse_args()

    profile_dir = args.gold_dir / "integrated_credit_profile"
    labels_dir = args.gold_dir / "training_labels"
    if not profile_dir.exists() or not labels_dir.exists():
        raise SystemExit(f"Gold tables not found under {args.gold_dir}")

    frame = load_frame(args.gold_dir, sample_size=args.sample_size)
    y = frame["target"]
    ids = frame["sk_id_curr"]
    idx_train, idx_val = train_test_split(
        frame.index,
        test_size=0.25,
        random_state=42,
        stratify=y,
    )

    X_train, X_val, features, medians, categories = fit_and_encode(frame.loc[idx_train], frame.loc[idx_val])
    y_train, y_val = y.loc[idx_train], y.loc[idx_val]

    # 1. Baseline Logistic Regression
    _, lr_metrics = train_logistic_regression_baseline(X_train, y_train, X_val, y_val)

    # 2. Calibrated Regularized LightGBM Classifier (unweighted logloss keeps PDs aligned with risk tiers)
    model = LGBMClassifier(
        objective="binary",
        metric="auc",
        n_estimators=600,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=80,
        reg_alpha=1.0,
        reg_lambda=5.0,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.7,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[early_stopping(stopping_rounds=50, verbose=False), log_evaluation(period=0)],
    )

    val_pd = model.predict_proba(X_val)[:, 1]
    train_pd = model.predict_proba(X_train)[:, 1]
    val_auc = float(roc_auc_score(y_val, val_pd))
    train_auc = float(roc_auc_score(y_train, train_pd))

    eval_results = evaluate_predictions(y_val.to_numpy(), val_pd, plot_path=OUT_ROC)
    importances = (
        pd.Series(model.feature_importances_, index=features).sort_values(ascending=False).head(15)
    )

    if not args.skip_shap:
        explainer = CreditRiskExplainer(model, features)
        shap_n = min(2000, len(X_val))
        shap_sample = X_val.sample(n=shap_n, random_state=42)
        explainer.explain_global(shap_sample, save_path=OUT_SHAP, max_display=15)

    scored = pd.DataFrame({"sk_id_curr": ids, "predicted_pd": np.nan, "score_basis": ""}, index=frame.index)
    scored.loc[idx_val, "predicted_pd"] = np.round(val_pd, 4)
    scored.loc[idx_val, "score_basis"] = "validation"
    scored.loc[idx_train, "predicted_pd"] = np.round(train_pd, 4)
    scored.loc[idx_train, "score_basis"] = "in-sample"
    tiers = scored["predicted_pd"].map(lambda p: tier_of(float(p))[0])
    actions = scored["predicted_pd"].map(lambda p: tier_of(float(p))[1])

    out = pd.DataFrame(
        {
            "SK_ID_CURR": scored["sk_id_curr"].astype(int),
            "PREDICTED_PD": scored["predicted_pd"],
            "RISK_TIER": tiers,
            "ACTION_RECOMMENDATION": actions,
            "AMT_INCOME_TOTAL": pd.to_numeric(frame["amt_income_total"], errors="coerce"),
            "AMT_CREDIT": pd.to_numeric(frame["amt_credit"], errors="coerce"),
            "OCCUPATION_TYPE": frame.get("occupation_type", pd.Series("", index=frame.index)).astype("string"),
            "NAME_EDUCATION_TYPE": frame.get("name_education_type", pd.Series("", index=frame.index)).astype("string"),
            "SCORE_SOURCE": scored["score_basis"].map(lambda basis: f"gold-{basis}"),
        }
    )
    OUT_SCORED.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_SCORED, index=False)

    validation = out[out["SCORE_SOURCE"] == "gold-validation"].copy()
    low = validation[validation["RISK_TIER"] == "LOW"].sort_values("PREDICTED_PD").head(1)
    high = validation[validation["RISK_TIER"] == "HIGH"].sort_values("PREDICTED_PD", ascending=False).head(1)
    demo = {
        "rows_labeled": int(len(frame)),
        "validation_rows": int(len(validation)),
        "val_roc_auc": round(val_auc, 4),
        "train_roc_auc": round(train_auc, 4),
        "low_example": None if low.empty else int(low["SK_ID_CURR"].iloc[0]),
        "high_example": None if high.empty else int(high["SK_ID_CURR"].iloc[0]),
        "low_pd": None if low.empty else float(low["PREDICTED_PD"].iloc[0]),
        "high_pd": None if high.empty else float(high["PREDICTED_PD"].iloc[0]),
    }
    OUT_IDS.write_text(json.dumps(demo, indent=2), encoding="utf-8")

    eval_payload = {
        "model_type": "LightGBM_GoldProfile",
        "rows_labeled": int(len(frame)),
        "feature_count": len(features),
        "baseline_logistic_regression": lr_metrics,
        "lightgbm_classifier": {
            "model_type": "LightGBM_GoldProfile",
            "train_roc_auc": round(train_auc, 4),
            "val_roc_auc": round(val_auc, 4),
            "best_iteration": int(getattr(model, "best_iteration_", 0) or 0),
        },
        "detailed_lightgbm_evaluation": eval_results,
        "train_roc_auc": round(train_auc, 4),
        "val_roc_auc": round(val_auc, 4),
        "best_iteration": int(getattr(model, "best_iteration_", 0) or 0),
        "top_10_features": {k: int(v) for k, v in importances.head(10).items()},
        "demo": demo,
    }
    OUT_METRICS.parent.mkdir(parents=True, exist_ok=True)
    OUT_METRICS.write_text(json.dumps(eval_payload, indent=2), encoding="utf-8")

    OUT_MODEL.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"model": model, "features": features, "medians": medians, "categories": categories},
        OUT_MODEL,
    )
    print(json.dumps(demo, indent=2))


if __name__ == "__main__":
    main()
