#!/usr/bin/env python3
"""Train LightGBM on the Spark gold profile and write dashboard lookup scores.

Reads apache-spark/data/gold/integrated_credit_profile joined to training_labels.
The existing Track A artifact stays in place. This script writes:

  outputs/models/gold_credit_risk_model.joblib
  outputs/metrics/gold_model_evaluation.json
  outputs/gold_scored_applications.csv
  outputs/gold_demo_ids.json
"""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, early_stopping, log_evaluation
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "apache-spark" / "data" / "gold"
PROFILE = GOLD / "integrated_credit_profile"
LABELS = GOLD / "training_labels"
OUT_MODEL = ROOT / "outputs" / "models" / "gold_credit_risk_model.joblib"
OUT_METRICS = ROOT / "outputs" / "metrics" / "gold_model_evaluation.json"
OUT_SCORED = ROOT / "outputs" / "gold_scored_applications.csv"
OUT_IDS = ROOT / "outputs" / "gold_demo_ids.json"

DROP = {
    "sk_id_curr",
    "dataset_split",
    "target",
    "synthetic_data_updated_at",
}


def tier_of(pd_val: float) -> tuple[str, str]:
    if pd_val < 0.05:
        return "LOW", "Auto-Approve"
    if pd_val < 0.15:
        return "MEDIUM", "Manual Review"
    return "HIGH", "Decline"


def load_frame() -> pd.DataFrame:
    profile = pd.read_parquet(PROFILE)
    labels = pd.read_parquet(LABELS)
    profile["sk_id_curr"] = pd.to_numeric(profile["sk_id_curr"], errors="coerce").astype("int64")
    labels["sk_id_curr"] = pd.to_numeric(labels["sk_id_curr"], errors="coerce").astype("int64")
    label_col = "target" if "target" in labels.columns else "TARGET"
    labels = labels.rename(columns={label_col: "target"})
    frame = profile.merge(labels[["sk_id_curr", "target"]], on="sk_id_curr", how="inner")
    frame = frame.dropna(subset=["sk_id_curr", "target"]).copy()
    frame["target"] = frame["target"].astype(int)
    return frame


def encode(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    work = frame.drop(columns=[c for c in DROP if c in frame.columns], errors="ignore")
    features: list[str] = []
    encoded = pd.DataFrame(index=frame.index)
    for column in work.columns:
        series = work[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            continue
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().mean() >= 0.8:
            fill = float(numeric.median()) if numeric.notna().any() else 0.0
            encoded[column] = numeric.fillna(fill)
            features.append(column)
            continue
        text = series.astype("string").fillna("MISSING")
        if text.nunique(dropna=True) > 40:
            continue
        codes = text.astype("category").cat.codes.astype(float)
        encoded[column] = codes
        features.append(column)
    return encoded[features], features


def main() -> None:
    if not PROFILE.exists() or not LABELS.exists():
        raise SystemExit(f"Gold tables not found under {GOLD}")
    frame = load_frame()
    matrix, features = encode(frame)
    y = frame["target"]
    ids = frame["sk_id_curr"]
    idx_train, idx_val = train_test_split(
        frame.index,
        test_size=0.25,
        random_state=42,
        stratify=y,
    )
    pos = int(y.loc[idx_train].sum())
    neg = len(idx_train) - pos
    model = LGBMClassifier(
        objective="binary",
        metric="auc",
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        scale_pos_weight=neg / max(pos, 1),
        verbose=-1,
    )
    model.fit(
        matrix.loc[idx_train],
        y.loc[idx_train],
        eval_set=[(matrix.loc[idx_val], y.loc[idx_val])],
        callbacks=[early_stopping(stopping_rounds=30, verbose=False), log_evaluation(period=0)],
    )
    val_pd = model.predict_proba(matrix.loc[idx_val])[:, 1]
    train_pd = model.predict_proba(matrix.loc[idx_train])[:, 1]
    val_auc = float(roc_auc_score(y.loc[idx_val], val_pd))
    train_auc = float(roc_auc_score(y.loc[idx_train], train_pd))
    importances = (
        pd.Series(model.feature_importances_, index=features).sort_values(ascending=False).head(10)
    )

    scored = pd.DataFrame({"sk_id_curr": ids, "predicted_pd": np.nan, "score_basis": ""})
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
    OUT_IDS.write_text(json.dumps(demo, indent=2))
    OUT_METRICS.parent.mkdir(parents=True, exist_ok=True)
    OUT_METRICS.write_text(
        json.dumps(
            {
                "model_type": "LightGBM_GoldProfile",
                "rows_labeled": int(len(frame)),
                "feature_count": len(features),
                "train_roc_auc": round(train_auc, 4),
                "val_roc_auc": round(val_auc, 4),
                "best_iteration": int(getattr(model, "best_iteration_", 0) or 0),
                "top_10_features": {k: int(v) for k, v in importances.items()},
                "demo": demo,
            },
            indent=2,
        )
    )
    joblib.dump({"model": model, "features": features}, OUT_MODEL)
    print(json.dumps(demo, indent=2))


if __name__ == "__main__":
    main()
