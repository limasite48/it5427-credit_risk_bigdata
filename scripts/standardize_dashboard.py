#!/usr/bin/env python3
"""Standardize dashboard portfolio metrics, model evaluation, and figures from Spark Gold outputs.

Preserves single-source application model artifacts under *_track_a* backups when first run.
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD = ROOT / "apache-spark" / "data" / "gold"
OUT = ROOT / "outputs"
METRICS = OUT / "metrics"
FIGURES = OUT / "figures"


def rate_block(frame: pd.DataFrame, column: str) -> dict:
    blocks = {}
    for label, part in frame.groupby(column, dropna=False):
        name = "Missing/Unspecified" if pd.isna(label) or str(label).strip() in {"", "<NA>", "nan"} else str(label)
        total = int(len(part))
        defaults = int(part["target"].sum())
        ratio = defaults / total if total else 0.0
        blocks[name] = {
            "total_count": total,
            "default_count": defaults,
            "default_rate": round(ratio, 4),
            "default_percent": f"{ratio * 100:.2f}%",
        }
    return blocks


def quintiles(frame: pd.DataFrame) -> dict:
    work = frame.dropna(subset=["ext_source_2"]).copy()
    work["bin"] = pd.qcut(work["ext_source_2"], 5, labels=False, duplicates="drop")
    names = {
        0: "Q1 (Lowest)",
        1: "Q2",
        2: "Q3",
        3: "Q4",
        4: "Q5 (Highest)",
    }
    work["bin_name"] = work["bin"].map(names)
    return rate_block(work, "bin_name")


def main() -> None:
    parser = argparse.ArgumentParser(description="Standardize dashboard artifacts from Gold layer outputs.")
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD, help="Directory containing Spark Gold parquet tables")
    args = parser.parse_args()
    gold_dir = args.gold_dir.resolve()

    profile = pd.read_parquet(gold_dir / "integrated_credit_profile")
    labels = pd.read_parquet(gold_dir / "training_labels")
    scores = pd.read_csv(OUT / "gold_scored_applications.csv")
    profile["sk_id_curr"] = pd.to_numeric(profile["sk_id_curr"]).astype("int64")
    labels["sk_id_curr"] = pd.to_numeric(labels["sk_id_curr"]).astype("int64")
    label_col = "target" if "target" in labels.columns else "TARGET"
    labeled = profile.merge(labels[["sk_id_curr", label_col]].rename(columns={label_col: "target"}), on="sk_id_curr")
    labeled["target"] = labeled["target"].astype(int)

    for src, dest in (
        (METRICS / "risk_summary.json", METRICS / "risk_summary_track_a.json"),
        (METRICS / "demographic_profiles.json", METRICS / "demographic_profiles_track_a.json"),
        (METRICS / "model_evaluation.json", METRICS / "model_evaluation_track_a.json"),
        (OUT / "scored_applications_sample.csv", OUT / "scored_applications_sample_track_a.csv"),
        (FIGURES / "roc_curve.png", FIGURES / "roc_curve_track_a.png"),
        (FIGURES / "shap_summary.png", FIGURES / "shap_summary_track_a.png"),
    ):
        if src.exists() and not dest.exists():
            shutil.copyfile(src, dest)

    population_label = "gold_full_dataset" if len(labeled) >= 100_000 else "gold_train_subset"

    defaults = int(labeled["target"].sum())
    total = int(len(labeled))
    income = pd.to_numeric(labeled["amt_income_total"], errors="coerce")
    credit = pd.to_numeric(labeled["amt_credit"], errors="coerce")
    annuity = pd.to_numeric(labeled["amt_annuity"], errors="coerce")
    ratio = (credit / income.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    def30 = pd.to_numeric(labeled.get("def_30_cnt_social_circle"), errors="coerce")
    def60 = pd.to_numeric(labeled.get("def_60_cnt_social_circle"), errors="coerce")
    summary = {
        "portfolio_summary": {
            "total_applications": total,
            "defaults_count": defaults,
            "non_defaults_count": total - defaults,
            "npl_default_ratio": round(defaults / total, 4),
            "npl_default_percent": f"{defaults / total * 100:.2f}%",
            "population": population_label,
        },
        "financial_exposure": {
            "average_loan_amount": round(float(credit.mean()), 2),
            "median_loan_amount": round(float(credit.median()), 2),
            "average_income": round(float(income.mean()), 2),
            "median_income": round(float(income.median()), 2),
            "average_annuity": round(float(annuity.mean()), 2),
            "average_credit_to_income_ratio": round(float(ratio.mean()), 2),
        },
        "delinquency_indicators": {
            "DEF_30_CNT_SOCIAL_CIRCLE_positive_rate": round(float((def30.fillna(0) > 0).mean()), 4),
            "DEF_60_CNT_SOCIAL_CIRCLE_positive_rate": round(float((def60.fillna(0) > 0).mean()), 4),
        },
    }
    (METRICS / "risk_summary.json").write_text(json.dumps(summary, indent=2))

    labeled = labeled.copy()
    labeled["occupation_type"] = labeled["occupation_type"].fillna("Missing/Unspecified")
    profiles = {
        "risk_by_occupation": rate_block(labeled, "occupation_type"),
        "risk_by_income_type": rate_block(labeled, "name_income_type"),
        "risk_by_education": rate_block(labeled, "name_education_type"),
        "risk_by_ext_source_2_quintile": quintiles(labeled),
    }
    (METRICS / "demographic_profiles.json").write_text(json.dumps(profiles, indent=2))

    # Portfolio NPL uses every labeled row. The tier chart uses the validation
    # fold only, because in-sample scores (train AUC near 1) are not a fair book.
    scores = scores[scores["SCORE_SOURCE"] == "gold-validation"].copy()
    scored = scores.merge(
        labeled[
            [
                "sk_id_curr",
                "region_population_relative",
                "region_rating_client",
            ]
        ],
        left_on="SK_ID_CURR",
        right_on="sk_id_curr",
        how="inner",
    )
    dashboard = pd.DataFrame(
        {
            "SK_ID_CURR": scored["SK_ID_CURR"].astype(int),
            "PREDICTED_PD": scored["PREDICTED_PD"],
            "RISK_TIER": scored["RISK_TIER"],
            "ACTION_RECOMMENDATION": scored["ACTION_RECOMMENDATION"],
            "AMT_INCOME_TOTAL": scored["AMT_INCOME_TOTAL"],
            "AMT_CREDIT": scored["AMT_CREDIT"],
            "OCCUPATION_TYPE": scored["OCCUPATION_TYPE"],
            "NAME_EDUCATION_TYPE": scored["NAME_EDUCATION_TYPE"],
            "REGION_POPULATION_RELATIVE": pd.to_numeric(scored["region_population_relative"], errors="coerce"),
            "REGION_RATING_CLIENT": pd.to_numeric(scored["region_rating_client"], errors="coerce").round().astype("Int64"),
            "SCORE_SOURCE": scored["SCORE_SOURCE"],
        }
    )
    dashboard = dashboard.dropna(subset=["REGION_POPULATION_RELATIVE", "REGION_RATING_CLIENT"])
    dashboard.to_csv(OUT / "scored_applications_sample.csv", index=False)

    # Promote Gold evaluation metrics & figures when generated by train_gold_model.py
    gold_eval = METRICS / "gold_model_evaluation.json"
    if gold_eval.exists():
        shutil.copyfile(gold_eval, METRICS / "model_evaluation.json")
    gold_roc = FIGURES / "gold_roc_curve.png"
    if gold_roc.exists():
        shutil.copyfile(gold_roc, FIGURES / "roc_curve.png")
    gold_shap = FIGURES / "gold_shap_summary.png"
    if gold_shap.exists():
        shutil.copyfile(gold_shap, FIGURES / "shap_summary.png")

    tiers = dashboard["RISK_TIER"].value_counts().to_dict()
    print(
        json.dumps(
            {
                "labeled": total,
                "defaults": defaults,
                "npl": summary["portfolio_summary"]["npl_default_ratio"],
                "scored": int(len(dashboard)),
                "high": int(tiers.get("HIGH", 0)),
                "medium": int(tiers.get("MEDIUM", 0)),
                "low": int(tiers.get("LOW", 0)),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
