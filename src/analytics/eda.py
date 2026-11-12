"""
Exploratory Data Analysis & Portfolio Risk Metrics Module
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional
import numpy as np
import pandas as pd

from ..config import METRICS_SUMMARY_PATH


def compute_portfolio_risk_metrics(
    df: pd.DataFrame,
    save_path: Optional[Path] = METRICS_SUMMARY_PATH
) -> Dict[str, Any]:
    """
    Computes key portfolio credit risk metrics, including NPL ratio,
    delinquency proxies, and financial distributions.
    """
    total_loans = len(df)
    target_available = "TARGET" in df.columns

    defaults = int(df["TARGET"].sum()) if target_available else 0
    non_defaults = total_loans - defaults
    npl_ratio = float(defaults / total_loans) if target_available and total_loans > 0 else 0.0

    # Loan & Income distributions
    credit_mean = float(df["AMT_CREDIT"].mean()) if "AMT_CREDIT" in df.columns else 0.0
    credit_median = float(df["AMT_CREDIT"].median()) if "AMT_CREDIT" in df.columns else 0.0
    income_mean = float(df["AMT_INCOME_TOTAL"].mean()) if "AMT_INCOME_TOTAL" in df.columns else 0.0
    income_median = float(df["AMT_INCOME_TOTAL"].median()) if "AMT_INCOME_TOTAL" in df.columns else 0.0
    annuity_mean = float(df["AMT_ANNUITY"].mean()) if "AMT_ANNUITY" in df.columns else 0.0

    # Credit to income
    if "AMT_CREDIT" in df.columns and "AMT_INCOME_TOTAL" in df.columns:
        credit_income_ratio_mean = float((df["AMT_CREDIT"] / df["AMT_INCOME_TOTAL"]).mean())
    else:
        credit_income_ratio_mean = 0.0

    # Social circle delinquency metrics
    delinquency_metrics = {}
    for col in ["DEF_30_CNT_SOCIAL_CIRCLE", "DEF_60_CNT_SOCIAL_CIRCLE"]:
        if col in df.columns:
            has_delinquency = (df[col] > 0).sum()
            delinquency_metrics[f"{col}_positive_rate"] = float(has_delinquency / total_loans)

    summary = {
        "portfolio_summary": {
            "total_applications": total_loans,
            "defaults_count": defaults,
            "non_defaults_count": non_defaults,
            "npl_default_ratio": round(npl_ratio, 4),
            "npl_default_percent": f"{npl_ratio * 100:.2f}%",
        },
        "financial_exposure": {
            "average_loan_amount": round(credit_mean, 2),
            "median_loan_amount": round(credit_median, 2),
            "average_income": round(income_mean, 2),
            "median_income": round(income_median, 2),
            "average_annuity": round(annuity_mean, 2),
            "average_credit_to_income_ratio": round(credit_income_ratio_mean, 2),
        },
        "delinquency_indicators": delinquency_metrics,
    }

    if save_path:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    return summary
