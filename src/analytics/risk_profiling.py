"""
Risk Profiling & Demographic Segment Analysis Module
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional
import numpy as np
import pandas as pd

from ..config import DEMOGRAPHICS_PATH


def _calculate_segment_rates(df: pd.DataFrame, col: str) -> Dict[str, Any]:
    """Calculate volume and default rates for categorical segments."""
    if col not in df.columns or "TARGET" not in df.columns:
        return {}

    grouped = (
        df.groupby(df[col].fillna("Missing/Unspecified"))["TARGET"]
        .agg(["count", "sum", "mean"])
        .reset_index()
    )
    grouped.columns = [col, "total_count", "default_count", "default_rate"]
    grouped = grouped.sort_values(by="total_count", ascending=False)

    result = {}
    for _, row in grouped.iterrows():
        segment_name = str(row[col])
        result[segment_name] = {
            "total_count": int(row["total_count"]),
            "default_count": int(row["default_count"]),
            "default_rate": round(float(row["default_rate"]), 4),
            "default_percent": f"{row['default_rate'] * 100:.2f}%",
        }
    return result


def compute_demographic_risk_profiles(
    df: pd.DataFrame,
    save_path: Optional[Path] = DEMOGRAPHICS_PATH
) -> Dict[str, Any]:
    """
    Computes default probability distributions across occupation, income type,
    education, and external bureau ratings.
    """
    profiles = {
        "risk_by_occupation": _calculate_segment_rates(df, "OCCUPATION_TYPE"),
        "risk_by_income_type": _calculate_segment_rates(df, "NAME_INCOME_TYPE"),
        "risk_by_education": _calculate_segment_rates(df, "NAME_EDUCATION_TYPE"),
        "risk_by_gender": _calculate_segment_rates(df, "CODE_GENDER"),
        "risk_by_contract_type": _calculate_segment_rates(df, "NAME_CONTRACT_TYPE"),
    }

    # External score quintile risk profile
    if "EXT_SOURCE_2" in df.columns and "TARGET" in df.columns:
        valid_ext = df.dropna(subset=["EXT_SOURCE_2"]).copy()
        if len(valid_ext) > 0:
            valid_ext["EXT_2_QUINTILE"] = pd.qcut(
                valid_ext["EXT_SOURCE_2"], q=5, labels=["Q1 (Lowest)", "Q2", "Q3", "Q4", "Q5 (Highest)"]
            )
            profiles["risk_by_ext_source_2_quintile"] = _calculate_segment_rates(valid_ext, "EXT_2_QUINTILE")

    if save_path:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(profiles, f, indent=2)

    return profiles
