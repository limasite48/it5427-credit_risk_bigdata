"""
Explainable AI (SHAP) Module for Credit Risk Modeling
"""
from pathlib import Path
from typing import Dict, List, Any, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import shap

from ..config import SHAP_SUMMARY_PLOT_PATH


class CreditRiskExplainer:
    """
    Provides global feature importance and applicant-level decision explanations
    using TreeSHAP.
    """

    def __init__(self, model, feature_names: List[str]):
        self.model = model
        self.feature_names = feature_names
        self.explainer = shap.TreeExplainer(model)

    def explain_global(
        self,
        X_sample: pd.DataFrame,
        save_path: Optional[Path] = SHAP_SUMMARY_PLOT_PATH,
        max_display: int = 15,
    ) -> np.ndarray:
        """
        Computes SHAP values for a representative sample and saves the summary beeswarm plot.
        """
        shap_values = self.explainer.shap_values(X_sample)

        # In binary classification, shap_values may be a list of two arrays [class 0, class 1]
        # or a single 2D array for class 1
        if isinstance(shap_values, list) and len(shap_values) == 2:
            vals_to_plot = shap_values[1]
        else:
            vals_to_plot = shap_values

        if save_path:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)

            plt.figure(figsize=(10, 8))
            shap.summary_plot(
                vals_to_plot,
                X_sample,
                feature_names=self.feature_names,
                max_display=max_display,
                show=False,
            )
            plt.title("Credit Risk Model: Top Global Risk Drivers (SHAP)", fontsize=13)
            plt.tight_layout()
            plt.savefig(save_path, dpi=200, bbox_inches="tight")
            plt.close()

        return vals_to_plot

    def explain_applicant(
        self,
        applicant_row: pd.DataFrame,
        top_n: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Returns the top contributing features for an individual loan applicant.
        """
        shap_vals = self.explainer.shap_values(applicant_row)
        if isinstance(shap_vals, list) and len(shap_vals) == 2:
            vals = shap_vals[1][0]
        elif len(shap_vals.shape) == 2:
            vals = shap_vals[0]
        else:
            vals = shap_vals

        feature_impacts = []
        for feat, val, feat_val in zip(self.feature_names, vals, applicant_row.iloc[0]):
            effect = "Increases Risk" if val > 0 else "Lowers Risk"
            feature_impacts.append(
                {
                    "feature": feat,
                    "feature_value": round(float(feat_val), 4) if isinstance(feat_val, (int, float, np.number)) else str(feat_val),
                    "shap_value": round(float(val), 4),
                    "effect": effect,
                    "magnitude": round(abs(float(val)), 4),
                }
            )

        # Sort by absolute magnitude
        feature_impacts.sort(key=lambda x: x["magnitude"], reverse=True)
        return feature_impacts[:top_n]
