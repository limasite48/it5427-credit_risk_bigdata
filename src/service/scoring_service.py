"""
Unified Credit Risk Scoring & Inference Service for Dashboard Integration
"""
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import joblib
import numpy as np
import pandas as pd

from ..config import MODEL_SAVE_PATH, RISK_TIERS, SCORED_SAMPLE_PATH
from ..models.explainability import CreditRiskExplainer


class CreditRiskScoringService:
    """
    Inference service for real-time and batch scoring:
    - Preprocesses applicant raw data
    - Computes default probability (PD)
    - Maps PD to operational Risk Tiers (LOW, MEDIUM, HIGH)
    - Generates local SHAP explanation vectors
    """

    def __init__(
        self,
        artifact_path: Optional[Union[str, Path]] = MODEL_SAVE_PATH,
        model=None,
        preprocessor=None,
        explainer: Optional[CreditRiskExplainer] = None,
    ):
        if model is not None and preprocessor is not None:
            self.model = model
            self.preprocessor = preprocessor
            self.explainer = explainer
        else:
            path = Path(artifact_path)
            if not path.exists():
                raise FileNotFoundError(f"Model artifact not found at {path}")
            artifact = joblib.load(path)
            self.model = artifact["model"]
            self.preprocessor = artifact["preprocessor"]
            self.explainer = CreditRiskExplainer(
                self.model, self.preprocessor.feature_names_
            )

    def assign_risk_tier(self, pd_val: float) -> Dict[str, str]:
        """Maps probability of default to risk tier and policy action."""
        for tier_name, policy in RISK_TIERS.items():
            if pd_val < policy["max_pd"]:
                return {
                    "risk_tier": tier_name,
                    "action": policy["action"],
                    "description": policy["description"],
                }
        return {
            "risk_tier": "HIGH",
            "action": RISK_TIERS["HIGH"]["action"],
            "description": RISK_TIERS["HIGH"]["description"],
        }

    def score_applicant(
        self, applicant_dict: Dict[str, Any], include_explanations: bool = True
    ) -> Dict[str, Any]:
        """
        Scores a single loan applicant.
        """
        raw_df = pd.DataFrame([applicant_dict])
        transformed_df = self.preprocessor.transform(raw_df)

        prob = float(self.model.predict_proba(transformed_df)[:, 1][0])
        tier_info = self.assign_risk_tier(prob)

        result = {
            "SK_ID_CURR": applicant_dict.get("SK_ID_CURR", "N/A"),
            "default_probability": round(prob, 4),
            "risk_tier": tier_info["risk_tier"],
            "action": tier_info["action"],
            "tier_description": tier_info["description"],
        }

        if include_explanations and self.explainer:
            top_drivers = self.explainer.explain_applicant(transformed_df, top_n=3)
            result["top_risk_drivers"] = top_drivers

        return result

    def score_batch(
        self, df: pd.DataFrame, save_path: Optional[Path] = SCORED_SAMPLE_PATH
    ) -> pd.DataFrame:
        """
        Scores a batch of applicants and exports the scored dataset for Dashboard consumption.
        """
        transformed_df = self.preprocessor.transform(df)
        probs = self.model.predict_proba(transformed_df)[:, 1]

        scored_df = df.copy()
        scored_df["PREDICTED_PD"] = np.round(probs, 4)

        tiers = []
        actions = []
        for p in probs:
            tier_info = self.assign_risk_tier(float(p))
            tiers.append(tier_info["risk_tier"])
            actions.append(tier_info["action"])

        scored_df["RISK_TIER"] = tiers
        scored_df["ACTION_RECOMMENDATION"] = actions

        if save_path:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            scored_df.to_csv(save_path, index=False)

        return scored_df
