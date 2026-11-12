"""
Unit tests for Credit Risk Scoring & Risk Tiering Service
"""
import pytest
from src.service.scoring_service import CreditRiskScoringService
from src.config import RISK_TIERS


class MockModel:
    """Mock model returning controllable default probabilities."""
    def __init__(self, proba: float = 0.08):
        self.proba = proba

    def predict_proba(self, X):
        import numpy as np
        return np.array([[1.0 - self.proba, self.proba]])


class MockPreprocessor:
    """Mock preprocessor returning input as-is."""
    def __init__(self):
        self.feature_names_ = ["AMT_CREDIT", "AMT_INCOME_TOTAL"]

    def transform(self, df):
        return df[self.feature_names_]


def test_risk_tier_assignment_low():
    service = CreditRiskScoringService(
        model=MockModel(proba=0.03),
        preprocessor=MockPreprocessor()
    )
    tier_info = service.assign_risk_tier(0.03)
    assert tier_info["risk_tier"] == "LOW"
    assert tier_info["action"] == "Auto-Approve"


def test_risk_tier_assignment_medium():
    service = CreditRiskScoringService(
        model=MockModel(proba=0.09),
        preprocessor=MockPreprocessor()
    )
    tier_info = service.assign_risk_tier(0.09)
    assert tier_info["risk_tier"] == "MEDIUM"
    assert tier_info["action"] == "Manual Review"


def test_risk_tier_assignment_high():
    service = CreditRiskScoringService(
        model=MockModel(proba=0.22),
        preprocessor=MockPreprocessor()
    )
    tier_info = service.assign_risk_tier(0.22)
    assert tier_info["risk_tier"] == "HIGH"
    assert tier_info["action"] == "Decline"


def test_score_applicant_output_schema():
    service = CreditRiskScoringService(
        model=MockModel(proba=0.18),
        preprocessor=MockPreprocessor(),
        explainer=None
    )
    applicant = {
        "SK_ID_CURR": 999999,
        "AMT_CREDIT": 500000.0,
        "AMT_INCOME_TOTAL": 100000.0,
    }
    result = service.score_applicant(applicant, include_explanations=False)

    assert result["SK_ID_CURR"] == 999999
    assert result["default_probability"] == 0.18
    assert result["risk_tier"] == "HIGH"
    assert result["action"] == "Decline"
    assert "tier_description" in result
