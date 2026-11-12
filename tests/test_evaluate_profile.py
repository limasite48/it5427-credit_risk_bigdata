"""
Unit tests for evaluate_profile.py (Manual Testing & Lookup Utility)
"""
import pytest
from evaluate_profile import lookup_applicant_in_database
from src.service.scoring_service import CreditRiskScoringService


def test_lookup_existing_applicant():
    """Verify looking up a known applicant ID from the dataset."""
    profile = lookup_applicant_in_database(100002)
    assert profile is not None
    assert profile["SK_ID_CURR"] == 100002
    assert "AMT_CREDIT" in profile
    assert profile["TARGET"] == 1


def test_score_sample_applicant_payload():
    """Verify evaluating an applicant dictionary produces complete decisions."""
    service = CreditRiskScoringService()
    sample = {
        "SK_ID_CURR": 888001,
        "AMT_INCOME_TOTAL": 350000.0,
        "AMT_CREDIT": 400000.0,
        "AMT_ANNUITY": 20000.0,
        "DAYS_BIRTH": -15000,
        "DAYS_EMPLOYED": -3000,
        "NAME_INCOME_TYPE": "Commercial associate",
        "OCCUPATION_TYPE": "Managers",
        "EXT_SOURCE_1": 0.85,
        "EXT_SOURCE_2": 0.80,
        "EXT_SOURCE_3": 0.75,
    }
    decision = service.score_applicant(sample, include_explanations=True)

    assert "default_probability" in decision
    assert decision["risk_tier"] == "LOW"
    assert decision["action"] == "Auto-Approve"
    assert len(decision["top_risk_drivers"]) > 0
