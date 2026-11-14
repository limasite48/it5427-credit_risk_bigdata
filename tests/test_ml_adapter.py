"""
Tests for the FastAPI ML service adapter.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pytest
from fastapi.testclient import TestClient

from tests.ml_service_test_helpers import load_adapter_app

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "test-internal-token"


class MockModel:
    def __init__(self, proba: float = 0.08):
        self.proba = proba

    def predict_proba(self, X):
        return np.array([[1.0 - self.proba, self.proba]])


class MockPreprocessor:
    def __init__(self):
        self.feature_names_ = ["AMT_CREDIT", "AMT_INCOME_TOTAL"]

    def transform(self, df):
        return df[self.feature_names_]


@pytest.fixture
def client(monkeypatch):
    from src.service.scoring_service import CreditRiskScoringService

    service = CreditRiskScoringService(
        model=MockModel(0.18),
        preprocessor=MockPreprocessor(),
        explainer=None,
    )
    app = load_adapter_app(service=service, token=TOKEN)
    # Injected mock is preserved because app lifespan skips reload when already set.
    with TestClient(app) as c:
        yield c


def test_health_ok(client):
    res = client.get("/health")
    assert res.status_code == 200
    body = res.json()
    assert body["model_loaded"] is True
    assert body["status"] == "ok"


def test_score_requires_token(client):
    res = client.post("/score", json={"applicant": {"AMT_CREDIT": 1}})
    assert res.status_code == 401


def test_score_rejects_empty_applicant(client):
    res = client.post(
        "/score",
        json={"applicant": {}},
        headers={"X-Internal-Token": TOKEN},
    )
    assert res.status_code == 400


def test_score_valid_applicant(client):
    res = client.post(
        "/score",
        json={
            "applicant": {
                "SK_ID_CURR": 1,
                "AMT_CREDIT": 100000.0,
                "AMT_INCOME_TOTAL": 50000.0,
            }
        },
        headers={"X-Internal-Token": TOKEN},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["default_probability"] == 0.18
    assert body["risk_tier"] == "HIGH"
    assert body["action"] == "Decline"


def test_score_real_sample_applicants():
    """Score the real demo JSON files through the real model when available."""
    model_path = ROOT / "outputs" / "models" / "credit_risk_model.joblib"
    if not model_path.exists():
        pytest.skip("Model artifact missing")

    from src.service.scoring_service import CreditRiskScoringService

    service = CreditRiskScoringService(artifact_path=model_path)
    app = load_adapter_app(service=service, token=TOKEN)

    results: Dict[str, Dict[str, Any]] = {}
    with TestClient(app) as client:
        for name in ("sample_full_applicant.json", "sample_subprime_applicant.json"):
            applicant = json.loads((ROOT / name).read_text())
            res = client.post(
                "/score",
                json={"applicant": applicant},
                headers={"X-Internal-Token": TOKEN},
            )
            assert res.status_code == 200, res.text
            body = res.json()
            assert "default_probability" in body
            assert body["risk_tier"] in {"LOW", "MEDIUM", "HIGH"}
            assert "action" in body
            assert "top_risk_drivers" in body
            assert len(body["top_risk_drivers"]) <= 3
            results[name] = {
                "risk_tier": body["risk_tier"],
                "action": body["action"],
                "default_probability": body["default_probability"],
            }

    # Document actual outcomes for the demo guide (do not fabricate).
    print("\nReal sample scoring results:")
    for name, outcome in results.items():
        print(f"  {name}: {outcome}")

    # Soft expectations from the project brief; fail only if clearly inverted.
    full = results["sample_full_applicant.json"]
    subprime = results["sample_subprime_applicant.json"]
    assert full["default_probability"] < subprime["default_probability"]
