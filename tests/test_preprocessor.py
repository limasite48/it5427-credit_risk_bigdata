"""
Unit tests for Data Loading & Feature Preprocessor
"""
import numpy as np
import pandas as pd
import pytest

from src.data.preprocessor import CreditRiskPreprocessor
from src.config import DAYS_EMPLOYED_ANOMALY


@pytest.fixture
def sample_raw_data():
    """Generates a synthetic sample raw application DataFrame."""
    return pd.DataFrame(
        {
            "SK_ID_CURR": [100001, 100002, 100003],
            "TARGET": [0, 1, 0],
            "AMT_INCOME_TOTAL": [100000.0, 200000.0, 50000.0],
            "AMT_CREDIT": [300000.0, 400000.0, 150000.0],
            "AMT_ANNUITY": [15000.0, 20000.0, 7500.0],
            "DAYS_BIRTH": [-10000, -15000, -20000],
            "DAYS_EMPLOYED": [-1000, DAYS_EMPLOYED_ANOMALY, -3000],
            "CNT_FAM_MEMBERS": [2, 1, 3],
            "OCCUPATION_TYPE": ["Laborers", None, "Managers"],
            "NAME_INCOME_TYPE": ["Working", "Pensioner", "State servant"],
            "EXT_SOURCE_1": [0.5, np.nan, 0.7],
            "EXT_SOURCE_2": [0.6, 0.3, 0.8],
            "EXT_SOURCE_3": [0.4, 0.2, np.nan],
        }
    )


def test_days_employed_anomaly_handling(sample_raw_data):
    preprocessor = CreditRiskPreprocessor()
    transformed = preprocessor.fit_transform(sample_raw_data)

    assert "DAYS_EMPLOYED_ANOM" in transformed.columns
    # Row 1 had DAYS_EMPLOYED == 365243 -> anomaly flag should be 1
    assert transformed["DAYS_EMPLOYED_ANOM"].iloc[1] == 1
    assert transformed["DAYS_EMPLOYED_ANOM"].iloc[0] == 0


def test_domain_ratios_calculation(sample_raw_data):
    preprocessor = CreditRiskPreprocessor()
    transformed = preprocessor.fit_transform(sample_raw_data)

    assert "CREDIT_INCOME_RATIO" in transformed.columns
    assert "ANNUITY_INCOME_RATIO" in transformed.columns
    assert "CREDIT_TERM" in transformed.columns
    assert "EXT_SOURCES_MEAN" in transformed.columns

    # Row 0: CREDIT / INCOME = 300,000 / 100,000 = 3.0
    assert pytest.approx(transformed["CREDIT_INCOME_RATIO"].iloc[0], rel=1e-3) == 3.0
    # Row 0: ANNUITY / INCOME = 15,000 / 100,000 = 0.15
    assert pytest.approx(transformed["ANNUITY_INCOME_RATIO"].iloc[0], rel=1e-3) == 0.15
    # Row 0: ANNUITY / CREDIT = 15,000 / 300,000 = 0.05
    assert pytest.approx(transformed["CREDIT_TERM"].iloc[0], rel=1e-3) == 0.05


def test_no_null_values_after_transform(sample_raw_data):
    preprocessor = CreditRiskPreprocessor()
    transformed = preprocessor.fit_transform(sample_raw_data)

    assert transformed.isnull().sum().sum() == 0
    assert not np.isinf(transformed.values).any()
