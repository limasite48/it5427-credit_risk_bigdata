"""
Feature Engineering and Preprocessing Pipeline
"""
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer

from ..config import DAYS_EMPLOYED_ANOMALY


class CreditRiskPreprocessor(BaseEstimator, TransformerMixin):
    """
    Transforms raw application loan data by:
    1. Remedying data anomalies (DAYS_EMPLOYED == 365243).
    2. Generating credit banking financial domain ratios.
    3. Imputing missing values and encoding categoricals.
    """

    def __init__(self, handle_categoricals: str = "ordinal"):
        self.handle_categoricals = handle_categoricals
        self.cat_cols: List[str] = []
        self.num_cols: List[str] = []
        self.feature_names_: List[str] = []
        self.imputer: Optional[SimpleImputer] = None
        self.encoder: Optional[OrdinalEncoder] = None
        self.medians_: Dict[str, float] = {}

    def _engineer_domain_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create domain credit risk features and fix anomalies."""
        data = df.copy()

        # Handle DAYS_EMPLOYED anomaly
        if "DAYS_EMPLOYED" in data.columns:
            data["DAYS_EMPLOYED_ANOM"] = (
                data["DAYS_EMPLOYED"] == DAYS_EMPLOYED_ANOMALY
            ).astype(np.int32)
            data["DAYS_EMPLOYED"] = data["DAYS_EMPLOYED"].replace(
                {DAYS_EMPLOYED_ANOMALY: np.nan}
            )

        # Domain Ratios
        if "AMT_CREDIT" in data.columns and "AMT_INCOME_TOTAL" in data.columns:
            data["CREDIT_INCOME_RATIO"] = data["AMT_CREDIT"] / np.maximum(
                data["AMT_INCOME_TOTAL"], 1.0
            )

        if "AMT_ANNUITY" in data.columns and "AMT_INCOME_TOTAL" in data.columns:
            data["ANNUITY_INCOME_RATIO"] = data["AMT_ANNUITY"] / np.maximum(
                data["AMT_INCOME_TOTAL"], 1.0
            )

        if "AMT_ANNUITY" in data.columns and "AMT_CREDIT" in data.columns:
            data["CREDIT_TERM"] = data["AMT_ANNUITY"] / np.maximum(
                data["AMT_CREDIT"], 1.0
            )

        if "DAYS_EMPLOYED" in data.columns and "DAYS_BIRTH" in data.columns:
            data["EMPLOYED_TO_AGE_RATIO"] = data["DAYS_EMPLOYED"] / data[
                "DAYS_BIRTH"
            ].replace({0: np.nan})

        # Aggregated External Sources (strongest individual predictors)
        ext_sources = [c for c in ["EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3"] if c in data.columns]
        if ext_sources:
            data["EXT_SOURCES_MEAN"] = data[ext_sources].mean(axis=1)
            data["EXT_SOURCES_STD"] = data[ext_sources].std(axis=1).fillna(0)
            data["EXT_SOURCES_MIN"] = data[ext_sources].min(axis=1)
            data["EXT_SOURCES_MAX"] = data[ext_sources].max(axis=1)

        if "AMT_INCOME_TOTAL" in data.columns and "CNT_FAM_MEMBERS" in data.columns:
            data["INCOME_PER_FAMILY_MEMBER"] = data["AMT_INCOME_TOTAL"] / np.maximum(
                data["CNT_FAM_MEMBERS"].fillna(1.0), 1.0
            )

        return data

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None):
        """Fit encoders, imputers, and record feature schema."""
        data = self._engineer_domain_features(X)

        # Separate target or ID if present
        cols_to_exclude = ["SK_ID_CURR", "TARGET"]
        active_cols = [c for c in data.columns if c not in cols_to_exclude]

        self.num_cols = [
            c for c in active_cols if pd.api.types.is_numeric_dtype(data[c])
        ]
        self.cat_cols = [c for c in active_cols if c not in self.num_cols]

        # Calculate medians for numerical columns
        for c in self.num_cols:
            med = data[c].median()
            self.medians_[c] = 0.0 if np.isnan(med) else float(med)

        # Fit ordinal encoder for categoricals with unknown category handling
        if self.cat_cols:
            self.encoder = OrdinalEncoder(
                handle_unknown="use_encoded_value",
                unknown_value=-1
            )
            # Fill NAs with 'Missing' before fitting
            cat_df = data[self.cat_cols].fillna("Missing").astype(str)
            self.encoder.fit(cat_df)

        self.feature_names_ = self.num_cols + self.cat_cols
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform raw data into a clean model-ready DataFrame."""
        data = self._engineer_domain_features(X)

        # Handle numeric imputation using precomputed medians
        for c in self.num_cols:
            if c in data.columns:
                data[c] = data[c].fillna(self.medians_.get(c, 0.0))
            else:
                data[c] = self.medians_.get(c, 0.0)

        # Handle categoricals
        if self.cat_cols and self.encoder:
            for c in self.cat_cols:
                if c not in data.columns:
                    data[c] = "Missing"
                else:
                    data[c] = data[c].fillna("Missing").astype(str)
            cat_encoded = self.encoder.transform(data[self.cat_cols])
            cat_df = pd.DataFrame(
                cat_encoded, columns=self.cat_cols, index=data.index
            )
            for c in self.cat_cols:
                data[c] = cat_df[c]

        return data[self.feature_names_]
