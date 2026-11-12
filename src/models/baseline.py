"""
Baseline Benchmark Model: Regularized Logistic Regression Scorecard
"""
from typing import Dict, Any, Tuple
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score

from ..config import RANDOM_STATE


def train_logistic_regression_baseline(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
) -> Tuple[Pipeline, Dict[str, Any]]:
    """
    Trains a standardized L2-regularized Logistic Regression pipeline with balanced weights.
    Returns the trained pipeline and evaluation metrics.
    """
    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    C=0.1,
                    class_weight="balanced",
                    solver="lbfgs",
                    max_iter=1000,
                    random_state=RANDOM_STATE,
                ),
            ),
        ]
    )

    pipeline.fit(X_train, y_train)

    train_preds_proba = pipeline.predict_proba(X_train)[:, 1]
    val_preds_proba = pipeline.predict_proba(X_val)[:, 1]

    train_auc = float(roc_auc_score(y_train, train_preds_proba))
    val_auc = float(roc_auc_score(y_val, val_preds_proba))

    metrics = {
        "model_type": "LogisticRegression_Baseline",
        "train_roc_auc": round(train_auc, 4),
        "val_roc_auc": round(val_auc, 4),
    }

    return pipeline, metrics
