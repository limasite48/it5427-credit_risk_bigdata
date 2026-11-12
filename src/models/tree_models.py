"""
High-Performance Gradient Boosting Model: LightGBM Classifier
"""
from typing import Dict, Any, Tuple, Optional
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, early_stopping, log_evaluation
from sklearn.metrics import roc_auc_score

from ..config import LGBM_PARAMS, RANDOM_STATE


def train_lightgbm_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    custom_params: Optional[Dict[str, Any]] = None,
) -> Tuple[LGBMClassifier, Dict[str, Any]]:
    """
    Trains a LightGBM Classifier with class weighting and early stopping.
    """
    params = LGBM_PARAMS.copy()
    if custom_params:
        params.update(custom_params)

    # Class imbalance weighting
    pos_count = int(y_train.sum())
    neg_count = len(y_train) - pos_count
    scale_pos = neg_count / max(pos_count, 1)

    model = LGBMClassifier(
        **params,
        scale_pos_weight=scale_pos,
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_train, y_train), (X_val, y_val)],
        eval_names=["train", "valid"],
        callbacks=[early_stopping(stopping_rounds=30, verbose=False), log_evaluation(period=0)],
    )

    train_proba = model.predict_proba(X_train)[:, 1]
    val_proba = model.predict_proba(X_val)[:, 1]

    train_auc = float(roc_auc_score(y_train, train_proba))
    val_auc = float(roc_auc_score(y_val, val_proba))

    # Top feature importances
    importances = pd.Series(model.feature_importances_, index=X_train.columns).sort_values(
        ascending=False
    )

    metrics = {
        "model_type": "LightGBM_Classifier",
        "train_roc_auc": round(train_auc, 4),
        "val_roc_auc": round(val_auc, 4),
        "best_iteration": int(model.best_iteration_) if hasattr(model, "best_iteration_") else params["n_estimators"],
        "top_10_features": importances.head(10).to_dict(),
    }

    return model, metrics
