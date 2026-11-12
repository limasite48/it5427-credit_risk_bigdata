"""
Model Evaluation & Cost-Sensitive Decision Matrix Optimization Module
"""
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    precision_recall_curve,
    roc_curve,
    f1_score,
    confusion_matrix,
    classification_report,
)

from ..config import COST_FALSE_NEGATIVE, COST_FALSE_POSITIVE, ROC_CURVE_PLOT_PATH


def optimize_decision_threshold(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    cost_fn: float = COST_FALSE_NEGATIVE,
    cost_fp: float = COST_FALSE_POSITIVE,
) -> Dict[str, Any]:
    """
    Finds the optimal operational decision cutoff p* that minimizes the expected financial loss:
    Loss(p) = cost_fn * FN(p) + cost_fp * FP(p)
    """
    thresholds = np.linspace(0.01, 0.99, 99)
    best_threshold = 0.5
    min_cost = float("inf")
    cost_curve = []

    for t in thresholds:
        preds = (y_prob >= t).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, preds).ravel()
        total_cost = (cost_fn * fn) + (cost_fp * fp)
        cost_curve.append({"threshold": round(float(t), 2), "total_cost": float(total_cost)})

        if total_cost < min_cost:
            min_cost = total_cost
            best_threshold = float(t)

    # Cost at standard 0.5 threshold
    std_preds = (y_prob >= 0.5).astype(int)
    std_tn, std_fp, std_fn, std_tp = confusion_matrix(y_true, std_preds).ravel()
    std_cost = (cost_fn * std_fn) + (cost_fp * std_fp)

    return {
        "optimal_threshold": round(best_threshold, 2),
        "minimized_cost": float(min_cost),
        "standard_threshold_0_5_cost": float(std_cost),
        "cost_reduction_amount": float(std_cost - min_cost),
        "cost_reduction_percent": f"{((std_cost - min_cost) / std_cost) * 100:.2f}%"
        if std_cost > 0
        else "0.0%",
        "cost_ratio_fn_to_fp": f"{cost_fn}:{cost_fp}",
    }


def evaluate_predictions(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    threshold: Optional[float] = None,
    plot_path: Optional[Path] = ROC_CURVE_PLOT_PATH,
) -> Dict[str, Any]:
    """
    Computes comprehensive evaluation metrics (ROC-AUC, PR-AUC, F1, Confusion Matrix)
    and optimizes the operational decision threshold.
    """
    roc_auc = float(roc_auc_score(y_true, y_prob))
    pr_auc = float(average_precision_score(y_true, y_prob))

    # Threshold optimization
    cost_opt = optimize_decision_threshold(y_true, y_prob)
    eval_threshold = threshold if threshold is not None else cost_opt["optimal_threshold"]

    y_pred = (y_prob >= eval_threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    precision = float(tp / max(tp + fp, 1))
    recall = float(tp / max(tp + fn, 1))
    specificity = float(tn / max(tn + fp, 1))
    f1 = float(f1_score(y_true, y_pred))

    metrics = {
        "roc_auc": round(roc_auc, 4),
        "pr_auc": round(pr_auc, 4),
        "applied_threshold": round(eval_threshold, 4),
        "precision": round(precision, 4),
        "recall_sensitivity": round(recall, 4),
        "specificity": round(specificity, 4),
        "f1_score": round(f1, 4),
        "confusion_matrix": {
            "true_negatives": int(tn),
            "false_positives": int(fp),
            "false_negatives": int(fn),
            "true_positives": int(tp),
        },
        "cost_optimization": cost_opt,
    }

    # Generate and save diagnostic curves
    if plot_path:
        plot_path = Path(plot_path)
        plot_path.parent.mkdir(parents=True, exist_ok=True)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # ROC Curve
        fpr, tpr, _ = roc_curve(y_true, y_prob)
        ax1.plot(fpr, tpr, color="crimson", lw=2, label=f"ROC (AUC = {roc_auc:.3f})")
        ax1.plot([0, 1], [0, 1], color="gray", linestyle="--")
        ax1.set_title("Receiver Operating Characteristic (ROC)")
        ax1.set_xlabel("False Positive Rate")
        ax1.set_ylabel("True Positive Rate")
        ax1.legend(loc="lower right")
        ax1.grid(True, alpha=0.3)

        # PR Curve
        prec_arr, rec_arr, _ = precision_recall_curve(y_true, y_prob)
        ax2.plot(rec_arr, prec_arr, color="navy", lw=2, label=f"PR (AUC = {pr_auc:.3f})")
        ax2.set_title("Precision-Recall Curve")
        ax2.set_xlabel("Recall")
        ax2.set_ylabel("Precision")
        ax2.legend(loc="upper right")
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(plot_path, dpi=200)
        plt.close(fig)

    return metrics
