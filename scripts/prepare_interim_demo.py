#!/usr/bin/env python3
"""Prepare fast interim demo & reporting artifacts locally (~2 minutes, no Spark wait).

Generates:
  1. Supplementary regional macro + occupation salary CSVs (`ingestion-minio-kafka/data/supplementary/`).
  2. Upgraded regularized Gold artifacts (`outputs/gold_*` & `outputs/metrics/gold_model_evaluation.json`)
     from the existing local `apache-spark/data/gold/` dev-subset Parquet tables.
  3. Full 307,511-row application scoring, portfolio risk summary, demographic segments,
     LR vs. LightGBM ROC/PR curves, 5:1 cost optimization, and TreeSHAP summary via
     `run_track_a.py --include-macro`.

Note: Intentionally does NOT run `standardize_dashboard.py` against the 5k Gold subset so the
307,511-row portfolio metrics stay active for presentations until Google Colab's full bundle arrives.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str], env: dict[str, str] | None = None) -> None:
    print(f"\n>>> Running: {' '.join(cmd)}")
    merged = os.environ.copy()
    if env:
        merged.update(env)
    subprocess.run(cmd, cwd=str(ROOT), env=merged, check=True)


def _print_summary() -> None:
    risk = json.loads((ROOT / "outputs" / "metrics" / "risk_summary.json").read_text(encoding="utf-8"))
    eval_data = json.loads((ROOT / "outputs" / "metrics" / "model_evaluation.json").read_text(encoding="utf-8"))
    gold_ids = json.loads((ROOT / "outputs" / "gold_demo_ids.json").read_text(encoding="utf-8"))
    spark_stats_path = ROOT / "apache-spark" / "reports" / "pipeline_stats.json"
    spark_label = "missing"
    if spark_stats_path.exists():
        spark_label = json.loads(spark_stats_path.read_text(encoding="utf-8")).get("dataset_label", "unknown")

    portfolio = risk.get("portfolio_summary", {})
    lgbm = eval_data.get("lightgbm_classifier", {})
    lr = eval_data.get("baseline_logistic_regression", {})
    detail = eval_data.get("detailed_lightgbm_evaluation", {})
    cost = detail.get("cost_optimization", {})
    summary = {
        "interim_portfolio_applications": portfolio.get("total_applications"),
        "interim_npl_percent": portfolio.get("npl_default_percent"),
        "baseline_lr_val_auc": lr.get("val_roc_auc"),
        "lightgbm_val_auc": lgbm.get("val_roc_auc"),
        "lightgbm_pr_auc": detail.get("pr_auc"),
        "optimal_threshold": cost.get("optimal_threshold"),
        "cost_reduction_percent": cost.get("cost_reduction_percent"),
        "gold_demo_high_id": gold_ids.get("high_example"),
        "gold_demo_low_id": gold_ids.get("low_example"),
        "spark_reports_dataset": spark_label,
    }
    print("\n=== Interim Demo Summary ===")
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build fast 307k interim demo artifacts locally.")
    parser.add_argument("--fast", action="store_true", help="Use fewer LightGBM trees in run_track_a.py")
    parser.add_argument("--skip-gold", action="store_true", help="Skip upgrading Gold artifacts from local apache-spark/data/gold")
    parser.add_argument("--skip-tests", action="store_true", help="Skip running pytest after generation")
    args = parser.parse_args()

    raw_dir = ROOT / "_data"
    if not (raw_dir / "application_train.csv").exists():
        raise SystemExit(f"Raw Kaggle dataset not found at {raw_dir}")

    py = sys.executable

    # 1. Refresh supplementary regional macro + occupation benchmarks
    _run([py, "ingestion-minio-kafka/ingestion/gen_macro.py"], env={"HC_RAW_DIR": str(raw_dir)})

    # 2. Upgrade Gold artifacts (regularized LightGBM, PR-AUC, 5:1 cost opt) from local Gold Parquet
    gold_dir = ROOT / "apache-spark" / "data" / "gold"
    if not args.skip_gold and (gold_dir / "integrated_credit_profile").exists():
        _run([py, "scripts/train_gold_model.py", "--gold-dir", str(gold_dir)])
    elif not args.skip_gold:
        print(f"Skipping Gold upgrade: {gold_dir / 'integrated_credit_profile'} not found.")

    # 3. Full 307,511-row application scoring, portfolio analytics, LR baseline, LightGBM, SHAP
    track_a_cmd = [py, "run_track_a.py", "--include-macro"]
    if args.fast:
        track_a_cmd.append("--fast")
    _run(track_a_cmd)

    # 4. Run Python unit & integration tests
    if not args.skip_tests:
        _run([py, "-m", "pytest", "tests/", "-v"])

    _print_summary()


if __name__ == "__main__":
    main()
