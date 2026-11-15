#!/usr/bin/env python3
"""Safely unpack Google Colab `credit_risk_full_artifacts.zip` and verify full-dataset markers."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_PREFIXES = (
    "outputs/",
    "apache-spark/reports/",
    "apache-spark/docs/",
    "ingestion-minio-kafka/data/supplementary/",
)


def _is_safe_member(name: str) -> bool:
    posix = PurePosixPath(name)
    if posix.is_absolute() or ".." in posix.parts:
        return False
    return any(name.startswith(prefix) for prefix in ALLOWED_PREFIXES)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract and validate Google Colab full-dataset artifacts.")
    parser.add_argument("--zip", type=Path, default=ROOT / "credit_risk_full_artifacts.zip", help="Path to Colab artifact zip")
    parser.add_argument("--run-tests", action="store_true", help="Run pytest tests/ after extraction")
    args = parser.parse_args()

    zip_path = args.zip.resolve()
    if not zip_path.is_file():
        raise SystemExit(f"Artifact bundle not found: {zip_path}")

    extracted = 0
    skipped = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            if not _is_safe_member(info.filename):
                skipped += 1
                continue
            target = (ROOT / info.filename).resolve()
            if ROOT.resolve() not in target.parents:
                raise SystemExit(f"Unsafe zip entry rejected: {info.filename}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(target, "wb") as dst:
                dst.write(src.read())
            extracted += 1

    print(f"Extracted {extracted} files into {ROOT} (ignored {skipped} non-artifact entries).")

    risk = json.loads((ROOT / "outputs" / "metrics" / "risk_summary.json").read_text(encoding="utf-8"))
    eval_data = json.loads((ROOT / "outputs" / "metrics" / "model_evaluation.json").read_text(encoding="utf-8"))
    spark_stats = json.loads((ROOT / "apache-spark" / "reports" / "pipeline_stats.json").read_text(encoding="utf-8"))

    summary = {
        "total_applications": risk.get("portfolio_summary", {}).get("total_applications"),
        "population": risk.get("portfolio_summary", {}).get("population"),
        "npl_default_percent": risk.get("portfolio_summary", {}).get("npl_default_percent"),
        "val_roc_auc": eval_data.get("val_roc_auc") or eval_data.get("lightgbm_classifier", {}).get("val_roc_auc"),
        "spark_dataset_label": spark_stats.get("dataset_label"),
        "gold_rows": spark_stats.get("gold", {}).get("rows"),
    }
    print("\n=== Post-Colab Artifact Verification ===")
    print(json.dumps(summary, indent=2))

    if args.run_tests:
        subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v"], cwd=str(ROOT), check=True)

    print("\nNext steps:")
    print("  docker compose up --build -d backend ml-service")
    print("  ./scripts/smoke.sh")


if __name__ == "__main__":
    main()
