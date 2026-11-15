#!/usr/bin/env python3
"""Unified end-to-end Credit Risk Intelligence pipeline runner (designed for Google Colab / high-RAM hosts).

Executes on the full 307,511-row Kaggle dataset in `_data/`:
  1. Generate supplementary regional macro + occupation salary benchmarks & run raw EDA.
  2. Run Apache Spark Medallion pipeline (profiling -> synthetic -> Bronze -> Silver -> Gold + streaming demo)
     and optionally verify Delta Lake read/write mode.
  3. Train the real-time interactive application model (`run_track_a.py`) on full `application_train.csv`.
  4. Train the full Gold Medallion Logistic Regression baseline + LightGBM + Cost Optimization + ROC + SHAP.
  5. Standardize unified dashboard artifacts into `outputs/` and optionally create a downloadable zip bundle.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _auto_driver_memory() -> str:
    """Choose a safe Spark JVM heap size (~55% of host RAM, clamped 4g..12g)."""
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("MemTotal:"):
                kb = int(line.split()[1])
                gb = kb / (1024 * 1024)
                return f"{max(4, min(12, int(gb * 0.55)))}g"
    return "6g"


def _ensure_java_home() -> str | None:
    """Resolve an existing JAVA_HOME (preferring Java 17, then Java 21) on Linux/Colab."""
    current = os.environ.get("JAVA_HOME")
    if current and (Path(current) / "bin" / "java").exists():
        return current
    for candidate in (
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk",
        "/usr/lib/jvm/java-21-openjdk-amd64",
        "/usr/lib/jvm/java-21-openjdk",
        "/usr/lib/jvm/default-java",
    ):
        if (Path(candidate) / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = candidate
            return candidate
    java_bin = shutil.which("java")
    if java_bin:
        inferred = str(Path(java_bin).resolve().parents[1])
        os.environ["JAVA_HOME"] = inferred
        return inferred
    return None


def _stage_csvs(src_dir: Path, dst_dir: Path) -> Path:
    """Copy raw CSVs once to fast local SSD (skips files that already match byte size)."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    for csv_file in sorted(src_dir.glob("*.csv")):
        target = dst_dir / csv_file.name
        if target.exists() and target.stat().st_size == csv_file.stat().st_size:
            continue
        print(f"Staging {csv_file.name} ({csv_file.stat().st_size / 1e6:.1f} MB) -> {dst_dir} ...")
        shutil.copy2(csv_file, target)
    return dst_dir


def _run(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    print(f"\n>>> Running ({cwd or ROOT}): {' '.join(cmd)}")
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    subprocess.run(cmd, cwd=str(cwd or ROOT), env=merged_env, check=True)


def export_bundle(zip_path: Path) -> None:
    """Package lightweight committed artifacts (`outputs/`, Spark reports/docs, supplementary CSVs)."""
    include_dirs = [
        ROOT / "outputs" / "figures",
        ROOT / "outputs" / "metrics",
        ROOT / "outputs" / "models",
        ROOT / "apache-spark" / "reports",
        ROOT / "apache-spark" / "docs",
        ROOT / "ingestion-minio-kafka" / "data" / "supplementary",
    ]
    include_files = [
        ROOT / "outputs" / "gold_demo_ids.json",
        ROOT / "outputs" / "gold_scored_applications.csv",
        ROOT / "outputs" / "scored_applications_sample.csv",
        ROOT / "outputs" / "scored_applications_sample_track_a.csv",
    ]
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for directory in include_dirs:
            if directory.exists():
                for file_path in sorted(directory.rglob("*")):
                    if file_path.is_file():
                        zf.write(file_path, file_path.relative_to(ROOT).as_posix())
        for file_path in include_files:
            if file_path.exists():
                zf.write(file_path, file_path.relative_to(ROOT).as_posix())
    print(f"\nArtifact bundle written: {zip_path} ({zip_path.stat().st_size / 1e6:.2f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full unified Credit Risk pipeline (Colab / high-RAM)")
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "_data", help="Directory containing raw Kaggle CSVs")
    parser.add_argument("--work-dir", type=Path, default=None, help="Fast local SSD directory for Spark lake/checkpoints (avoids Drive FUSE overhead)")
    parser.add_argument("--stage-raw", action="store_true", help="Copy raw CSVs into <work-dir>/_data before Spark runs")
    parser.add_argument("--fast", action="store_true", help="Skip duplicate raw EDA and Spark column profiling to finish much faster")
    parser.add_argument("--driver-memory", default="auto", help="Spark driver memory (default: auto ~55%% of RAM, or e.g. 6g/10g)")
    parser.add_argument("--shuffle-partitions", default="32", help="Spark SQL shuffle partitions")
    parser.add_argument("--skip-eda", action="store_true", help="Skip pandas raw EDA printout")
    parser.add_argument("--skip-profiling", action="store_true", help="Skip Spark raw profiling stage")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark Medallion batch+streaming stage")
    parser.add_argument(
        "--resume-if-gold-exists",
        action="store_true",
        help="Skip Spark stage when Gold parquet tables already exist in gold_dir (resumes fast after downstream failure)",
    )
    parser.add_argument("--verify-delta", action="store_true", help="Run Delta Lake smoke round-trip check")
    parser.add_argument("--skip-track-a", action="store_true", help="Skip full-dataset real-time model training")
    parser.add_argument("--bundle", type=Path, default=ROOT / "credit_risk_full_artifacts.zip", help="Output zip bundle path")
    args = parser.parse_args()

    java_home = _ensure_java_home()
    driver_memory = _auto_driver_memory() if args.driver_memory == "auto" else args.driver_memory
    print(f"Environment: JAVA_HOME={java_home or 'UNSET'} | driver_memory={driver_memory}")

    raw_dir = args.raw_dir.resolve()
    if not (raw_dir / "application_train.csv").exists():
        raise SystemExit(f"Raw Kaggle dataset not found at {raw_dir}")

    spark_dir = ROOT / "apache-spark"
    spark_env = {
        "CRB_SPARK_DRIVER_MEMORY": driver_memory,
        "CRB_SPARK_SHUFFLE_PARTITIONS": args.shuffle_partitions,
        "CRB_SPARK_MASTER": "local[*]",
    }
    if java_home:
        spark_env["JAVA_HOME"] = java_home

    if args.work_dir:
        work_dir = args.work_dir.resolve()
        work_dir.mkdir(parents=True, exist_ok=True)
        if args.stage_raw:
            raw_dir = _stage_csvs(raw_dir, work_dir / "_data")
        lake_dir = work_dir / "lake"
        spark_env.update({
            "CRB_PATHS_SYNTHETIC_DIR": str(work_dir / "synthetic"),
            "CRB_PATHS_LOCAL_LAKE_DIR": str(lake_dir),
            "CRB_PATHS_CHECKPOINT_DIR": str(work_dir / "_checkpoints"),
            "CRB_PATHS_STREAM_LANDING_DIR": str(work_dir / "_stream_landing"),
            "CRB_PATHS_STATE_DIR": str(work_dir / "_state"),
        })
        gold_dir = lake_dir / "gold"
    else:
        gold_dir = spark_dir / "data" / "gold"

    spark_env["CRB_PATHS_RAW_DIR"] = str(raw_dir)
    py = sys.executable

    # 1. Supplementary data (macro indicators + occupation salary benchmarks)
    _run([py, "ingestion-minio-kafka/ingestion/gen_macro.py"], env={"HC_RAW_DIR": str(raw_dir)})
    if not (args.skip_eda or args.fast):
        _run([py, "ingestion-minio-kafka/scripts/eda.py", str(raw_dir)])

    # 2. Apache Spark Medallion Pipeline + Streaming Demo (+ optional Delta check)
    gold_ready = (
        any((gold_dir / "integrated_credit_profile").glob("*.parquet"))
        and any((gold_dir / "training_labels").glob("*.parquet"))
        if (gold_dir / "integrated_credit_profile").exists() and (gold_dir / "training_labels").exists()
        else False
    )
    if args.skip_spark:
        print("Skipping Spark stage (--skip-spark).")
    elif args.resume_if_gold_exists and gold_ready:
        print(f"Existing Spark Gold tables found at {gold_dir} — resuming directly from Step 3.")
    else:
        spark_cmd = [py, "-m", "scripts.run_pipeline", "--raw-dir", str(raw_dir), "--streaming-demo", "--force-ingest"]
        if args.skip_profiling or args.fast:
            spark_cmd.append("--skip-profiling")
        _run(spark_cmd, cwd=spark_dir, env=spark_env)
    if args.verify_delta:
        _run([py, "-m", "pytest", "tests/test_06_delta_smoke.py", "-v"], cwd=spark_dir, env={**spark_env, "CRB_TEST_DELTA": "1"})

    # 3. Full-dataset Real-Time Application Scorer
    if not args.skip_track_a:
        _run([py, "run_track_a.py", "--include-macro"])

    # 4. Full-dataset Gold Medallion Model + ROC/PR + 5:1 Cost Matrix + Global SHAP
    _run([py, "scripts/train_gold_model.py", "--gold-dir", str(gold_dir)])

    # 5. Standardize unified dashboard artifacts
    _run([py, "scripts/standardize_dashboard.py", "--gold-dir", str(gold_dir)])

    # 6. Verify Python unit/integration tests pass against newly generated artifacts
    _run([py, "-m", "pytest", "tests/", "-v"])

    if args.bundle:
        export_bundle(args.bundle)


if __name__ == "__main__":
    main()
