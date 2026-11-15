"""Run the whole batch pipeline and write the statistics report.

    python -m scripts.run_pipeline                               # data/raw -> bronze -> silver -> gold
    python -m scripts.run_pipeline --raw-dir data/fixture/raw     # on the test fixture
    python -m scripts.run_pipeline --sync-minio --streaming-demo  # + upload to MinIO + replay/stream demo

Stages: profiling -> synthetic generation -> ingestion (bronze) -> cleaning (silver) -> integration (gold + DQ)
        [-> MinIO sync] [-> streaming demo]. Each stage is timed; a failed stage stops the run.
Outputs: reports/PIPELINE_STATS.md, reports/pipeline_stats.json
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from common.config import PROJECT_ROOT, load_config, path_of
from common.logging_utils import get_logger

log = get_logger("pipeline")


def _portable_path(p: str | Path) -> str:
    """Serialize paths relative to apache-spark/ or the repo root when possible."""
    path = Path(p)
    repo_root = PROJECT_ROOT.parent
    for base in (PROJECT_ROOT, repo_root):
        try:
            return path.resolve().relative_to(base.resolve()).as_posix()
        except ValueError:
            continue
    return str(p)


def _stage(name: str, fn, timings: dict):
    log.info(">>> stage %s", name)
    t0 = time.time()
    out = fn()
    timings[name] = round(time.time() - t0, 2)
    log.info("<<< stage %s done in %.1fs", name, timings[name])
    return out


def run(raw_dir: Path | None, skip_profiling: bool, sync_minio: bool, streaming_demo: bool, force_ingest: bool) -> dict:
    if raw_dir:
        os.environ["CRB_PATHS_RAW_DIR"] = str(raw_dir)
    cfg = load_config()
    raw_dir = path_of(cfg, "raw_dir")
    timings: dict[str, float] = {}
    stats: dict = {"started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "raw_dir": _portable_path(raw_dir)}

    from processing.profiling.profile_raw import detect_label
    stats["dataset_label"] = detect_label(raw_dir)

    if not skip_profiling:
        from processing.profiling import profile_raw
        _stage("profiling", lambda: profile_raw.run(raw_dir), timings)

    from ingestion.synthetic_source import generator
    stats["synthetic"] = _stage("synthetic_generation",
                                lambda: generator.generate(raw_dir, path_of(cfg, "synthetic_dir"), cfg["project"]["seed"]),
                                timings)["rows"]

    from ingestion import run_ingestion
    ing = _stage("ingestion_bronze", lambda: run_ingestion.run(raw_dir, force=force_ingest), timings)
    failed = [r for r in ing if not r.ok]
    if failed:
        raise RuntimeError(f"ingestion failed for {[r.table for r in failed]}")
    stats["ingestion"] = [{"table": r.table, "source_id": r.source_id, "status": r.status, "rows": r.rows,
                           "size_bytes": r.size_bytes, "seconds": r.seconds} for r in ing]

    from processing.cleaning import clean_silver
    _stage("cleaning_silver", clean_silver.run, timings)
    from processing.integration import build_gold
    gold = _stage("integration_gold", build_gold.run, timings)
    if isinstance(gold, dict) and isinstance(gold.get("outputs"), dict):
        gold["outputs"] = {k: _portable_path(v) for k, v in gold["outputs"].items()}
    stats["gold"] = gold

    if sync_minio:
        from storage.minio import sync_lake
        stats["minio_sync"] = _stage("minio_sync", sync_lake.run, timings)

    if streaming_demo:
        import shutil

        from common.spark import stop_spark
        stop_spark()
        shutil.rmtree(path_of(cfg, "stream_landing_dir"), ignore_errors=True)   # demo starts from an empty landing zone
        from streaming.consumers import snapshot, stream_processor
        from streaming.producers import replay_producer
        stats["producer"] = _stage("stream_producer", lambda: replay_producer.run(
            ["installment_payment_system", "credit_card_system", "pos_cash_system", "loan_application_system"],
            rate=0, max_events=2000, sink_kind="file", from_start=True, dup_rate=0.05, late_rate=0.02,
            malformed_rate=0.01), timings)
        stats["stream"] = _stage("stream_processing", lambda: stream_processor.start("file", "available-now",
                                                                                      reset=True), timings)
        stats["stream_snapshot"] = _stage("stream_snapshot", snapshot.build, timings)

    from scripts import generate_data_dictionary
    _stage("data_dictionary", generate_data_dictionary.main, timings)

    stats["timings_seconds"] = timings
    stats["silver"] = [{k: m[k] for k in ("table", "rows_in", "rows_out", "rows_quarantined", "exact_duplicates_removed")}
                       for m in (json.loads((path_of(cfg, "reports_dir") / "data_quality" / "silver_metrics.json").read_text()))]
    stats["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_stats(stats, cfg)
    return stats


def write_stats(stats: dict, cfg: dict) -> None:
    rep = path_of(cfg, "reports_dir")
    (rep / "pipeline_stats.json").write_text(json.dumps(stats, indent=2, default=str))
    manifests = path_of(cfg, "local_lake_dir") / "bronze" / "_manifests"
    ing_rows = []
    for f in sorted(manifests.glob("*.jsonl")):
        recs = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
        ok = [r for r in recs if r["status"].startswith("SUCCESS")]
        if ok:
            ing_rows.append(ok[-1])
    tot_b = sum(r["size_bytes"] for r in ing_rows)
    tot_r = sum(r["rows"] for r in ing_rows)
    t = stats["timings_seconds"]
    L = ["# Pipeline statistics", "", f"- Dataset: **{stats['dataset_label']}** (`{stats['raw_dir']}`)",
         f"- Run: {stats['started_at']} → {stats['finished_at']}", ""]
    if "real Kaggle" not in stats["dataset_label"]:
        L += ["> ⚠️ Figures below come from the dataset named above, not from the full Home Credit data.", ""]
    L += ["## Volume and ingestion speed (latest successful Bronze snapshot per table)", "",
          "| Table | Origin | Rows | Size (MB) | Ingest s | Rows/s | MB/s |", "|---|---|---:|---:|---:|---:|---:|"]
    for r in ing_rows:
        L.append(f"| {r['table']} | {r['origin']} | {r['rows']:,} | {r['size_bytes']/1e6:,.2f} | {r['seconds']} | "
                 f"{r.get('rows_per_sec') or ''} | {r.get('mb_per_sec') or ''} |")
    L += ["", f"Total: **{tot_r:,} rows, {tot_b/1e6:,.1f} MB**.", "",
          "## Processing time per stage", "", "| Stage | Seconds |", "|---|---:|"]
    L += [f"| {k} | {v} |" for k, v in t.items()]
    L += ["", f"Total: **{sum(t.values()):.1f} s**.", "", "## Rows before / after cleaning", "",
          "| Table | Bronze rows | Silver rows | Quarantined | Exact dups removed |", "|---|---:|---:|---:|---:|"]
    L += [f"| {s['table']} | {s['rows_in']:,} | {s['rows_out']:,} | {s['rows_quarantined']:,} | {s['exact_duplicates_removed']:,} |"
          for s in stats["silver"]]
    g = stats.get("gold", {})
    L += ["", "## Gold outputs", "", "| Table | Rows |", "|---|---:|"]
    L += [f"| {k} | {v:,} |" for k, v in g.get("rows", {}).items()]
    L += ["", f"Features after integration: **{g.get('feature_count')}** columns in `integrated_credit_profile` "
          f"({g.get('customer_feature_count')} aggregated customer features + application attributes + engineered).", ""]
    if "stream" in stats:
        s, p = stats["stream"], stats["producer"]
        L += ["## Streaming demo (file replay)", "",
              f"- Producer: {p['sent']:,} events in {p['seconds']} s ({p['events_per_sec']} ev/s); injected per source: "
              + "; ".join(f"{k}: dup {v['duplicates_injected']}, late {v['late_injected']}, malformed {v['malformed_injected']}"
                          for k, v in p["per_source"].items()),
              f"- Consumer: {s['micro_batches']} micro-batches, {s['input_rows']:,} input rows, {s['quarantined_rows']} quarantined, "
              f"{s['duplicates_dropped']} duplicates dropped, {s['late_rows_dropped']} late rows dropped, "
              f"{s['events_written']:,} events written, {s['seconds']} s.",
              f"- Snapshot: {stats['stream_snapshot']}", ""]
    if "minio_sync" in stats:
        L += ["## MinIO sync", ""] + [f"- {r['bucket']}: uploaded {r['uploaded']}, unchanged {r['skipped_unchanged']}, "
                                      f"{r['bytes_uploaded']/1e6:.1f} MB" for r in stats["minio_sync"]] + [""]
    (rep / "PIPELINE_STATS.md").write_text("\n".join(L), encoding="utf-8")
    log.info("stats written to %s", rep / "PIPELINE_STATS.md")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", type=Path)
    ap.add_argument("--skip-profiling", action="store_true")
    ap.add_argument("--sync-minio", action="store_true")
    ap.add_argument("--streaming-demo", action="store_true")
    ap.add_argument("--force-ingest", action="store_true")
    a = ap.parse_args()
    out = run(a.raw_dir.resolve() if a.raw_dir else None, a.skip_profiling, a.sync_minio, a.streaming_demo, a.force_ingest)
    print(json.dumps({k: out[k] for k in ("dataset_label", "timings_seconds")}, indent=2))
