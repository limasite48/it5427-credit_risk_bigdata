"""Data-quality report: merges profiling, Silver metrics and Gold checks.

Outputs
* gold/data_quality_report         long-format table (run_at, layer, table, check, metric, value)
* docs/DATA_QUALITY_REPORT.md      human-readable report (regenerated on every run)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from common.config import load_config, path_of
from common.lake import write_table
from common.logging_utils import get_logger
from processing.profiling.profile_raw import detect_label

log = get_logger("dq")


def _load(path):
    return json.loads(path.read_text()) if path.exists() else None


def build_dq_report(spark, cfg=None) -> str:
    cfg = cfg or load_config()
    rep = path_of(cfg, "reports_dir")
    silver = _load(rep / "data_quality" / "silver_metrics.json") or []
    gold = _load(rep / "data_quality" / "gold_checks.json") or {"checks": [], "excluded_rows": {}, "stats": {}}
    raw_dir = path_of(cfg, "raw_dir")
    current_label = detect_label(raw_dir) if raw_dir.exists() else "unknown (raw directory missing)"
    profile = _load(rep / "data_profiling" / "profile.json")
    if profile and profile.get("dataset_label") != current_label:
        log.warning(
            "Ignoring stale profile.json (%r) because current raw dataset is %r",
            profile.get("dataset_label"),
            current_label,
        )
        profile = None
    run_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    label = profile["dataset_label"] if profile else current_label

    rows = []
    for m in silver:
        t = m["table"]
        for k in ("rows_in", "rows_out", "rows_quarantined", "exact_duplicates_removed", "pk_duplicates_quarantined",
                  "quarantined_required_null"):
            rows.append(("silver", t, "volume", k, float(m.get(k, 0))))
        rows.append(("silver", t, "reconciliation", "rows_in = out + quarantined + exact_dups", float(m["reconciliation_ok"])))
        for k, v in m["flags"].items():
            rows.append(("silver", t, "flag", k, float(v)))
        for k, v in m["cast_failures"].items():
            rows.append(("silver", t, "cast_failure", k, float(v)))
        for c, v in m["missing"].items():
            rows.append(("silver", t, "missing_pct_after", c, float(v["after_pct"])))
    for c in gold["checks"]:
        rows.append(("gold", "integration", "check", c["check"], float(c["passed"])))
    for t, e in gold["excluded_rows"].items():
        for k, v in e.items():
            rows.append(("gold", t, "excluded", k, float(v)))
    df = spark.createDataFrame([(run_at, label, *r) for r in rows],
                               "run_at string, dataset_label string, layer string, table string, check_type string, metric string, value double")
    write_table(df, "gold", "data_quality_report", num_files=1, cfg=cfg)

    L = ["# Data Quality Report", "",
         f"- Generated: {run_at}", f"- Dataset: **{label}**", ""]
    if profile and "real Kaggle" not in label:
        L += ["> ⚠️ Numbers below were produced on the dataset named above. Re-run `python -m scripts.run_pipeline` "
              "on the real Kaggle files to regenerate this report with real figures.", ""]
    L += ["## 1. Profiling summary (raw files)", ""]
    if profile:
        L += ["| File | Rows | Cols | Size MB | Exact dup rows | PK unique |", "|---|---:|---:|---:|---:|---|"]
        for t in profile["tables"]:
            pk = t["primary_key"]
            L.append(f"| {t['file']} | {t['rows']:,} | {t['columns']} | {t['size_bytes']/1e6:,.1f} | {t['exact_duplicate_rows']:,} | "
                     f"{'yes' if pk.get('is_unique') else ('no' if pk else '—')} |")
        L += ["", "Full detail: `reports/data_profiling/PROFILING_REPORT.md`.", ""]
    else:
        L += ["Profiling not run.", ""]

    L += ["## 2. Before / after cleaning (Bronze → Silver)", "",
          "| Table | Rows in | Exact dups removed | PK dups quarantined | Required-null quarantined | Rows out | Reconciles |",
          "|---|---:|---:|---:|---:|---:|---|"]
    for m in silver:
        L.append(f"| {m['table']} | {m['rows_in']:,} | {m['exact_duplicates_removed']:,} | {m['pk_duplicates_quarantined']:,} | "
                 f"{m['quarantined_required_null']:,} | {m['rows_out']:,} | {'✅' if m['reconciliation_ok'] else '❌'} |")
    L += ["", "Duplicate rate = exact duplicates removed / rows in.", ""]

    L += ["## 3. Flags (rows kept, flagged)", "",
          "| Table | Orphan rows | After application date | Negative values | Outliers | Business-key dups | 365243 sentinels |",
          "|---|---:|---:|---:|---:|---:|---:|"]
    for m in silver:
        f = m["flags"]
        sm = lambda pred: sum(v for k, v in f.items() if pred(k))  # noqa: E731
        L.append(f"| {m['table']} | {sm(lambda k: k.startswith('_orphan')):,} | {f.get('_after_application', 0):,} | "
                 f"{f.get('_invalid_negative', 0):,} | {sm(lambda k: k.endswith('_outlier')):,} | {f.get('_dup_business_key', 0):,} | "
                 f"{sm(lambda k: k.endswith('_is_sentinel')):,} |")
    L.append("")
    sem = [(m["table"], m["semantic_checks"]) for m in silver if m.get("semantic_checks")]
    if sem:
        L += ["Semantic missing-value checks: " + "; ".join(f"`{t}` {s}" for t, s in sem), ""]
    casts = [(m["table"], m["cast_failures"]) for m in silver if m["cast_failures"]]
    L += [f"Cast failures: {casts if casts else 'none'}", ""]

    L += ["## 4. Missing values (Silver, top 8 per table)", ""]
    for m in silver:
        top = sorted(m["missing"].items(), key=lambda kv: -kv[1]["after_pct"])[:8]
        top = [(c, v) for c, v in top if v["after_pct"] > 0]
        if top:
            L.append(f"- **{m['table']}**: " + ", ".join(
                f"{c} {v['after_pct']:.1f}%" + (f" (raw {v['before_pct']:.1f}%)" if abs(v['after_pct'] - v['before_pct']) > 0.05 else "")
                for c, v in top))
    L += ["", "Differences between raw and Silver missing rates come from documented conversions "
          "(365243 → NULL, `XNA` → NULL). No column was dropped.", ""]

    L += ["## 5. Gold integration checks", "", "| Check | Result | Details |", "|---|---|---|"]
    for c in gold["checks"]:
        det = {k: v for k, v in c.items() if k not in ("check", "passed", "excluded")}
        L.append(f"| {c['check']} | {'✅ pass' if c['passed'] else '❌ FAIL'} | {json.dumps(det)[:120]} |")
    L += ["", "Rows excluded before aggregation (leakage / integrity):", "",
          "| Table | Silver rows | Used | Excluded after-application | Excluded total |", "|---|---:|---:|---:|---:|"]
    for t, e in gold["excluded_rows"].items():
        L.append(f"| {t} | {e['rows_silver']:,} | {e['rows_used']:,} | {e['excluded_after_application']:,} | {e['excluded_total']:,} |")
    if gold.get("stats"):
        L += ["", f"Outputs: {gold['stats'].get('rows')}; features in integrated profile: {gold['stats'].get('feature_count')}."]
    out = path_of(cfg, "docs_dir") / "DATA_QUALITY_REPORT.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    log.info("DQ report written: %s (%d metric rows)", out, len(rows))
    return str(out)
