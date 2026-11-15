"""Data profiling of the raw Home Credit files with Spark (Phase 1).

Measures, per file: size, rows, columns, declared types and cast failures, missing rate per column,
exact duplicates, primary-key uniqueness, numeric distributions (min/max/mean/p50/p99, negatives,
365243 sentinels), top categorical values, TARGET balance, values AFTER the application date, and -
for every expected relationship - orphan rows, parent coverage and the observed cardinality.
Nothing is joined because two columns share a name: every relation in common/schemas.py is
*tested* here and its measured cardinality is written next to it in the ERD.

    python -m processing.profiling.profile_raw --raw-dir data/raw
Outputs: reports/data_profiling/profile.json, PROFILING_REPORT.md, reports/architecture/ERD.md
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, functions as F

from common.config import load_config, path_of
from common.logging_utils import get_logger
from common.schemas import DOUBLE, INT, LONG, STRING, TABLES, TableSpec
from common.spark import get_spark

log = get_logger("profiling")
NUMERIC = {LONG, INT, DOUBLE}


def read_raw(spark, path: Path) -> DataFrame:
    # every column as string first: profiling must see the file as it is, not as Spark guesses it
    return spark.read.option("header", True).option("inferSchema", False).option("mode", "PERMISSIVE").csv(str(path))


def typed(df: DataFrame, spec: TableSpec) -> tuple[DataFrame, dict]:
    """Cast to the registry types. Returns (typed df, {col: cast-failure count expr alias})."""
    exprs, fail_exprs = [], []
    for c in df.columns:
        t = spec.type_of(c)
        raw = F.col(f"`{c}`")
        if t == STRING:
            exprs.append(F.when(F.trim(raw) == "", None).otherwise(raw).alias(c))
            continue
        casted = raw.cast(t)
        exprs.append(casted.alias(c))
        fail_exprs.append(F.sum(F.when(raw.isNotNull() & (F.trim(raw) != "") & casted.isNull(), 1).otherwise(0)).alias(c))
    return df.select(*exprs), fail_exprs


def profile_table(spark, spec: TableSpec, path: Path, descriptions: dict) -> tuple[dict, DataFrame]:
    t0 = time.time()
    raw = read_raw(spark, path)
    df, fail_exprs = typed(raw, spec)
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    n = df.count()
    cols = df.columns

    expected_required = set(spec.required)
    header_report = {
        "missing_required_columns": sorted(expected_required - set(cols)),
        "unregistered_columns": sorted(set(cols) - set(spec.columns)) if spec.columns else [],
    }
    cast_fail = raw.agg(*fail_exprs).first().asDict() if fail_exprs else {}

    nulls = df.agg(*[F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c) for c in cols]).first().asDict()
    distinct_rows = df.distinct().count()

    pk = {}
    if spec.primary_key:
        k = list(spec.primary_key)
        null_pk = df.filter(" OR ".join(f"`{c}` IS NULL" for c in k)).count()
        distinct_pk = df.select(*k).distinct().count()
        pk = {"columns": k, "null_rows": null_pk, "distinct": distinct_pk, "duplicate_rows": n - distinct_pk,
              "is_unique": distinct_pk == n and null_pk == 0}
    biz = {}
    if spec.dedup_key:
        k = list(spec.dedup_key)
        biz = {"columns": k, "duplicate_rows": n - df.select(*k).distinct().count()}

    numeric_cols = [c for c in cols if spec.type_of(c) in NUMERIC and not c.startswith("SK_ID")]
    numeric: dict[str, dict] = {}
    if numeric_cols:
        aggs = []
        for c in numeric_cols:
            cc = F.col(f"`{c}`")
            aggs += [F.min(cc).alias(f"{c}__min"), F.max(cc).alias(f"{c}__max"), F.avg(cc).alias(f"{c}__mean"),
                     F.sum((cc < 0).cast("int")).alias(f"{c}__neg"),
                     F.sum((cc == 365243).cast("int")).alias(f"{c}__sentinel")]
        row = df.agg(*aggs).first().asDict()
        key_cols = [c for c in numeric_cols if c.startswith(("AMT_", "DAYS_", "MONTHS_", "SK_DPD", "CNT_"))][:30]
        qs = df.approxQuantile(key_cols, [0.5, 0.99], 0.01) if key_cols else []
        q_by_col = dict(zip(key_cols, qs))
        for c in numeric_cols:
            numeric[c] = {s: row[f"{c}__{s}"] for s in ("min", "max", "mean", "neg", "sentinel")}
            if c in q_by_col and q_by_col[c]:
                numeric[c]["p50"], numeric[c]["p99"] = q_by_col[c]

    categorical = {}
    for c in [c for c in cols if spec.type_of(c) == STRING][:40]:
        top = df.groupBy(f"`{c}`").count().orderBy(F.desc("count")).limit(6).collect()
        categorical[c] = {"distinct": df.select(f"`{c}`").distinct().count(),
                          "top": [[r[0], r[1]] for r in top]}

    after_app = {}
    for c in spec.point_in_time_cols:
        if c in cols:
            after_app[c] = df.filter(F.col(c) > 0).count()

    target = {}
    if "TARGET" in cols:
        dist = {str(r["TARGET"]): r["count"] for r in df.groupBy("TARGET").count().collect()}
        pos = dist.get("1", 0)
        target = {"distribution": dist, "positive_rate": pos / n if n else None,
                  "imbalance_ratio_neg_per_pos": (dist.get("0", 0) / pos) if pos else None}

    desc_key = "application_{train|test}.csv" if spec.name.startswith("application") else spec.file
    file_desc = descriptions.get(desc_key, {})
    columns = [{"name": c, "declared_type": spec.type_of(c), "missing": nulls[c],
                "missing_pct": round(100 * nulls[c] / n, 3) if n else 0.0,
                "cast_failures": cast_fail.get(c, 0), "description": file_desc.get(c, "")} for c in cols]

    result = {
        "table": spec.name, "file": spec.file, "source_id": spec.source_id, "origin": spec.origin, "grain": spec.grain,
        "size_bytes": path.stat().st_size, "rows": n, "columns": len(cols), "header": header_report,
        "exact_duplicate_rows": n - distinct_rows, "primary_key": pk, "business_key": biz,
        "columns_detail": columns, "numeric": numeric, "categorical": categorical,
        "rows_after_application_date": after_app, "target": target,
        "seconds": round(time.time() - t0, 2),
    }
    log.info("profiled %-24s rows=%-10d cols=%-4d dup=%-6d %.1fs", spec.name, n, len(cols), result["exact_duplicate_rows"],
             result["seconds"])
    return result, df


def profile_relationships(dfs: dict[str, DataFrame]) -> list[dict]:
    rels = []
    app_ids = None
    if "application_train" in dfs and "application_test" in dfs:
        app_ids = dfs["application_train"].select("SK_ID_CURR").union(dfs["application_test"].select("SK_ID_CURR")).distinct()
        overlap = dfs["application_train"].select("SK_ID_CURR").intersect(dfs["application_test"].select("SK_ID_CURR")).count()
        rels.append({"check": "train/test SK_ID_CURR overlap", "overlap_ids": overlap})
    for name, spec in TABLES.items():
        if name not in dfs:
            continue
        child = dfs[name]
        for fk in spec.foreign_keys:
            parent = app_ids if fk.parent == "application" else (dfs[fk.parent].select(*fk.parent_columns).distinct()
                                                                if fk.parent in dfs else None)
            if parent is None:
                continue
            ck, pkc = list(fk.columns), list(fk.parent_columns)
            keyed = child.filter(" AND ".join(f"{c} IS NOT NULL" for c in ck))
            per_parent = keyed.groupBy(*ck).count()
            joined = per_parent.join(parent.withColumnRenamed(pkc[0], "__p"), F.col(ck[0]) == F.col("__p"), "left")
            orphan = joined.filter(F.col("__p").isNull()).agg(F.count("*").alias("k"), F.sum("count").alias("r")).first()
            matched = joined.filter(F.col("__p").isNotNull())
            stats = matched.agg(F.count("*").alias("parents_with_children"), F.min("count").alias("min"),
                                F.avg("count").alias("avg"), F.max("count").alias("max")).first()
            parent_n = parent.count()
            rels.append({
                "child": name, "child_columns": ck, "parent": fk.parent, "parent_columns": pkc,
                "child_rows": keyed.count(), "orphan_keys": orphan["k"] or 0, "orphan_rows": int(orphan["r"] or 0),
                "parent_rows": parent_n, "parents_with_children": stats["parents_with_children"],
                "parent_coverage_pct": round(100 * stats["parents_with_children"] / parent_n, 2) if parent_n else None,
                "children_per_parent": {"min": stats["min"], "avg": round(stats["avg"] or 0, 2), "max": stats["max"]},
                "cardinality": "1:N" if (stats["max"] or 0) > 1 else "1:1",
            })
    # children of previous_application also carry SK_ID_CURR: is it the same customer as in previous_application?
    if "previous_application" in dfs:
        prev = dfs["previous_application"].select(F.col("SK_ID_PREV").alias("p_prev"), F.col("SK_ID_CURR").alias("p_curr"))
        for name in ("installments_payments", "POS_CASH_balance", "credit_card_balance"):
            if name in dfs:
                j = dfs[name].join(prev, dfs[name]["SK_ID_PREV"] == prev["p_prev"], "inner")
                rels.append({"check": f"{name}.SK_ID_CURR consistent with previous_application via SK_ID_PREV",
                             "matched_rows": j.count(),
                             "mismatched_rows": j.filter(F.col("SK_ID_CURR") != F.col("p_curr")).count()})
    return rels


def load_descriptions(raw_dir: Path) -> dict[str, dict[str, str]]:
    p = raw_dir / "HomeCredit_columns_description.csv"
    if not p.exists():
        return {}
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            d = pd.read_csv(p, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    out: dict[str, dict[str, str]] = {}
    for r in d.itertuples(index=False):
        out.setdefault(str(r.Table), {})[str(r.Row)] = str(r.Description)
    return out


def _fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:,.4g}" if abs(v) < 1e6 else f"{v:,.0f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def write_reports(profile: dict, out_dir: Path, erd_path: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "profile.json").write_text(json.dumps(profile, indent=2, default=str))
    L = [f"# Data Profiling Report", "",
         f"- Generated: {profile['generated_at']}",
         f"- Input: `{profile['raw_dir']}`",
         f"- Dataset label: **{profile['dataset_label']}**", ""]
    if profile["dataset_label"] != "HOME CREDIT (real Kaggle files)":
        L += ["> ⚠️ These numbers describe the dataset named above, **not** the real Home Credit data.", ""]
    L += ["## 1. Files", "", "| File | Logical source | Size (MB) | Rows | Cols | Exact dup rows | PK | PK unique |",
          "|---|---|---:|---:|---:|---:|---|---|"]
    for t in profile["tables"]:
        pk = t["primary_key"]
        L.append(f"| {t['file']} | {t['source_id']} | {t['size_bytes']/1e6:,.1f} | {t['rows']:,} | {t['columns']} | "
                 f"{t['exact_duplicate_rows']:,} | {','.join(pk.get('columns', [])) or '— (event table)'} | "
                 f"{'yes' if pk.get('is_unique') else ('no' if pk else '—')} |")
    tot_b = sum(t["size_bytes"] for t in profile["tables"])
    tot_r = sum(t["rows"] for t in profile["tables"])
    L += ["", f"Total: **{tot_b/1e6:,.1f} MB**, **{tot_r:,} rows** across {len(profile['tables'])} business files.", ""]
    if profile.get("reference_files"):
        L += ["Reference files (not business sources): " + ", ".join(f"`{k}` ({v} rows)" for k, v in profile["reference_files"].items()), ""]

    L += ["## 2. TARGET", ""]
    for t in profile["tables"]:
        if t["target"]:
            tg = t["target"]
            L.append(f"- `{t['table']}`: distribution {tg['distribution']}, positive rate "
                     f"**{100*tg['positive_rate']:.2f}%**, ≈ {tg['imbalance_ratio_neg_per_pos']:.1f} negatives per positive.")
    L += ["- `application_test` has no TARGET (expected).", ""]

    L += ["## 3. Relationships (verified on data)", "",
          "| Child.key → Parent.key | Child rows | Orphan rows | Orphan keys | Parent coverage | Children/parent (min/avg/max) | Observed |",
          "|---|---:|---:|---:|---:|---|---|"]
    for r in profile["relationships"]:
        if "child" in r:
            c = r["children_per_parent"]
            L.append(f"| {r['child']}.{','.join(r['child_columns'])} → {r['parent']}.{','.join(r['parent_columns'])} | "
                     f"{r['child_rows']:,} | {r['orphan_rows']:,} | {r['orphan_keys']:,} | {r['parent_coverage_pct']}% | "
                     f"{c['min']}/{c['avg']}/{c['max']} | {r['cardinality']} |")
    L.append("")
    for r in profile["relationships"]:
        if "check" in r:
            L.append(f"- {r['check']}: " + ", ".join(f"{k}={_fmt(v)}" for k, v in r.items() if k != "check"))
    L.append("")

    L += ["## 4. Per-table detail", ""]
    for t in profile["tables"]:
        L += [f"### {t['table']}  ({t['grain']})", ""]
        h = t["header"]
        if h["missing_required_columns"] or h["unregistered_columns"]:
            L.append(f"- Schema: missing required {h['missing_required_columns']}, unregistered {h['unregistered_columns']}")
        if t["business_key"]:
            L.append(f"- Business key {t['business_key']['columns']}: {t['business_key']['duplicate_rows']:,} rows share a key with another row")
        if t["rows_after_application_date"]:
            L.append(f"- Rows dated AFTER the current application (leakage risk): {t['rows_after_application_date']}")
        cf = {c['name']: c['cast_failures'] for c in t['columns_detail'] if c['cast_failures']}
        if cf:
            L.append(f"- Cast failures: {cf}")
        miss = sorted(t["columns_detail"], key=lambda c: -c["missing_pct"])
        miss = [c for c in miss if c["missing_pct"] > 0][:12]
        if miss:
            L += ["", "Top missing columns:", "", "| Column | Missing % | Description |", "|---|---:|---|"]
            L += [f"| {c['name']} | {c['missing_pct']:.2f} | {c['description'][:90]} |" for c in miss]
        anomalies = [(c, s) for c, s in t["numeric"].items() if (s["neg"] and c.startswith("AMT_")) or s["sentinel"]]
        key_num = [c for c in t["numeric"] if c.startswith(("AMT_", "DAYS_", "MONTHS_", "SK_DPD", "CNT_"))][:14]
        if key_num:
            L += ["", "| Numeric | min | p50 | p99 | max | mean | <0 | =365243 |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
            for c in key_num:
                s = t["numeric"][c]
                L.append(f"| {c} | {_fmt(s['min'])} | {_fmt(s.get('p50'))} | {_fmt(s.get('p99'))} | {_fmt(s['max'])} | "
                         f"{_fmt(s['mean'])} | {_fmt(s['neg'])} | {_fmt(s['sentinel'])} |")
        if anomalies:
            L.append("")
            L += [f"- Anomaly `{c}`: {s['neg']:,} negative, {s['sentinel']:,} equal to 365243" for c, s in anomalies]
        cats = [(c, v) for c, v in t["categorical"].items() if any(x[0] in ("XNA", "XAP") for x in v["top"])]
        if cats:
            L.append("- Placeholder categories: " + "; ".join(
                f"`{c}` " + ", ".join(f"{x[0]}={x[1]:,}" for x in v["top"] if x[0] in ("XNA", "XAP")) for c, v in cats))
        L.append("")
    (out_dir / "PROFILING_REPORT.md").write_text("\n".join(L), encoding="utf-8")
    write_erd(profile, erd_path)


def write_erd(profile: dict, path: Path) -> None:
    rel = {(r["child"], r["parent"], r["child_columns"][0]): r for r in profile["relationships"] if "child" in r}

    def lab(child, parent, col):
        r = rel.get((child, parent, col))
        if not r:
            return f"{col}"
        return f"{col} (orphans {r['orphan_rows']:,}, max {r['children_per_parent']['max']}/parent)"

    mer = ["erDiagram",
           "    APPLICATION ||--o{ BUREAU : \"" + lab("bureau", "application", "SK_ID_CURR") + "\"",
           "    BUREAU ||--o{ BUREAU_BALANCE : \"" + lab("bureau_balance", "bureau", "SK_ID_BUREAU") + "\"",
           "    APPLICATION ||--o{ PREVIOUS_APPLICATION : \"" + lab("previous_application", "application", "SK_ID_CURR") + "\"",
           "    PREVIOUS_APPLICATION ||--o{ INSTALLMENTS_PAYMENTS : \"" + lab("installments_payments", "previous_application", "SK_ID_PREV") + "\"",
           "    PREVIOUS_APPLICATION ||--o{ POS_CASH_BALANCE : \"" + lab("POS_CASH_balance", "previous_application", "SK_ID_PREV") + "\"",
           "    PREVIOUS_APPLICATION ||--o{ CREDIT_CARD_BALANCE : \"" + lab("credit_card_balance", "previous_application", "SK_ID_PREV") + "\"",
           "    APPLICATION ||--o| SYNTHETIC_TRAFFIC_PROFILE : \"SK_ID_CURR (SYNTHETIC)\"",
           "    APPLICATION { bigint SK_ID_CURR PK \"train + test\" int TARGET \"train only, label\" }",
           "    BUREAU { bigint SK_ID_BUREAU PK bigint SK_ID_CURR FK }",
           "    BUREAU_BALANCE { bigint SK_ID_BUREAU FK int MONTHS_BALANCE \"PK part\" string STATUS }",
           "    PREVIOUS_APPLICATION { bigint SK_ID_PREV PK bigint SK_ID_CURR FK }",
           "    INSTALLMENTS_PAYMENTS { bigint SK_ID_PREV FK bigint SK_ID_CURR \"FK, redundant\" int NUM_INSTALMENT_NUMBER }",
           "    POS_CASH_BALANCE { bigint SK_ID_PREV FK bigint SK_ID_CURR \"FK, redundant\" int MONTHS_BALANCE }",
           "    CREDIT_CARD_BALANCE { bigint SK_ID_PREV FK bigint SK_ID_CURR \"FK, redundant\" int MONTHS_BALANCE }",
           "    SYNTHETIC_TRAFFIC_PROFILE { bigint SK_ID_CURR PK \"FK to application\" }"]
    text = ["# ERD – Home Credit tables and logical sources", "",
            f"Measured on: **{profile['dataset_label']}** (`{profile['raw_dir']}`), {profile['generated_at']}.",
            "Edge labels carry the orphan count and the maximum number of children per parent observed in the data.", "",
            "```mermaid", *mer, "```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(text), encoding="utf-8")


def detect_label(raw_dir: Path) -> str:
    if (raw_dir / "FIXTURE_NOT_REAL_DATA.txt").exists():
        return "TEST FIXTURE (random values, NOT Home Credit)"
    if (raw_dir / "SUBSET_OF_REAL_DATA.txt").exists():
        return "SUBSET of real Home Credit files"
    return "HOME CREDIT (real Kaggle files)"


def run(raw_dir: Path, out_dir: Path | None = None) -> dict:
    cfg = load_config()
    out_dir = out_dir or path_of(cfg, "reports_dir") / "data_profiling"
    spark = get_spark("profiling")
    descriptions = load_descriptions(raw_dir)
    tables, dfs = [], {}
    for name, spec in TABLES.items():
        if spec.origin != "ORIGINAL":
            continue
        p = raw_dir / spec.file
        if not p.exists():
            log.warning("missing file %s - skipped", p)
            continue
        res, df = profile_table(spark, spec, p, descriptions)
        tables.append(res)
        dfs[name] = df
    refs = {}
    for f in ("sample_submission.csv",):
        if (raw_dir / f).exists():
            refs[f] = sum(1 for _ in open(raw_dir / f, encoding="utf-8")) - 1
    profile = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "raw_dir": str(raw_dir), "dataset_label": detect_label(raw_dir),
               "tables": tables, "relationships": profile_relationships(dfs), "reference_files": refs}
    write_reports(profile, out_dir, path_of(cfg, "reports_dir") / "architecture" / "ERD.md")
    for df in dfs.values():
        df.unpersist()
    return profile


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default=None)
    a = ap.parse_args()
    cfg = load_config()
    raw = Path(a.raw_dir) if a.raw_dir else path_of(cfg, "raw_dir")
    run(raw.resolve())
