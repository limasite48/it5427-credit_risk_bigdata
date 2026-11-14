"""Bronze -> Silver: schema validation, typing, cleaning, de-duplication, integrity flags (Spark DataFrame API).

    python -m processing.cleaning.clean_silver                 # every table in SILVER_ORDER
    python -m processing.cleaning.clean_silver --tables bureau bureau_balance
Rules: processing/cleaning/rules.py (generated doc: reports/data_quality/CLEANING_RULES.md).
Metrics: reports/data_quality/silver_metrics.json (consumed by the DQ report).
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window, functions as F

from common.config import load_config, path_of
from common.lake import LakeStore, files_for_size, read_table, write_table
from common.logging_utils import get_logger
from common.schemas import SILVER_ORDER, STRING, TABLES, TableSpec, silver_name
from common.spark import get_spark
from ingestion.base import latest_bronze_uri, latest_success
from processing.cleaning import rules

log = get_logger("silver")
SENTINEL = 365243


def _cast_columns(raw: DataFrame, spec: TableSpec) -> tuple[DataFrame, list]:
    exprs, fail_exprs = [], []
    xna = set(rules.XNA_TO_NULL.get(spec.name, ()))
    yn = set(rules.YN_TO_INT.get(spec.name, ()))
    for c in raw.columns:
        src = F.col(f"`{c}`")
        s = F.when(F.trim(src) == "", None).otherwise(F.regexp_replace(F.trim(src), r"\s+", " "))
        t = spec.type_of(c)
        if c in yn:
            e = F.when(s == "Y", 1).when(s == "N", 0).cast("int")
            fail_exprs.append(F.sum(F.when(s.isNotNull() & e.isNull(), 1).otherwise(0)).alias(c))
        elif t == STRING:
            e = F.when(s == "XNA", None).otherwise(s) if c in xna else s
        elif t == "timestamp":
            e = F.to_timestamp(s)
            fail_exprs.append(F.sum(F.when(s.isNotNull() & e.isNull(), 1).otherwise(0)).alias(c))
        else:
            e = s.cast(t)
            fail_exprs.append(F.sum(F.when(s.isNotNull() & e.isNull(), 1).otherwise(0)).alias(c))
        exprs.append(e.alias(silver_name(c)))
    return raw.select(*exprs), fail_exprs


def _parent_keys(spark, parent: str, col: str, cfg: dict) -> DataFrame:
    if parent == "application":
        parts = [read_table(spark, "silver", t, cfg).select(F.col("sk_id_curr").alias("__k"))
                 for t in ("application_train", "application_test")]
        return reduce(DataFrame.unionByName, parts).distinct()
    return read_table(spark, "silver", parent, cfg).select(F.col(silver_name(col)).alias("__k")).distinct()


def clean_table(spark, spec: TableSpec, cfg: dict, store: LakeStore) -> dict:
    t0 = time.time()
    manifest = latest_success(store, spec.name)
    uri = latest_bronze_uri(spec.name, cfg)
    raw = spark.read.option("header", True).option("inferSchema", False).csv(uri)
    n_in = raw.count()
    original_cols = raw.columns
    m: dict = {"table": spec.name, "source_id": spec.source_id, "origin": spec.origin, "bronze_uri": uri,
               "ingest_id": manifest["ingest_id"], "rows_in": n_in, "columns_in": len(original_cols)}

    # A. schema validation ------------------------------------------------------------
    missing_required = [c for c in spec.required if c not in original_cols]
    if missing_required:
        raise ValueError(f"{spec.name}: required columns missing {missing_required}")
    m["schema"] = {"unregistered_columns": sorted(set(original_cols) - set(spec.columns)) if spec.columns else [],
                   "registered_but_absent": sorted(set(spec.columns) - set(original_cols)) if spec.columns else []}
    raw_missing = raw.agg(*[F.sum((F.col(f"`{c}`").isNull() | (F.trim(F.col(f"`{c}`")) == "")).cast("int")).alias(c)
                            for c in original_cols]).first().asDict()

    df, fail_exprs = _cast_columns(raw, spec)
    m["cast_failures"] = {}
    if fail_exprs:
        fails = raw.agg(*fail_exprs).first().asDict()
        m["cast_failures"] = {k: v for k, v in fails.items() if v}
    base_cols = [silver_name(c) for c in original_cols]

    # sentinel 365243 -> NULL + flag
    for c in spec.sentinel_cols:
        s = silver_name(c)
        if s in df.columns:
            df = df.withColumn(f"{s}_is_sentinel", F.coalesce(F.col(s) == SENTINEL, F.lit(False)))
            df = df.withColumn(s, F.when(F.col(s) == SENTINEL, None).otherwise(F.col(s)))

    # required NULL -> quarantine
    req = [silver_name(c) for c in spec.mandatory_values]
    reason = F.concat_ws(",", *[F.when(F.col(c).isNull(), F.lit(f"required_null:{c}")) for c in req])
    df = df.withColumn("_reject_reason", F.when(reason == "", None).otherwise(reason))
    quarantine = [df.filter(F.col("_reject_reason").isNotNull())]
    valid = df.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    n_valid = valid.count()
    m["quarantined_required_null"] = n_in - n_valid

    # C. duplicates ----------------------------------------------------------------------
    valid = valid.dropDuplicates(base_cols).persist(StorageLevel.MEMORY_AND_DISK)
    n_dedup = valid.count()
    m["exact_duplicates_removed"] = n_valid - n_dedup
    m["pk_duplicates_quarantined"] = 0
    if spec.primary_key:
        pk = [silver_name(c) for c in spec.primary_key]
        nonnull = reduce(lambda a, b: a + b, [F.col(c).isNotNull().cast("int") for c in base_cols])
        w = Window.partitionBy(*pk).orderBy(F.desc("__nn"), F.col("__h"))
        ranked = (valid.withColumn("__nn", nonnull)
                  .withColumn("__h", F.xxhash64(*[F.col(c) for c in base_cols]))
                  .withColumn("__rn", F.row_number().over(w)))
        dup = ranked.filter("__rn > 1").drop("__nn", "__h", "__rn").withColumn("_reject_reason", F.lit("pk_duplicate"))
        m["pk_duplicates_quarantined"] = dup.count()
        if m["pk_duplicates_quarantined"]:
            quarantine.append(dup)
        valid = ranked.filter("__rn = 1").drop("__nn", "__h", "__rn")
    if spec.dedup_key:
        bk = [silver_name(c) for c in spec.dedup_key]
        valid = valid.withColumn("_dup_business_key", F.count(F.lit(1)).over(Window.partitionBy(*bk)) > 1)

    # D. validity / outliers (flag only) -----------------------------------------------
    neg = [silver_name(c) for c in spec.nonnegative_cols if silver_name(c) in valid.columns]
    valid = valid.withColumn("_invalid_negative", reduce(lambda a, b: a | b, [F.coalesce(F.col(c) < 0, F.lit(False)) for c in neg])
                             if neg else F.lit(False))
    thresholds = {}
    out_cols = [silver_name(c) for c in rules.OUTLIER_COLS.get(spec.name, ()) if silver_name(c) in valid.columns]
    if out_cols:
        q = cfg["quality"]["income_outlier_quantile"]
        for c, v in zip(out_cols, valid.approxQuantile(out_cols, [q], 0.0005)):
            thresholds[c] = v[0] if v else None
            valid = valid.withColumn(f"{c}_outlier", F.coalesce(F.col(c) >= F.lit(v[0] if v else None), F.lit(False)))
    pit = [silver_name(c) for c in spec.point_in_time_cols if silver_name(c) in valid.columns]
    valid = valid.withColumn("_after_application", reduce(lambda a, b: a | b, [F.coalesce(F.col(c) > 0, F.lit(False)) for c in pit])
                             if pit else F.lit(False))

    # E. referential integrity (flag only) -------------------------------------------
    orphan_cols = []
    for fk in spec.foreign_keys:
        child_col = silver_name(fk.columns[0])
        flag = f"_orphan_{fk.parent}" if fk.parent != "application" else "_orphan_application"
        keys = _parent_keys(spark, fk.parent, fk.parent_columns[0], cfg)
        valid = (valid.join(F.broadcast(keys) if fk.parent.startswith("application") else keys,
                            F.col(child_col) == F.col("__k"), "left")
                 .withColumn(flag, F.col("__k").isNull()).drop("__k"))
        orphan_cols.append(flag)

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    valid = (valid.withColumn("_source_id", F.lit(spec.source_id)).withColumn("_origin", F.lit(spec.origin))
             .withColumn("_ingest_id", F.lit(manifest["ingest_id"]))
             .withColumn("_ingested_at", F.to_timestamp(F.lit(manifest["started_at"])))
             .withColumn("_silver_processed_at", F.to_timestamp(F.lit(now))))

    # F. metrics + write -----------------------------------------------------------------
    flag_cols = (["_invalid_negative", "_after_application"] + orphan_cols + [f"{c}_outlier" for c in out_cols]
                 + (["_dup_business_key"] if spec.dedup_key else [])
                 + [f"{silver_name(c)}_is_sentinel" for c in spec.sentinel_cols])
    agg = valid.agg(F.count(F.lit(1)).alias("__n"), *[F.sum(F.col(c).cast("int")).alias(c) for c in flag_cols],
                    *[F.sum(F.col(c).isNull().cast("int")).alias(f"null::{c}") for c in base_cols]).first().asDict()
    n_out = agg["__n"]
    m["rows_out"] = n_out
    m["flags"] = {c: agg[c] or 0 for c in flag_cols}
    m["outlier_thresholds"] = thresholds
    m["missing"] = {c: {"before": raw_missing[orig], "after": agg[f"null::{silver_name(orig)}"],
                        "before_pct": round(100 * raw_missing[orig] / n_in, 3) if n_in else 0,
                        "after_pct": round(100 * agg[f"null::{silver_name(orig)}"] / n_out, 3) if n_out else 0}
                    for orig in original_cols for c in [silver_name(orig)]}
    if spec.name.startswith("application"):
        m["semantic_checks"] = {
            "own_car_age_null_while_owns_car": valid.filter("flag_own_car = 1 AND own_car_age IS NULL").count()
            if "own_car_age" in valid.columns else None,
            "organization_xna_vs_days_employed_sentinel": valid.filter(
                "organization_type = 'XNA' AND NOT days_employed_is_sentinel").count()
            if "organization_type" in valid.columns else None,
        }

    n_files = files_for_size(manifest["size_bytes"] // 3, cfg)
    m["silver_uri"] = write_table(valid, "silver", spec.name, num_files=n_files, cfg=cfg)
    q_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), quarantine)
    m["rows_quarantined"] = m["quarantined_required_null"] + m["pk_duplicates_quarantined"]
    write_table(q_all.withColumn("_quarantined_at", F.lit(now)), "silver", f"_quarantine/{spec.name}", num_files=1, cfg=cfg)
    valid.unpersist()
    m["seconds"] = round(time.time() - t0, 2)
    m["reconciliation_ok"] = (n_in == n_out + m["rows_quarantined"] + m["exact_duplicates_removed"])
    log.info("%-26s in=%-9d out=%-9d quarantined=%-5d exact_dups=%-5d reconcile=%s %.1fs", spec.name, n_in, n_out,
             m["rows_quarantined"], m["exact_duplicates_removed"], m["reconciliation_ok"], m["seconds"])
    return m


def write_rules_doc(cfg: dict) -> None:
    out = path_of(cfg, "reports_dir") / "data_quality" / "CLEANING_RULES.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    L = ["# Cleaning rules (Bronze → Silver)", "", "Generated from `processing/cleaning/rules.py`; the code applies exactly these rules.", ""]
    L += [f"{i}. {r}" for i, r in enumerate(rules.RULES_TEXT, 1)]
    L += ["", "## Missing values", "", "| Case | Meaning | Handling |", "|---|---|---|"]
    L += [f"| {a} | {b} | {c} |" for a, b, c in rules.MISSING_POLICY]
    L += ["", "## Per-table lists", "", f"- `XNA` → NULL: {rules.XNA_TO_NULL}", f"- Y/N → 1/0: {rules.YN_TO_INT}",
          f"- Outlier-flagged columns (quantile {cfg['quality']['income_outlier_quantile']}): {rules.OUTLIER_COLS}", "",
          "## Keys used for duplicates", "", "| Table | Primary key (dedup) | Business key (flag only) |", "|---|---|---|"]
    L += [f"| {s.name} | {', '.join(s.primary_key) if s.primary_key else '—'} | {', '.join(s.dedup_key) if s.dedup_key else '—'} |"
          for s in TABLES.values()]
    out.write_text("\n".join(L) + "\n", encoding="utf-8")


def run(tables: list[str] | None = None) -> list[dict]:
    cfg = load_config()
    spark = get_spark("silver")
    store = LakeStore(cfg)
    write_rules_doc(cfg)
    metrics_path = path_of(cfg, "reports_dir") / "data_quality" / "silver_metrics.json"
    existing = {m["table"]: m for m in json.loads(metrics_path.read_text())} if metrics_path.exists() else {}
    results = []
    for name in SILVER_ORDER:
        if tables and name not in tables:
            continue
        if latest_success(store, name) is None:
            log.warning("%s not in bronze - skipped", name)
            continue
        m = clean_table(spark, TABLES[name], cfg, store)
        existing[name] = m
        results.append(m)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps([existing[t] for t in SILVER_ORDER if t in existing], indent=2, default=str))
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*")
    a = ap.parse_args()
    run(a.tables)
