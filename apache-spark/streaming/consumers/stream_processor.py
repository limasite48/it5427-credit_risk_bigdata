"""Spark Structured Streaming consumer (micro-batch).

    python -m streaming.consumers.stream_processor --source file --trigger available-now
    python -m streaming.consumers.stream_processor --source file --trigger 5 --timeout 120
    python -m streaming.consumers.stream_processor --source kafka --trigger 5      # needs docker compose + internet once

Two queries read the same input:
  Q1 quarantine : events that are not valid JSON, miss a required envelope field, come from an unknown
                  source, have an unparseable event_time or carry TARGET -> silver/stream_quarantine
  Q2 main       : valid events -> watermark(event_time) -> dropDuplicatesWithinWatermark(event_id)
                  -> foreachBatch:
                       silver/stream_events                 (partition source_id, batch_id)
                       gold/stream_customer_activity_delta  (per SK_ID_CURR per micro-batch)
                       gold/stream_batch_metrics            (counts + latency per micro-batch)
Exactly-once w.r.t. restarts: both queries keep checkpoints; foreachBatch output is written with dynamic
partition overwrite on `batch_id`, so a micro-batch re-executed after a crash replaces its own output.
Duplicates (same event_id) inside the watermark and events later than the watermark are dropped by the
state store and counted in the query progress (reports/streaming/query_progress.jsonl).
"""
from __future__ import annotations

import argparse
import json
import shutil
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.streaming import StreamingQueryListener

from common.config import load_config, path_of
from common.lake import spark_uri, write_table
from common.logging_utils import get_logger
from common.spark import get_spark, stop_spark
from streaming.schemas.event_schema import ENVELOPE_SCHEMA, EVENT_SOURCES, FORBIDDEN_PAYLOAD_FIELDS, REQUIRED_ENVELOPE

log = get_logger("stream")


class ProgressFileListener(StreamingQueryListener):
    """Appends every query progress (rows in, rate, duplicates dropped, late rows dropped) to a JSONL file."""

    def __init__(self, path):
        super().__init__()
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def onQueryStarted(self, event):
        log.info("query started %s", event.name)

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        rec = {"name": p.get("name"), "batchId": p.get("batchId"), "timestamp": p.get("timestamp"),
               "numInputRows": p.get("numInputRows"), "inputRowsPerSecond": p.get("inputRowsPerSecond"),
               "processedRowsPerSecond": p.get("processedRowsPerSecond"),
               "durationMs": p.get("durationMs", {}).get("triggerExecution"),
               "watermark": p.get("eventTime", {}).get("watermark"),
               "sinkOutputRows": (p.get("sink") or {}).get("numOutputRows"),
               "stateOperators": [{"operatorName": s.get("operatorName"), "numRowsTotal": s.get("numRowsTotal"),
                                   "numRowsUpdated": s.get("numRowsUpdated"),
                                   "numRowsDroppedByWatermark": s.get("numRowsDroppedByWatermark"),
                                   "numDroppedDuplicateRows": (s.get("customMetrics") or {}).get("numDroppedDuplicateRows")}
                                  for s in p.get("stateOperators", [])]}
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.info("query terminated %s exception=%s", event.id, event.exception)


def read_input(spark, kind: str, cfg: dict) -> DataFrame:
    if kind == "kafka":
        topics = ",".join(s.topic for s in EVENT_SOURCES.values())
        return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
                .option("subscribe", topics).option("startingOffsets", "earliest").option("failOnDataLoss", "false")
                .load().select(F.col("value").cast("string").alias("value")))
    landing = path_of(cfg, "stream_landing_dir")
    landing.mkdir(parents=True, exist_ok=True)
    return (spark.readStream.format("text")
            .option("maxFilesPerTrigger", cfg["streaming"]["files_per_trigger"]).load(str(landing)))


def parse(raw: DataFrame) -> DataFrame:
    known = list(EVENT_SOURCES)
    p = raw.withColumn("e", F.from_json("value", ENVELOPE_SCHEMA, {"mode": "PERMISSIVE"}))
    missing = [F.when(F.col(f"e.{c}").isNull(), F.lit(f"missing:{c}")) for c in REQUIRED_ENVELOPE]
    reason = F.concat_ws(",",
                         F.when(F.col("e").isNull(), F.lit("invalid_json")),
                         *missing,
                         F.when(F.col("e.source_id").isNotNull() & ~F.col("e.source_id").isin(known), F.lit("unknown_source")),
                         F.when(F.col("e.event_time").isNotNull() & F.to_timestamp("e.event_time").isNull(), F.lit("bad_event_time")),
                         *[F.when(F.col("e.payload").getItem(f).isNotNull(), F.lit(f"forbidden_field:{f}"))
                           for f in FORBIDDEN_PAYLOAD_FIELDS])
    return (p.withColumn("reject_reason", F.when(reason == "", None).otherwise(reason))
            .withColumn("event_ts", F.to_timestamp("e.event_time"))
            .withColumn("ingestion_ts", F.to_timestamp("e.ingestion_time")))


def _num(name: str):
    return F.col("payload").getItem(name).cast("double")


def process_batch(df: DataFrame, batch_id: int, cfg: dict, sink_counts: list | None = None) -> None:
    t0 = time.time()
    df = df.persist()
    n = df.count()
    if sink_counts is not None:
        sink_counts.append(n)
    if n == 0:
        df.unpersist()
        return
    now = datetime.now(timezone.utc)
    ev = (df.select("e.*", "event_ts", "ingestion_ts")
          .withColumn("processing_ts", F.lit(now).cast("timestamp"))
          .withColumn("event_date", F.to_date("event_ts"))
          .withColumn("batch_id", F.lit(batch_id)))
    write_table(ev.drop("event_time", "ingestion_time"), "silver", "stream_events", partition_by=["source_id", "batch_id"],
                num_files=1, cfg=cfg)

    is_ = lambda s: F.col("source_id") == s  # noqa: E731
    due, paid = _num("DAYS_INSTALMENT"), _num("DAYS_ENTRY_PAYMENT")
    delta = ev.groupBy("sk_id_curr").agg(
        F.count(F.lit(1)).alias("events_total"),
        F.sum(is_("loan_application_system").cast("int")).alias("application_events"),
        F.sum(is_("external_credit_bureau").cast("int")).alias("bureau_events"),
        F.sum(is_("installment_payment_system").cast("int")).alias("installment_events"),
        F.sum(is_("credit_card_system").cast("int")).alias("credit_card_events"),
        F.sum(is_("pos_cash_system").cast("int")).alias("pos_cash_events"),
        F.sum(is_("synthetic_customer_behavior").cast("int")).alias("synthetic_events"),
        F.sum(F.when(is_("installment_payment_system"), _num("AMT_INSTALMENT"))).alias("installment_amount_due"),
        F.sum(F.when(is_("installment_payment_system"), _num("AMT_PAYMENT"))).alias("installment_amount_paid"),
        F.sum(F.when(is_("installment_payment_system") & (paid > due), 1).otherwise(0)).alias("late_installment_events"),
        F.max(F.when(is_("pos_cash_system"), _num("SK_DPD"))).alias("max_pos_dpd"),
        F.max(F.when(is_("credit_card_system"), _num("SK_DPD"))).alias("max_credit_card_dpd"),
        F.sum(F.when(is_("external_credit_bureau"), _num("AMT_CREDIT_SUM_DEBT"))).alias("bureau_debt_reported"),
        F.max("event_ts").alias("last_event_time"),
    ).withColumn("batch_id", F.lit(batch_id))
    write_table(delta, "gold", "stream_customer_activity_delta", partition_by=["batch_id"], num_files=1, cfg=cfg)

    lat = ev.agg(F.avg(F.col("processing_ts").cast("double") - F.col("ingestion_ts").cast("double")).alias("avg_latency_s"),
                 F.max(F.col("processing_ts").cast("double") - F.col("ingestion_ts").cast("double")).alias("max_latency_s"),
                 F.avg(F.col("processing_ts").cast("double") - F.col("event_ts").cast("double")).alias("avg_event_lag_s")).first()
    by_src = {r["source_id"]: r["count"] for r in ev.groupBy("source_id").count().collect()}
    metrics = {"batch_id": batch_id, "processed_at": now.isoformat(timespec="seconds"), "events": n, "by_source": by_src,
               "customers_touched": delta.count(), "avg_latency_s": lat["avg_latency_s"],
               "max_latency_s": lat["max_latency_s"], "avg_event_lag_s": lat["avg_event_lag_s"],
               "batch_seconds": round(time.time() - t0, 3)}
    spark = df.sparkSession
    m_df = spark.createDataFrame([(batch_id, metrics["processed_at"], n, json.dumps(by_src), metrics["customers_touched"],
                                   metrics["avg_latency_s"], metrics["max_latency_s"], metrics["batch_seconds"])],
                                 "batch_id long, processed_at string, events long, by_source string, customers_touched long, "
                                 "avg_latency_s double, max_latency_s double, batch_seconds double")
    write_table(m_df, "gold", "stream_batch_metrics", partition_by=["batch_id"], num_files=1, cfg=cfg)
    out = path_of(cfg, "reports_dir") / "streaming" / "batch_metrics.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "a", encoding="utf-8") as f:
        f.write(json.dumps(metrics, default=str) + "\n")
    log.info("batch %d: %d events %s, customers=%d, avg latency %.2fs", batch_id, n, by_src, metrics["customers_touched"],
             metrics["avg_latency_s"] or 0)
    df.unpersist()


def start(source: str = "file", trigger: str = "available-now", timeout: float | None = None,
          reset: bool = False) -> dict:
    cfg = load_config()
    spark = get_spark("stream-processor", need_kafka=(source == "kafka"))
    spark.sparkContext.setLogLevel("ERROR")    # state-store reload messages are expected on restart
    ckpt = path_of(cfg, "checkpoint_dir")
    if reset:
        shutil.rmtree(ckpt / "stream_main", ignore_errors=True)
        shutil.rmtree(ckpt / "stream_quarantine", ignore_errors=True)
    listener = ProgressFileListener(path_of(cfg, "reports_dir") / "streaming" / "query_progress.jsonl")
    spark.streams.addListener(listener)
    processed: list[int] = []

    quarantine_uri = spark_uri("silver", "stream_quarantine", cfg=cfg)

    def quarantine_count() -> int:
        try:
            return spark.read.format(cfg["lake"]["format"]).load(quarantine_uri).count()
        except Exception:  # noqa: BLE001 - table does not exist yet
            return 0

    q_before = quarantine_count()
    parsed = parse(read_input(spark, source, cfg))
    trig = {"availableNow": True} if trigger == "available-now" else {"processingTime": f"{int(trigger)} seconds"}

    q_bad = (parsed.filter(F.col("reject_reason").isNotNull())
             .select("value", "reject_reason", F.current_timestamp().alias("received_at"))
             .writeStream.queryName("stream_quarantine").format(cfg["lake"]["format"]).outputMode("append")
             .option("checkpointLocation", str(ckpt / "stream_quarantine"))
             .trigger(**trig).start(quarantine_uri))

    valid = (parsed.filter(F.col("reject_reason").isNull())
             .withColumn("dedup_event_id", F.col("e.event_id"))
             .withWatermark("event_ts", cfg["streaming"]["watermark"])
             .dropDuplicatesWithinWatermark(["dedup_event_id"]))
    q_main = (valid.writeStream.queryName("stream_main")
              .foreachBatch(lambda d, b: process_batch(d, b, cfg, processed))
              .option("checkpointLocation", str(ckpt / "stream_main"))
              .trigger(**trig).start())

    t0 = time.time()
    if trigger == "available-now":
        q_main.awaitTermination()
        q_bad.awaitTermination()
    else:
        try:
            while time.time() - t0 < (timeout or 1e12) and q_main.isActive:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Ctrl-C: stopping queries (state is safe in the checkpoint)")
        q_main.stop()
        q_bad.stop()
    progress = [p for p in q_main.recentProgress if p.get("numInputRows")]
    ops = [s for p in progress for s in p.get("stateOperators", [])]
    input_rows = sum(p.get("numInputRows", 0) for p in progress)
    quarantined = quarantine_count() - q_before
    summary = {"micro_batches": len(progress), "input_rows": input_rows, "quarantined_rows": quarantined,
               "valid_rows": input_rows - quarantined, "events_written": sum(processed),
               "duplicates_dropped": sum((s.get("customMetrics") or {}).get("numDroppedDuplicateRows", 0) or 0 for s in ops),
               "late_rows_dropped": sum(s.get("numRowsDroppedByWatermark", 0) or 0 for s in ops),
               "seconds": round(time.time() - t0, 2)}
    summary["dropped_total"] = summary["valid_rows"] - summary["events_written"]
    summary["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summary["source"] = source
    with open(path_of(cfg, "reports_dir") / "streaming" / "stream_runs.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")
    log.info("stream summary %s", summary)
    spark.streams.removeListener(listener)
    stop_spark()
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["file", "kafka"], default=load_config()["streaming"]["sink"])
    ap.add_argument("--trigger", default="available-now", help="'available-now' or a processing-time interval in seconds")
    ap.add_argument("--timeout", type=float, default=None, help="stop a continuous run after N seconds")
    ap.add_argument("--reset-checkpoint", action="store_true")
    a = ap.parse_args()
    print(json.dumps(start(a.source, a.trigger, a.timeout, a.reset_checkpoint), indent=2))


if __name__ == "__main__":
    main()
