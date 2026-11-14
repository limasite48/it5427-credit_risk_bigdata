"""Materialize the current streaming state per customer from the per-batch deltas.

gold/stream_customer_activity_delta holds one row per (SK_ID_CURR, batch_id). Counts and amounts are
additive, DPD maxima are max-able, so the current view is a simple re-aggregation:

    python -m streaming.consumers.snapshot          -> gold/stream_customer_snapshot + console summary
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from common.config import load_config
from common.lake import read_table, write_table
from common.spark import get_spark

SUM_COLS = ["events_total", "application_events", "bureau_events", "installment_events", "credit_card_events",
            "pos_cash_events", "synthetic_events", "installment_amount_due", "installment_amount_paid",
            "late_installment_events", "bureau_debt_reported"]
MAX_COLS = ["max_pos_dpd", "max_credit_card_dpd", "last_event_time"]


def build() -> dict:
    cfg = load_config()
    spark = get_spark("stream-snapshot")
    d = read_table(spark, "gold", "stream_customer_activity_delta", cfg)
    snap = d.groupBy("sk_id_curr").agg(*[F.sum(c).alias(c) for c in SUM_COLS], *[F.max(c).alias(c) for c in MAX_COLS],
                                       F.countDistinct("batch_id").alias("batches_seen"))
    snap = snap.withColumn("stream_late_payment_ratio",
                           F.when(F.col("installment_events") > 0, F.col("late_installment_events") / F.col("installment_events")))
    write_table(snap, "gold", "stream_customer_snapshot", num_files=1, cfg=cfg)
    snap = read_table(spark, "gold", "stream_customer_snapshot", cfg)
    out = {"customers": snap.count(), "events_total": snap.agg(F.sum("events_total")).first()[0],
           "batches": d.select("batch_id").distinct().count()}
    return out


if __name__ == "__main__":
    print(json.dumps(build(), indent=2, default=str))
