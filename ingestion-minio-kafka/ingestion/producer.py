"""Stream: simulate the source systems publishing events to Kafka in event-time order.

Takes the staged events with event_time in [--start, --end), merges all tables by event_time
and publishes them at --speed (simulated seconds per real second). Key = customer_id, so all
events of one customer land in the same partition in order: history always precedes the application.

Examples:
  .venv/Scripts/python -I ingestion/producer.py                    # whole realtime window, 1 simulated hour/second
  .venv/Scripts/python -I ingestion/producer.py --start 2025-12-01 --end 2025-12-02 --speed 0
  .venv/Scripts/python -I ingestion/producer.py --dry-run out.jsonl  # write to a file, no Kafka needed
"""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import common as C  # noqa: E402


def read_window(table, start, end):
    return pd.read_parquet(C.STAGED_DIR / f"{table}.parquet",
                           filters=[("event_time", ">=", start), ("event_time", "<", end)])


def kafka_sender():
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "client.id": "hc-source-simulator",
                         "enable.idempotence": True, "acks": "all", "compression.type": "lz4", "linger.ms": 20})
    failed = []

    def on_delivery(err, msg):
        if err:
            failed.append(err)
            if len(failed) <= 5:
                print(f"  ! delivery failed to {msg.topic()}: {err}")

    def send(src, key, value):
        while True:
            try:
                producer.produce(src.topic, key=key, value=value.encode(), on_delivery=on_delivery,
                                 headers=[("event_type", src.event_type.encode()),
                                          ("schema_version", C.SCHEMA_VERSION.encode())])
                break
            except BufferError:  # local queue full -> wait for broker acknowledgements
                producer.poll(0.5)
        producer.poll(0)

    def close():
        producer.flush(60)
        return len(failed)

    return send, close


def file_sender(path):
    out = open(path, "w", encoding="utf-8")

    def send(src, key, value):
        out.write(value + "\n")

    def close():
        out.close()
        return 0

    return send, close


def main():
    ap = argparse.ArgumentParser(description="Simulated sources -> Kafka")
    ap.add_argument("--start", default=str(C.LIVE_START.date()), help="window start in simulated time (default LIVE_START)")
    ap.add_argument("--end", default="2025-12-29", help="window end (exclusive)")
    ap.add_argument("--speed", type=float, default=3600, help="simulated seconds per real second; 0 = as fast as possible")
    ap.add_argument("--tables", default=",".join(C.SOURCES), help="comma-separated list of tables")
    ap.add_argument("--limit", type=int, help="send only the first N events (for testing)")
    ap.add_argument("--dry-run", metavar="FILE", help="write JSON Lines to a file instead of Kafka")
    args = ap.parse_args()

    start, end = pd.Timestamp(args.start, tz=C.TZ), pd.Timestamp(args.end, tz=C.TZ)
    if start < C.LIVE_START:
        print(f"! Warning: events before {C.LIVE_START.date()} were already loaded into bronze by backfill "
              f"-> duplicates (dedupe by event_id).")

    sources = [C.SOURCES[t] for t in args.tables.split(",")]
    values, keys, order = [], [], []
    print(f"Preparing events {start:%Y-%m-%d %H:%M} .. {end:%Y-%m-%d %H:%M}:")
    for i, src in enumerate(sources):
        df = read_window(src.table, start, end)
        values.append(C.serialize_events(src, df))
        keys.append([str(k) for k in df.customer_id.tolist()])
        order.append(pd.DataFrame({"sec": (df.event_time - start).dt.total_seconds().to_numpy(),
                                   "src": i, "row": np.arange(len(df))}))
        print(f"  {src.topic:40s} {len(df):>10,}")
    order = pd.concat(order, ignore_index=True).sort_values(["sec", "src"], kind="stable")
    if args.limit:
        order = order.head(args.limit)
    print(f"Total {len(order):,} events -> {args.dry_run or C.KAFKA_BOOTSTRAP}, speed x{args.speed:g}")

    send, close = file_sender(args.dry_run) if args.dry_run else kafka_sender()
    t0 = last_log = time.monotonic()
    for n, (sec, i, row) in enumerate(zip(order.sec.to_numpy(), order.src.to_numpy(), order.row.to_numpy()), 1):
        if args.speed > 0:
            lag = sec / args.speed - (time.monotonic() - t0)
            if lag > 0.05:
                time.sleep(lag)
        produced_at = datetime.now(C.TZ).isoformat(timespec="milliseconds")
        send(sources[i], keys[i][row], values[i][row].replace(C.PRODUCED_AT_TOKEN, produced_at, 1))
        if time.monotonic() - last_log >= 5:
            last_log = time.monotonic()
            print(f"  simulated time {start + pd.Timedelta(seconds=float(sec)):%Y-%m-%d %H:%M} | sent {n:,}")
    failed = close()
    print(f"Done: {len(order):,} events in {time.monotonic() - t0:.1f}s, delivery errors: {failed}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
