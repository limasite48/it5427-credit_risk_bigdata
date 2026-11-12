"""Sink Kafka -> data lake (bronze bucket): reads every source topic, validates the schema, writes Parquet in batches.

At-least-once: offsets are committed only after the Parquet file has been written; if the sink
dies mid-batch, that batch is read again and the duplicates are removed in silver by event_id.
Bad events (unparseable / schema violation) -> topic hc.dlq.v1 and the quarantine bucket.

Run (MinIO):  LAKE_MODE=minio .venv/Scripts/python -I ingestion/kafka_to_bronze.py
Quick test:   ... kafka_to_bronze.py --idle-exit 20   # stop after 20 s without new messages
"""
import argparse
import json
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402
from confluent_kafka import Consumer, Producer  # noqa: E402
from jsonschema import Draft202012Validator  # noqa: E402

import common as C  # noqa: E402

ENVELOPE = C.ENVELOPE_COLS + ["produced_at"]


def load_contracts():
    """topic -> (validator, {payload column: JSON type}) from the schemas/*.schema.json files."""
    contracts = {}
    for topic in C.SOURCE_BY_TOPIC:
        schema = json.loads((C.SCHEMA_DIR / f"{topic}.schema.json").read_text(encoding="utf-8"))
        types = {}
        for col, p in schema["properties"]["payload"]["properties"].items():
            t = p["type"]
            types[col] = next(x for x in t if x != "null") if isinstance(t, list) else t
        contracts[topic] = (Draft202012Validator(schema), types)
    return contracts


def to_frame(rows, types):
    """Cast to the schema types so stream files have the same column types as backfill files."""
    df = pd.DataFrame(rows)
    for col in ("event_time", "produced_at"):
        df[col] = pd.to_datetime(df[col], format="ISO8601").dt.tz_convert(C.TZ)
    df["customer_id"] = df.customer_id.astype("int64")
    for col, t in types.items():
        if col not in df:
            df[col] = None
        if t == "integer":
            df[col] = pd.to_numeric(df[col]).astype("Int64")
        elif t == "number":
            df[col] = pd.to_numeric(df[col]).astype("float64")
        else:
            df[col] = df[col].astype("string")
    df["kafka_partition"] = df.kafka_partition.astype("Int64")
    df["kafka_offset"] = df.kafka_offset.astype("Int64")
    return df[ENVELOPE + list(types) + ["kafka_topic", "kafka_partition", "kafka_offset"]]


def main():
    ap = argparse.ArgumentParser(description="Kafka -> bronze (Parquet)")
    ap.add_argument("--group", default="hc-bronze-sink")
    ap.add_argument("--batch-size", type=int, default=20_000, help="write a file once N events are buffered")
    ap.add_argument("--flush-seconds", type=float, default=15, help="or once N seconds have passed since the last write")
    ap.add_argument("--idle-exit", type=float, help="stop after N seconds without messages (for testing)")
    ap.add_argument("--no-validate", action="store_true", help="skip JSON Schema validation (faster)")
    args = ap.parse_args()

    contracts = load_contracts()
    consumer = Consumer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "group.id": args.group,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe(list(contracts))
    dlq = Producer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "client.id": "hc-bronze-sink-dlq"})
    buffers, bad = defaultdict(list), []
    stats = defaultdict(int)
    last_flush = last_msg = time.monotonic()

    def flush():
        now = pd.Timestamp.now(tz=C.TZ)
        part = f"ingest_date={now:%Y-%m-%d}/part-{now:%H%M%S}-{uuid.uuid4().hex[:8]}.parquet"
        for topic, rows in buffers.items():
            if rows:
                df = to_frame(rows, contracts[topic][1]).assign(ingest_time=now, ingest_mode="stream")
                C.write_parquet(df, "bronze", f"{C.SOURCE_BY_TOPIC[topic].lake_prefix}/{part}")
        if bad:
            C.write_parquet(pd.DataFrame(bad), "quarantine", f"stream/{part}")
            dlq.flush(30)
        consumer.commit(asynchronous=False)
        n = sum(map(len, buffers.values()))
        buffers.clear()
        bad.clear()
        return n

    print(f"Reading {len(contracts)} topics from {C.KAFKA_BOOTSTRAP} -> bronze ({C.LAKE_MODE})")
    try:
        while True:
            msg = consumer.poll(1.0)
            idle = time.monotonic() - last_msg
            if msg is not None and msg.error():
                print(f"  ! consumer error: {msg.error()}")
                msg = None
            if msg is not None:
                last_msg = time.monotonic()
                topic, raw = msg.topic(), msg.value()
                try:
                    event = json.loads(raw)
                    if not args.no_validate:
                        err = next(contracts[topic][0].iter_errors(event), None)
                        if err:
                            raise ValueError(f"{'/'.join(map(str, err.absolute_path)) or '<root>'}: {err.message}")
                    row = {k: event[k] for k in ENVELOPE}
                    row.update(event["payload"])
                    row.update(kafka_topic=topic, kafka_partition=msg.partition(), kafka_offset=msg.offset())
                    buffers[topic].append(row)
                    stats[topic] += 1
                except Exception as e:  # noqa: BLE001 - any data error goes to the DLQ instead of stopping the sink
                    record = {"original_topic": topic, "partition": msg.partition(), "offset": msg.offset(),
                              "error": str(e)[:500], "raw_value": raw.decode("utf-8", "replace"),
                              "failed_at": datetime.now(C.TZ).isoformat(timespec="milliseconds")}
                    bad.append(record)
                    dlq.produce(C.DLQ_TOPIC, key=msg.key(), value=json.dumps(record, ensure_ascii=False).encode())
                    stats["__dlq__"] += 1
            pending = sum(map(len, buffers.values())) + len(bad)
            if pending and (pending >= args.batch_size or time.monotonic() - last_flush >= args.flush_seconds):
                n = flush()
                last_flush = time.monotonic()
                print(f"  wrote {n:,} events | total: {sum(v for k, v in stats.items() if k != '__dlq__'):,} "
                      f"| DLQ: {stats['__dlq__']:,}")
            if args.idle_exit and idle >= args.idle_exit and not pending:
                break
    except KeyboardInterrupt:
        pass
    finally:
        if sum(map(len, buffers.values())) + len(bad):
            flush()
        consumer.close()
    print("Stopped. Per topic:", {k: v for k, v in stats.items()})


if __name__ == "__main__":
    main()
