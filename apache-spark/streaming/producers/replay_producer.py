"""Replay producer: turns historical Home Credit rows (latest Bronze snapshot) into a continuous event stream.

    python -m streaming.producers.replay_producer --sources installment_payment_system credit_card_system \
           --rate 100 --max-events 5000 --sink file
    python -m streaming.producers.control pause | resume | stop | status     # from another terminal

Features
* configurable rate (events/second, token bucket) and max events
* several sources interleaved round-robin, one topic (Kafka) / one file stream (file sink)
* resumable: the row offset per source is saved in data/_state/producer_offsets.json after each flush;
  `--from-start` replays from row 0 (event ids are deterministic, so the consumer deduplicates)
* pause / resume / stop through data/_state/producer.control
* optional fault injection to demonstrate consumer handling: --dup-rate, --late-rate, --malformed-rate
* TARGET is stripped from every payload (label leakage guard)
"""
from __future__ import annotations

import argparse
import itertools
import json
import random
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd

from common.config import load_config, path_of
from common.lake import LakeStore
from common.logging_utils import get_logger
from ingestion.base import latest_success
from streaming.producers.sinks import FileSink, KafkaSink
from streaming.schemas.event_schema import (EVENT_SOURCES, FORBIDDEN_PAYLOAD_FIELDS, SCHEMA_VERSION, TIME_SEMANTICS,
                                            EventSource)

log = get_logger("producer")
CHUNK = 50_000


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def bronze_file(table: str, cfg: dict) -> Path:
    store = LakeStore(cfg)
    rec = latest_success(store, table)
    if rec is None:
        raise FileNotFoundError(f"{table} not ingested yet - run ingestion first")
    if store.backend == "local":
        return store.local_path(rec["bronze_key"])
    tmp = Path(tempfile.mkdtemp()) / Path(rec["bronze_key"]).name      # s3a backend: download once
    bucket, rest = store._split(rec["bronze_key"])  # noqa: SLF001
    store._s3.download_file(bucket, rest, str(tmp))  # noqa: SLF001
    return tmp


def to_event(src: EventSource, row: dict, offset: int, now: datetime, delay_s: float) -> dict:
    payload = {k: v for k, v in row.items() if k not in FORBIDDEN_PAYLOAD_FIELDS and v != ""}

    def key(name):
        v = row.get(name, "")
        return int(float(v)) if v not in ("", None) else None

    return {
        "event_id": f"evt_{src.table.lower()}_{offset:09d}",
        "source_id": src.source_id, "event_type": src.event_type,
        "event_time": _iso(now - timedelta(seconds=delay_s)), "ingestion_time": _iso(now),
        "time_semantics": TIME_SEMANTICS,
        "sk_id_curr": key("SK_ID_CURR"), "sk_id_prev": key("SK_ID_PREV"), "sk_id_bureau": key("SK_ID_BUREAU"),
        "payload": payload, "schema_version": SCHEMA_VERSION,
    }


def rows_from(path: Path, start: int) -> Iterator[tuple[int, dict]]:
    offset = 0
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=CHUNK):
        if offset + len(chunk) <= start:
            offset += len(chunk)
            continue
        for rec in chunk.to_dict("records"):
            if offset >= start:
                yield offset, rec
            offset += 1


class Control:
    def __init__(self, state_dir: Path):
        self.path = state_dir / "producer.control"

    def state(self) -> str:
        return self.path.read_text().strip() if self.path.exists() else "RUNNING"

    def set(self, value: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(value)


def run(sources: list[str], rate: float, max_events: int | None, sink_kind: str, from_start: bool = False,
        dup_rate: float = 0.0, late_rate: float = 0.0, malformed_rate: float = 0.0, seed: int | None = None,
        landing_dir: Path | None = None) -> dict:
    cfg = load_config()
    state_dir = path_of(cfg, "state_dir")
    state_dir.mkdir(parents=True, exist_ok=True)
    offsets_path = state_dir / "producer_offsets.json"
    offsets = {} if from_start or not offsets_path.exists() else json.loads(offsets_path.read_text())
    rng = random.Random(seed if seed is not None else cfg["project"]["seed"])
    control = Control(state_dir)
    control.set("RUNNING")

    if sink_kind == "kafka":
        sink = KafkaSink(cfg["kafka"]["bootstrap_servers"])
    else:
        sink = FileSink(landing_dir or path_of(cfg, "stream_landing_dir"), cfg["streaming"]["events_per_file"])
    late_by = _parse_minutes(cfg["streaming"]["watermark"]) * 60 * 3        # far beyond the watermark

    iters = {}
    for s in sources:
        iters[s] = rows_from(bronze_file(EVENT_SOURCES[s].table, cfg), offsets.get(s, 0))
    stats = {s: {"sent": 0, "duplicates_injected": 0, "late_injected": 0, "malformed_injected": 0} for s in sources}
    sent, t_start = 0, time.time()
    interval = 1.0 / rate if rate > 0 else 0.0
    next_at = time.time()
    log.info("producer start sources=%s rate=%s/s sink=%s from_start=%s offsets=%s", sources, rate, sink_kind,
             from_start, {s: offsets.get(s, 0) for s in sources})
    try:
        for s in itertools.cycle(list(iters)):
            if not iters:
                break
            if s not in iters:
                continue
            state = control.state()
            while state == "PAUSED":
                sink.flush()
                time.sleep(0.5)
                state = control.state()
            if state == "STOP" or (max_events and sent >= max_events):
                break
            try:
                offset, row = next(iters[s])
            except StopIteration:
                del iters[s]
                continue
            src = EVENT_SOURCES[s]
            now = datetime.now(timezone.utc)
            if malformed_rate and rng.random() < malformed_rate:
                sink.send(src.topic, None, '{"event_id": "broken", "source_id": ' + f'"{s}", "payload": ')   # invalid JSON
                stats[s]["malformed_injected"] += 1
            late = late_rate and rng.random() < late_rate
            ev = to_event(src, row, offset, now, late_by if late else rng.uniform(0, 3))
            stats[s]["late_injected"] += int(bool(late))
            sink.send(src.topic, str(ev["sk_id_curr"]), ev)
            if dup_rate and rng.random() < dup_rate:
                sink.send(src.topic, str(ev["sk_id_curr"]), ev)                        # exact duplicate delivery
                stats[s]["duplicates_injected"] += 1
            stats[s]["sent"] += 1
            sent += 1
            offsets[s] = offset + 1
            if sent % 500 == 0:
                sink.flush()
                offsets_path.write_text(json.dumps(offsets))
            if interval:
                next_at += interval
                delay = next_at - time.time()
                if delay > 0:
                    time.sleep(delay)
                else:
                    next_at = time.time()
    finally:
        sink.close()
        offsets_path.write_text(json.dumps(offsets))
    elapsed = time.time() - t_start
    summary = {"sent": sent, "seconds": round(elapsed, 2), "events_per_sec": round(sent / elapsed, 1) if elapsed else None,
               "per_source": stats, "offsets": offsets, "sink": sink_kind,
               "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    with open(state_dir / "producer_runs.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")
    log.info("producer done %s", {k: summary[k] for k in ("sent", "seconds", "events_per_sec")})
    return summary


def _parse_minutes(s: str) -> float:
    n, unit = s.split()
    return float(n) * {"second": 1 / 60, "seconds": 1 / 60, "minute": 1, "minutes": 1, "hour": 60, "hours": 60}[unit]


def main():
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sources", nargs="*", default=list(EVENT_SOURCES))
    ap.add_argument("--rate", type=float, default=cfg["streaming"]["rate_per_sec"], help="events/second (0 = unthrottled)")
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--sink", choices=["file", "kafka"], default=cfg["streaming"]["sink"])
    ap.add_argument("--from-start", action="store_true", help="ignore saved offsets and replay from row 0")
    ap.add_argument("--dup-rate", type=float, default=0.0)
    ap.add_argument("--late-rate", type=float, default=0.0)
    ap.add_argument("--malformed-rate", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    print(json.dumps(run(a.sources, a.rate, a.max_events, a.sink, a.from_start, a.dup_rate, a.late_rate,
                         a.malformed_rate, a.seed), indent=2))


if __name__ == "__main__":
    main()
