"""Requirements 9-11: producer sends events, consumer processes them, checkpoint/restart and replay are safe."""
from __future__ import annotations

import json

import pytest

SOURCES = ["installment_payment_system", "credit_card_system", "loan_application_system"]


@pytest.fixture(scope="module")
def produced(gold, workdir):
    from streaming.producers.replay_producer import run
    return run(SOURCES, rate=0, max_events=900, sink_kind="file", from_start=True,
               dup_rate=0.1, late_rate=0.0, malformed_rate=0.02, seed=3)


@pytest.fixture(scope="module")
def first_run(produced):
    from streaming.consumers.stream_processor import start
    return start("file", "available-now", reset=True)


def _events(workdir):
    lines = []
    for f in sorted((workdir / "stream_landing_dir").glob("*.jsonl")):
        lines += f.read_text().splitlines()
    return lines


def test_producer_writes_valid_envelopes(produced, workdir):
    assert produced["sent"] == 900
    good = []
    for line in _events(workdir):
        try:
            good.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    assert good, "no parseable events"
    for e in good:
        assert e["time_semantics"] == "SYNTHETIC_REPLAY"
        assert {"event_id", "source_id", "event_type", "event_time", "ingestion_time", "sk_id_curr"} <= set(e)
        assert "TARGET" not in e["payload"]
    assert {e["source_id"] for e in good} == set(SOURCES)


def test_event_ids_are_deterministic(gold):
    from streaming.producers.replay_producer import to_event
    from streaming.schemas.event_schema import EVENT_SOURCES
    from datetime import datetime, timezone
    src = EVENT_SOURCES["installment_payment_system"]
    now = datetime.now(timezone.utc)
    a = to_event(src, {"SK_ID_CURR": "1", "SK_ID_PREV": "2"}, 42, now, 0)
    b = to_event(src, {"SK_ID_CURR": "1", "SK_ID_PREV": "2"}, 42, now, 1)
    assert a["event_id"] == b["event_id"] == "evt_installments_payments_000000042"


def test_target_never_emitted(gold):
    from datetime import datetime, timezone
    from streaming.producers.replay_producer import to_event
    from streaming.schemas.event_schema import EVENT_SOURCES
    e = to_event(EVENT_SOURCES["loan_application_system"], {"SK_ID_CURR": "5", "TARGET": "1", "AMT_CREDIT": "10"}, 0,
                 datetime.now(timezone.utc), 0)
    assert "TARGET" not in e["payload"]


def test_kafka_sink_routes_to_topic(monkeypatch):
    import sys
    import types
    sent = []

    class FakeProducer:
        def __init__(self, **kw):
            self.kw = kw

        def send(self, topic, key=None, value=None):
            sent.append((topic, self.kw["key_serializer"](key), self.kw["value_serializer"](value)))

        def flush(self):
            pass

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaProducer=FakeProducer))
    from streaming.producers.sinks import KafkaSink
    s = KafkaSink("localhost:9092")
    s.send("installment_payment_events", "100006", {"event_id": "x"})
    s.close()
    assert sent == [("installment_payment_events", b"100006", b'{"event_id": "x"}')]


def test_consumer_processes_dedups_and_quarantines(first_run, produced, read):
    dups = sum(v["duplicates_injected"] for v in produced["per_source"].values())
    bad = sum(v["malformed_injected"] for v in produced["per_source"].values())
    assert first_run["quarantined_rows"] == bad
    assert first_run["events_written"] == produced["sent"]                 # every unique event, once
    assert first_run["valid_rows"] - first_run["events_written"] == dups
    ev = read("silver", "stream_events")
    assert ev.count() == ev.select("event_id").distinct().count() == produced["sent"]
    snap_src = read("gold", "stream_customer_activity_delta").agg({"events_total": "sum"}).first()[0]
    assert snap_src == produced["sent"]


def test_restart_from_checkpoint_reprocesses_nothing(first_run, read):
    from streaming.consumers.stream_processor import start
    again = start("file", "available-now")
    assert again["input_rows"] == 0 and again["events_written"] == 0
    assert read("silver", "stream_events").count() == first_run["events_written"]


def test_replaying_same_events_creates_no_duplicates(first_run, produced, read):
    from streaming.consumers.stream_processor import start
    from streaming.producers.replay_producer import run
    run(SOURCES, rate=0, max_events=300, sink_kind="file", from_start=True, seed=3)   # same rows -> same event ids
    res = start("file", "available-now")
    assert res["input_rows"] == 300 and res["events_written"] == 0
    assert read("silver", "stream_events").count() == produced["sent"]


def test_snapshot_matches_events(first_run, produced, read):
    from streaming.consumers.snapshot import build
    out = build()
    assert out["events_total"] == produced["sent"]
    snap = read("gold", "stream_customer_snapshot")
    assert snap.count() == snap.select("sk_id_curr").distinct().count()
