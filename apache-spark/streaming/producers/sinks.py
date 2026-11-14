"""Event sinks: Kafka (one topic per logical source) or file replay (JSONL files a Spark file stream can read)."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path


class FileSink:
    """Writes JSONL files atomically (tmp + rename) so Spark never reads a half-written file.

    A new file is started every `events_per_file` events or `max_seconds` seconds.
    """

    def __init__(self, landing_dir: Path, events_per_file: int = 200, max_seconds: float = 2.0):
        self.dir = Path(landing_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.events_per_file, self.max_seconds = events_per_file, max_seconds
        self._buf: list[str] = []
        self._opened = time.time()
        self._seq = 0
        self.files_written = 0

    def send(self, topic: str, key: str | None, value: dict | str) -> None:
        self._buf.append(value if isinstance(value, str) else json.dumps(value, ensure_ascii=False))
        if len(self._buf) >= self.events_per_file or time.time() - self._opened >= self.max_seconds:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        self._seq += 1
        name = f"events-{time.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}-{self._seq:06d}.jsonl"
        tmp = self.dir / f".{name}.tmp"
        tmp.write_text("\n".join(self._buf) + "\n", encoding="utf-8")
        tmp.replace(self.dir / name)
        self._buf.clear()
        self._opened = time.time()
        self.files_written += 1

    def close(self) -> None:
        self.flush()


class KafkaSink:
    """kafka-python producer; key = SK_ID_CURR so one customer's events stay ordered within a partition."""

    def __init__(self, bootstrap_servers: str):
        from kafka import KafkaProducer  # imported lazily: not needed for the file sink

        self._p = KafkaProducer(bootstrap_servers=bootstrap_servers, acks="all", linger_ms=20, retries=5,
                                key_serializer=lambda k: None if k is None else str(k).encode(),
                                value_serializer=lambda v: (v if isinstance(v, str) else json.dumps(v)).encode())

    def send(self, topic: str, key: str | None, value: dict | str) -> None:
        self._p.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._p.flush()

    def close(self) -> None:
        self._p.flush()
        self._p.close()
