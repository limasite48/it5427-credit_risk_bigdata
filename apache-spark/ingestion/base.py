"""Generic, idempotent ingestion of one source file into the Bronze layer.

Guarantees
* The raw file is never modified: it is copied byte-for-byte (sha256 recorded) into
  bronze/<origin>/<source_id>/<table>/ingest_id=<id>/<file>.
* Idempotent: if a file with the same sha256 was already ingested successfully for that table,
  the run is SKIPPED (no second copy, so Silver cannot see the same data twice).
  `--force` creates a new ingest_id; Silver always reads the latest successful snapshot only.
* Large files are streamed: hashing in 8 MB blocks, validation in pandas chunks (default 500k rows).
* Every attempt (SUCCESS / SKIPPED / FAILED) is appended to bronze/_manifests/<table>.jsonl with
  source, file, size, rows, timings, schema-drift and validation results.
"""
from __future__ import annotations

import csv
import hashlib
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

from common.config import load_config
from common.lake import LakeStore, spark_uri
from common.logging_utils import get_logger
from common.schemas import TABLES, TableSpec

log = get_logger("ingestion")
CHUNK_ROWS = 500_000
HASH_BLOCK = 8 * 1024 * 1024

ChunkValidator = Callable[[TableSpec, pd.DataFrame], dict]


@dataclass
class IngestResult:
    table: str
    source_id: str
    status: str                     # SUCCESS | SUCCESS_WITH_DRIFT | SKIPPED_ALREADY_INGESTED | FAILED
    ingest_id: str | None = None
    bronze_key: str | None = None
    rows: int | None = None
    size_bytes: int | None = None
    sha256: str | None = None
    seconds: float = 0.0
    schema_drift: dict = field(default_factory=dict)
    validation: dict = field(default_factory=dict)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status != "FAILED"


def manifest_key(table: str) -> str:
    return f"bronze/_manifests/{table}.jsonl"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(HASH_BLOCK):
            h.update(block)
    return h.hexdigest()


def read_header(path: Path) -> list[str]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return next(csv.reader(f))


def common_chunk_checks(spec: TableSpec, chunk: pd.DataFrame) -> dict:
    """Checks every source shares: required columns empty, key columns not integer."""
    out: dict[str, int] = {}
    for c in spec.mandatory_values:
        if c in chunk.columns:
            out[f"null_{c}"] = int((chunk[c] == "").sum())
    for c in [c for c in chunk.columns if c.startswith("SK_ID_")]:
        bad = chunk[c][(chunk[c] != "")]
        out[f"non_integer_{c}"] = int((~bad.str.fullmatch(r"-?\d+(\.0+)?")).sum())
    return out


def scan_file(spec: TableSpec, path: Path, validator: ChunkValidator | None) -> tuple[int, dict]:
    rows, totals = 0, {}
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=CHUNK_ROWS):
        rows += len(chunk)
        for check in (common_chunk_checks, validator):
            if check is None:
                continue
            for k, v in check(spec, chunk).items():
                totals[k] = totals.get(k, 0) + int(v)
    return rows, {k: v for k, v in totals.items() if v}


def latest_success(store: LakeStore, table: str) -> dict | None:
    ok = [r for r in store.read_jsonl(manifest_key(table)) if r["status"].startswith("SUCCESS")]
    return ok[-1] if ok else None


def latest_bronze_uri(table: str, cfg: dict | None = None) -> str:
    """Spark URI of the latest successfully ingested snapshot of a table (used by Silver)."""
    cfg = cfg or load_config()
    rec = latest_success(LakeStore(cfg), table)
    if rec is None:
        raise FileNotFoundError(f"table {table} has never been ingested into bronze")
    _, rest = rec["bronze_key"].split("/", 1)
    return spark_uri("bronze", *rest.split("/"), cfg=cfg)


def ingest_table(spec: TableSpec, src: Path, validator: ChunkValidator | None = None, force: bool = False,
                 store: LakeStore | None = None) -> IngestResult:
    store = store or LakeStore()
    t0 = time.time()
    res = IngestResult(table=spec.name, source_id=spec.source_id, status="FAILED")
    started = datetime.now(timezone.utc)
    try:
        if not src.exists():
            raise FileNotFoundError(f"source file not found: {src}")
        header = read_header(src)
        missing = [c for c in spec.required if c not in header]
        if missing:
            raise ValueError(f"required columns missing from {src.name}: {missing}")

        res.size_bytes = src.stat().st_size
        res.sha256 = sha256_of(src)
        history = store.read_jsonl(manifest_key(spec.name))
        already = [r for r in history if r.get("sha256") == res.sha256 and r["status"].startswith("SUCCESS")]
        if already and not force:
            res.status = "SKIPPED_ALREADY_INGESTED"
            res.ingest_id, res.bronze_key, res.rows = already[-1]["ingest_id"], already[-1]["bronze_key"], already[-1]["rows"]
            log.info("%-26s skipped (same sha256 already in bronze as %s)", spec.name, res.ingest_id)
            return res

        prev = latest_success(store, spec.name)
        if prev and prev.get("header"):
            added, removed = sorted(set(header) - set(prev["header"])), sorted(set(prev["header"]) - set(header))
            if added or removed:
                res.schema_drift = {"added_columns": added, "removed_columns": removed,
                                    "compared_to_ingest_id": prev["ingest_id"]}

        res.rows, res.validation = scan_file(spec, src, validator)
        res.ingest_id = started.strftime("%Y%m%dT%H%M%S") + "_" + res.sha256[:8]
        prefix = f"bronze/{spec.origin.lower()}/{spec.source_id}/{spec.name}/ingest_id={res.ingest_id}"
        res.bronze_key = f"{prefix}/{src.name}"
        store.put_file(src, res.bronze_key)
        res.status = "SUCCESS_WITH_DRIFT" if res.schema_drift else "SUCCESS"
        res.seconds = round(time.time() - t0, 3)
        store.put_text(_metadata_json(res, spec, src, header, started), f"{prefix}/_metadata.json")
        log.info("%-26s %s rows=%d size=%.1fMB %.1fs -> %s", spec.name, res.status, res.rows, res.size_bytes / 1e6,
                 res.seconds, res.bronze_key)
        return res
    except Exception as exc:  # noqa: BLE001 - one bad file must not stop the other sources
        res.status = "FAILED"
        res.error = f"{type(exc).__name__}: {exc}"
        log.error("%s FAILED: %s\n%s", spec.name, res.error, traceback.format_exc(limit=3))
        return res
    finally:
        res.seconds = round(time.time() - t0, 3)
        record = {**res.__dict__, "file": src.name, "origin": spec.origin,
                  "started_at": started.isoformat(timespec="seconds"),
                  "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                  "header": read_header(src) if src.exists() and res.status.startswith("SUCCESS") else None,
                  "rows_per_sec": round(res.rows / res.seconds) if res.rows and res.seconds else None,
                  "mb_per_sec": round(res.size_bytes / 1e6 / res.seconds, 2) if res.size_bytes and res.seconds else None}
        store.append_jsonl(record, manifest_key(spec.name))


def _metadata_json(res: IngestResult, spec: TableSpec, src: Path, header: list[str], started: datetime) -> str:
    import json
    return json.dumps({
        "table": spec.name, "source_id": spec.source_id, "origin": spec.origin, "grain": spec.grain,
        "original_file": src.name, "sha256": res.sha256, "size_bytes": res.size_bytes, "rows": res.rows,
        "columns": header, "ingest_id": res.ingest_id, "ingested_at": started.isoformat(timespec="seconds"),
        "data_semantics": "SYNTHETIC - generated, not real" if spec.origin == "SYNTHETIC"
        else "ORIGINAL historical data (Kaggle Home Credit), copied unchanged",
        "validation": res.validation, "schema_drift": res.schema_drift,
    }, indent=2)


def ingest_source(source_id: str, src_dir: Path, validator: ChunkValidator | None, force: bool = False,
                  store: LakeStore | None = None) -> list[IngestResult]:
    store = store or LakeStore()
    return [ingest_table(spec, src_dir / spec.file, validator, force, store)
            for spec in TABLES.values() if spec.source_id == source_id]
