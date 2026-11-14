"""Ingest every logical source independently into Bronze.

    python -m ingestion.run_ingestion                       # all sources from data/raw (+ data/synthetic)
    python -m ingestion.run_ingestion --sources external_credit_bureau --force
Exit code 1 if any table FAILED (the other sources are still ingested).
"""
from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

from common.config import load_config, path_of
from common.lake import LakeStore
from common.logging_utils import get_logger

log = get_logger("ingestion")


def run(raw_dir: Path | None = None, synthetic_dir: Path | None = None, sources: list[str] | None = None,
        force: bool = False) -> list:
    cfg = load_config()
    raw_dir = raw_dir or path_of(cfg, "raw_dir")
    synthetic_dir = synthetic_dir or path_of(cfg, "synthetic_dir")
    store = LakeStore(cfg)
    results = []
    for source_id, meta in cfg["sources"].items():
        if sources and source_id not in sources:
            continue
        module = importlib.import_module(meta["module"] + ".ingest")
        src = synthetic_dir if meta["origin"] == "SYNTHETIC" else raw_dir
        log.info("== source %s (%s) from %s", source_id, meta["origin"], src)
        results.extend(module.run(src, force=force, store=store))
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir")
    ap.add_argument("--synthetic-dir")
    ap.add_argument("--sources", nargs="*", help="logical source ids (default: all)")
    ap.add_argument("--force", action="store_true", help="re-ingest even if the same file was already ingested")
    a = ap.parse_args()
    res = run(Path(a.raw_dir) if a.raw_dir else None, Path(a.synthetic_dir) if a.synthetic_dir else None,
              a.sources, a.force)
    print(json.dumps([{"table": r.table, "status": r.status, "rows": r.rows, "seconds": r.seconds,
                       "error": r.error} for r in res], indent=2))
    return 0 if all(r.ok for r in res) else 1


if __name__ == "__main__":
    sys.exit(main())
