"""SYNTHETIC customer-behaviour source: traffic-violation profile per SK_ID_CURR.

!!! SYNTHETIC - NOT REAL TRAFFIC DATA. Must never be presented or used as real violation records. !!!

Design decisions (see docs/SYNTHETIC_DATA.md):
* Only real SK_ID_CURR values from application_train + application_test get a record (1 row each).
* No Home Credit column is read except SK_ID_CURR, and no Home Credit column is modified.
* Values are drawn INDEPENDENTLY of TARGET and of every Home Credit attribute, so the source adds no
  fabricated signal about credit risk. Only internal consistency is enforced:
    - TRAFFIC_VIOLATION_COUNT ~ NegativeBinomial(r=0.6, p=0.6) -> mean 0.4, many zeros, long tail
    - MAJOR_VIOLATION_COUNT   ~ Binomial(TRAFFIC_VIOLATION_COUNT, 0.12)  (major <= total)
    - TOTAL_TRAFFIC_FINE      = sum of per-violation fines from a fixed tariff (minor / major bands)
    - ACCIDENT_COUNT          ~ Poisson(0.03 + 0.10 * MAJOR_VIOLATION_COUNT)
    - DATA_UPDATED_AT         uniform in [2025-01-01, 2025-12-31] (synthetic refresh time)
* Deterministic: the same seed and the same SK_ID_CURR set always give the same file.
  Each customer's values are drawn from a generator seeded by (seed, SK_ID_CURR), so adding
  customers does not change existing customers' values.

    python -m ingestion.synthetic_source.generator --raw-dir data/raw --out data/synthetic
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from common.config import load_config, path_of

GENERATOR_VERSION = "1.0.0"
MINOR_FINES = np.array([300_000, 500_000, 800_000, 1_000_000, 2_000_000])      # synthetic tariff, VND-like units
MAJOR_FINES = np.array([6_000_000, 8_000_000, 12_000_000, 18_000_000])
WINDOW_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
WINDOW_SECONDS = 365 * 24 * 3600


def _one(seed: int, sk_id: int) -> tuple[int, int, float, int, str]:
    rng = np.random.default_rng([seed, sk_id])
    total = int(rng.negative_binomial(0.6, 0.6))
    major = int(rng.binomial(total, 0.12))
    fine = float(rng.choice(MINOR_FINES, total - major).sum() + rng.choice(MAJOR_FINES, major).sum())
    accidents = int(rng.poisson(0.03 + 0.10 * major))
    updated = WINDOW_START + timedelta(seconds=int(rng.integers(0, WINDOW_SECONDS)))
    return total, major, fine, accidents, updated.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate(raw_dir: Path, out_dir: Path, seed: int) -> dict:
    ids = []
    for f in ("application_train.csv", "application_test.csv"):
        p = raw_dir / f
        if p.exists():
            ids.append(pd.read_csv(p, usecols=["SK_ID_CURR"])["SK_ID_CURR"])
    if not ids:
        raise FileNotFoundError(f"no application files in {raw_dir}")
    sk = pd.concat(ids).dropna().astype("int64").drop_duplicates().sort_values().to_numpy()
    rows = [_one(seed, int(i)) for i in sk]
    df = pd.DataFrame(rows, columns=["TRAFFIC_VIOLATION_COUNT", "MAJOR_VIOLATION_COUNT", "TOTAL_TRAFFIC_FINE",
                                     "ACCIDENT_COUNT", "DATA_UPDATED_AT"])
    df.insert(0, "SK_ID_CURR", sk)
    df["DATA_ORIGIN"] = "SYNTHETIC"
    df["GENERATOR_VERSION"] = GENERATOR_VERSION
    df["GENERATOR_SEED"] = seed

    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / "synthetic_traffic_profile.csv", index=False)
    meta = {
        "WARNING": "SYNTHETIC DATA - generated, not real traffic records",
        "generator_version": GENERATOR_VERSION, "seed": seed, "rows": int(len(df)),
        "source_ids_from": [f for f in ("application_train.csv", "application_test.csv") if (raw_dir / f).exists()],
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "summary": {c: {"mean": float(df[c].mean()), "zero_share": float((df[c] == 0).mean()), "max": float(df[c].max())}
                    for c in ["TRAFFIC_VIOLATION_COUNT", "MAJOR_VIOLATION_COUNT", "TOTAL_TRAFFIC_FINE", "ACCIDENT_COUNT"]},
    }
    (out_dir / "synthetic_traffic_profile.meta.json").write_text(json.dumps(meta, indent=2))
    return meta


if __name__ == "__main__":
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", default=str(path_of(cfg, "raw_dir")))
    ap.add_argument("--out", default=str(path_of(cfg, "synthetic_dir")))
    ap.add_argument("--seed", type=int, default=cfg["project"]["seed"])
    a = ap.parse_args()
    print(json.dumps(generate(Path(a.raw_dir), Path(a.out), a.seed), indent=2))
