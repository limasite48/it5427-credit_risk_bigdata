"""Batch: load history into the data lake before the stream starts.

- bronze:     every event with event_time < LIVE_START (same columns as the files the sink writes from Kafka)
- landing:    raw files exactly as the source sent them (Kaggle CSVs with --landing; macro CSVs always)
- bronze:     macro data as Parquet (source STATS_OFFICE)
- reference:  timeline, labels, region list, column descriptions (UTF-8), JSON Schemas
- quarantine: orphan bureau_balance records

Run: .venv/Scripts/python -I ingestion/backfill.py              # local lake (lake/ folder)
     LAKE_MODE=minio .venv/Scripts/python -I ingestion/backfill.py --landing
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

import common as C  # noqa: E402

ROWS_PER_FILE = 2_000_000
MACRO = {"region_dim.csv": None,  # a lookup table -> reference, not a published statistic
         "regional_gdp_growth_quarterly.csv": "gdp_growth_quarterly",
         "regional_unemployment_quarterly.csv": "unemployment_quarterly",
         "regional_cpi_monthly.csv": "cpi_monthly"}


def backfill_table(src, ingest_time, part_dir):
    pf = pq.ParquetFile(C.STAGED_DIR / f"{src.table}.parquet")
    payload_cols = [c for c in pf.schema_arrow.names if c not in C.ENVELOPE_COLS]
    df = pd.read_parquet(C.STAGED_DIR / f"{src.table}.parquet", filters=[("event_time", "<", C.LIVE_START)])
    df.insert(len(C.ENVELOPE_COLS), "produced_at", pd.Series(pd.NaT, index=df.index, dtype=df.event_time.dtype))
    df = df.assign(kafka_topic=pd.Series(pd.NA, index=df.index, dtype="string"),
                   kafka_partition=pd.Series(pd.NA, index=df.index, dtype="Int64"),
                   kafka_offset=pd.Series(pd.NA, index=df.index, dtype="Int64"),
                   ingest_time=ingest_time, ingest_mode="batch_backfill")
    assert list(df.columns[len(C.ENVELOPE_COLS) + 1:][:len(payload_cols)]) == payload_cols
    for k, i in enumerate(range(0, len(df), ROWS_PER_FILE)):
        C.write_parquet(df.iloc[i:i + ROWS_PER_FILE], "bronze", f"{src.lake_prefix}/{part_dir}/part-{k:05d}.parquet")
    print(f"  bronze/{src.lake_prefix:38s} {len(df):>11,} rows")


def main():
    ap = argparse.ArgumentParser(description="Load history into the data lake")
    ap.add_argument("--landing", action="store_true", help="also copy the raw Kaggle CSVs (2.7 GB) into the landing bucket")
    ap.add_argument("--tables", default=",".join(C.SOURCES))
    args = ap.parse_args()

    t0 = time.time()
    ingest_time = pd.Timestamp.now(tz=C.TZ)
    part_dir = f"ingest_date={ingest_time:%Y-%m-%d}"
    print(f"Backfill -> {C.LAKE_MODE} ({C.MINIO_ENDPOINT if C.LAKE_MODE == 'minio' else C.LOCAL_LAKE})")

    for table in args.tables.split(","):
        src = C.SOURCES[table]
        backfill_table(src, ingest_time, part_dir)
        if args.landing:
            for f in src.files:
                C.upload_file(C.RAW_DIR / f, "landing", f"{src.lake_prefix}/{part_dir}/{f}")

    for f, dataset in MACRO.items():
        if dataset is None:
            continue
        C.upload_file(C.SUPP_DIR / f, "landing", f"stats_office/{dataset}/{part_dir}/{f}")
        df = pd.read_csv(C.SUPP_DIR / f).assign(source_system="STATS_OFFICE", is_simulated=True,
                                                 ingest_time=ingest_time, ingest_mode="batch")
        C.write_parquet(df, "bronze", f"stats_office/{dataset}/{part_dir}/part-00000.parquet")
        print(f"  bronze/stats_office/{dataset:27s} {len(df):>11,} rows")

    ref = {"timeline/application_timeline.parquet": C.STAGED_DIR / "_timeline.parquet",
           "loan_outcomes/loan_outcomes.parquet": C.STAGED_DIR / "_labels.parquet",
           "region_dim/region_dim.csv": C.SUPP_DIR / "region_dim.csv"}
    ref.update({f"schemas/{p.name}": p for p in sorted(C.SCHEMA_DIR.glob("*.schema.json"))})
    for key, path in ref.items():
        C.upload_file(path, "reference", key)
    desc = pd.read_csv(C.RAW_DIR / "HomeCredit_columns_description.csv", encoding="cp1252")
    desc_utf8 = C.STAGED_DIR / "_columns_description_utf8.csv"
    desc.drop(columns=desc.columns[0]).to_csv(desc_utf8, index=False, encoding="utf-8")
    C.upload_file(desc_utf8, "reference", "column_descriptions/columns_description.csv")
    print(f"  reference/ {len(ref) + 1} files")

    orphans = pd.read_parquet(C.STAGED_DIR / "_quarantine_bureau_balance.parquet")
    C.write_parquet(orphans, "quarantine", f"credit_bureau/bureau_balance/reason=orphan_sk_id_bureau/{part_dir}/part-00000.parquet")
    print(f"  quarantine/credit_bureau/bureau_balance {len(orphans):>11,} rows")
    print(f"Done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
