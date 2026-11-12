"""Shared configuration and helpers for the ingestion layer.

Covers: the source catalogue (table -> source system -> Kafka topic), the simulated
timeline convention, the event envelope, and data lake access (a local folder or MinIO via the S3 API).
"""
import hashlib
import os
import sys
from dataclasses import dataclass
from datetime import timedelta, timezone
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to cp1252

ROOT = Path(__file__).resolve().parents[1]


def _resolve_raw_dir() -> Path:
    """Resolve raw data directory: env var -> local data/raw -> project root _data/."""
    env_dir = os.getenv("HC_RAW_DIR")
    if env_dir and Path(env_dir).exists():
        return Path(env_dir)
    default_dir = ROOT / "data" / "raw"
    if default_dir.exists() and any(default_dir.glob("*.csv")):
        return default_dir
    root_data = ROOT.parent / "_data"
    if root_data.exists() and any(root_data.glob("*.csv")):
        return root_data
    return default_dir


RAW_DIR = _resolve_raw_dir()
STAGED_DIR = Path(os.getenv("HC_STAGED_DIR", ROOT / "data" / "staged"))
SUPP_DIR = ROOT / "data" / "supplementary"
SCHEMA_DIR = ROOT / "schemas"

# ---- Simulated timeline (the raw data only has day/month offsets relative to the application) ----
TZ = timezone(timedelta(hours=7))
TIMELINE_START = pd.Timestamp("2024-01-01", tz=TZ)  # a Monday
TIMELINE_WEEKS = 104                                 # applications fall in 2024-01-01 .. 2025-12-28
LIVE_START = pd.Timestamp(os.getenv("HC_LIVE_START", "2025-12-01"), tz=TZ)  # streamed from here on
SEED = 2026
BUREAU_REPORT_LEAD = pd.Timedelta(minutes=5)  # credit bureau report arrives 5 min before submission
HISTORY_GUARD = pd.Timedelta(minutes=10)      # all other history happens >= 10 min before submission
WEEKDAYS = {d: i for i, d in enumerate(
    ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"])}

# ---- Kafka / MinIO ----
SCHEMA_VERSION = "1.0.0"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DLQ_TOPIC = "hc.dlq.v1"
LAKE_MODE = os.getenv("LAKE_MODE", "local")  # local | minio
LOCAL_LAKE = Path(os.getenv("LOCAL_LAKE", ROOT / "lake"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "svc-ingest")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "ingest-secret-123")

ENVELOPE_COLS = ["event_id", "event_type", "schema_version", "source_system", "source_table",
                 "event_time", "customer_id"]
PRODUCED_AT_TOKEN = "__PRODUCED_AT__"


@dataclass(frozen=True)
class Source:
    table: str          # entity name (matches the Kaggle table)
    files: tuple        # raw CSV files
    source_system: str  # simulated source system
    topic: str
    event_type: str
    key_cols: tuple     # natural key -> event_id
    time_rule: str      # how event_time is derived

    @property
    def lake_prefix(self):
        return f"{self.source_system.lower()}/{self.table}"


SOURCES = {s.table: s for s in [
    Source("application", ("application_train.csv", "application_test.csv"), "LOS",
           "hc.los.loan-application.v1", "loan_application.submitted", ("SK_ID_CURR",),
           "Submission time: random week (fixed seed) on the application's own "
           "WEEKDAY_APPR_PROCESS_START and HOUR_APPR_PROCESS_START."),
    Source("bureau", ("bureau.csv",), "CREDIT_BUREAU",
           "hc.bureau.credit-record.v1", "bureau.credit_record.reported", ("SK_ID_BUREAU",),
           "Submission time - 5 minutes (the credit bureau returns its report during underwriting)."),
    Source("bureau_balance", ("bureau_balance.csv",), "CREDIT_BUREAU",
           "hc.bureau.credit-balance-monthly.v1", "bureau.credit_balance.reported",
           ("SK_ID_BUREAU", "MONTHS_BALANCE"),
           "Same as bureau (same credit report); customer resolved via SK_ID_BUREAU -> bureau."),
    Source("previous_application", ("previous_application.csv",), "LOAN_HISTORY_DB",
           "hc.history.previous-application.v1", "previous_application.decided", ("SK_ID_PREV",),
           "Current submission date + DAYS_DECISION, hour = HOUR_APPR_PROCESS_START of the previous application."),
    Source("pos_cash_balance", ("POS_CASH_balance.csv",), "POS_PARTNER",
           "hc.pos.cash-balance-monthly.v1", "pos_cash.statement_issued", ("SK_ID_PREV", "MONTHS_BALANCE"),
           "02:00 on day 1 of the month after MONTHS_BALANCE (counted from the submission month)."),
    Source("credit_card_balance", ("credit_card_balance.csv",), "CARD_SYSTEM",
           "hc.card.balance-monthly.v1", "card.statement_issued", ("SK_ID_PREV", "MONTHS_BALANCE"),
           "02:00 on day 1 of the month after MONTHS_BALANCE (counted from the submission month)."),
    Source("installments_payments", ("installments_payments.csv",), "CORE_BANKING",
           "hc.core.installment-payment.v1", "installment.payment_recorded",
           ("SK_ID_PREV", "NUM_INSTALMENT_VERSION", "NUM_INSTALMENT_NUMBER", "DAYS_ENTRY_PAYMENT", "AMT_PAYMENT"),
           "Submission date + DAYS_ENTRY_PAYMENT (DAYS_INSTALMENT if missing), during business hours."),
]}
SOURCE_BY_TOPIC = {s.topic: s for s in SOURCES.values()}


def build_timeline(apps):
    """Assign simulated submission times, deterministic by SEED, matching each application's weekday + hour."""
    apps = apps.sort_values("SK_ID_CURR", ignore_index=True)
    rng = np.random.default_rng(SEED)
    week = rng.integers(0, TIMELINE_WEEKS, len(apps))
    second = rng.integers(0, 3600, len(apps))
    day = week * 7 + apps.WEEKDAY_APPR_PROCESS_START.map(WEEKDAYS).to_numpy()
    t = (TIMELINE_START + pd.to_timedelta(day, unit="D")
         + pd.to_timedelta(apps.HOUR_APPR_PROCESS_START.to_numpy(), unit="h")
         + pd.to_timedelta(second, unit="s"))
    return pd.DataFrame({"SK_ID_CURR": apps.SK_ID_CURR, "application_time": t,
                         "phase": np.where(t < LIVE_START, "history", "live")})


def event_ids(table, df, key_cols):
    """event_id = BLAKE2b-128 of "table|natural key": stable across reruns, distinct across tables."""
    prefix = f"{table}|"
    cols = [df[c].tolist() for c in key_cols]
    return [hashlib.blake2b((prefix + "|".join(map(str, k))).encode(), digest_size=16).hexdigest()
            for k in zip(*cols)]


def iso(ts):
    """Timezone-aware Series -> ISO-8601 strings such as 2025-12-03T09:41:27+07:00."""
    s = ts.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return s.str.slice(0, -2) + ":" + s.str.slice(-2)


def serialize_events(src, df):
    """Staged rows -> JSON strings (envelope + payload, NaN -> null).

    produced_at is left as a token; the producer replaces it with the actual send time.
    """
    payload_cols = [c for c in df.columns if c not in ENVELOPE_COLS]
    payloads = df[payload_cols].to_json(orient="records", lines=True, double_precision=10).splitlines()
    head = (f'"event_type":"{src.event_type}","schema_version":"{SCHEMA_VERSION}",'
            f'"source_system":"{src.source_system}","source_table":"{src.table}",')
    return [f'{{"event_id":"{eid}",{head}"event_time":"{et}","produced_at":"{PRODUCED_AT_TOKEN}",'
            f'"customer_id":{cid},"payload":{pl}}}'
            for eid, et, cid, pl in zip(df.event_id, iso(df.event_time), df.customer_id.tolist(), payloads)]


# ---- Data lake: a local folder mimicking buckets, or MinIO ----
@lru_cache(maxsize=1)
def lake_fs():
    if LAKE_MODE == "minio":
        fs = pafs.S3FileSystem(endpoint_override=MINIO_ENDPOINT, scheme="http", region="us-east-1",
                               access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY)
        return fs, ""
    return pafs.LocalFileSystem(), LOCAL_LAKE.as_posix() + "/"


def lake_path(bucket, key):
    fs, base = lake_fs()
    path = f"{base}{bucket}/{key}"
    if LAKE_MODE != "minio":
        fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
    return fs, path


def write_parquet(df, bucket, key):
    fs, path = lake_path(bucket, key)
    table = df if isinstance(df, pa.Table) else pa.Table.from_pandas(df, preserve_index=False)
    # Spark cannot read TIMESTAMP(NANOS) -> always write microseconds (pandas 2 defaults to nanoseconds)
    pq.write_table(table, path, filesystem=fs, compression="snappy",
                   coerce_timestamps="us", allow_truncated_timestamps=True)
    return path


def upload_file(local_path, bucket, key):
    fs, path = lake_path(bucket, key)
    pafs.copy_files(str(local_path), path, source_filesystem=pafs.LocalFileSystem(), destination_filesystem=fs)
    return path
