"""Step 1: prepare data for the simulated sources.

Reads the Kaggle CSVs, assigns a simulated event_time to every record and attaches the
event envelope (event_id, event_type, ...). Output: data/staged/<table>.parquet sorted by
event_time, shared by backfill (batch) and producer (stream). Side tables:
_timeline (submission times), _labels (TARGET), _quarantine_bureau_balance.

Run: .venv/Scripts/python -I ingestion/stage.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402

import common as C  # noqa: E402


def read_raw(name):
    return pd.read_csv(C.RAW_DIR / name, engine="pyarrow")


def save(df, name):
    df.to_parquet(C.STAGED_DIR / f"{name}.parquet", index=False, row_group_size=500_000)


def clamp(t, app_time):
    """History may not happen after (submission time - HISTORY_GUARD)."""
    limit = app_time - C.HISTORY_GUARD
    return t.where(t <= limit, limit)


def statement_time(app_time, months_balance, spread_id):
    """02:00 (+ id-based spread) on day 1 of the month after MONTHS_BALANCE."""
    local = app_time.dt.tz_localize(None)
    m = local.dt.year * 12 + (local.dt.month - 1) + months_balance + 1
    first = pd.to_datetime(pd.DataFrame({"year": m // 12, "month": m % 12 + 1, "day": 1}))
    return first.dt.tz_localize(C.TZ) + pd.Timedelta(hours=2) + pd.to_timedelta(spread_id % 3600, unit="s")


def stage(table, df, event_time, customer_id):
    src = C.SOURCES[table]
    env = pd.DataFrame({
        "event_id": C.event_ids(table, df, src.key_cols),
        "event_type": src.event_type,
        "schema_version": C.SCHEMA_VERSION,
        "source_system": src.source_system,
        "source_table": table,
        "event_time": event_time,
        "customer_id": customer_id,
    }, index=df.index)
    out = pd.concat([env, df], axis=1).sort_values("event_time", kind="stable", ignore_index=True)
    save(out, table)
    live = (out.event_time >= C.LIVE_START).sum()
    print(f"{table:24s} {len(out):>11,} events | realtime (>= {C.LIVE_START.date()}): {live:>9,} | "
          f"{out.event_time.min():%Y-%m-%d} .. {out.event_time.max():%Y-%m-%d}")


def main():
    t0 = time.time()
    C.STAGED_DIR.mkdir(parents=True, exist_ok=True)

    train, test = read_raw("application_train.csv"), read_raw("application_test.csv")
    apps = pd.concat([train, test], ignore_index=True)
    timeline = C.build_timeline(apps[["SK_ID_CURR", "WEEKDAY_APPR_PROCESS_START", "HOUR_APPR_PROCESS_START"]])
    save(timeline, "_timeline")
    labels = train[["SK_ID_CURR", "TARGET"]].merge(timeline, on="SK_ID_CURR")
    labels["split"] = labels.phase.map({"history": "train", "live": "live_holdout"})
    save(labels.drop(columns="phase"), "_labels")
    app_time = timeline.set_index("SK_ID_CURR").application_time

    # TARGET is only known after disbursement -> it is not part of the submission event
    apps = apps.drop(columns="TARGET")
    stage("application", apps, apps.SK_ID_CURR.map(app_time), apps.SK_ID_CURR)

    bureau = read_raw("bureau.csv")
    stage("bureau", bureau, bureau.SK_ID_CURR.map(app_time) - C.BUREAU_REPORT_LEAD, bureau.SK_ID_CURR)

    bb = read_raw("bureau_balance.csv")
    owner = bb.SK_ID_BUREAU.map(bureau.set_index("SK_ID_BUREAU").SK_ID_CURR)
    orphan = owner.isna()
    save(bb[orphan].assign(quarantine_reason="orphan_sk_id_bureau"), "_quarantine_bureau_balance")
    print(f"{'bureau_balance':24s} {orphan.sum():>11,} orphan rows (SK_ID_BUREAU not in bureau) -> quarantine")
    bb, owner = bb[~orphan], owner[~orphan].astype("int64")
    del bureau
    stage("bureau_balance", bb, owner.map(app_time) - C.BUREAU_REPORT_LEAD, owner)
    del bb, owner

    prev = read_raw("previous_application.csv")
    at = prev.SK_ID_CURR.map(app_time)
    t = (at.dt.normalize() + pd.to_timedelta(prev.DAYS_DECISION, unit="D")
         + pd.to_timedelta(prev.HOUR_APPR_PROCESS_START, unit="h") + pd.to_timedelta(prev.SK_ID_PREV % 3600, unit="s"))
    stage("previous_application", prev, clamp(t, at), prev.SK_ID_CURR)
    del prev

    for table, file in [("pos_cash_balance", "POS_CASH_balance.csv"), ("credit_card_balance", "credit_card_balance.csv")]:
        df = read_raw(file)
        at = df.SK_ID_CURR.map(app_time)
        stage(table, df, clamp(statement_time(at, df.MONTHS_BALANCE, df.SK_ID_PREV), at), df.SK_ID_CURR)
        del df

    inst = read_raw("installments_payments.csv")
    at = inst.SK_ID_CURR.map(app_time)
    days = inst.DAYS_ENTRY_PAYMENT.fillna(inst.DAYS_INSTALMENT)
    t = (at.dt.normalize() + pd.to_timedelta(days, unit="D") + pd.Timedelta(hours=8)
         + pd.to_timedelta(inst.SK_ID_PREV % 36000, unit="s"))
    stage("installments_payments", inst, clamp(t, at), inst.SK_ID_CURR)

    print(f"Done in {time.time() - t0:.0f}s -> {C.STAGED_DIR}")


if __name__ == "__main__":
    main()
