"""Cut a small but referentially complete subset of the REAL Home Credit files.

Picks N customers (SK_ID_CURR) from application_train/test with a fixed seed, then keeps every
related row: bureau (by SK_ID_CURR) -> bureau_balance (by the selected SK_ID_BUREAU),
previous_application (by SK_ID_CURR) -> installments / POS_CASH / credit_card
(by the selected SK_ID_PREV, or by SK_ID_CURR so that orphan SK_ID_PREV rows of those customers
are kept too and remain visible to the integrity checks).

Files are streamed in chunks, so this runs on a laptop even for bureau_balance (27M rows).

    python -m scripts.make_dev_subset --src data/raw --out data/dev_subset/raw --customers 5000
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

CHUNK = 1_000_000


def _filter_csv(src: Path, dst: Path, predicate) -> int:
    n = 0
    header = True
    for chunk in pd.read_csv(src, chunksize=CHUNK, dtype=str, keep_default_na=False):
        part = chunk[predicate(chunk)]
        part.to_csv(dst, mode="w" if header else "a", header=header, index=False)
        header = False
        n += len(part)
    return n


def main(src: Path, out: Path, n_customers: int, seed: int) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    ids_train = pd.read_csv(src / "application_train.csv", usecols=["SK_ID_CURR"], dtype=str)["SK_ID_CURR"]
    ids_test = pd.read_csv(src / "application_test.csv", usecols=["SK_ID_CURR"], dtype=str)["SK_ID_CURR"]
    n_test = max(1, int(n_customers * len(ids_test) / (len(ids_train) + len(ids_test))))
    keep = set(rng.choice(ids_train, n_customers - n_test, replace=False)) | set(rng.choice(ids_test, n_test, replace=False))

    counts = {}
    for f in ("application_train.csv", "application_test.csv"):
        counts[f] = _filter_csv(src / f, out / f, lambda d: d["SK_ID_CURR"].isin(keep))
    counts["bureau.csv"] = _filter_csv(src / "bureau.csv", out / "bureau.csv", lambda d: d["SK_ID_CURR"].isin(keep))
    bureau_ids = set(pd.read_csv(out / "bureau.csv", usecols=["SK_ID_BUREAU"], dtype=str)["SK_ID_BUREAU"])
    counts["bureau_balance.csv"] = _filter_csv(src / "bureau_balance.csv", out / "bureau_balance.csv",
                                               lambda d: d["SK_ID_BUREAU"].isin(bureau_ids))
    counts["previous_application.csv"] = _filter_csv(src / "previous_application.csv", out / "previous_application.csv",
                                                     lambda d: d["SK_ID_CURR"].isin(keep))
    prev_ids = set(pd.read_csv(out / "previous_application.csv", usecols=["SK_ID_PREV"], dtype=str)["SK_ID_PREV"])
    for f in ("installments_payments.csv", "POS_CASH_balance.csv", "credit_card_balance.csv"):
        counts[f] = _filter_csv(src / f, out / f, lambda d: d["SK_ID_PREV"].isin(prev_ids) | d["SK_ID_CURR"].isin(keep))
    # keep the fixture marker so a subset of the TEST FIXTURE is never labelled as real data
    for f in ("HomeCredit_columns_description.csv", "sample_submission.csv", "FIXTURE_NOT_REAL_DATA.txt"):
        if (src / f).exists():
            shutil.copyfile(src / f, out / f)
    if not (src / "FIXTURE_NOT_REAL_DATA.txt").exists():
        (out / "SUBSET_OF_REAL_DATA.txt").write_text(
            f"Subset of the real Home Credit files: {n_customers} SK_ID_CURR, seed={seed}\n{counts}\n")
    return counts


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="data/raw")
    ap.add_argument("--out", default="data/dev_subset/raw")
    ap.add_argument("--customers", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=20261113)
    a = ap.parse_args()
    print(main(Path(a.src), Path(a.out), a.customers, a.seed))
