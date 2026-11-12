"""Quick EDA of the Home Credit Default Risk dataset.

Checks: table sizes, missing data, placeholder/anomalous values, key consistency
across tables, time offsets (for the realtime simulation) and default rates by
segment (for the business questions).

Run: .venv/Scripts/python -I scripts/eda.py data/raw
"""
import sys
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")
def _get_raw_path() -> Path:
    if len(sys.argv) > 1:
        return Path(sys.argv[1])
    root_data = Path(__file__).resolve().parents[2] / "_data"
    if root_data.exists() and any(root_data.glob("*.csv")):
        return root_data
    return Path(__file__).resolve().parents[1] / "data" / "raw"


RAW = _get_raw_path()
PLACEHOLDER_DAYS = 365243  # placeholder meaning "not applicable" in DAYS_* columns


def read(name):
    return pd.read_csv(RAW / f"{name}.csv", engine="pyarrow")


def section(title):
    print(f"\n{'=' * 8} {title} {'=' * 8}")


def text_cols(df):
    return [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]


def overview(name, df, key):
    miss = df.isna().mean()
    dup = df.duplicated(key).sum()
    print(f"{name:26s} {len(df):>11,} rows x {df.shape[1]:>3} cols | missing cells {df.isna().to_numpy().mean():5.1%}"
          f" | cols with missing {int((miss > 0).sum()):>3} | cols >50% missing {int((miss > 0.5).sum()):>3}"
          f" | duplicate key {key}: {dup:,}")


def placeholder_report(name, df):
    for c in [c for c in df.columns if c.startswith("DAYS_")]:
        n = int((df[c] == PLACEHOLDER_DAYS).sum())
        if n:
            print(f"   {name}.{c}: {n:,} rows = 365243 ({n / len(df):.1%})")


def xna_report(name, df):
    for c in text_cols(df):
        vc = df[c].value_counts()
        hits = {v: int(vc[v]) for v in ("XNA", "XAP", "Unknown") if v in vc.index}
        if hits:
            print(f"   {name}.{c}: {hits}")


def rate_by(df, col, min_n=500):
    g = df.groupby(col, observed=True, dropna=False)["TARGET"].agg(["size", "mean"])
    g = g[g["size"] >= min_n].sort_values("mean", ascending=False)
    print(f"-- default rate by {col} (groups < {min_n} applications dropped)")
    for k, r in g.iterrows():
        print(f"   {str(k):48s} n={int(r['size']):>7,}  default={r['mean']:6.2%}")


# ---------------------------------------------------------------- application
section("1. Size & missing data overview")
train = read("application_train")
test = read("application_test")
overview("application_train", train, "SK_ID_CURR")
overview("application_test", test, "SK_ID_CURR")
print(f"   SK_ID_CURR shared by train/test: {len(set(train.SK_ID_CURR) & set(test.SK_ID_CURR))}")

cust = train[["SK_ID_CURR", "TARGET"]].copy()
cust_ids = set(train.SK_ID_CURR) | set(test.SK_ID_CURR)

prev = read("previous_application")
overview("previous_application", prev, "SK_ID_PREV")
bureau = read("bureau")
overview("bureau", bureau, "SK_ID_BUREAU")
bb = read("bureau_balance")
overview("bureau_balance", bb, ["SK_ID_BUREAU", "MONTHS_BALANCE"])
pos = read("POS_CASH_balance")
overview("POS_CASH_balance", pos, ["SK_ID_PREV", "MONTHS_BALANCE"])
cc = read("credit_card_balance")
overview("credit_card_balance", cc, ["SK_ID_PREV", "MONTHS_BALANCE"])
inst = read("installments_payments")
overview("installments_payments", inst, ["SK_ID_PREV", "NUM_INSTALMENT_VERSION", "NUM_INSTALMENT_NUMBER"])

section("2. application_train: label & missing data")
vc = train.TARGET.value_counts()
print(f"TARGET=0: {vc[0]:,} | TARGET=1 (default): {vc[1]:,} | default rate {train.TARGET.mean():.2%}")
miss = train.isna().mean()
buckets = pd.cut(miss, [-0.01, 0, 0.1, 0.5, 1], labels=["0%", "0-10%", "10-50%", ">50%"]).value_counts().sort_index()
print("Columns by missing share:", buckets.to_dict())
print("Top 12 columns by missing share:")
for c, v in miss.sort_values(ascending=False).head(12).items():
    print(f"   {c:30s} {v:6.1%}")
housing = [c for c in train.columns if c.endswith(("_AVG", "_MODE", "_MEDI"))]
print(f"Housing columns (*_AVG/_MODE/_MEDI): {len(housing)} columns, "
      f"mean missing share {train[housing].isna().mean().mean():.1%}")
for c in ["EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "OWN_CAR_AGE", "OCCUPATION_TYPE", "AMT_ANNUITY"]:
    print(f"   {c:20s} missing {train[c].isna().mean():6.1%}")

section("3. Placeholder / anomalous values")
placeholder_report("application_train", train)
placeholder_report("application_test", test)
emp_anom = train.DAYS_EMPLOYED == PLACEHOLDER_DAYS
print(f"   -> NAME_INCOME_TYPE of the 365243 group: {train.loc[emp_anom, 'NAME_INCOME_TYPE'].value_counts().to_dict()}")
print(f"   -> default rate of the 365243 group: {train.TARGET[emp_anom].mean():.2%} vs others {train.TARGET[~emp_anom].mean():.2%}")
placeholder_report("previous_application", prev)
print("'Not available / not applicable' values (XNA, XAP, Unknown):")
xna_report("application_train", train)
xna_report("previous_application", prev)
inc = train.AMT_INCOME_TOTAL
print(f"AMT_INCOME_TOTAL: median {inc.median():,.0f} | p99 {inc.quantile(0.99):,.0f} | max {inc.max():,.0f}")
age = -train.DAYS_BIRTH / 365.25
print(f"Age (from DAYS_BIRTH): {age.min():.1f} - {age.max():.1f}")
yrs_emp = -train.DAYS_EMPLOYED[~emp_anom] / 365.25
print(f"Years employed (365243 excluded): {yrs_emp.min():.1f} - {yrs_emp.max():.1f}")
print(f"bureau.DAYS_CREDIT_ENDDATE min {bureau.DAYS_CREDIT_ENDDATE.min():,.0f} (~{bureau.DAYS_CREDIT_ENDDATE.min() / 365:.0f} years)"
      f" | bureau.DAYS_ENDDATE_FACT min {bureau.DAYS_ENDDATE_FACT.min():,.0f}")
print(f"bureau.AMT_CREDIT_SUM_DEBT < 0: {(bureau.AMT_CREDIT_SUM_DEBT < 0).sum():,} rows")
print(f"credit_card.AMT_BALANCE < 0: {(cc.AMT_BALANCE < 0).sum():,} rows")
print(f"bureau.CREDIT_CURRENCY: {bureau.CREDIT_CURRENCY.value_counts().to_dict()}")
print(f"bureau.CREDIT_ACTIVE: {bureau.CREDIT_ACTIVE.value_counts().to_dict()}")
print(f"bureau_balance.STATUS: {bb.STATUS.value_counts().to_dict()}")
print(f"previous_application.NAME_CONTRACT_STATUS: {prev.NAME_CONTRACT_STATUS.value_counts().to_dict()}")

section("4. Key links between tables")
for name, df in [("bureau", bureau), ("previous_application", prev), ("POS_CASH_balance", pos),
                 ("credit_card_balance", cc), ("installments_payments", inst)]:
    ids = set(df.SK_ID_CURR.unique())
    cust[f"has_{name}"] = cust.SK_ID_CURR.isin(ids)
    print(f"{name:24s} covers {cust[f'has_{name}'].mean():6.1%} of train customers | "
          f"SK_ID_CURR not in application: {len(ids - cust_ids):,}")
bureau_ids = set(bureau.SK_ID_BUREAU)
bb_ids = set(bb.SK_ID_BUREAU.unique())
print(f"bureau records with monthly history in bureau_balance: {len(bureau_ids & bb_ids) / len(bureau_ids):.1%} | "
      f"orphan SK_ID_BUREAU in bureau_balance (not in bureau): {len(bb_ids - bureau_ids):,}")
prev_ids = set(prev.SK_ID_PREV)
for name, df in [("POS_CASH_balance", pos), ("credit_card_balance", cc), ("installments_payments", inst)]:
    ids = set(df.SK_ID_PREV.unique())
    print(f"{name:24s} SK_ID_PREV matching previous_application: {len(ids & prev_ids) / len(ids):.1%}")

section("5. Time offsets (for the realtime simulation)")
print("application_*: NO absolute date column; every DAYS_* is counted back from the application date")
print(f"previous_application.DAYS_DECISION: {prev.DAYS_DECISION.min():,} .. {prev.DAYS_DECISION.max():,}")
print(f"bureau.DAYS_CREDIT: {bureau.DAYS_CREDIT.min():,} .. {bureau.DAYS_CREDIT.max():,}")
for name, df in [("bureau_balance", bb), ("POS_CASH_balance", pos), ("credit_card_balance", cc)]:
    print(f"{name}.MONTHS_BALANCE: {df.MONTHS_BALANCE.min()} .. {df.MONTHS_BALANCE.max()}")
print(f"installments.DAYS_INSTALMENT: {inst.DAYS_INSTALMENT.min():,.0f} .. {inst.DAYS_INSTALMENT.max():,.0f}")

section("6. Repayment behaviour (installments)")
inst_key = ["SK_ID_PREV", "NUM_INSTALMENT_VERSION", "NUM_INSTALMENT_NUMBER"]
split = inst.duplicated(inst_key, keep=False)
print(f"Duplicate instalment key = one instalment paid in several payments: {split.sum():,} rows / "
      f"{inst.loc[split, inst_key].drop_duplicates().shape[0]:,} instalments -> aggregate per instalment first")
per_inst = inst.groupby(inst_key).agg(SK_ID_CURR=("SK_ID_CURR", "first"), due_amt=("AMT_INSTALMENT", "first"),
                                      paid=("AMT_PAYMENT", "sum"), due_day=("DAYS_INSTALMENT", "first"),
                                      last_pay=("DAYS_ENTRY_PAYMENT", "max"))
late = per_inst.last_pay - per_inst.due_day
print(f"After aggregation: {len(per_inst):,} instalments | late: {(late > 0).mean():.1%} | late > 30 days: {(late > 30).mean():.2%}"
      f" | underpaid (> 1 currency unit): {(per_inst.paid < per_inst.due_amt - 1).mean():.2%}"
      f" | rows without payment date: {inst.DAYS_ENTRY_PAYMENT.isna().sum():,}")
late_cust = set(per_inst.loc[late > 30, "SK_ID_CURR"])
cust["inst_history"] = pd.Series("no instalment history", index=cust.index)
cust.loc[cust.has_installments_payments, "inst_history"] = "history, never > 30 days late"
cust.loc[cust.SK_ID_CURR.isin(late_cust), "inst_history"] = "ever > 30 days late"
refused = prev.loc[prev.NAME_CONTRACT_STATUS == "Refused"].groupby("SK_ID_CURR").size()
cust["prev_refused"] = pd.cut(cust.SK_ID_CURR.map(refused).fillna(0), [-1, 0, 2, 1000],
                              labels=["never refused", "refused 1-2 times", "refused >= 3 times"])

section("7. Default rate by segment (business questions)")
seg = train.join(cust.drop(columns=["SK_ID_CURR", "TARGET"]))
seg["AGE_GROUP"] = pd.cut(-seg.DAYS_BIRTH / 365.25, [20, 25, 30, 35, 40, 50, 60, 70], right=False)
seg["EMPLOYED_365243"] = emp_anom
for c in ["EXT_SOURCE_2", "EXT_SOURCE_3"]:
    seg[f"{c}_QUINTILE"] = pd.qcut(seg[c], 5, labels=["Q1 (low)", "Q2", "Q3", "Q4", "Q5 (high)"])
for col in ["NAME_CONTRACT_TYPE", "AGE_GROUP", "NAME_INCOME_TYPE", "NAME_EDUCATION_TYPE", "OCCUPATION_TYPE",
            "EMPLOYED_365243", "has_bureau", "prev_refused", "inst_history",
            "EXT_SOURCE_2_QUINTILE", "EXT_SOURCE_3_QUINTILE"]:
    rate_by(seg, col)
