"""Generate a small, schema-faithful TEST FIXTURE that mimics the 10 Home Credit files.

!!! THIS IS NOT HOME CREDIT DATA !!!
It exists only so that the pipeline and the test-suite can run without the Kaggle download.
Column names / types / file names / key structure follow the Kaggle files; values are random.
Known quirks of the real data are reproduced (DAYS_EMPLOYED=365243, 'XNA'/'XAP', bureau_balance
orphans, partial-payment rows ...) and a few defects are INJECTED on purpose so the cleaning,
integrity and leakage checks have something to find. Every injection is listed in
`fixture_injections.json` next to the CSVs, and the tests assert on it.

Usage:
    python -m scripts.make_test_fixture --out data/fixture/raw --customers 3000
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

SENTINEL = 365243

APP_COLUMNS = [
    "SK_ID_CURR", "TARGET", "NAME_CONTRACT_TYPE", "CODE_GENDER", "FLAG_OWN_CAR", "FLAG_OWN_REALTY", "CNT_CHILDREN",
    "AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE", "NAME_TYPE_SUITE", "NAME_INCOME_TYPE",
    "NAME_EDUCATION_TYPE", "NAME_FAMILY_STATUS", "NAME_HOUSING_TYPE", "REGION_POPULATION_RELATIVE", "DAYS_BIRTH",
    "DAYS_EMPLOYED", "DAYS_REGISTRATION", "DAYS_ID_PUBLISH", "OWN_CAR_AGE", "FLAG_MOBIL", "FLAG_EMP_PHONE",
    "FLAG_WORK_PHONE", "FLAG_CONT_MOBILE", "FLAG_PHONE", "FLAG_EMAIL", "OCCUPATION_TYPE", "CNT_FAM_MEMBERS",
    "REGION_RATING_CLIENT", "REGION_RATING_CLIENT_W_CITY", "WEEKDAY_APPR_PROCESS_START", "HOUR_APPR_PROCESS_START",
    "REG_REGION_NOT_LIVE_REGION", "ORGANIZATION_TYPE", "EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3",
    "APARTMENTS_AVG", "YEARS_BUILD_AVG", "FLOORSMAX_MEDI", "TOTALAREA_MODE", "EMERGENCYSTATE_MODE",
    "OBS_30_CNT_SOCIAL_CIRCLE", "DEF_30_CNT_SOCIAL_CIRCLE", "DAYS_LAST_PHONE_CHANGE", "FLAG_DOCUMENT_3",
    "AMT_REQ_CREDIT_BUREAU_YEAR",
]
OCCUPATIONS = ["Laborers", "Sales staff", "Core staff", "Managers", "Drivers", "High skill tech staff",
               "Accountants", "Medicine staff", "Security staff", "Cooking staff"]
WEEKDAYS = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]


def _choice(rng, values, n, p=None):
    return rng.choice(np.array(values, dtype=object), size=n, p=p)


def _with_nans(rng, arr, frac):
    arr = np.array(arr, dtype=object)
    arr[rng.random(len(arr)) < frac] = np.nan
    return arr


def make_applications(rng, ids: np.ndarray, with_target: bool) -> pd.DataFrame:
    n = len(ids)
    income_type = _choice(rng, ["Working", "Commercial associate", "Pensioner", "State servant"], n,
                          [0.52, 0.23, 0.18, 0.07])
    income = np.round(np.exp(rng.normal(11.9, 0.5, n)) / 450) * 450
    credit = np.round(income * rng.uniform(1.0, 6.0, n) / 4500) * 4500
    annuity = np.round(credit / rng.choice([12, 18, 24, 36, 48, 60], n) * 1.15, 1)
    goods = np.where(rng.random(n) < 0.001, np.nan, np.round(credit * rng.uniform(0.8, 1.0, n) / 4500) * 4500)
    days_employed = np.where(income_type == "Pensioner", SENTINEL, -rng.integers(30, 12000, n))
    target = (rng.random(n) < 0.08).astype(int)
    df = pd.DataFrame({
        "SK_ID_CURR": ids,
        "TARGET": target,
        "NAME_CONTRACT_TYPE": _choice(rng, ["Cash loans", "Revolving loans"], n, [0.9, 0.1]),
        "CODE_GENDER": _choice(rng, ["F", "M", "XNA"], n, [0.655, 0.344, 0.001]),
        "FLAG_OWN_CAR": _choice(rng, ["N", "Y"], n, [0.66, 0.34]),
        "FLAG_OWN_REALTY": _choice(rng, ["Y", "N"], n, [0.69, 0.31]),
        "CNT_CHILDREN": rng.choice([0, 1, 2, 3], n, p=[0.7, 0.2, 0.08, 0.02]),
        "AMT_INCOME_TOTAL": income,
        "AMT_CREDIT": credit,
        "AMT_ANNUITY": _with_nans(rng, annuity, 0.0005),
        "AMT_GOODS_PRICE": goods,
        "NAME_TYPE_SUITE": _with_nans(rng, _choice(rng, ["Unaccompanied", "Family", "Spouse, partner"], n), 0.004),
        "NAME_INCOME_TYPE": income_type,
        "NAME_EDUCATION_TYPE": _choice(rng, ["Secondary / secondary special", "Higher education",
                                             "Incomplete higher", "Lower secondary"], n, [0.71, 0.24, 0.035, 0.015]),
        "NAME_FAMILY_STATUS": _choice(rng, ["Married", "Single / not married", "Civil marriage", "Separated",
                                            "Widow"], n, [0.64, 0.15, 0.1, 0.06, 0.05]),
        "NAME_HOUSING_TYPE": _choice(rng, ["House / apartment", "With parents", "Municipal apartment"], n,
                                     [0.89, 0.05, 0.06]),
        "REGION_POPULATION_RELATIVE": np.round(rng.uniform(0.001, 0.072, n), 6),
        "DAYS_BIRTH": -rng.integers(7500, 25200, n),
        "DAYS_EMPLOYED": days_employed,
        "DAYS_REGISTRATION": -np.round(rng.uniform(0, 20000, n)),
        "DAYS_ID_PUBLISH": -rng.integers(0, 7000, n),
        "OWN_CAR_AGE": _with_nans(rng, rng.integers(0, 40, n).astype(float), 0.66),
        "FLAG_MOBIL": 1,
        "FLAG_EMP_PHONE": (income_type != "Pensioner").astype(int),
        "FLAG_WORK_PHONE": rng.integers(0, 2, n),
        "FLAG_CONT_MOBILE": 1,
        "FLAG_PHONE": rng.integers(0, 2, n),
        "FLAG_EMAIL": (rng.random(n) < 0.06).astype(int),
        "OCCUPATION_TYPE": _with_nans(rng, _choice(rng, OCCUPATIONS, n), 0.31),
        "CNT_FAM_MEMBERS": None,
        "REGION_RATING_CLIENT": rng.choice([1, 2, 3], n, p=[0.1, 0.74, 0.16]),
        "REGION_RATING_CLIENT_W_CITY": rng.choice([1, 2, 3], n, p=[0.11, 0.75, 0.14]),
        "WEEKDAY_APPR_PROCESS_START": _choice(rng, WEEKDAYS, n),
        "HOUR_APPR_PROCESS_START": rng.integers(6, 20, n),
        "REG_REGION_NOT_LIVE_REGION": (rng.random(n) < 0.015).astype(int),
        "ORGANIZATION_TYPE": np.where(income_type == "Pensioner", "XNA",
                                      _choice(rng, ["Business Entity Type 3", "Self-employed", "Other", "Medicine",
                                                    "Government", "School", "Trade: type 7"], n)),
        "EXT_SOURCE_1": _with_nans(rng, np.round(rng.beta(3, 3, n), 6), 0.56),
        "EXT_SOURCE_2": _with_nans(rng, np.round(rng.beta(4, 2.5, n), 6), 0.002),
        "EXT_SOURCE_3": _with_nans(rng, np.round(rng.beta(3.5, 2.5, n), 6), 0.2),
        "APARTMENTS_AVG": _with_nans(rng, np.round(rng.uniform(0, 0.3, n), 4), 0.5),
        "YEARS_BUILD_AVG": _with_nans(rng, np.round(rng.uniform(0.5, 1, n), 4), 0.66),
        "FLOORSMAX_MEDI": _with_nans(rng, np.round(rng.uniform(0, 0.7, n), 4), 0.5),
        "TOTALAREA_MODE": _with_nans(rng, np.round(rng.uniform(0, 0.3, n), 4), 0.48),
        "EMERGENCYSTATE_MODE": _with_nans(rng, _choice(rng, ["No", "Yes"], n, [0.98, 0.02]), 0.47),
        "OBS_30_CNT_SOCIAL_CIRCLE": _with_nans(rng, rng.poisson(1.4, n).astype(float), 0.003),
        "DEF_30_CNT_SOCIAL_CIRCLE": _with_nans(rng, rng.poisson(0.14, n).astype(float), 0.003),
        "DAYS_LAST_PHONE_CHANGE": -rng.integers(0, 4000, n).astype(float),
        "FLAG_DOCUMENT_3": (rng.random(n) < 0.71).astype(int),
        "AMT_REQ_CREDIT_BUREAU_YEAR": _with_nans(rng, rng.poisson(1.9, n).astype(float), 0.135),
    })
    fam = df["CNT_CHILDREN"] + np.where(df["NAME_FAMILY_STATUS"].isin(["Married", "Civil marriage"]), 2, 1)
    df["CNT_FAM_MEMBERS"] = _with_nans(rng, fam.astype(float), 0.0001)
    if not with_target:
        df = df.drop(columns=["TARGET"])
    return df[[c for c in APP_COLUMNS if c in df.columns]]


def make_bureau(rng, cust_ids, start_id):
    rows, bal_rows = [], []
    bid = start_id
    for cid in cust_ids:
        k = rng.choice([0, 1, 2, 3, 4, 5, 6, 8, 10], p=[0.14, 0.15, 0.16, 0.15, 0.12, 0.1, 0.08, 0.06, 0.04])
        for _ in range(k):
            bid += int(rng.integers(1, 4))
            active = rng.choice(["Closed", "Active", "Sold", "Bad debt"], p=[0.63, 0.365, 0.004, 0.001])
            days_credit = -int(rng.integers(1, 2900))
            credit_sum = float(np.round(np.exp(rng.normal(12.0, 1.0)) / 100) * 100)
            debt = float(np.round(credit_sum * rng.uniform(0, 1), 2)) if active == "Active" else 0.0
            overdue_days = int(rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 5, 40])) if active == "Active" else 0
            end_fact = np.nan if active == "Active" else float(days_credit + rng.integers(30, 1500))
            if not np.isnan(end_fact):
                end_fact = min(end_fact, -1.0)
            rows.append({
                "SK_ID_CURR": cid, "SK_ID_BUREAU": bid, "CREDIT_ACTIVE": active,
                "CREDIT_CURRENCY": rng.choice(["currency 1", "currency 2"], p=[0.999, 0.001]),
                "DAYS_CREDIT": days_credit, "CREDIT_DAY_OVERDUE": overdue_days,
                "DAYS_CREDIT_ENDDATE": float(days_credit + rng.integers(90, 3000)),
                "DAYS_ENDDATE_FACT": end_fact,
                "AMT_CREDIT_MAX_OVERDUE": np.nan if rng.random() < 0.65 else float(rng.integers(0, 5000)),
                "CNT_CREDIT_PROLONG": int(rng.random() < 0.005),
                "AMT_CREDIT_SUM": credit_sum,
                "AMT_CREDIT_SUM_DEBT": np.nan if rng.random() < 0.15 else debt,
                "AMT_CREDIT_SUM_LIMIT": np.nan if rng.random() < 0.34 else 0.0,
                "AMT_CREDIT_SUM_OVERDUE": float(overdue_days * 50),
                "CREDIT_TYPE": rng.choice(["Consumer credit", "Credit card", "Car loan", "Mortgage"],
                                          p=[0.73, 0.23, 0.02, 0.02]),
                "DAYS_CREDIT_UPDATE": int(min(-1, days_credit + rng.integers(0, 3000))),
                "AMT_ANNUITY": np.nan if rng.random() < 0.7 else float(rng.integers(0, 30000)),
            })
            # bureau_balance exists only for part of the bureau credits (as in the real data)
            if rng.random() < 0.6:
                first_month = max(-96, days_credit // 30)
                for m in range(first_month, 1):
                    if active == "Closed" and end_fact is not None and not np.isnan(end_fact) and m * 30 > end_fact:
                        status = "C"
                    elif rng.random() < 0.12:
                        status = "X"
                    else:
                        status = rng.choice(["0", "1", "2", "3", "4", "5"], p=[0.95, 0.035, 0.007, 0.004, 0.002, 0.002])
                    bal_rows.append({"SK_ID_BUREAU": bid, "MONTHS_BALANCE": m, "STATUS": status})
    return pd.DataFrame(rows), pd.DataFrame(bal_rows)


def make_previous(rng, cust_ids, start_id):
    rows = []
    pid = start_id
    for cid in cust_ids:
        k = rng.choice([0, 1, 2, 3, 4, 5, 7], p=[0.05, 0.2, 0.22, 0.2, 0.15, 0.1, 0.08])
        for _ in range(k):
            pid += int(rng.integers(1, 5))
            ctype = rng.choice(["Cash loans", "Consumer loans", "Revolving loans", "XNA"], p=[0.45, 0.44, 0.109, 0.001])
            status = rng.choice(["Approved", "Canceled", "Refused", "Unused offer"], p=[0.62, 0.19, 0.17, 0.02])
            decision = -int(rng.integers(30, 2900))
            amt_app = float(np.round(np.exp(rng.normal(11.5, 1.0)) / 100) * 100)
            amt_credit = float(np.round(amt_app * rng.uniform(0.9, 1.15), 1))
            cnt_payment = float(rng.choice([6, 12, 18, 24])) if status == "Approved" else np.nan
            approved = status == "Approved"
            first_due = float(decision + 30) if approved else np.nan
            last_due_1 = first_due + 30 * (cnt_payment - 1) if approved else np.nan
            closed = approved and last_due_1 < -1
            rows.append({
                "SK_ID_PREV": pid, "SK_ID_CURR": cid, "NAME_CONTRACT_TYPE": ctype,
                "AMT_ANNUITY": np.round(amt_credit / cnt_payment * 1.2, 2) if approved else np.nan,
                "AMT_APPLICATION": amt_app, "AMT_CREDIT": amt_credit if status != "Refused" or rng.random() < 0.9 else 0.0,
                "AMT_DOWN_PAYMENT": np.nan if rng.random() < 0.53 else float(rng.integers(0, 20000)),
                "AMT_GOODS_PRICE": np.nan if ctype == "Cash loans" and rng.random() < 0.5 else amt_app,
                "WEEKDAY_APPR_PROCESS_START": rng.choice(WEEKDAYS), "HOUR_APPR_PROCESS_START": int(rng.integers(6, 20)),
                "FLAG_LAST_APPL_PER_CONTRACT": "Y" if rng.random() < 0.995 else "N",
                "NFLAG_LAST_APPL_IN_DAY": 1,
                "RATE_DOWN_PAYMENT": np.nan if rng.random() < 0.53 else round(float(rng.uniform(0, 0.2)), 6),
                "RATE_INTEREST_PRIMARY": np.nan if rng.random() < 0.996 else round(float(rng.uniform(0.03, 0.2)), 6),
                "RATE_INTEREST_PRIVILEGED": np.nan if rng.random() < 0.996 else round(float(rng.uniform(0.3, 0.9)), 6),
                "NAME_CASH_LOAN_PURPOSE": "XAP" if ctype != "Cash loans" else rng.choice(["XNA", "Repairs", "Other"]),
                "NAME_CONTRACT_STATUS": status, "DAYS_DECISION": decision,
                "NAME_PAYMENT_TYPE": rng.choice(["Cash through the bank", "XNA"], p=[0.62, 0.38]),
                "CODE_REJECT_REASON": "XAP" if status != "Refused" else rng.choice(["HC", "LIMIT", "SCO", "XNA"]),
                "NAME_TYPE_SUITE": np.nan if rng.random() < 0.49 else "Unaccompanied",
                "NAME_CLIENT_TYPE": rng.choice(["Repeater", "New", "Refreshed"], p=[0.74, 0.18, 0.08]),
                "NAME_GOODS_CATEGORY": rng.choice(["XNA", "Mobile", "Consumer Electronics", "Computers"]),
                "NAME_PORTFOLIO": {"Cash loans": "Cash", "Consumer loans": "POS", "Revolving loans": "Cards"}.get(ctype, "XNA"),
                "NAME_PRODUCT_TYPE": rng.choice(["XNA", "x-sell", "walk-in"], p=[0.64, 0.27, 0.09]),
                "CHANNEL_TYPE": rng.choice(["Credit and cash offices", "Country-wide", "Stone"]),
                "SELLERPLACE_AREA": int(rng.choice([-1, 0, 50, 1200])),
                "NAME_SELLER_INDUSTRY": rng.choice(["XNA", "Consumer electronics", "Connectivity"]),
                "CNT_PAYMENT": cnt_payment,
                "NAME_YIELD_GROUP": rng.choice(["XNA", "middle", "high", "low_normal"]),
                "PRODUCT_COMBINATION": rng.choice(["Cash", "POS household with interest", "Card Street"]),
                "DAYS_FIRST_DRAWING": float(SENTINEL) if approved else np.nan,
                "DAYS_FIRST_DUE": first_due,
                "DAYS_LAST_DUE_1ST_VERSION": last_due_1,
                "DAYS_LAST_DUE": (last_due_1 if closed else float(SENTINEL)) if approved else np.nan,
                "DAYS_TERMINATION": (last_due_1 + 3 if closed else float(SENTINEL)) if approved else np.nan,
                "NFLAG_INSURED_ON_APPROVAL": float(rng.integers(0, 2)) if approved else np.nan,
            })
    return pd.DataFrame(rows)


def make_children_of_previous(rng, prev: pd.DataFrame, risk_by_cust: dict):
    inst, pos, cc = [], [], []
    approved = prev[prev["NAME_CONTRACT_STATUS"] == "Approved"]
    for r in approved.itertuples(index=False):
        risk = risk_by_cust.get(r.SK_ID_CURR, 0.05)
        n_inst = int(r.CNT_PAYMENT)
        if r.NAME_CONTRACT_TYPE == "Revolving loans":
            limit = float(rng.choice([45000, 90000, 135000, 180000, 270000]))
            for m in range(int(r.DAYS_DECISION // 30), 0):
                bal = float(np.round(limit * rng.uniform(0, 1.05), 2)) if rng.random() < 0.7 else 0.0
                draw = float(np.round(rng.uniform(0, 30000), 2)) if rng.random() < 0.3 else 0.0
                dpd = int(rng.choice([0, 0, 0, 15, 45])) if rng.random() < risk * 3 else 0
                cc.append({
                    "SK_ID_PREV": r.SK_ID_PREV, "SK_ID_CURR": r.SK_ID_CURR, "MONTHS_BALANCE": m,
                    "AMT_BALANCE": bal, "AMT_CREDIT_LIMIT_ACTUAL": limit if rng.random() > 0.01 else 0.0,
                    "AMT_DRAWINGS_ATM_CURRENT": draw * 0.5 if draw else 0.0, "AMT_DRAWINGS_CURRENT": draw,
                    "AMT_DRAWINGS_OTHER_CURRENT": 0.0, "AMT_DRAWINGS_POS_CURRENT": draw * 0.5 if draw else 0.0,
                    "AMT_INST_MIN_REGULARITY": round(bal * 0.05, 2), "AMT_PAYMENT_CURRENT": np.nan if rng.random() < 0.2 else round(bal * 0.1, 2),
                    "AMT_PAYMENT_TOTAL_CURRENT": round(bal * 0.1, 2), "AMT_RECEIVABLE_PRINCIPAL": round(bal * 0.95, 2),
                    "AMT_RECIVABLE": bal, "AMT_TOTAL_RECEIVABLE": bal,
                    "CNT_DRAWINGS_ATM_CURRENT": 1.0 if draw else 0.0, "CNT_DRAWINGS_CURRENT": 2 if draw else 0,
                    "CNT_DRAWINGS_OTHER_CURRENT": 0.0, "CNT_DRAWINGS_POS_CURRENT": 1.0 if draw else 0.0,
                    "CNT_INSTALMENT_MATURE_CUM": float(max(0, m - r.DAYS_DECISION // 30)),
                    "NAME_CONTRACT_STATUS": "Active", "SK_DPD": dpd, "SK_DPD_DEF": 0 if dpd < 30 else dpd,
                })
            continue
        # cash / consumer loans: installment schedule + monthly POS/CASH snapshot
        version = 1.0
        for k in range(1, n_inst + 1):
            due = r.DAYS_FIRST_DUE + 30 * (k - 1)
            if due > -1:              # not yet due at the time of the current application
                break
            u = rng.random()
            if u < 0.01:              # no payment recorded
                inst.append({"SK_ID_PREV": r.SK_ID_PREV, "SK_ID_CURR": r.SK_ID_CURR,
                             "NUM_INSTALMENT_VERSION": version, "NUM_INSTALMENT_NUMBER": k,
                             "DAYS_INSTALMENT": due, "DAYS_ENTRY_PAYMENT": np.nan,
                             "AMT_INSTALMENT": r.AMT_ANNUITY, "AMT_PAYMENT": np.nan})
                continue
            late = rng.random() < risk
            entry = due + (rng.integers(1, 60) if late else -rng.integers(0, 15))
            entry = min(entry, -1)
            if rng.random() < 0.05:   # partial payments -> 2 rows for the same installment
                a = round(r.AMT_ANNUITY * 0.6, 3)
                inst.append({"SK_ID_PREV": r.SK_ID_PREV, "SK_ID_CURR": r.SK_ID_CURR, "NUM_INSTALMENT_VERSION": version,
                             "NUM_INSTALMENT_NUMBER": k, "DAYS_INSTALMENT": due, "DAYS_ENTRY_PAYMENT": float(entry - 5),
                             "AMT_INSTALMENT": r.AMT_ANNUITY, "AMT_PAYMENT": a})
                inst.append({"SK_ID_PREV": r.SK_ID_PREV, "SK_ID_CURR": r.SK_ID_CURR, "NUM_INSTALMENT_VERSION": version,
                             "NUM_INSTALMENT_NUMBER": k, "DAYS_INSTALMENT": due, "DAYS_ENTRY_PAYMENT": float(entry),
                             "AMT_INSTALMENT": r.AMT_ANNUITY,
                             "AMT_PAYMENT": round(r.AMT_ANNUITY - a - (500 if late else 0), 3)})
            else:
                inst.append({"SK_ID_PREV": r.SK_ID_PREV, "SK_ID_CURR": r.SK_ID_CURR, "NUM_INSTALMENT_VERSION": version,
                             "NUM_INSTALMENT_NUMBER": k, "DAYS_INSTALMENT": due, "DAYS_ENTRY_PAYMENT": float(entry),
                             "AMT_INSTALMENT": r.AMT_ANNUITY,
                             "AMT_PAYMENT": round(r.AMT_ANNUITY * (0.9 if late and rng.random() < 0.3 else 1.0), 3)})
        first_m = int(r.DAYS_DECISION // 30)
        for m in range(first_m, 0):
            paid = max(0, m - first_m)
            fut = max(0.0, r.CNT_PAYMENT - paid)
            dpd = int(rng.integers(1, 90)) if rng.random() < risk * 0.5 else 0
            pos.append({"SK_ID_PREV": r.SK_ID_PREV, "SK_ID_CURR": r.SK_ID_CURR, "MONTHS_BALANCE": m,
                        "CNT_INSTALMENT": r.CNT_PAYMENT, "CNT_INSTALMENT_FUTURE": fut,
                        "NAME_CONTRACT_STATUS": "Active" if fut > 0 else "Completed",
                        "SK_DPD": dpd, "SK_DPD_DEF": dpd if dpd > 30 else 0})
    return pd.DataFrame(inst), pd.DataFrame(pos), pd.DataFrame(cc)


DESCRIPTIONS = {
    "application_{train|test}.csv": {
        "SK_ID_CURR": "ID of loan in our sample", "TARGET": "Target variable (1 - client with payment difficulties, 0 - all other cases)",
        "AMT_INCOME_TOTAL": "Income of the client", "AMT_CREDIT": "Credit amount of the loan",
        "AMT_ANNUITY": "Loan annuity", "AMT_GOODS_PRICE": "Price of the goods for which the loan is given",
        "DAYS_BIRTH": "Client's age in days at the time of application", "DAYS_EMPLOYED": "How many days before the application the person started current employment",
        "OCCUPATION_TYPE": "What kind of occupation does the client have", "CNT_FAM_MEMBERS": "How many family members does client have",
    },
    "bureau.csv": {"SK_ID_BUREAU": "Recoded ID of previous Credit Bureau credit related to our loan (unique coding for each loan application)",
                   "CREDIT_ACTIVE": "Status of the Credit Bureau (CB) reported credits", "AMT_CREDIT_SUM_DEBT": "Current debt on Credit Bureau credit",
                   "CREDIT_DAY_OVERDUE": "Number of days past due on CB credit at the time of application for related loan in our sample",
                   "DAYS_CREDIT": "How many days before current application did client apply for Credit Bureau credit"},
    "bureau_balance.csv": {"MONTHS_BALANCE": "Month of balance relative to application date (-1 means the freshest balance date)",
                           "STATUS": "Status of Credit Bureau loan during the month (C closed, X unknown, 0 no DPD, 1 maximal DPD 1-30, ..., 5 120+ or sold/written off)"},
    "previous_application.csv": {"SK_ID_PREV": "ID of previous credit in Home credit related to loan in our sample",
                                 "NAME_CONTRACT_STATUS": "Contract status (approved, cancelled, ...) of previous application",
                                 "DAYS_DECISION": "Relative to current application when was the decision about previous application made"},
    "installments_payments.csv": {"DAYS_INSTALMENT": "When the installment of previous credit was supposed to be paid (relative to application date of current loan)",
                                  "DAYS_ENTRY_PAYMENT": "When was the installments of previous credit paid actually (relative to application date of current loan)",
                                  "AMT_INSTALMENT": "What was the prescribed installment amount of previous credit on this installment",
                                  "AMT_PAYMENT": "What the client actually paid on previous credit on this installment"},
    "POS_CASH_balance.csv": {"SK_DPD": "DPD (days past due) during the month of previous credit",
                             "MONTHS_BALANCE": "Month of balance relative to application date (-1 means the information to the freshest monthly snapshot)"},
    "credit_card_balance.csv": {"AMT_BALANCE": "Balance during the month of previous credit",
                                "AMT_CREDIT_LIMIT_ACTUAL": "Credit card limit during the month of the previous credit",
                                "SK_DPD": "DPD (Days past due) during the month on the previous credit"},
}


def main(out: Path, n_customers: int, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)
    all_ids = np.sort(rng.choice(np.arange(100002, 100002 + n_customers * 3), size=n_customers, replace=False))
    test_mask = rng.random(n_customers) < 0.14
    train_ids, test_ids = all_ids[~test_mask], all_ids[test_mask]

    train = make_applications(rng, train_ids, with_target=True)
    test = make_applications(rng, test_ids, with_target=False)
    injections: dict[str, object] = {}

    # --- injected defects (documented) ------------------------------------------------
    train.loc[train.index[0], "AMT_INCOME_TOTAL"] = 117000000.0                    # extreme income outlier
    injections["application_train.income_outlier_ids"] = [int(train.iloc[0]["SK_ID_CURR"])]
    dup_rows = train.iloc[[5, 6]]
    train = pd.concat([train, dup_rows], ignore_index=True)                         # 2 exact duplicates
    injections["application_train.exact_duplicates"] = 2

    risk_by_cust = {int(c): float(rng.choice([0.02, 0.05, 0.15, 0.35], p=[0.5, 0.3, 0.15, 0.05])) for c in all_ids}
    bureau, bureau_balance = make_bureau(rng, all_ids, 5_000_000)
    prev = make_previous(rng, all_ids, 1_000_000)
    inst, pos, cc = make_children_of_previous(rng, prev, risk_by_cust)

    # bureau: orphan SK_ID_CURR, one row after the application date, one row with missing SK_ID_CURR
    orphan_bureau = bureau.iloc[:4].copy()
    orphan_bureau["SK_ID_CURR"] = [999990001, 999990002, 999990003, 999990004]
    orphan_bureau["SK_ID_BUREAU"] = [9_900_001, 9_900_002, 9_900_003, 9_900_004]
    future = bureau.iloc[[10]].copy()
    future["SK_ID_BUREAU"] = 9_900_010
    future["DAYS_CREDIT"] = 5
    missing_key = bureau.iloc[[11]].copy()
    missing_key["SK_ID_BUREAU"] = 9_900_011
    missing_key["SK_ID_CURR"] = np.nan
    bureau = pd.concat([bureau, orphan_bureau, future, missing_key], ignore_index=True)
    injections["bureau.orphan_sk_id_curr_rows"] = 4
    injections["bureau.after_application_rows"] = 1
    injections["bureau.missing_required_key_rows"] = 1
    # bureau_balance orphans (the real file also has SK_ID_BUREAU values absent from bureau)
    orphan_bb = pd.DataFrame({"SK_ID_BUREAU": [9_999_001] * 3, "MONTHS_BALANCE": [-2, -1, 0], "STATUS": ["0", "0", "X"]})
    bureau_balance = pd.concat([bureau_balance, orphan_bb], ignore_index=True)
    injections["bureau_balance.orphan_rows"] = 3

    # installments: exact duplicates, orphan SK_ID_PREV, payments after the application date
    inst = pd.concat([inst, inst.iloc[[1, 2, 3]]], ignore_index=True)
    orphan_inst = inst.iloc[:5].copy()
    orphan_inst["SK_ID_PREV"] = 8_888_888
    late_rows = inst.iloc[[20, 21]].copy()
    late_rows["DAYS_ENTRY_PAYMENT"] = [3.0, 7.0]
    late_rows["NUM_INSTALMENT_VERSION"] = 2.0         # distinct business key -> not an exact duplicate
    inst = pd.concat([inst, orphan_inst, late_rows], ignore_index=True)
    injections["installments_payments.exact_duplicates"] = 3
    injections["installments_payments.orphan_sk_id_prev_rows"] = 5
    injections["installments_payments.after_application_rows"] = 2
    pos = pd.concat([pos, pos.iloc[[0, 1]]], ignore_index=True)
    injections["POS_CASH_balance.exact_duplicates"] = 2

    files = {
        "application_train.csv": train, "application_test.csv": test, "bureau.csv": bureau,
        "bureau_balance.csv": bureau_balance, "previous_application.csv": prev, "installments_payments.csv": inst,
        "POS_CASH_balance.csv": pos, "credit_card_balance.csv": cc,
        "sample_submission.csv": pd.DataFrame({"SK_ID_CURR": test_ids, "TARGET": 0.5}),
    }
    int_cols = {"SK_ID_CURR", "SK_ID_PREV", "SK_ID_BUREAU"}
    for name, df in files.items():
        for c in int_cols & set(df.columns):
            df[c] = df[c].astype("Int64")
        df.to_csv(out / name, index=False)

    desc = [{"Table": t, "Row": c, "Description": d, "Special": ""} for t, cols in DESCRIPTIONS.items() for c, d in cols.items()]
    pd.DataFrame(desc).to_csv(out / "HomeCredit_columns_description.csv", encoding="latin-1")

    counts = {name: int(len(df)) for name, df in files.items()}
    meta = {"WARNING": "TEST FIXTURE - random values with the Home Credit schema. NOT real data.",
            "seed": seed, "customers": int(n_customers), "row_counts": counts, "injections": injections}
    (out / "fixture_injections.json").write_text(json.dumps(meta, indent=2))
    (out / "FIXTURE_NOT_REAL_DATA.txt").write_text(meta["WARNING"] + "\n")
    return meta


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data/fixture/raw")
    ap.add_argument("--customers", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=20261113)
    a = ap.parse_args()
    m = main(Path(a.out), a.customers, a.seed)
    print(json.dumps(m["row_counts"], indent=2))
