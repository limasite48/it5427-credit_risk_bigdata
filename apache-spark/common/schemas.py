"""Schema registry: one entry per table, shared by profiling, ingestion, cleaning, integration and tests.

Column names here are the ORIGINAL Kaggle names. Silver/Gold use the lower-snake-case version
(see `silver_name`). Keys and relationships below are the *expected* model from the Kaggle
documentation; profiling (processing/profiling) verifies each of them against the actual data.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# ------------------------------------------------------------------ type rules
LONG, INT, DOUBLE, STRING = "bigint", "int", "double", "string"

# application_* has 122/121 columns; they follow stable prefix conventions, so types are derived
# from rules + explicit overrides instead of a hand-typed list of 122 entries.
_APP_STRING_PREFIXES = ("NAME_", "CODE_", "OCCUPATION_TYPE", "ORGANIZATION_TYPE", "WEEKDAY_",
                        "FONDKAPREMONT_MODE", "HOUSETYPE_MODE", "WALLSMATERIAL_MODE", "EMERGENCYSTATE_MODE")
_APP_INT_PREFIXES = ("FLAG_DOCUMENT_", "FLAG_MOBIL", "FLAG_EMP_PHONE", "FLAG_WORK_PHONE", "FLAG_CONT_MOBILE",
                     "FLAG_PHONE", "FLAG_EMAIL", "REG_", "LIVE_", "REGION_RATING_", "HOUR_APPR_PROCESS_START",
                     "CNT_CHILDREN")


def application_type(col: str) -> str:
    if col == "SK_ID_CURR":
        return LONG
    if col == "TARGET":
        return INT
    if col in ("FLAG_OWN_CAR", "FLAG_OWN_REALTY"):          # 'Y' / 'N'
        return STRING
    if col.startswith(_APP_STRING_PREFIXES):
        return STRING
    if col.startswith(_APP_INT_PREFIXES):
        return INT
    return DOUBLE   # AMT_*, DAYS_*, EXT_SOURCE_*, *_AVG/_MODE/_MEDI, OBS_/DEF_*, AMT_REQ_*, CNT_FAM_MEMBERS ...


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    parent: str
    parent_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableSpec:
    name: str
    file: str
    source_id: str
    origin: str                               # ORIGINAL | SYNTHETIC
    grain: str
    primary_key: tuple[str, ...] | None       # None = no natural PK (event tables)
    required: tuple[str, ...]
    columns: dict[str, str] = field(default_factory=dict)   # explicit types (complete for non-application tables)
    foreign_keys: tuple[ForeignKey, ...] = ()
    # columns whose value is relative to the CURRENT application date (negative = before it).
    # A value > 0 means "after the application" -> excluded from features (leakage guard).
    point_in_time_cols: tuple[str, ...] = ()
    sentinel_cols: tuple[str, ...] = ()       # 365243 means "not applicable / open-ended"
    nonnegative_cols: tuple[str, ...] = ()    # flagged (not dropped) when negative
    dedup_key: tuple[str, ...] | None = None  # business key for duplicate DETECTION (exact dups are always removed)
    # `required` = must exist in the header (schema validation); `not_null` = value must be present,
    # otherwise the row is quarantined. Optional attributes (e.g. OCCUPATION_TYPE) are required
    # in the header but may legitimately be empty.
    not_null: tuple[str, ...] = ()

    @property
    def mandatory_values(self) -> tuple[str, ...]:
        return self.not_null or self.required

    def type_of(self, col: str) -> str:
        if self.columns:
            return self.columns.get(col, STRING)
        return application_type(col)


def silver_name(col: str) -> str:
    """Kaggle column -> silver column (lower snake case, safe characters)."""
    c = re.sub(r"[^0-9A-Za-z]+", "_", col.strip()).strip("_").lower()
    return re.sub(r"_+", "_", c)


_APP_COMMON_REQUIRED = ("SK_ID_CURR", "NAME_CONTRACT_TYPE", "CODE_GENDER", "AMT_INCOME_TOTAL", "AMT_CREDIT",
                        "AMT_ANNUITY", "AMT_GOODS_PRICE", "DAYS_BIRTH", "DAYS_EMPLOYED", "OCCUPATION_TYPE",
                        "CNT_FAM_MEMBERS", "NAME_INCOME_TYPE", "NAME_FAMILY_STATUS", "FLAG_OWN_CAR",
                        "FLAG_OWN_REALTY")

TABLES: dict[str, TableSpec] = {
    "application_train": TableSpec(
        name="application_train", file="application_train.csv", source_id="loan_application_system",
        not_null=("SK_ID_CURR", "TARGET", "AMT_INCOME_TOTAL", "AMT_CREDIT"),
        origin="ORIGINAL", grain="1 row = 1 current loan application (SK_ID_CURR)",
        primary_key=("SK_ID_CURR",), required=_APP_COMMON_REQUIRED + ("TARGET",),
        sentinel_cols=("DAYS_EMPLOYED",),
        nonnegative_cols=("AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE", "CNT_CHILDREN"),
    ),
    "application_test": TableSpec(
        name="application_test", file="application_test.csv", source_id="loan_application_system",
        not_null=("SK_ID_CURR", "AMT_INCOME_TOTAL", "AMT_CREDIT"),
        origin="ORIGINAL", grain="1 row = 1 current loan application (SK_ID_CURR), no TARGET",
        primary_key=("SK_ID_CURR",), required=_APP_COMMON_REQUIRED,
        sentinel_cols=("DAYS_EMPLOYED",),
        nonnegative_cols=("AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE", "CNT_CHILDREN"),
    ),
    "bureau": TableSpec(
        name="bureau", file="bureau.csv", source_id="external_credit_bureau", origin="ORIGINAL",
        not_null=("SK_ID_CURR", "SK_ID_BUREAU", "CREDIT_ACTIVE", "DAYS_CREDIT"),
        grain="1 row = 1 credit reported by another institution (SK_ID_BUREAU)",
        primary_key=("SK_ID_BUREAU",), required=("SK_ID_CURR", "SK_ID_BUREAU", "CREDIT_ACTIVE", "DAYS_CREDIT"),
        columns={
            "SK_ID_CURR": LONG, "SK_ID_BUREAU": LONG, "CREDIT_ACTIVE": STRING, "CREDIT_CURRENCY": STRING,
            "DAYS_CREDIT": INT, "CREDIT_DAY_OVERDUE": INT, "DAYS_CREDIT_ENDDATE": DOUBLE,
            "DAYS_ENDDATE_FACT": DOUBLE, "AMT_CREDIT_MAX_OVERDUE": DOUBLE, "CNT_CREDIT_PROLONG": INT,
            "AMT_CREDIT_SUM": DOUBLE, "AMT_CREDIT_SUM_DEBT": DOUBLE, "AMT_CREDIT_SUM_LIMIT": DOUBLE,
            "AMT_CREDIT_SUM_OVERDUE": DOUBLE, "CREDIT_TYPE": STRING, "DAYS_CREDIT_UPDATE": INT,
            "AMT_ANNUITY": DOUBLE,
        },
        foreign_keys=(ForeignKey(("SK_ID_CURR",), "application", ("SK_ID_CURR",)),),
        point_in_time_cols=("DAYS_CREDIT",),
        nonnegative_cols=("AMT_CREDIT_SUM", "CREDIT_DAY_OVERDUE", "AMT_CREDIT_SUM_OVERDUE"),
    ),
    "bureau_balance": TableSpec(
        name="bureau_balance", file="bureau_balance.csv", source_id="external_credit_bureau", origin="ORIGINAL",
        grain="1 row = 1 bureau credit x 1 month (SK_ID_BUREAU, MONTHS_BALANCE)",
        primary_key=("SK_ID_BUREAU", "MONTHS_BALANCE"), required=("SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"),
        columns={"SK_ID_BUREAU": LONG, "MONTHS_BALANCE": INT, "STATUS": STRING},
        foreign_keys=(ForeignKey(("SK_ID_BUREAU",), "bureau", ("SK_ID_BUREAU",)),),
        point_in_time_cols=("MONTHS_BALANCE",),
    ),
    "previous_application": TableSpec(
        name="previous_application", file="previous_application.csv", source_id="previous_loan_system",
        not_null=("SK_ID_PREV", "SK_ID_CURR", "NAME_CONTRACT_STATUS", "DAYS_DECISION"),
        origin="ORIGINAL", grain="1 row = 1 previous Home Credit application (SK_ID_PREV)",
        primary_key=("SK_ID_PREV",),
        required=("SK_ID_PREV", "SK_ID_CURR", "NAME_CONTRACT_TYPE", "NAME_CONTRACT_STATUS", "DAYS_DECISION"),
        columns={
            "SK_ID_PREV": LONG, "SK_ID_CURR": LONG, "NAME_CONTRACT_TYPE": STRING, "AMT_ANNUITY": DOUBLE,
            "AMT_APPLICATION": DOUBLE, "AMT_CREDIT": DOUBLE, "AMT_DOWN_PAYMENT": DOUBLE, "AMT_GOODS_PRICE": DOUBLE,
            "WEEKDAY_APPR_PROCESS_START": STRING, "HOUR_APPR_PROCESS_START": INT,
            "FLAG_LAST_APPL_PER_CONTRACT": STRING, "NFLAG_LAST_APPL_IN_DAY": INT, "RATE_DOWN_PAYMENT": DOUBLE,
            "RATE_INTEREST_PRIMARY": DOUBLE, "RATE_INTEREST_PRIVILEGED": DOUBLE, "NAME_CASH_LOAN_PURPOSE": STRING,
            "NAME_CONTRACT_STATUS": STRING, "DAYS_DECISION": INT, "NAME_PAYMENT_TYPE": STRING,
            "CODE_REJECT_REASON": STRING, "NAME_TYPE_SUITE": STRING, "NAME_CLIENT_TYPE": STRING,
            "NAME_GOODS_CATEGORY": STRING, "NAME_PORTFOLIO": STRING, "NAME_PRODUCT_TYPE": STRING,
            "CHANNEL_TYPE": STRING, "SELLERPLACE_AREA": INT, "NAME_SELLER_INDUSTRY": STRING, "CNT_PAYMENT": DOUBLE,
            "NAME_YIELD_GROUP": STRING, "PRODUCT_COMBINATION": STRING, "DAYS_FIRST_DRAWING": DOUBLE,
            "DAYS_FIRST_DUE": DOUBLE, "DAYS_LAST_DUE_1ST_VERSION": DOUBLE, "DAYS_LAST_DUE": DOUBLE,
            "DAYS_TERMINATION": DOUBLE, "NFLAG_INSURED_ON_APPROVAL": DOUBLE,
        },
        foreign_keys=(ForeignKey(("SK_ID_CURR",), "application", ("SK_ID_CURR",)),),
        point_in_time_cols=("DAYS_DECISION",),
        sentinel_cols=("DAYS_FIRST_DRAWING", "DAYS_FIRST_DUE", "DAYS_LAST_DUE_1ST_VERSION", "DAYS_LAST_DUE",
                       "DAYS_TERMINATION"),
        nonnegative_cols=("AMT_APPLICATION", "AMT_CREDIT", "AMT_ANNUITY"),
    ),
    "installments_payments": TableSpec(
        name="installments_payments", file="installments_payments.csv", source_id="installment_payment_system",
        not_null=("SK_ID_PREV", "SK_ID_CURR", "NUM_INSTALMENT_NUMBER", "DAYS_INSTALMENT"),
        origin="ORIGINAL",
        grain="1 row = 1 payment record of 1 installment of a previous loan "
              "(an installment can have several partial-payment rows)",
        primary_key=None,
        required=("SK_ID_PREV", "SK_ID_CURR", "NUM_INSTALMENT_NUMBER", "DAYS_INSTALMENT", "AMT_INSTALMENT"),
        columns={
            "SK_ID_PREV": LONG, "SK_ID_CURR": LONG, "NUM_INSTALMENT_VERSION": DOUBLE, "NUM_INSTALMENT_NUMBER": INT,
            "DAYS_INSTALMENT": DOUBLE, "DAYS_ENTRY_PAYMENT": DOUBLE, "AMT_INSTALMENT": DOUBLE, "AMT_PAYMENT": DOUBLE,
        },
        foreign_keys=(ForeignKey(("SK_ID_PREV",), "previous_application", ("SK_ID_PREV",)),
                      ForeignKey(("SK_ID_CURR",), "application", ("SK_ID_CURR",))),
        point_in_time_cols=("DAYS_INSTALMENT", "DAYS_ENTRY_PAYMENT"),
        nonnegative_cols=("AMT_INSTALMENT", "AMT_PAYMENT"),
        dedup_key=("SK_ID_PREV", "NUM_INSTALMENT_VERSION", "NUM_INSTALMENT_NUMBER", "DAYS_ENTRY_PAYMENT",
                   "AMT_PAYMENT"),
    ),
    "POS_CASH_balance": TableSpec(
        name="POS_CASH_balance", file="POS_CASH_balance.csv", source_id="pos_cash_system", origin="ORIGINAL",
        grain="1 row = 1 previous POS/cash loan x 1 month (SK_ID_PREV, MONTHS_BALANCE)",
        primary_key=None, required=("SK_ID_PREV", "SK_ID_CURR", "MONTHS_BALANCE"),
        columns={
            "SK_ID_PREV": LONG, "SK_ID_CURR": LONG, "MONTHS_BALANCE": INT, "CNT_INSTALMENT": DOUBLE,
            "CNT_INSTALMENT_FUTURE": DOUBLE, "NAME_CONTRACT_STATUS": STRING, "SK_DPD": INT, "SK_DPD_DEF": INT,
        },
        foreign_keys=(ForeignKey(("SK_ID_PREV",), "previous_application", ("SK_ID_PREV",)),
                      ForeignKey(("SK_ID_CURR",), "application", ("SK_ID_CURR",))),
        point_in_time_cols=("MONTHS_BALANCE",),
        nonnegative_cols=("SK_DPD", "SK_DPD_DEF"),
        dedup_key=("SK_ID_PREV", "MONTHS_BALANCE"),
    ),
    "credit_card_balance": TableSpec(
        name="credit_card_balance", file="credit_card_balance.csv", source_id="credit_card_system",
        origin="ORIGINAL", grain="1 row = 1 previous credit card x 1 month (SK_ID_PREV, MONTHS_BALANCE)",
        primary_key=None, required=("SK_ID_PREV", "SK_ID_CURR", "MONTHS_BALANCE"),
        columns={
            "SK_ID_PREV": LONG, "SK_ID_CURR": LONG, "MONTHS_BALANCE": INT, "AMT_BALANCE": DOUBLE,
            "AMT_CREDIT_LIMIT_ACTUAL": DOUBLE, "AMT_DRAWINGS_ATM_CURRENT": DOUBLE, "AMT_DRAWINGS_CURRENT": DOUBLE,
            "AMT_DRAWINGS_OTHER_CURRENT": DOUBLE, "AMT_DRAWINGS_POS_CURRENT": DOUBLE,
            "AMT_INST_MIN_REGULARITY": DOUBLE, "AMT_PAYMENT_CURRENT": DOUBLE, "AMT_PAYMENT_TOTAL_CURRENT": DOUBLE,
            "AMT_RECEIVABLE_PRINCIPAL": DOUBLE, "AMT_RECIVABLE": DOUBLE, "AMT_TOTAL_RECEIVABLE": DOUBLE,
            "CNT_DRAWINGS_ATM_CURRENT": DOUBLE, "CNT_DRAWINGS_CURRENT": DOUBLE, "CNT_DRAWINGS_OTHER_CURRENT": DOUBLE,
            "CNT_DRAWINGS_POS_CURRENT": DOUBLE, "CNT_INSTALMENT_MATURE_CUM": DOUBLE, "NAME_CONTRACT_STATUS": STRING,
            "SK_DPD": INT, "SK_DPD_DEF": INT,
        },
        foreign_keys=(ForeignKey(("SK_ID_PREV",), "previous_application", ("SK_ID_PREV",)),
                      ForeignKey(("SK_ID_CURR",), "application", ("SK_ID_CURR",))),
        point_in_time_cols=("MONTHS_BALANCE",),
        nonnegative_cols=("AMT_CREDIT_LIMIT_ACTUAL", "SK_DPD", "SK_DPD_DEF"),
        dedup_key=("SK_ID_PREV", "MONTHS_BALANCE"),
    ),
    "synthetic_traffic_profile": TableSpec(
        name="synthetic_traffic_profile", file="synthetic_traffic_profile.csv",
        not_null=("SK_ID_CURR", "DATA_ORIGIN"),
        source_id="synthetic_customer_behavior", origin="SYNTHETIC",
        grain="1 row = 1 SK_ID_CURR (SYNTHETIC traffic profile, not real data)",
        primary_key=("SK_ID_CURR",),
        required=("SK_ID_CURR", "TRAFFIC_VIOLATION_COUNT", "MAJOR_VIOLATION_COUNT", "TOTAL_TRAFFIC_FINE",
                  "ACCIDENT_COUNT", "DATA_UPDATED_AT", "DATA_ORIGIN"),
        columns={"SK_ID_CURR": LONG, "TRAFFIC_VIOLATION_COUNT": INT, "MAJOR_VIOLATION_COUNT": INT,
                 "TOTAL_TRAFFIC_FINE": DOUBLE, "ACCIDENT_COUNT": INT, "DATA_UPDATED_AT": "timestamp",
                 "DATA_ORIGIN": STRING, "GENERATOR_VERSION": STRING, "GENERATOR_SEED": LONG},
        foreign_keys=(ForeignKey(("SK_ID_CURR",), "application", ("SK_ID_CURR",)),),
        nonnegative_cols=("TRAFFIC_VIOLATION_COUNT", "MAJOR_VIOLATION_COUNT", "TOTAL_TRAFFIC_FINE", "ACCIDENT_COUNT"),
    ),
}

ORIGINAL_TABLES = [t for t, s in TABLES.items() if s.origin == "ORIGINAL"]
REFERENCE_FILES = ("HomeCredit_columns_description.csv", "sample_submission.csv")
# Processing order: parents before children (FK checks need the parent in silver)
SILVER_ORDER = ["application_train", "application_test", "bureau", "bureau_balance", "previous_application",
                "installments_payments", "POS_CASH_balance", "credit_card_balance", "synthetic_traffic_profile"]


def tables_of_source(source_id: str) -> list[TableSpec]:
    return [s for s in TABLES.values() if s.source_id == source_id]
