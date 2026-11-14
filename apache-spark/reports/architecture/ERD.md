# ERD – Home Credit tables and logical sources

Measured on: **TEST FIXTURE (random values, NOT Home Credit)** (`/home/claude/credit-risk-bigdata/data/fixture/raw`), 2026-11-13T00:54:39+00:00.
Edge labels carry the orphan count and the maximum number of children per parent observed in the data.

```mermaid
erDiagram
    APPLICATION ||--o{ BUREAU : "SK_ID_CURR (orphans 4, max 10/parent)"
    BUREAU ||--o{ BUREAU_BALANCE : "SK_ID_BUREAU (orphans 3, max 97/parent)"
    APPLICATION ||--o{ PREVIOUS_APPLICATION : "SK_ID_CURR (orphans 0, max 7/parent)"
    PREVIOUS_APPLICATION ||--o{ INSTALLMENTS_PAYMENTS : "SK_ID_PREV (orphans 5, max 30/parent)"
    PREVIOUS_APPLICATION ||--o{ POS_CASH_BALANCE : "SK_ID_PREV (orphans 0, max 97/parent)"
    PREVIOUS_APPLICATION ||--o{ CREDIT_CARD_BALANCE : "SK_ID_PREV (orphans 0, max 97/parent)"
    APPLICATION ||--o| SYNTHETIC_TRAFFIC_PROFILE : "SK_ID_CURR (SYNTHETIC)"
    APPLICATION { bigint SK_ID_CURR PK "train + test" int TARGET "train only, label" }
    BUREAU { bigint SK_ID_BUREAU PK bigint SK_ID_CURR FK }
    BUREAU_BALANCE { bigint SK_ID_BUREAU FK int MONTHS_BALANCE "PK part" string STATUS }
    PREVIOUS_APPLICATION { bigint SK_ID_PREV PK bigint SK_ID_CURR FK }
    INSTALLMENTS_PAYMENTS { bigint SK_ID_PREV FK bigint SK_ID_CURR "FK, redundant" int NUM_INSTALMENT_NUMBER }
    POS_CASH_BALANCE { bigint SK_ID_PREV FK bigint SK_ID_CURR "FK, redundant" int MONTHS_BALANCE }
    CREDIT_CARD_BALANCE { bigint SK_ID_PREV FK bigint SK_ID_CURR "FK, redundant" int MONTHS_BALANCE }
    SYNTHETIC_TRAFFIC_PROFILE { bigint SK_ID_CURR PK "FK to application" }
```
