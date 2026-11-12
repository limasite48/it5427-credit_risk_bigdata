# Cross-Team Data Contract: Track B Requirements

This document outlines the data schemas, aggregations, and deliverables required from **Member 1 (Data Collection & Storage)** and **Member 2 (Data Integration & Processing)** to enable **Track B (Multi-Source Retraining, Correlation Analysis & Real-Time Scoring)**.

---

## 1. Requirements for Member 1 (Data Collection & Storage)

### 1.1 MinIO Bucket Hierarchy
MinIO should expose an S3-compatible API structured according to the Medallion architecture:
* `s3://credit-risk/raw/`: Original raw Kaggle CSV dumps and incoming raw streams.
* `s3://credit-risk/clean/`: Deduplicated, validated files with basic type casting.
* `s3://credit-risk/processed/`: The merged feature store prepared by Member 2.

### 1.2 Supplementary Macroeconomic Data
To demonstrate true heterogeneous multi-source integration, please acquire or generate external indicators joinable via `REGION_RATING_CLIENT` or application date:
1. **Regional Macroeconomic Indicators:**
   * `region_gdp_growth_rate`: Annual regional GDP growth percentage.
   * `region_unemployment_rate`: Regional unemployment percentage.
   * `region_cpi_inflation`: Consumer price inflation index for the region.
2. **Occupation Benchmarks:**
   * `occ_median_salary`: Market median salary per occupation category for debt-to-income stress testing.

### 1.3 Kafka Event Streaming Specification
To support simulated real-time inference in Track B:
* **Topic: `credit.application.inbound`**
  * Emits simulated incoming loan applications (derived from `application_test.csv`).
  * Payload: JSON dictionary containing applicant demographics, income, and requested credit.
* **Topic: `credit.installment.events`**
  * Emits repayment and delinquency events (derived from `installments_payments.csv`).
  * Enables dynamic recalculation of delinquent flags.

---

## 2. Requirements for Member 2 (Data Integration & Processing)

### 2.1 Primary Deliverable
* **Storage Location:** `s3://credit-risk/processed/` (or local Parquet/Delta: `data/processed/`).
* **Format:** Apache Parquet or Delta Lake format.
* **Partitioning / Keys:** Primary key `SK_ID_CURR`.
* **Output Tables:**
  * `processed_train.parquet`: Includes `TARGET` label (~307,511 rows).
  * `processed_test.parquet`: Inference set (~48,744 rows).

### 2.2 Required Aggregation Features by Table

All historical aggregations must strictly use records where `DAYS_* < 0` (prior to current loan application) to prevent target leakage.

#### A. Credit Bureau (`bureau.csv` & `bureau_balance.csv`)
* `bureau_total_credit_count`: `COUNT(SK_ID_BUREAU)` per `SK_ID_CURR`.
* `bureau_active_credit_count`: Count of loans where `CREDIT_ACTIVE == 'Active'`.
* `bureau_total_debt_amt`: `SUM(AMT_CREDIT_SUM_DEBT)`.
* `bureau_debt_to_credit_ratio`: `SUM(AMT_CREDIT_SUM_DEBT) / SUM(AMT_CREDIT_SUM)`.
* `bureau_overdue_amt_sum`: `SUM(AMT_CREDIT_SUM_OVERDUE)`.
* `bureau_max_dpd`: Maximum days past due recorded across all months in `bureau_balance`.

#### B. Past Applications (`previous_application.csv`)
* `prev_app_count`: `COUNT(SK_ID_PREV)`.
* `prev_app_refused_count`: Count where `NAME_CONTRACT_STATUS == 'Refused'`.
* `prev_app_refusal_rate`: Refused count / Total previous applications.
* `prev_credit_to_applied_ratio`: `SUM(AMT_CREDIT) / SUM(AMT_APPLICATION)`.
* `prev_days_decision_min`: Days since the most recent past application.

#### C. Point of Sale & Cash Loans (`POS_CASH_balance.csv`)
* `pos_cash_count`: Total monthly snapshot records.
* `pos_cash_dpd_max`: Maximum `SK_DPD` across all contracts.
* `pos_cash_dpd_mean`: Average `SK_DPD`.
* `pos_completed_contracts_count`: Count where `NAME_CONTRACT_STATUS == 'Completed'`.

#### D. Installment Payments (`installments_payments.csv`)
* `inst_late_payment_count`: Total payments where `DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT`.
* `inst_max_days_late`: `MAX(DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT)`.
* `inst_underpayment_count`: Count where `AMT_PAYMENT < AMT_INSTALMENT`.
* `inst_underpayment_total_amt`: `SUM(AMT_INSTALMENT - AMT_PAYMENT)` for underpaid installments.

#### E. Credit Card Balances (`credit_card_balance.csv`)
* `cc_monthly_balance_mean`: `AVG(AMT_BALANCE)`.
* `cc_utilization_ratio_max`: `MAX(AMT_BALANCE / AMT_CREDIT_LIMIT_ACTUAL)`.
* `cc_drawing_count_sum`: `SUM(CNT_DRAWINGS_ATM_CURRENT + CNT_DRAWINGS_CURRENT)`.
* `cc_dpd_max`: Maximum `SK_DPD` on revolving credit card accounts.

### 2.3 Quality Standards
1. **Null Policy:** For borrowers with no records in an auxiliary table (e.g. no credit card), counts should be `0` rather than `NaN`.
2. **Column Naming:** Follow the explicit prefixes above (`bureau_*`, `prev_*`, `pos_*`, `inst_*`, `cc_*`).
3. **Data Dictionary:** Provide a mapping table or markdown document describing the aggregation formulas applied in Spark.
