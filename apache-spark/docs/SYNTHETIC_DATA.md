# SYNTHETIC DATA – Synthetic Customer Behaviour (traffic profile)

> ⚠️ **SYNTHETIC – NOT REAL DATA.** These are not real traffic violation records for any person. They must not be used or presented as real data. Their sole purpose is to demonstrate integrating an external data source into the pipeline.

## 1. Why Only 4 Fields Are Added

Home Credit already includes occupation, income, employment tenure, household size, loan details, and full credit/installment histories. The synthetic source adds one **non-financial behavioral data domain** missing from Home Credit, at the minimum scope needed to demonstrate multi-source integration:

| Field | Type | Meaning |
|---|---|---|
| `TRAFFIC_VIOLATION_COUNT` | `int` | Total number of traffic violations |
| `MAJOR_VIOLATION_COUNT` | `int` | Number of major violations (≤ total) |
| `TOTAL_TRAFFIC_FINE` | `double` | Total fine amount (simulated VND-like units) |
| `ACCIDENT_COUNT` | `int` | Number of accidents |
| `DATA_UPDATED_AT` | `timestamp` | Simulated refresh timestamp |
| `DATA_ORIGIN` | `string` | Always `SYNTHETIC` |
| `GENERATOR_VERSION`, `GENERATOR_SEED` | | For reproducibility |

## 2. Generation Logic & Distributions

| Field | Distribution | Notes |
|---|---|---|
| `TRAFFIC_VIOLATION_COUNT` | `NegativeBinomial(r=0.6, p=0.6)`, mean 0.4 | Many zeros, long tail (over-dispersion) |
| `MAJOR_VIOLATION_COUNT` | `Binomial(TRAFFIC_VIOLATION_COUNT, 0.12)` | Guarantees ≤ total |
| `TOTAL_TRAFFIC_FINE` | Sum of randomly sampled fines from fixed tiers: minor `{0.3; 0.5; 0.8; 1; 2}`M, major `{6; 8; 12; 18}`M | Consistent with violation counts |
| `ACCIDENT_COUNT` | `Poisson(0.03 + 0.10 × MAJOR_VIOLATION_COUNT)` | Depends only on other synthetic fields |
| `DATA_UPDATED_AT` | Uniform across year 2025 | |

These parameters are team-chosen assumptions for demonstration, not derived from real statistics.

## 3. Compliance Rules

- **Generated only for real `SK_ID_CURR` keys** (`application_train` ∪ `application_test`), one row per customer (verified by tests).
- **Never reads any Home Credit column other than `SK_ID_CURR`** and never modifies original data.
- **No spurious correlation with credit risk.** Values are generated independently of `TARGET` and all Home Credit attributes. Dependencies exist only among synthetic fields (`major <= total`, accidents slightly increase with major violations). Any model trained on this source will find **no** predictive signal—which is the intended behavior.
- **Reproducible:** each customer uses a per-customer RNG seeded with `(global_seed, SK_ID_CURR)`. Re-runs yield identical outputs, and adding new customers does not alter existing customers' values (verified by determinism tests).
- **Stored separately:** `data/synthetic/` → `bronze/synthetic/…`, `_origin = SYNTHETIC` in Silver, `(SYNTHETIC)` label in `DATA_DICTIONARY`. Gold adds a `has_synthetic_profile` flag.

## 4. Command

```bash
python -m ingestion.synthetic_source.generator --raw-dir data/raw --out data/synthetic --seed 20261113
```
Outputs `synthetic_traffic_profile.csv` and `synthetic_traffic_profile.meta.json` (summary statistics of the run).
