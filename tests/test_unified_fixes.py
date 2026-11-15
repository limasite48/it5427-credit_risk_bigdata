"""Unit tests verifying cross-component fixes and unified pipeline integration."""
from __future__ import annotations

import pandas as pd

from src.config import BASE_DIR
from src.data.macro_loader import MacroFeatureJoiner
from src.integration.kafka_consumer_scorer import RealTimeCreditScorer


def test_macro_joiner_point_in_time_and_occupation_salary(tmp_path):
    """Verify application_dates prevents lookahead bias and occupation salary benchmarks join cleanly."""
    supp_src = BASE_DIR / "ingestion-minio-kafka" / "data" / "supplementary"
    # Copy core CSVs into tmp_path and write a deterministic occupation_benchmarks.csv
    for name in (
        "region_dim.csv",
        "regional_gdp_growth_quarterly.csv",
        "regional_unemployment_quarterly.csv",
        "regional_cpi_monthly.csv",
    ):
        (tmp_path / name).write_text((supp_src / name).read_text(encoding="utf-8"), encoding="utf-8")

    pd.DataFrame(
        [
            {"OCCUPATION_TYPE": "Managers", "occ_median_salary": 225000.0},
            {"OCCUPATION_TYPE": "Laborers", "occ_median_salary": 135000.0},
        ]
    ).to_csv(tmp_path / "occupation_benchmarks.csv", index=False)

    joiner = MacroFeatureJoiner(tmp_path)
    region_pop = float(joiner.region_dim["region_population_relative"].iloc[0])
    region_code = str(joiner.region_dim["region_code"].iloc[0])

    df = pd.DataFrame(
        {
            "SK_ID_CURR": [1, 2],
            "REGION_POPULATION_RELATIVE": [region_pop, region_pop],
            "OCCUPATION_TYPE": ["Managers", "Laborers"],
            "AMT_INCOME_TOTAL": [450000.0, 135000.0],
        }
    )
    app_dates = pd.Series(["2020-07-15", "2025-11-15"])

    pit = joiner.join_macro_features(df, application_dates=app_dates)

    assert "occ_median_salary" in pit.columns
    assert "income_to_occ_median_ratio" in pit.columns
    assert pit.loc[0, "occ_median_salary"] == 225000.0
    assert pit.loc[0, "income_to_occ_median_ratio"] == 2.0
    assert pit.loc[1, "income_to_occ_median_ratio"] == 1.0

    # Verify each date receives the latest macro release strictly <= its application_date (no lookahead)
    expected_2020_gdp = (
        joiner.gdp[(joiner.gdp["region_code"] == region_code) & (joiner.gdp["release_date"] <= pd.Timestamp("2020-07-15"))]
        .sort_values("release_date")
        .iloc[-1]["gdp_growth_yoy_pct"]
    )
    expected_nov_2025_gdp = (
        joiner.gdp[(joiner.gdp["region_code"] == region_code) & (joiner.gdp["release_date"] <= pd.Timestamp("2025-11-15"))]
        .sort_values("release_date")
        .iloc[-1]["gdp_growth_yoy_pct"]
    )
    assert pit.loc[0, "gdp_growth_yoy_pct"] == expected_2020_gdp
    assert pit.loc[1, "gdp_growth_yoy_pct"] == expected_nov_2025_gdp
    assert pit.loc[0, "gdp_growth_yoy_pct"] != pit.loc[1, "gdp_growth_yoy_pct"]


def test_real_time_scorer_dry_run_handles_zero_and_positive_limits():
    scorer = RealTimeCreditScorer()
    empty = scorer.run_dry_run_simulation(sample_limit=0)
    assert empty == []

    decisions = scorer.run_dry_run_simulation(sample_limit=2)
    assert len(decisions) == 2
    assert all("default_probability" in d and "risk_tier" in d for d in decisions)
