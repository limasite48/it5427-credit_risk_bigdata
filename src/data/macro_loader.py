"""
Point-in-Time Macroeconomic & Occupation Salary Benchmark Loader
Integrates supplementary regional indicators and occupation benchmarks into the unified ML pipeline.
"""
from pathlib import Path
from typing import Optional, Union
import numpy as np
import pandas as pd

from ..config import BASE_DIR

DEFAULT_SUPP_DIR = BASE_DIR / "ingestion-minio-kafka" / "data" / "supplementary"


class MacroFeatureJoiner:
    """
    Loads regional macroeconomic and occupation benchmark data and joins it point-in-time
    to borrower applications to prevent future lookahead bias.
    """

    def __init__(self, supp_dir: Optional[Union[str, Path]] = None):
        self.supp_dir = Path(supp_dir) if supp_dir else DEFAULT_SUPP_DIR
        if not self.supp_dir.exists():
            raise FileNotFoundError(
                f"Supplementary data folder not found at {self.supp_dir}. "
                f"Run 'python ingestion-minio-kafka/ingestion/gen_macro.py' first."
            )

        self.region_dim = pd.read_csv(self.supp_dir / "region_dim.csv")
        self.gdp = pd.read_csv(self.supp_dir / "regional_gdp_growth_quarterly.csv")
        self.unemp = pd.read_csv(self.supp_dir / "regional_unemployment_quarterly.csv")
        self.cpi = pd.read_csv(self.supp_dir / "regional_cpi_monthly.csv")

        occ_path = self.supp_dir / "occupation_benchmarks.csv"
        self.occupation_benchmarks = pd.read_csv(occ_path) if occ_path.exists() else None

        # Convert release dates to timezone-naive datetime64[ns] for deterministic point-in-time joins
        self.gdp["release_date"] = pd.to_datetime(self.gdp["release_date"]).dt.tz_localize(None)
        self.unemp["release_date"] = pd.to_datetime(self.unemp["release_date"]).dt.tz_localize(None)
        self.cpi["release_date"] = pd.to_datetime(self.cpi["release_date"]).dt.tz_localize(None)

        self.gdp = self.gdp.sort_values(["release_date", "region_code"]).reset_index(drop=True)
        self.unemp = self.unemp.sort_values(["release_date", "region_code"]).reset_index(drop=True)
        self.cpi = self.cpi.sort_values(["release_date", "region_code"]).reset_index(drop=True)

        # Precompute latest values per region (fallback when exact application dates are absent)
        self.latest_gdp = (
            self.gdp.groupby("region_code", as_index=False).last()[["region_code", "gdp_growth_yoy_pct"]]
        )
        self.latest_unemp = (
            self.unemp.groupby("region_code", as_index=False).last()[["region_code", "unemployment_rate_pct"]]
        )
        self.latest_cpi = (
            self.cpi.groupby("region_code", as_index=False).last()[["region_code", "cpi_index", "cpi_yoy_pct"]]
        )

        self.latest_macro = (
            self.region_dim[["region_code", "region_population_relative"]]
            .merge(self.latest_gdp, on="region_code")
            .merge(self.latest_unemp, on="region_code")
            .merge(self.latest_cpi, on="region_code")
        )

    def get_region_dim(self) -> pd.DataFrame:
        """Return the 82-region dimension table."""
        return self.region_dim.copy()

    def _asof_join_indicator(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        value_cols: list[str],
    ) -> pd.DataFrame:
        """Backward point-in-time merge_asof on region_code and release_date <= _app_date."""
        valid = left.dropna(subset=["region_code", "_app_date"]).sort_values("_app_date")
        missing = left[left["region_code"].isna() | left["_app_date"].isna()]

        right_sorted = right[["region_code", "release_date", *value_cols]].sort_values("release_date")
        if not valid.empty:
            joined = pd.merge_asof(
                valid,
                right_sorted,
                left_on="_app_date",
                right_on="release_date",
                by="region_code",
                direction="backward",
            ).drop(columns=["release_date"])
        else:
            joined = valid.copy()
            for col in value_cols:
                joined[col] = np.nan

        if not missing.empty:
            empty_missing = missing.copy()
            for col in value_cols:
                empty_missing[col] = np.nan
            combined = pd.concat([joined, empty_missing], axis=0)
        else:
            combined = joined
        return combined.sort_values("_orig_row_order")

    def join_macro_features(
        self,
        df: pd.DataFrame,
        application_dates: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        Enriches an applications DataFrame with:
        - region_code, gdp_growth_yoy_pct, unemployment_rate_pct, cpi_index, cpi_yoy_pct
          (point-in-time as-of join when `application_dates` is supplied, else latest release)
        - occ_median_salary and income_to_occ_median_ratio when occupation benchmarks are available
        """
        data = df.copy()
        data["_orig_row_order"] = np.arange(len(data))

        if "REGION_POPULATION_RELATIVE" in data.columns:
            data["_pop_rel_round"] = pd.to_numeric(data["REGION_POPULATION_RELATIVE"], errors="coerce").round(6)
            if application_dates is not None and pd.to_datetime(application_dates, errors="coerce").notna().any():
                app_dt = pd.to_datetime(application_dates, errors="coerce")
                if getattr(app_dt.dt, "tz", None) is not None:
                    app_dt = app_dt.dt.tz_convert(None)
                data["_app_date"] = app_dt.to_numpy()
                data = data.merge(
                    self.region_dim[["region_code", "region_population_relative"]],
                    left_on="_pop_rel_round",
                    right_on="region_population_relative",
                    how="left",
                )
                data = self._asof_join_indicator(data, self.gdp, ["gdp_growth_yoy_pct"])
                data = self._asof_join_indicator(data, self.unemp, ["unemployment_rate_pct"])
                data = self._asof_join_indicator(data, self.cpi, ["cpi_index", "cpi_yoy_pct"])
            else:
                data = data.merge(
                    self.latest_macro,
                    left_on="_pop_rel_round",
                    right_on="region_population_relative",
                    how="left",
                )

        if self.occupation_benchmarks is not None and "OCCUPATION_TYPE" in data.columns:
            data["_occ_key"] = data["OCCUPATION_TYPE"].astype("string").fillna("Missing/Unspecified").astype(str)
            data = data.merge(
                self.occupation_benchmarks[["OCCUPATION_TYPE", "occ_median_salary"]],
                left_on="_occ_key",
                right_on="OCCUPATION_TYPE",
                how="left",
                suffixes=("", "_bench"),
            )
            if "OCCUPATION_TYPE_bench" in data.columns:
                data = data.drop(columns=["OCCUPATION_TYPE_bench"])
            if "AMT_INCOME_TOTAL" in data.columns:
                inc = pd.to_numeric(data["AMT_INCOME_TOTAL"], errors="coerce")
                med = pd.to_numeric(data["occ_median_salary"], errors="coerce").replace(0, np.nan)
                data["income_to_occ_median_ratio"] = (inc / med).round(4)

        data = data.sort_values("_orig_row_order")
        cols_to_drop = [
            c for c in ["_orig_row_order", "_pop_rel_round", "region_population_relative", "_app_date", "_occ_key"]
            if c in data.columns
        ]
        return data.drop(columns=cols_to_drop).reset_index(drop=True)
