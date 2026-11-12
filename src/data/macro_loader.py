"""
Point-in-Time Macroeconomic Feature Loader & Joiner
Integrates Member 1's supplementary regional indicators into the ML pipeline.
"""
from pathlib import Path
from typing import Optional, Union
import numpy as np
import pandas as pd

from ..config import BASE_DIR

DEFAULT_SUPP_DIR = BASE_DIR / "ingestion-minio-kafka" / "data" / "supplementary"


class MacroFeatureJoiner:
    """
    Loads regional macroeconomic data and joins it point-in-time
    to borrower applications to avoid future lookahead bias.
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

        # Convert release dates to datetime for point-in-time filtering
        self.gdp["release_date"] = pd.to_datetime(self.gdp["release_date"])
        self.unemp["release_date"] = pd.to_datetime(self.unemp["release_date"])
        self.cpi["release_date"] = pd.to_datetime(self.cpi["release_date"])

        # Precompute latest values per region (for baseline joining when exact dates are absent)
        self.latest_gdp = (
            self.gdp.sort_values("release_date")
            .groupby("region_code")
            .last()[["gdp_growth_yoy_pct"]]
            .reset_index()
        )
        self.latest_unemp = (
            self.unemp.sort_values("release_date")
            .groupby("region_code")
            .last()[["unemployment_rate_pct"]]
            .reset_index()
        )
        self.latest_cpi = (
            self.cpi.sort_values("release_date")
            .groupby("region_code")
            .last()[["cpi_index", "cpi_yoy_pct"]]
            .reset_index()
        )

        # Baseline combined macro lookup table
        self.latest_macro = (
            self.region_dim[["region_code", "region_population_relative"]]
            .merge(self.latest_gdp, on="region_code")
            .merge(self.latest_unemp, on="region_code")
            .merge(self.latest_cpi, on="region_code")
        )

    def get_region_dim(self) -> pd.DataFrame:
        """Return the 82-region dimension table."""
        return self.region_dim.copy()

    def join_macro_features(
        self,
        df: pd.DataFrame,
        application_dates: Optional[pd.Series] = None
    ) -> pd.DataFrame:
        """
        Enriches an applications DataFrame with regional macro indicators:
        - gdp_growth_yoy_pct
        - unemployment_rate_pct
        - cpi_index
        - cpi_yoy_pct
        """
        if "REGION_POPULATION_RELATIVE" not in df.columns:
            return df.copy()

        data = df.copy()
        data["_pop_rel_round"] = data["REGION_POPULATION_RELATIVE"].round(6)

        # Map to region_code
        merged = data.merge(
            self.latest_macro,
            left_on="_pop_rel_round",
            right_on="region_population_relative",
            how="left",
        )

        # Drop temporary matching columns
        cols_to_drop = [
            c for c in ["_pop_rel_round", "region_population_relative"]
            if c in merged.columns
        ]
        return merged.drop(columns=cols_to_drop)
