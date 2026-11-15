"""Generate supplementary regional macro data (SIMULATED): GDP growth, unemployment rate, CPI.

The Home Credit data has no region names and no real dates, so real statistics cannot be
joined honestly. Regions are identified through REGION_POPULATION_RELATIVE (82 values, each
tied to exactly one REGION_RATING_CLIENT). Values = national trend + region-rating effect +
AR(1) noise, with a fixed seed. TARGET is NOT used, so no artificial signal leaks into the model.

Outputs (data/supplementary), deliberately in 3 different publication formats like real statistics offices:
  region_dim.csv                       region_code, region_population_relative, region_rating, n_applications
  regional_gdp_growth_quarterly.csv    region_code, period (2025-Q3), gdp_growth_yoy_pct, release_date
  regional_unemployment_quarterly.csv  region_code, year, quarter, unemployment_rate_pct, release_date
  regional_cpi_monthly.csv             region_code, month (2025-09), cpi_index (2015-01 = 100), cpi_yoy_pct, release_date

Run: .venv/Scripts/python -I ingestion/gen_macro.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import common as C  # noqa: E402

BASE = {"gdp": 5.5, "unemp": 3.0, "infl": 3.2}  # national baseline (%)
RATING_EFFECT = {1: {"gdp": 1.3, "unemp": -0.9, "infl": 0.4},  # rating 1 = best region
                 2: {"gdp": 0.0, "unemp": 0.0, "infl": 0.0},
                 3: {"gdp": -1.1, "unemp": 1.4, "infl": -0.2}}
GDP_SHOCK = {"2020Q1": -2.0, "2020Q2": -4.5, "2020Q3": -3.0, "2020Q4": -1.8, "2021Q1": -0.8,
             "2021Q2": 1.2, "2021Q3": 0.8}
UNEMP_SHOCK = {"2020Q2": 1.2, "2020Q3": 1.0, "2020Q4": 0.7, "2021Q1": 0.5, "2021Q2": 0.3, "2021Q3": 0.2}


def region_dim():
    cols = ["SK_ID_CURR", "REGION_POPULATION_RELATIVE", "REGION_RATING_CLIENT"]
    apps = pd.concat([pd.read_csv(C.RAW_DIR / f, engine="pyarrow", usecols=cols)
                      for f in C.SOURCES["application"].files])
    apps["region_population_relative"] = apps.REGION_POPULATION_RELATIVE.round(6)
    assert apps.region_population_relative.nunique() == apps.REGION_POPULATION_RELATIVE.nunique()
    assert (apps.groupby("region_population_relative").REGION_RATING_CLIENT.nunique() == 1).all()
    dim = (apps.groupby("region_population_relative")
           .agg(region_rating=("REGION_RATING_CLIENT", "first"), n_applications=("SK_ID_CURR", "size"))
           .sort_index(ascending=False).reset_index())
    dim.insert(0, "region_code", [f"R{i:02d}" for i in range(1, len(dim) + 1)])
    return dim


def ar1(rng, shape, phi, sigma):
    e = rng.normal(0, sigma, shape)
    x = np.zeros(shape)
    for t in range(shape[1]):
        x[:, t] = (phi * x[:, t - 1] if t else 0) + e[:, t]
    return x


def regional(rng, dim, key, fe_sigma):
    """Fixed effect per region: by region rating + a random region-specific part. Returns shape (n_regions, 1)."""
    rating = dim.region_rating.map(lambda r: RATING_EFFECT[r][key]).to_numpy()
    return (rating + rng.normal(0, fe_sigma, len(dim)))[:, None]


DEFAULT_OCC_MEDIANS = {
    "Accountants": 157500.0,
    "Cleaning staff": 112500.0,
    "Cooking staff": 112500.0,
    "Core staff": 157500.0,
    "Drivers": 180000.0,
    "HR staff": 157500.0,
    "High skill tech staff": 180000.0,
    "IT staff": 202500.0,
    "Laborers": 135000.0,
    "Low-skill Laborers": 112500.0,
    "Managers": 225000.0,
    "Medicine staff": 135000.0,
    "Private service staff": 157500.0,
    "Realty agents": 180000.0,
    "Sales staff": 135000.0,
    "Secretaries": 135000.0,
    "Security staff": 135000.0,
    "Waiters/barmen staff": 112500.0,
    "Missing/Unspecified": 135000.0,
}


def build_occupation_benchmarks() -> pd.DataFrame:
    """Compute per-occupation median/mean/IQR salary benchmarks from raw application_train.csv when present."""
    train_csv = C.RAW_DIR / "application_train.csv"
    if train_csv.exists():
        app = pd.read_csv(train_csv, usecols=["OCCUPATION_TYPE", "AMT_INCOME_TOTAL"])
        app["OCCUPATION_TYPE"] = app["OCCUPATION_TYPE"].fillna("Missing/Unspecified")
        grouped = app.groupby("OCCUPATION_TYPE")["AMT_INCOME_TOTAL"]
        bench = pd.DataFrame({
            "OCCUPATION_TYPE": grouped.median().index,
            "occ_median_salary": grouped.median().round(2).to_numpy(),
            "occ_mean_salary": grouped.mean().round(2).to_numpy(),
            "occ_p25_salary": grouped.quantile(0.25).round(2).to_numpy(),
            "occ_p75_salary": grouped.quantile(0.75).round(2).to_numpy(),
            "occ_applicant_count": grouped.size().to_numpy(),
            "salary_scale_source": "application_train.csv",
        })
        return bench.sort_values("OCCUPATION_TYPE").reset_index(drop=True)

    rows = []
    for occ, med in sorted(DEFAULT_OCC_MEDIANS.items()):
        rows.append({
            "OCCUPATION_TYPE": occ,
            "occ_median_salary": med,
            "occ_mean_salary": round(med * 1.12, 2),
            "occ_p25_salary": round(med * 0.75, 2),
            "occ_p75_salary": round(med * 1.35, 2),
            "occ_applicant_count": 0,
            "salary_scale_source": "fallback_benchmark",
        })
    return pd.DataFrame(rows)


def main():
    rng = np.random.default_rng(C.SEED)
    C.SUPP_DIR.mkdir(parents=True, exist_ok=True)
    dim = region_dim()
    n = len(dim)
    sensitivity = rng.uniform(0.7, 1.3, n)[:, None]  # how strongly each region feels common shocks

    quarters = pd.period_range("2016Q1", "2025Q4", freq="Q")
    q = np.arange(len(quarters))
    gdp_shock = np.array([GDP_SHOCK.get(str(p), 0.0) for p in quarters])
    gdp = (BASE["gdp"] + 0.5 * np.sin(2 * np.pi * q / 16) + sensitivity * gdp_shock
           + regional(rng, dim, "gdp", 0.6) + ar1(rng, (n, len(q)), 0.6, 0.5))
    unemp_shock = np.array([UNEMP_SHOCK.get(str(p), 0.0) for p in quarters])
    unemp = np.clip(BASE["unemp"] - 0.03 * q / 4 + sensitivity * unemp_shock
                    + regional(rng, dim, "unemp", 0.4) + ar1(rng, (n, len(q)), 0.7, 0.15), 0.8, None)

    months = pd.period_range("2015-01", "2025-12", freq="M")  # 2015 included so CPI YoY is defined from 2016
    m = np.arange(len(months))
    bump = lambda center, width: np.exp(-(((m - months.get_loc(pd.Period(center, "M"))) / width) ** 2))  # noqa: E731
    infl = (BASE["infl"] + 0.6 * np.sin(2 * np.pi * m / 60) - 1.2 * bump("2020-09", 6) + 1.8 * bump("2022-09", 7)
            + regional(rng, dim, "infl", 0.3) + ar1(rng, (n, len(m)), 0.8, 0.25))
    index = 100 * np.exp(np.cumsum(np.concatenate([np.zeros((n, 1)), infl[:, 1:] / 1200], axis=1), axis=1))
    yoy = (index[:, 12:] / index[:, :-12] - 1) * 100

    def long(values, periods):
        return pd.DataFrame({"region_code": np.repeat(dim.region_code.to_numpy(), len(periods)),
                             "p": np.tile(periods, n), "value": values.ravel().round(2)})

    g = long(gdp, quarters)
    g = pd.DataFrame({"region_code": g.region_code, "period": [f"{p.year}-Q{p.quarter}" for p in g.p],
                      "gdp_growth_yoy_pct": g.value,
                      "release_date": [(p.end_time.normalize() + pd.Timedelta(days=30)).date() for p in g.p]})
    u = long(unemp, quarters)
    u = pd.DataFrame({"region_code": u.region_code, "year": [p.year for p in u.p], "quarter": [p.quarter for p in u.p],
                      "unemployment_rate_pct": u.value,
                      "release_date": [(p.end_time.normalize() + pd.Timedelta(days=45)).date() for p in u.p]})
    c = long(index[:, 12:], months[12:])
    c = pd.DataFrame({"region_code": c.region_code, "month": [f"{p.year}-{p.month:02d}" for p in c.p],
                      "cpi_index": c.value, "cpi_yoy_pct": yoy.ravel().round(2),
                      "release_date": [((p + 1).start_time + pd.Timedelta(days=5)).date() for p in c.p]})
    occ_bench = build_occupation_benchmarks()

    outputs = {"region_dim.csv": dim, "regional_gdp_growth_quarterly.csv": g,
               "regional_unemployment_quarterly.csv": u, "regional_cpi_monthly.csv": c,
               "occupation_benchmarks.csv": occ_bench}
    for name, df in outputs.items():
        df.to_csv(C.SUPP_DIR / name, index=False)
        print(f"{name:38s} {len(df):>6,} rows")

    by_rating = dim.set_index("region_code").region_rating
    last = pd.DataFrame({
        "GDP 2025 (%)": g[g.period.str.startswith("2025")].groupby("region_code").gdp_growth_yoy_pct.mean(),
        "Unemployment 2025 (%)": u[u.year == 2025].groupby("region_code").unemployment_rate_pct.mean(),
        "CPI yoy 2025 (%)": c[c.month.str.startswith("2025")].groupby("region_code").cpi_yoy_pct.mean(),
    }).groupby(by_rating).mean().round(2)
    print(f"\nRegions per rating: {by_rating.value_counts().sort_index().to_dict()}")
    print("2025 averages by region rating (REGION_RATING_CLIENT):")
    print(last.to_string())
    print(f"\nRanges: GDP {g.gdp_growth_yoy_pct.min()}..{g.gdp_growth_yoy_pct.max()} | "
          f"unemployment {u.unemployment_rate_pct.min()}..{u.unemployment_rate_pct.max()} | "
          f"CPI yoy {c.cpi_yoy_pct.min()}..{c.cpi_yoy_pct.max()}")


if __name__ == "__main__":
    main()
