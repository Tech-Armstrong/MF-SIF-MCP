"""
make_sample_data.py  —  SYNTHETIC fixture generator (DEV / SMOKE-TEST ONLY)
===========================================================================

Writes two parquet files the standalone NAV MCP server can read:
    ./data/nav_history.parquet     scheme_code, nav_date, nav
    ./data/scheme_master.parquet   scheme_code, scheme_name, fund_house, category

The NAVs are a random walk — they are NOT real fund data and must never be used
for anything but verifying that the server runs. Replace ./data/*.parquet with
real AMFI-derived parquet (or set AZURE_STORAGE_CONNECTION_STRING and point the
path env vars at az:// URLs) for real answers.

Run:
    python make_sample_data.py
"""

import os
import random
from datetime import date, timedelta

import duckdb

random.seed(42)  # reproducible fixture

# (scheme_code, scheme_name, fund_house, category, start_nav, daily_drift, vol, last_nav_lag_days)
SCHEMES = [
    ("100001", "Alpha Bluechip Fund - Direct Growth",        "Alpha AMC",  "Large Cap",         100.0, 0.00045, 0.010, 0),
    ("100002", "Alpha Bluechip Fund - Regular Growth",       "Alpha AMC",  "Large Cap",         100.0, 0.00040, 0.010, 0),
    ("100003", "Beta Emerging Equity Fund - Direct Growth",  "Beta AMC",   "Mid Cap",            50.0, 0.00060, 0.016, 0),
    ("100004", "Beta Emerging Equity Fund - Direct IDCW",    "Beta AMC",   "Mid Cap",            50.0, 0.00058, 0.016, 0),
    ("100005", "Gamma Smallcap Fund - Direct Growth",        "Gamma AMC",  "Small Cap",          25.0, 0.00070, 0.022, 0),
    ("100006", "Delta Balanced Advantage Fund - Direct Growth", "Delta AMC", "Balanced Advantage", 30.0, 0.00035, 0.008, 0),
    ("100007", "Delta Balanced Advantage Fund - Regular Growth", "Delta AMC", "Balanced Advantage", 30.0, 0.00030, 0.008, 0),
    ("100008", "Epsilon Liquid Fund - Direct Growth",        "Epsilon AMC", "Liquid",           1000.0, 0.00018, 0.0006, 0),
    # deliberately stale: last NAV lags 20 days, to exercise the staleness guard
    ("100009", "Zeta Frozen Largecap Fund - Direct Growth",  "Zeta AMC",   "Large Cap",         100.0, 0.00040, 0.010, 20),
]

YEARS_HISTORY = 6
TODAY = date.today()


def business_days(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri
            yield d
        d += timedelta(days=1)


def main() -> None:
    os.makedirs("data", exist_ok=True)
    start_hist = TODAY - timedelta(days=int(YEARS_HISTORY * 365.25))

    master_rows, nav_rows = [], []
    for code, name, house, cat, nav0, drift, vol, lag in SCHEMES:
        master_rows.append((code, name, house, cat))
        last_day = TODAY - timedelta(days=lag)
        nav = nav0
        for d in business_days(start_hist, last_day):
            nav *= (1 + drift + random.gauss(0, vol))
            nav_rows.append((code, d, round(nav, 4)))

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE scheme_master(scheme_code VARCHAR, scheme_name VARCHAR, "
                "fund_house VARCHAR, category VARCHAR);")
    con.executemany("INSERT INTO scheme_master VALUES (?, ?, ?, ?);", master_rows)
    con.execute("CREATE TABLE nav_history(scheme_code VARCHAR, nav_date DATE, nav DOUBLE);")
    con.executemany("INSERT INTO nav_history VALUES (?, ?, ?);", nav_rows)

    con.execute("COPY scheme_master TO './data/scheme_master.parquet' (FORMAT PARQUET);")
    con.execute("COPY nav_history   TO './data/nav_history.parquet'   (FORMAT PARQUET);")

    print(f"Wrote ./data/scheme_master.parquet  ({len(master_rows)} schemes)")
    print(f"Wrote ./data/nav_history.parquet    ({len(nav_rows)} NAV rows)")
    print("SYNTHETIC data — replace with real AMFI parquet before trusting any number.")


if __name__ == "__main__":
    main()
