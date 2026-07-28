"""
demo.py  —  run the NAV MCP tools directly and print readable output.

No MCP client needed: this calls the same functions the server exposes and
formats the results. It auto-generates the synthetic fixture if ./data is empty.

    python demo.py
"""

import os

# Ensure the fixture exists BEFORE importing server (server opens its DuckDB
# connection at import time and needs the parquet to be present).
if not (os.path.exists("data/nav_history.parquet")
        and os.path.exists("data/scheme_master.parquet")):
    print(">> No data found — generating synthetic fixture...\n")
    import make_sample_data
    make_sample_data.main()
    print()

import server  # noqa: E402


def rule(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def print_returns(payload: dict) -> None:
    hdr = f"{'code':<8} {'fund':<44} {'ret%':>9} {'cagr%':>9}"
    stale_cols = any("stale" in r for r in payload["results"])
    if stale_cols:
        hdr += f" {'lag':>4} {'stale':>6}"
    print(hdr)
    print("-" * len(hdr))
    for r in payload["results"]:
        if r["error"]:
            print(f"{r['scheme_code']:<8} {'— ' + r['error']:<44}")
            continue
        name = (r["scheme_name"] or "")[:43]
        ret = f"{r['return_pct']:.2f}" if r["return_pct"] is not None else "—"
        cagr = f"{r['cagr_pct']:.2f}" if r["cagr_pct"] is not None else "—"
        line = f"{r['scheme_code']:<8} {name:<44} {ret:>9} {cagr:>9}"
        if stale_cols:
            lag = r.get("as_of_lag_days")
            lag_s = str(lag) if lag is not None else "—"
            line += f" {lag_s:>4} {('YES' if r.get('stale') else 'no'):>6}"
        print(line)


# 1. discovery
rule("1. list_categories()")
print(", ".join(server.list_categories()["categories"]))

# 2. name -> code resolver
rule("2. search_funds('emerging equity')")
for c in server.search_funds("emerging equity")["candidates"]:
    print(f"  {c['scheme_code']}  {c['scheme_name']}  (score {c['score']})")

# 3. one fund, short window -> CAGR is null
rule("3. get_fund_returns('100003', '1Y')   [<=1Y so cagr is null]")
print_returns(server.get_fund_returns("100003", "1Y"))

# 4. one fund, long window -> CAGR populated
rule("4. get_fund_returns('100003', '5Y')   [>1Y so cagr shows]")
print_returns(server.get_fund_returns("100003", "5Y"))

# 5. many funds at once (+ a deliberately bad code)
rule("5. get_fund_returns(['100001','100003','100005','999999'], '3Y')")
print_returns(server.get_fund_returns(["100001", "100003", "100005", "999999"], "3Y"))

# 6. a whole category, ranked, with the staleness guard
rule("6. get_category_returns('Large Cap', '1Y')   [Zeta 100009 is stale]")
cat = server.get_category_returns("Large Cap", "1Y")
print(f"as_of={cat['as_of']}  computed={cat['computed']}  "
      f"excluded_stale={cat['excluded_stale']}  avg_return={cat['avg_return_pct']}%")
print_returns(cat)

print("\n(Reminder: NAVs are a synthetic random walk — the numbers are for "
      "wiring validation only, not real fund performance.)")