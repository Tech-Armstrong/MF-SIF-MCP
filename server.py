"""
NAV Analytics MCP Server (standalone)
=====================================

A self-contained MCP server that exposes Indian mutual fund NAV analytics as
tools, querying parquet data through DuckDB. Point it at local parquet files or
at Azure Blob parquet — the server only ever runs read-only SELECTs and never
fabricates data.

Tools exposed
-------------
    search_funds(query, limit)          fuzzy fund-name -> scheme_code resolver
    get_fund_returns(scheme_codes, period)
                                        point-to-point + CAGR for 1..N funds
    get_category_returns(category, period, sort_by, ascending, staleness_days)
                                        returns for every fund in a category
    list_categories()                   distinct category names
    list_funds_in_category(category)    schemes in a category

Supported period strings
------------------------
    1W, 2W                weeks
    1M, 3M, 6M, 9M        months
    1Y, 2Y, 3Y, 5Y        years
    YTD                   Jan 1 (of the fund's last-NAV year) -> last NAV
    MTD                   1st of month -> last NAV
    SI                    since inception (earliest NAV -> last NAV)

Every window ENDS at the fund's own last available NAV (its "anchor"), not the
calendar today. This matches how published sources (Value Research / ET Money)
report: the latest NAV may be a day or two behind, and non-trading days have no
NAV.

Expected parquet schema
-----------------------
    nav_history     scheme_code VARCHAR, nav_date DATE, nav DOUBLE
    scheme_master   scheme_code VARCHAR, scheme_name VARCHAR,
                    fund_house VARCHAR, category VARCHAR

Configuration (environment variables)
-------------------------------------
    NAV_HISTORY_PATH     parquet path/glob for NAV history
                         (default: ./data/nav_history.parquet)
    SCHEME_MASTER_PATH   parquet path/glob for scheme master
                         (default: ./data/scheme_master.parquet)
    AZURE_STORAGE_CONNECTION_STRING
                         if set, DuckDB's azure extension is loaded and a secret
                         registered, so the paths above may be az:// URLs, e.g.
                         az://mycontainer/nav/*.parquet

    MCP_TRANSPORT        "stdio" for local Cursor/Claude Desktop use; anything
                         else (default) serves streamable HTTP at /mcp.
    PORT                 HTTP listen port (default 8000; App Service injects this).

    PUBLIC_BASE_URL      Enables OAuth for the claude.ai connector when set to the
                         externally reachable https base of the deployed server,
                         e.g. https://<app>.azurewebsites.net. A self-contained
                         OAuth server (InMemoryOAuthProvider) handles the connector
                         handshake — no external identity provider or app
                         registration needed. If unset, the HTTP server runs
                         UNAUTHENTICATED — fine for local testing, not for public
                         hosting.

Run
---
    python server.py                 # streamable HTTP (for the claude.ai connector)
    MCP_TRANSPORT=stdio python server.py   # stdio (for Cursor / Claude Desktop)

No data yet? Generate a synthetic fixture to smoke-test the server:
    python make_sample_data.py
"""

from __future__ import annotations

import os
import sys
import re
from datetime import date, timedelta
from typing import Optional, Union

import duckdb
from dateutil.relativedelta import relativedelta
from fastmcp import FastMCP

# ── configuration ─────────────────────────────────────────────────────────────

NAV_HISTORY_PATH = os.environ.get("NAV_HISTORY_PATH", "./data/nav_history.parquet")
SCHEME_MASTER_PATH = os.environ.get("SCHEME_MASTER_PATH", "./data/scheme_master.parquet")
AZURE_CONN = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

_VALID_PERIODS = {
    "1W", "2W",
    "1M", "3M", "6M", "9M",
    "1Y", "2Y", "3Y", "5Y",
    "YTD", "MTD", "SI",
}
# Periods long enough that an annualized CAGR is the meaningful comparison metric.
_LONG_PERIODS = {"2Y", "3Y", "5Y"}


# ── connection (single, long-lived, read-only) ─────────────────────────────────

def _build_connection() -> duckdb.DuckDBPyConnection:
    """
    Open ONE in-memory DuckDB connection for the life of the server and expose
    the two parquet sources as read-only views. A stdio MCP server is a
    long-lived process handling many calls, so the connection is created once at
    import time and never closed per call — the failure mode of closing a shared
    handle mid-session simply cannot occur here.
    """
    con = duckdb.connect(database=":memory:")

    # DuckDB can't bind prepared parameters inside CREATE VIEW/SECRET, so paths
    # are inlined as string literals with single quotes doubled to stay safe.
    def _lit(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    if AZURE_CONN:
        con.execute("INSTALL azure;")
        con.execute("LOAD azure;")

        # DuckDB's azure extension makes HTTPS calls to Blob storage and verifies
        # the TLS cert against a CA bundle. Some hosts (e.g. Azure App Service's
        # Linux container) don't expose the bundle where DuckDB's bundled libcurl
        # looks, causing: "Problem with the SSL CA cert (path? access rights?)".
        # Point DuckDB at the system CA bundle explicitly when one is present.
        # Guarded by existence so local runs (Windows/macOS) are unaffected.
        for _ca in ("/etc/ssl/certs/ca-certificates.crt",   # Debian/Ubuntu (App Service)
                    "/etc/pki/tls/certs/ca-bundle.crt"):     # RHEL/CentOS
            if os.path.exists(_ca):
                con.execute(f"SET ca_cert_file = {_lit(_ca)};")
                break

        # CONNECTION_STRING secret lets read_parquet resolve az:// URLs.
        con.execute(
            f"CREATE OR REPLACE SECRET azblob (TYPE azure, CONNECTION_STRING {_lit(AZURE_CONN)});"
        )
    else:
        # Fail early and clearly if local files are missing, rather than at
        # first query time with an opaque parquet error.
        missing = [p for p in (NAV_HISTORY_PATH, SCHEME_MASTER_PATH)
                   if not p.startswith(("az://", "azure://")) and not os.path.exists(p)]
        if missing:
            sys.stderr.write(
                "NAV MCP: parquet not found: " + ", ".join(missing) + "\n"
                "Set NAV_HISTORY_PATH / SCHEME_MASTER_PATH, provide "
                "AZURE_STORAGE_CONNECTION_STRING for az:// paths, or run "
                "`python make_sample_data.py` to create a local fixture.\n"
            )

    con.execute(
        f"CREATE OR REPLACE VIEW nav_history AS SELECT * FROM read_parquet({_lit(NAV_HISTORY_PATH)});"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW scheme_master AS SELECT * FROM read_parquet({_lit(SCHEME_MASTER_PATH)});"
    )
    return con


CON = _build_connection()


# ── period resolution ──────────────────────────────────────────────────────────

def _resolve_window(period: str, anchor: date, inception: date) -> tuple[date, date, str]:
    """
    Return (start_target, end_target, start_snap) for a period anchored to the
    fund's last-NAV date.

    start_snap tells the caller which trading day to snap the start to:
        "before" -> latest NAV on/before start_target (walk back over holidays);
                    used for trailing windows so we capture a FULL period.
        "after"  -> earliest NAV on/after start_target (walk forward); used for
                    calendar-anchored starts (YTD/MTD) and inception (SI).

    end_target is always the anchor; the end NAV is the latest NAV on/before it.
    Raises ValueError for unrecognised periods.
    """
    p = period.upper().strip()

    if p == "YTD":
        return date(anchor.year, 1, 1), anchor, "after"
    if p == "MTD":
        return date(anchor.year, anchor.month, 1), anchor, "after"
    if p == "SI":
        return inception, anchor, "after"

    if p.endswith("W") and p[:-1].isdigit():
        return anchor - timedelta(weeks=int(p[:-1])), anchor, "before"
    if p.endswith("M") and p[:-1].isdigit():
        return anchor - relativedelta(months=int(p[:-1])), anchor, "before"
    if p.endswith("Y") and p[:-1].isdigit():
        return anchor - relativedelta(years=int(p[:-1])), anchor, "before"

    raise ValueError(
        f"Unrecognised period '{period}'. Valid: {', '.join(sorted(_VALID_PERIODS))}"
    )


# ── returns math ────────────────────────────────────────────────────────────────

def _absolute_return(nav_start: float, nav_end: float) -> Optional[float]:
    if not nav_start:
        return None
    return round((nav_end - nav_start) / nav_start * 100, 4)


def _cagr(nav_start: float, nav_end: float,
          d_start: date, d_end: date, period: str) -> Optional[float]:
    """
    Annualized CAGR (%) over the REALIZED window, using actual day-count.
    Returned only when the window represents more than a year — for <=1Y periods
    an annualized figure is misleading, so it's None.
    """
    years = (d_end - d_start).days / 365.25
    long_enough = period in _LONG_PERIODS or (period in {"SI", "YTD"} and years > 1.0)
    if not long_enough or not nav_start or years <= 0:
        return None
    return round(((nav_end / nav_start) ** (1 / years) - 1) * 100, 4)


# ── core: point-to-point returns for many funds (set-based) ─────────────────────

def _compute_returns(scheme_codes: list[str], period: str) -> list[dict]:
    """
    Set-based returns for a list of scheme_codes. Regardless of list length this
    issues a constant handful of queries (metadata, anchors, targets, start
    NAVs, end NAVs) rather than looping per fund.
    """
    p = period.upper().strip()
    if p not in _VALID_PERIODS:
        raise ValueError(
            f"Unrecognised period '{period}'. Valid: {', '.join(sorted(_VALID_PERIODS))}"
        )

    codes = list(dict.fromkeys(scheme_codes))  # de-dupe, preserve order
    if not codes:
        return []

    ph = ", ".join(["?"] * len(codes))

    # 1) metadata for all requested codes
    meta_rows = CON.execute(
        f"""SELECT scheme_code, scheme_name, fund_house, category
            FROM scheme_master WHERE scheme_code IN ({ph})""",
        codes,
    ).fetchall()
    meta = {r[0]: {"scheme_name": r[1], "fund_house": r[2], "category": r[3]}
            for r in meta_rows}

    # 2) per-fund anchor (last NAV) and inception (first NAV), one grouped query
    anchor_rows = CON.execute(
        f"""SELECT scheme_code, MAX(nav_date), MIN(nav_date)
            FROM nav_history WHERE scheme_code IN ({ph})
            GROUP BY scheme_code""",
        codes,
    ).fetchall()
    anchors = {r[0]: (r[1], r[2]) for r in anchor_rows}  # code -> (anchor, inception)

    def _row(code, **extra):
        m = meta.get(code, {})
        base = {
            "scheme_code": code,
            "scheme_name": m.get("scheme_name"),
            "fund_house": m.get("fund_house"),
            "category": m.get("category"),
            "start_nav_date": None, "start_nav": None,
            "end_nav_date": None, "end_nav": None,
            "return_pct": None, "cagr_pct": None, "error": None,
        }
        base.update(extra)
        return base

    # 3) build a targets table; funds missing metadata or NAVs get an error row
    targets = []            # (code, start_target, end_target)
    results = {}            # code -> row dict (order restored at the end)
    start_snap = None
    for code in codes:
        if code not in meta:
            results[code] = _row(code, error=f"scheme_code '{code}' not found in scheme_master")
            continue
        if code not in anchors or anchors[code][0] is None:
            results[code] = _row(code, error="no NAV data for this scheme")
            continue
        anchor, inception = anchors[code]
        s_target, e_target, snap = _resolve_window(p, anchor, inception)
        start_snap = snap  # same for every fund in a single call
        targets.append((code, s_target, e_target))

    start_navs, end_navs = {}, {}
    if targets:
        CON.execute("CREATE OR REPLACE TEMP TABLE _targets("
                    "scheme_code VARCHAR, start_target DATE, end_target DATE);")
        CON.executemany("INSERT INTO _targets VALUES (?, ?, ?);", targets)

        # 4) start NAV per fund — direction depends on the window type
        if start_snap == "before":
            start_sql = """
                SELECT t.scheme_code, n.nav_date, n.nav
                FROM _targets t
                JOIN nav_history n
                  ON n.scheme_code = t.scheme_code AND n.nav_date <= t.start_target
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t.scheme_code ORDER BY n.nav_date DESC) = 1
            """
        else:  # "after"
            start_sql = """
                SELECT t.scheme_code, n.nav_date, n.nav
                FROM _targets t
                JOIN nav_history n
                  ON n.scheme_code = t.scheme_code AND n.nav_date >= t.start_target
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t.scheme_code ORDER BY n.nav_date ASC) = 1
            """
        for c, d, v in CON.execute(start_sql).fetchall():
            start_navs[c] = (d, v)

        # 5) end NAV per fund — latest NAV on/before the anchor
        end_sql = """
            SELECT t.scheme_code, n.nav_date, n.nav
            FROM _targets t
            JOIN nav_history n
              ON n.scheme_code = t.scheme_code AND n.nav_date <= t.end_target
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.scheme_code ORDER BY n.nav_date DESC) = 1
        """
        for c, d, v in CON.execute(end_sql).fetchall():
            end_navs[c] = (d, v)

    # assemble
    for code, s_target, _e in targets:
        s = start_navs.get(code)
        e = end_navs.get(code)
        if not s:
            results[code] = _row(code,
                                 end_nav_date=e[0] if e else None,
                                 end_nav=e[1] if e else None,
                                 error=f"no NAV on/around start target {s_target}")
            continue
        if not e:
            results[code] = _row(code, start_nav_date=s[0], start_nav=s[1],
                                 error="no NAV on/before anchor")
            continue
        results[code] = _row(
            code,
            start_nav_date=s[0], start_nav=s[1],
            end_nav_date=e[0], end_nav=e[1],
            return_pct=_absolute_return(s[1], e[1]),
            cagr_pct=_cagr(s[1], e[1], s[0], e[0], p),
        )

    return [results[c] for c in codes]


# ── auth (optional; enabled when PUBLIC_BASE_URL is set) ────────────────────────

def _build_auth():
    """
    Return an OAuth provider for the claude.ai custom-connector handshake, or None.

    claude.ai's "Add custom connector" flow performs an OAuth 2.0 exchange, so a
    hosted server needs an OAuth provider — a static token is not accepted by the
    connector UI. We use fastmcp's self-contained InMemoryOAuthProvider: it stands
    up its own OAuth authorization server and supports Dynamic Client Registration,
    so claude.ai can register and complete the handshake with no external identity
    provider, no app registration, and no scope configuration. Any client that
    completes the flow is granted access — appropriate for a small trusted team
    behind a URL you control, gating on knowledge of the URL plus the handshake.

    Auth turns on only when PUBLIC_BASE_URL is set (to the externally reachable
    https base of the deployed server, e.g. https://<app>.azurewebsites.net —
    OAuth endpoints are advertised relative to it). With it unset the server runs
    unauthenticated, which is what local stdio / HTTP smoke tests (and the existing
    mcp_client_test.py) rely on.

    Note: tokens live in memory, so a server restart forces teammates to
    re-authenticate (a one-click re-consent, not a re-setup). For durable tokens
    or real per-user identity + revocation, swap in a hosted provider (Google/
    GitHub/WorkOS) later — same call site, only this function changes.
    """
    base_url = os.environ.get("PUBLIC_BASE_URL")
    if not base_url:
        return None

    from fastmcp.server.auth.providers.in_memory import InMemoryOAuthProvider

    return InMemoryOAuthProvider(base_url=base_url)


# ── MCP app + tools ─────────────────────────────────────────────────────────────

mcp = FastMCP("nav-analytics", auth=_build_auth())


@mcp.tool()
def search_funds(query: str, limit: int = 10) -> dict:
    """Resolve a plain-English fund name to scheme_code(s).

    Fuzzy, word-order-insensitive match against scheme_name: every whitespace
    token in `query` must appear somewhere in the name. Returns ranked candidates
    with their scheme_code, fund_house and category. Plan variants (Regular vs
    Direct, Growth vs IDCW) come back as distinct candidates so the caller can
    pick an exact scheme_code. Use this first when the user names a fund; feed the
    resulting scheme_code into get_fund_returns.

    Args:
        query: Free-text fund name, e.g. "HDFC Balanced Advantage".
        limit: Max candidates to return (default 10).
    """
    tokens = [t for t in re.split(r"\s+", query.strip().lower()) if t]
    if not tokens:
        return {"query": query, "count": 0, "candidates": []}

    where = " AND ".join(["lower(scheme_name) LIKE ?"] * len(tokens))
    params = [f"%{t}%" for t in tokens]
    rows = CON.execute(
        f"""SELECT scheme_code, scheme_name, fund_house, category
            FROM scheme_master WHERE {where}
            ORDER BY length(scheme_name), scheme_name""",
        params,
    ).fetchall()

    q_norm = query.strip().lower()
    candidates = []
    for code, name, house, cat in rows[:limit]:
        nm = (name or "").lower()
        score = 1.0 if nm == q_norm else (0.85 if nm.startswith(q_norm) else 0.7)
        candidates.append({
            "scheme_code": code, "scheme_name": name,
            "fund_house": house, "category": cat, "score": round(score, 2),
        })
    return {"query": query, "count": len(candidates), "candidates": candidates}


@mcp.tool()
def get_fund_returns(scheme_codes: Union[str, list[str]], period: str) -> dict:
    """Point-to-point return and CAGR for one or many funds over a named period.

    Each fund's window ends at its own latest available NAV. return_pct is the
    absolute point-to-point return; cagr_pct is the annualized return and is only
    populated for windows longer than a year (2Y/3Y/5Y, and SI when the fund is
    older than a year) — for shorter windows it is null. Resolve names to
    scheme_codes with search_funds first.

    Args:
        scheme_codes: A single scheme_code or a list of them.
        period: One of 1W,2W,1M,3M,6M,9M,1Y,2Y,3Y,5Y,YTD,MTD,SI.
    """
    if isinstance(scheme_codes, str):
        scheme_codes = [scheme_codes]
    results = _compute_returns(scheme_codes, period)
    return {
        "period": period.upper().strip(),
        "period_end": "per-fund-last-nav",
        "results": results,
    }


@mcp.tool()
def get_category_returns(
    category: str,
    period: str,
    sort_by: str = "return_pct",
    ascending: bool = False,
    staleness_days: int = 7,
) -> dict:
    """Returns for every fund in a category, ranked, with a staleness guard.

    Because each fund anchors to its own last NAV, funds can have different as-of
    dates. This flags any fund whose last NAV lags the category's most-recent
    as-of date by more than `staleness_days`, keeps it in the results list marked
    stale, and EXCLUDES it (and any errored/None-return fund) from avg_return_pct
    and avg_cagr_pct so the averages never blend mismatched dates.

    Args:
        category: Category name (case-insensitive).
        period: One of 1W,2W,1M,3M,6M,9M,1Y,2Y,3Y,5Y,YTD,MTD,SI.
        sort_by: return_pct | cagr_pct | scheme_name | fund_house | start_nav | end_nav.
        ascending: Sort direction (default False = best/highest first).
        staleness_days: Max allowed lag from the category as-of date (default 7).
    """
    rows = CON.execute(
        """SELECT scheme_code FROM scheme_master
           WHERE UPPER(category) = UPPER(?) ORDER BY scheme_name""",
        [category],
    ).fetchall()

    if not rows:
        return {
            "category": category.upper(), "period": period.upper().strip(),
            "total_funds": 0, "computed": 0, "excluded_stale": 0,
            "avg_return_pct": None, "avg_cagr_pct": None, "results": [],
            "error": f"No funds found for category '{category}'. "
                     "Use list_categories() to see valid names.",
        }

    results = _compute_returns([r[0] for r in rows], period)

    # staleness: lag each fund's end NAV against the category's freshest as-of
    end_dates = [r["end_nav_date"] for r in results if r["end_nav_date"]]
    as_of = max(end_dates) if end_dates else None
    for r in results:
        if as_of and r["end_nav_date"]:
            lag = (as_of - r["end_nav_date"]).days
            r["as_of_lag_days"] = lag
            r["stale"] = lag > staleness_days
        else:
            r["as_of_lag_days"] = None
            r["stale"] = r["return_pct"] is None

    valid_cols = {"return_pct", "cagr_pct", "scheme_name", "fund_house", "start_nav", "end_nav"}
    key = sort_by if sort_by in valid_cols else "return_pct"
    none_rows = [r for r in results if r.get(key) is None]
    good_rows = [r for r in results if r.get(key) is not None]
    good_rows.sort(key=lambda r: r[key], reverse=not ascending)
    ordered = good_rows + none_rows

    fresh = [r for r in ordered if not r["stale"] and r["return_pct"] is not None]
    avg_ret = round(sum(r["return_pct"] for r in fresh) / len(fresh), 4) if fresh else None
    cagrs = [r["cagr_pct"] for r in fresh if r["cagr_pct"] is not None]
    avg_cagr = round(sum(cagrs) / len(cagrs), 4) if cagrs else None

    return {
        "category": category.upper(),
        "period": period.upper().strip(),
        "as_of": str(as_of) if as_of else None,
        "staleness_days": staleness_days,
        "total_funds": len(ordered),
        "computed": len([r for r in ordered if r["return_pct"] is not None]),
        "excluded_stale": len([r for r in ordered if r["stale"]]),
        "avg_return_pct": avg_ret,
        "avg_cagr_pct": avg_cagr,
        "results": ordered,
    }


@mcp.tool()
def list_categories() -> dict:
    """List every distinct category name present in scheme_master (sorted)."""
    rows = CON.execute(
        """SELECT DISTINCT category FROM scheme_master
           WHERE category IS NOT NULL ORDER BY category"""
    ).fetchall()
    return {"categories": [r[0] for r in rows]}


@mcp.tool()
def list_funds_in_category(category: str) -> dict:
    """List every scheme in a category with its scheme_code, name and fund house.

    Args:
        category: Category name (case-insensitive).
    """
    rows = CON.execute(
        """SELECT scheme_code, scheme_name, fund_house FROM scheme_master
           WHERE UPPER(category) = UPPER(?) ORDER BY fund_house, scheme_name""",
        [category],
    ).fetchall()
    funds = [{"scheme_code": r[0], "scheme_name": r[1], "fund_house": r[2]} for r in rows]
    return {"category": category.upper(), "total": len(funds), "funds": funds}


if __name__ == "__main__":
    # Transport selection:
    #   MCP_TRANSPORT=stdio  -> local use with Cursor / Claude Desktop (original mode)
    #   otherwise            -> streamable HTTP, for hosting as a claude.ai connector.
    # App Service (and most PaaS) inject the listen port via $PORT; default to 8000
    # locally. Bind 0.0.0.0 so the container's port mapping can reach it. The MCP
    # endpoint is served at /mcp on the deployed host.
    transport = os.environ.get("MCP_TRANSPORT", "http")
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        # stateless_http: each request is self-contained (no server-side session
        # pinned to one worker). Required for horizontally-scaled hosts like App
        # Service, and it's what the claude.ai connector expects.
        mcp.run(
            transport="http",
            host="0.0.0.0",
            port=int(os.environ.get("PORT", "8000")),
            stateless_http=True,
        )
