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
    sif_get_holdings(fund, year, month, asset_class, top)
                                        SIF monthly portfolio (Neo4j)
    sif_get_managers(fund, manager)     SIF fund-manager tenures (Neo4j)
    sif_get_aum_history(fund)           SIF month-by-month AUM (Neo4j)

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

    NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
                         the SIF Neo4j database (e.g. neo4j+s://<id>.databases.neo4j.io)
                         for the sif_* tools; unset = those tools report unavailable.
                         For local runs these may also live in a .env beside server.py.
    NEO4J_DATABASE       database name (default: neo4j)
    SIF_QUERY_TIMEOUT_SECONDS
                         per-query timeout for the sif_* tools (default 30)

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

import datetime as _dt
import os
import sys
import re
from datetime import date, timedelta
import threading
from typing import Optional, Union

import duckdb
from dateutil.relativedelta import relativedelta
from fastmcp import FastMCP

# ── configuration ─────────────────────────────────────────────────────────────

# Local runs: take the data settings (NEO4J_*, SIF_*, and the SIF NAV Blob
# settings NAV_HISTORY_PATH / SCHEME_MASTER_PATH / AZURE_STORAGE_CONNECTION_STRING)
# from the .env beside this file, so `python server.py` and the MCP Inspector
# reach the real data without exporting anything first. Variables already set
# win. The deployed image has no .env (see .dockerignore), so App Service
# settings are untouched.
_DOTENV_KEYS = ("NAV_HISTORY_PATH", "SCHEME_MASTER_PATH", "AZURE_STORAGE_CONNECTION_STRING")
try:
    from dotenv import dotenv_values
    for _key, _value in dotenv_values(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")).items():
        if (_key.startswith(("NEO4J_", "SIF_")) or _key in _DOTENV_KEYS) and _value is not None:
            os.environ.setdefault(_key, _value)
except ImportError:
    pass

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


# ── Azure Blob → local parquet (via the azure-storage-blob SDK) ────────────────
#
# We deliberately do NOT use DuckDB's `azure` extension to read az:// URLs. On
# locked-down hosts (Azure App Service's Linux container) that extension's bundled
# libcurl can't locate the CA bundle and fails with "Problem with the SSL CA cert".
# Instead we download the parquet with the azure-storage-blob SDK (which verifies
# TLS against certifi and works everywhere), cache it under a local dir, and hand
# DuckDB plain local files. Robust and cert-issue-free.

_LOCAL_CACHE_DIR = os.environ.get("BLOB_CACHE_DIR", "/tmp/navdata")


def _parse_az_uri(uri: str) -> tuple[str, str]:
    """'az://container/path/to/blob' -> ('container', 'path/to/blob')."""
    rest = re.sub(r"^(az|azure)://", "", uri)
    container, _, blob_path = rest.partition("/")
    return container, blob_path


def _download_az_to_local(uri: str) -> str:
    """
    Download the blob(s) named by an az:// URI to the local cache and return a
    LOCAL path (or glob) DuckDB can read. Handles two shapes:
      - a single blob:  az://c/processed/scheme_master.parquet
      - a glob:         az://c/processed/nav_history/year=*/*.parquet
    For a glob we list blobs under the fixed prefix (everything before the first
    wildcard), download each *.parquet, and return a local recursive glob.
    """
    from azure.storage.blob import ContainerClient

    container, blob_path = _parse_az_uri(uri)
    cc = ContainerClient.from_connection_string(AZURE_CONN, container_name=container)

    has_glob = "*" in blob_path
    # Fixed prefix = the path up to the first path-segment containing a wildcard.
    prefix = blob_path
    if has_glob:
        segments = blob_path.split("/")
        keep = []
        for seg in segments:
            if "*" in seg:
                break
            keep.append(seg)
        prefix = "/".join(keep)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

    dest_root = os.path.join(_LOCAL_CACHE_DIR, container)
    downloaded = 0
    for blob in cc.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith(".parquet"):
            continue
        local_path = os.path.join(dest_root, blob.name.replace("/", os.sep))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(cc.download_blob(blob.name).readall())
        downloaded += 1

    if downloaded == 0:
        raise RuntimeError(
            f"No .parquet blobs found under az://{container}/{prefix} "
            f"(from URI {uri}). Check the container name and path."
        )

    if has_glob:
        # Root the recursive glob at the fixed PREFIX subdir (e.g. nav_history/),
        # not the container root — otherwise it would also match sibling datasets
        # like scheme_master.parquet, and DuckDB would try to union mismatched
        # schemas ("column nav_date ... could not be found"). The glob covers the
        # year=*/*.parquet layout regardless of depth.
        prefix_local = os.path.join(dest_root, prefix.rstrip("/").replace("/", os.sep))
        return os.path.join(prefix_local, "**", "*.parquet")
    return os.path.join(dest_root, blob_path.replace("/", os.sep))


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

    # DuckDB can't bind prepared parameters inside CREATE VIEW, so paths are
    # inlined as string literals with single quotes doubled to stay safe.
    def _lit(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    if AZURE_CONN:
        # Download from Blob via the SDK, then point DuckDB at local files.
        nav_path = _download_az_to_local(NAV_HISTORY_PATH)
        scheme_path = _download_az_to_local(SCHEME_MASTER_PATH)
    else:
        # Local mode: fail early and clearly if the parquet is missing.
        nav_path, scheme_path = NAV_HISTORY_PATH, SCHEME_MASTER_PATH
        missing = [p for p in (nav_path, scheme_path) if not os.path.exists(p)]
        if missing:
            sys.stderr.write(
                "NAV MCP: parquet not found: " + ", ".join(missing) + "\n"
                "Set NAV_HISTORY_PATH / SCHEME_MASTER_PATH, provide "
                "AZURE_STORAGE_CONNECTION_STRING for az:// paths, or run "
                "`python make_sample_data.py` to create a local fixture.\n"
            )

    con.execute(
        f"CREATE OR REPLACE VIEW nav_history AS "
        f"SELECT * FROM read_parquet({_lit(nav_path)});"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW scheme_master AS "
        f"SELECT * FROM read_parquet({_lit(scheme_path)});"
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

    claude.ai's "Add custom connector" flow performs an OAuth 2.0 exchange. It does
    NOT support Dynamic Client Registration — the connector UI requires you to paste
    a pre-existing OAuth Client ID (and Secret). So we stand up fastmcp's
    self-contained InMemoryOAuthProvider (its own authorization server — no external
    identity provider, no Azure app registration, no scope config) and pre-register
    ONE fixed client from env vars. You paste that same client_id / client_secret
    into claude.ai when adding the connector.

    Required env vars to enable auth:
        PUBLIC_BASE_URL     externally reachable https base, e.g.
                            https://<app>.azurewebsites.net (OAuth endpoints are
                            advertised relative to it).
        OAUTH_CLIENT_ID     the client id you'll paste into claude.ai.
        OAUTH_CLIENT_SECRET the client secret you'll paste into claude.ai.
    Optional:
        OAUTH_REDIRECT_URIS space-separated allowed redirect URIs; defaults to
                            claude.ai's connector callback.

    If PUBLIC_BASE_URL is unset the server runs UNAUTHENTICATED (local stdio / HTTP
    smoke tests, and the existing mcp_client_test.py, rely on this). If
    PUBLIC_BASE_URL is set but the client id/secret are not, we fail fast with a
    clear message rather than booting an unusable connector.

    Note: registrations/tokens live in memory. The pre-registered client is
    re-seeded on every startup (so it survives restarts), but issued access tokens
    do not — a restart makes teammates click "reconnect" once. For durable tokens
    or real per-user identity + revocation, swap in a hosted provider (Google/
    GitHub/WorkOS) later — same call site, only this function changes.
    """
    base_url = os.environ.get("PUBLIC_BASE_URL")
    if not base_url:
        return None

    client_id = os.environ.get("OAUTH_CLIENT_ID")
    client_secret = os.environ.get("OAUTH_CLIENT_SECRET")
    if not client_id or not client_secret:
        sys.stderr.write(
            "NAV MCP: PUBLIC_BASE_URL is set (OAuth on) but OAUTH_CLIENT_ID / "
            "OAUTH_CLIENT_SECRET are not. Set both, then paste the same values "
            "into claude.ai's Add-custom-connector form.\n"
        )
        raise SystemExit(1)

    redirect_uris = os.environ.get(
        "OAUTH_REDIRECT_URIS", "https://claude.ai/api/mcp/auth_callback"
    ).split()

    from fastmcp.server.auth.providers.in_memory import InMemoryOAuthProvider
    from mcp.shared.auth import OAuthClientInformationFull

    provider = InMemoryOAuthProvider(base_url=base_url)
    # Pre-register the fixed client so claude.ai (which doesn't do Dynamic Client
    # Registration) can authenticate with the id/secret you paste into its UI.
    provider.clients[client_id] = OAuthClientInformationFull(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uris=redirect_uris,
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        token_endpoint_auth_method="client_secret_post",
    )
    return provider

#--Neo4j Connection--
# ── SIF portfolios (Neo4j) ─────────────────────────────────────────────────────
#
# Monthly SIF portfolios, AUM and fund managers, loaded into Neo4j by the
# historical-sif project (ingestion-pipeline/neo4j_parser.py). Optional, like the
# index data: without NEO4J_* settings (or the neo4j package) the server still
# starts and only the sif_* tools report that the data is unavailable.
#
# There is no "connect" tool. One driver is created on first use and reused for
# the life of the process (it pools its own connections), the same way _db()
# serves DuckDB. Every query runs in a READ transaction, so a write is refused by
# the database itself; _SIF_WRITE_RX only turns that into a clearer message.

NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")
SIF_QUERY_TIMEOUT_SECONDS = float(os.environ.get("SIF_QUERY_TIMEOUT_SECONDS", "30"))
SIF_MAX_ROWS = 1000

_NEO4J_LOCK = threading.Lock()
_NEO4J_DRIVER = None

_MONTHS = ["January", "February", "March", "April", "May", "June", "July",
           "August", "September", "October", "November", "December"]

# write / admin clauses, matched after string literals and comments are removed
_SIF_WRITE_RX = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|FOREACH|LOAD\s+CSV|GRANT|DENY|REVOKE|ALTER|RENAME)\b"
    r"|\bIN\s+TRANSACTIONS\b|\b(apoc\.(create|merge|refactor|periodic|load|import|export|trigger|atomic|cypher)|dbms\.)",
    re.I,
)

_SIF_SCHEMA = {
    "nodes": {
        "SIF_FUND": {
            "sifCode": "AMFI SIF code, e.g. 'SIF-13' (unique)",
            "sifFundName": "e.g. 'Magnum Hybrid Long-Short Fund'",
            "sifCategory": "SEBI SIF strategy, e.g. 'Hybrid Long-Short', 'Equity Long-Short', 'Equity Ex-Top 100 Long-Short'",
        },
        "SIF_MONTHLY_SNAPSHOT": {
            "sifCode": "the fund's code (unique with sifYear + sifMonth)",
            "sifYear": "integer, e.g. 2026",
            "sifMonth": "FULL month name string, e.g. 'August' (not 8, not 'Aug')",
            "sifReportedDate": "FIRST day of the portfolio month, e.g. date('2026-08-01') for the 31 Aug 2026 portfolio; use it to order or filter months",
            "sifAUM": "month-end AUM in Rs crore from the factsheet; null if no factsheet",
        },
        "SIF_ASSET_CLASS": {
            "sifName": "'Equity', 'Debt', 'Cash and Cash Equivalents', 'REITs & InvITs', 'MF and ETF', 'Commodities' or 'AIF'",
            "sifSnapshotYear / sifSnapshotMonth": "copy of the snapshot's year / month",
        },
        "SIF_INSTRUMENT": {
            "sifISIN": "ISIN (unique). Derivatives have synthetic keys (e.g. 'HBL-SF-R3ELM1', 'INR-IRS-1JKT2Y'); "
                       "each fund-month's cash is one row keyed 'CASHANDEQ'",
            "sifName": "instrument name",
            "sifAssetClass": "asset class",
            "sifSubType": "e.g. 'Listed Equity', 'Corporate Bonds / Debentures', 'Treasury Bills'; null for derivatives",
            "sifSector / sifIndustry": "sector and industry (equity); 'Others' for non-equity",
            "sifSymbol": "NSE symbol (equity)",
            "sifMarketCap": "'Large Cap' / 'Mid Cap' / 'Small Cap' (equity)",
        },
        "SIF_FUND_MANAGER": {"sifName": "manager's name (unique)"},
    },
    "relationships": {
        "(SIF_FUND)-[:SIF_LATEST_MONTHLY_SNAPSHOT]->(SIF_MONTHLY_SNAPSHOT)": "the fund's newest month",
        "(SIF_MONTHLY_SNAPSHOT)-[:SIF_PREVIOUS_MONTHLY_SNAPSHOT]->(SIF_MONTHLY_SNAPSHOT)": "newer -> the month before it",
        "(SIF_MONTHLY_SNAPSHOT)-[:SIF_HAS_ASSET_CLASS]->(SIF_ASSET_CLASS)": "one asset-class node per snapshot",
        "(SIF_ASSET_CLASS)-[:SIF_HAS_INSTRUMENT {sifHoldings, sifSubType, sifIncludedInNetAssets}]->(SIF_INSTRUMENT)":
            "sifHoldings = % of net assets in that month; sifSubType adds the derivative type "
            "('derivative-hedging', 'derivative-directional-position', 'derivatives', 'interest-rate-swaps-derivative')",
        "(SIF_FUND_MANAGER)-[:SIF_MANAGES {sifStartDate, sifEndDate}]->(SIF_FUND)":
            "dates are strings: sifStartDate 'Oct-2025'; sifEndDate 'Mon-YYYY', 'present' or null",
    },
    "rules": [
        "A snapshot is NOT linked to its fund except through SIF_LATEST_MONTHLY_SNAPSHOT: join any month on "
        "sifCode, e.g. MATCH (f:SIF_FUND), (s:SIF_MONTHLY_SNAPSHOT {sifCode: f.sifCode}).",
        "Holdings live on the SIF_HAS_INSTRUMENT relationship, not on the instrument node; instrument nodes are "
        "shared by every fund and month that holds them.",
        "A month's holdings sum to ~100 (only what the AMC counts in net assets). Short option / futures legs "
        "are negative. Weights are as printed by the AMC, so a few months differ from 100 by rounding.",
        "Order months by sifReportedDate, not by sifMonth (a string).",
        "Some SIF_FUND nodes have managers but no snapshots yet (portfolio not loaded).",
    ],
    "examples": {
        "total holdings per fund-month":
            "MATCH (f:SIF_FUND) MATCH (s:SIF_MONTHLY_SNAPSHOT {sifCode: f.sifCode})-[:SIF_HAS_ASSET_CLASS]->()"
            "-[h:SIF_HAS_INSTRUMENT]->() RETURN f.sifFundName, s.sifYear, s.sifMonth, round(sum(h.sifHoldings), 2) "
            "AS total ORDER BY f.sifFundName, s.sifReportedDate",
        "funds holding a stock in their latest month":
            "MATCH (f:SIF_FUND)-[:SIF_LATEST_MONTHLY_SNAPSHOT]->(s)-[:SIF_HAS_ASSET_CLASS]->()-[h:SIF_HAS_INSTRUMENT]->"
            "(i:SIF_INSTRUMENT {sifSymbol: $symbol}) RETURN f.sifFundName, s.sifMonth, s.sifYear, h.sifHoldings "
            "ORDER BY h.sifHoldings DESC",
        "sector split of one fund-month":
            "MATCH (s:SIF_MONTHLY_SNAPSHOT {sifCode: $code, sifYear: 2026, sifMonth: 'August'})-[:SIF_HAS_ASSET_CLASS]->"
            "(:SIF_ASSET_CLASS {sifName: 'Equity'})-[h:SIF_HAS_INSTRUMENT]->(i) WHERE h.sifSubType = 'Listed Equity' "
            "RETURN i.sifSector AS sector, round(sum(h.sifHoldings), 2) AS pct ORDER BY pct DESC",
    },
}


def _neo4j():
    """The shared Neo4j driver, created and checked on first use."""
    global _NEO4J_DRIVER
    with _NEO4J_LOCK:
        if _NEO4J_DRIVER is None:
            if not (NEO4J_URI and NEO4J_USERNAME and NEO4J_PASSWORD):
                raise ValueError(
                    "SIF data is not configured on this server: set NEO4J_URI, "
                    "NEO4J_USERNAME and NEO4J_PASSWORD.")
            try:
                from neo4j import GraphDatabase
            except ImportError as exc:
                raise ValueError(
                    "The neo4j package is not installed on this server "
                    "(pip install -r requirements.txt).") from exc
            # liveness check: Aura drops idle connections, so test a pooled one
            # that has been idle a while before handing it out
            driver = GraphDatabase.driver(
                NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD), liveness_check_timeout=60)
            try:
                driver.verify_connectivity()
            except Exception as exc:
                driver.close()
                raise ValueError(f"Could not connect to the SIF Neo4j database: {exc}") from exc
            _NEO4J_DRIVER = driver
        return _NEO4J_DRIVER
    
def _sif_value(value):
    """A Neo4j value as plain JSON: dates as ISO strings, nodes / relationships as property dicts."""
    from neo4j.graph import Node, Path, Relationship

    if isinstance(value, Node):
        return {"_labels": sorted(value.labels), **{k: _sif_value(v) for k, v in value.items()}}
    if isinstance(value, Relationship):
        return {"_type": value.type, **{k: _sif_value(v) for k, v in value.items()}}
    if isinstance(value, Path):
        return {"nodes": [_sif_value(n) for n in value.nodes],
                "relationships": [_sif_value(r) for r in value.relationships]}
    if isinstance(value, (list, tuple)):
        return [_sif_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _sif_value(v) for k, v in value.items()}
    if hasattr(value, "iso_format"):  # neo4j.time Date / DateTime / Time / Duration
        return value.iso_format()
    return value


def _sif_read(cypher: str, params: Optional[dict] = None, limit: int = SIF_MAX_ROWS) -> tuple[list[dict], bool]:
    """(rows, truncated): run one Cypher query in a read transaction, at most `limit` rows."""
    from neo4j import READ_ACCESS, unit_of_work

    @unit_of_work(timeout=SIF_QUERY_TIMEOUT_SECONDS)
    def work(tx):
        rows = []
        for record in tx.run(cypher, params or {}):
            if len(rows) >= limit:
                return rows, True
            rows.append({k: _sif_value(v) for k, v in record.items()})
        return rows, False

    with _neo4j().session(database=NEO4J_DATABASE, default_access_mode=READ_ACCESS) as session:
        return session.execute_read(work)


def _sif_norm(text: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).split())


def _sif_fund(fund: str) -> dict:
    """{'sif_code', 'fund_name', 'category'} for a SIF code or a (partial) fund name."""
    funds, _ = _sif_read(
        "MATCH (f:SIF_FUND) RETURN f.sifCode AS sif_code, f.sifFundName AS fund_name, f.sifCategory AS category")
    wanted = _sif_norm(fund)
    for f in funds:
        if wanted in (_sif_norm(f["sif_code"]), _sif_norm(f["fund_name"])):
            return f
    tokens = wanted.split()
    hits = [f for f in funds if tokens and all(t in _sif_norm(f["fund_name"]) for t in tokens)]
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise ValueError(f"No SIF fund matches '{fund}'. Use sif_list_funds() to see the funds and their codes.")
    raise ValueError(f"'{fund}' matches {len(hits)} SIF funds; pass one sif_code: "
                     + "; ".join(f"{f['sif_code']} = {f['fund_name']}" for f in hits))


def _sif_month(month: Union[int, str, None]) -> Optional[str]:
    """8 / '08' / 'aug' / 'August' -> 'August' (the form stored on snapshots)."""
    if month is None or str(month).strip() == "":
        return None
    m = str(month).strip().lower()
    if m.isdigit() and 1 <= int(m) <= 12:
        return _MONTHS[int(m) - 1]
    for name in _MONTHS:
        if m in (name.lower(), name[:3].lower()):
            return name
    raise ValueError(f"Invalid month '{month}': use 1-12, 'Aug' or 'August'.")


def _sif_months_available(code: str) -> list[str]:
    rows, _ = _sif_read(
        "MATCH (s:SIF_MONTHLY_SNAPSHOT {sifCode: $code}) RETURN s.sifMonth + ' ' + toString(s.sifYear) AS m "
        "ORDER BY s.sifReportedDate", {"code": code})
    return [r["m"] for r in rows]

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


@mcp.tool()
def sif_get_holdings(
    fund: str,
    year: int = 0,
    month: str = "",
    asset_class: str = "",
    top: int = 0,
) -> dict:
    """A SIF fund's portfolio for one month: every holding with its weight, plus AUM.

    Leave year (0) and month ("") empty for the latest month; give only a year
    for that year's latest month. `requested` echoes the filters received and
    `month` / `year` are the month actually returned. Weights are % of net assets and sum to ~100 across all
    asset classes; short option / futures legs are negative. Cash and cash
    equivalents are one row. `by_asset_class` gives the split.

    Args:
        fund: sif_code (e.g. 'SIF-13') or fund name (e.g. 'Magnum Hybrid').
        year: e.g. 2026. 0 = any year.
        month: '8', 'Aug' or 'August'. Empty = latest.
        asset_class: Only this asset class, e.g. 'Equity' or 'Debt'. The totals
            still cover the whole portfolio. Empty = all.
        top: Only the N largest holdings (by absolute weight). 0 = all.
    """
    f = _sif_fund(fund)
    month_name = _sif_month(month)
    year = int(year) if year else None
    requested = {"year": year, "month": month_name, "asset_class": asset_class or None, "top": top or None}
    rows, _ = _sif_read(
        """MATCH (s:SIF_MONTHLY_SNAPSHOT {sifCode: $code})
           WHERE ($year IS NULL OR s.sifYear = $year) AND ($month IS NULL OR s.sifMonth = $month)
           WITH s ORDER BY s.sifReportedDate DESC LIMIT 1
           MATCH (s)-[:SIF_HAS_ASSET_CLASS]->(ac:SIF_ASSET_CLASS)-[h:SIF_HAS_INSTRUMENT]->(i:SIF_INSTRUMENT)
           RETURN s.sifYear AS year, s.sifMonth AS month, s.sifReportedDate AS month_start, s.sifAUM AS aum_cr,
                  ac.sifName AS asset_class, i.sifName AS instrument, i.sifISIN AS isin,
                  h.sifHoldings AS weight_pct, h.sifSubType AS sub_type, i.sifSector AS sector,
                  i.sifIndustry AS industry, i.sifSymbol AS symbol, i.sifMarketCap AS market_cap
           ORDER BY abs(h.sifHoldings) DESC""",
        {"code": f["sif_code"], "year": year, "month": month_name})
    if not rows:
        available = _sif_months_available(f["sif_code"])
        when = " ".join(str(x) for x in (month_name, year) if x) or "any month"
        return {**f, "found": False, "requested": requested, "months_available": available,
                "error": f"No portfolio for {f['fund_name']} in {when}."}

    by_class = {}
    for r in rows:
        c = by_class.setdefault(r["asset_class"], {"asset_class": r["asset_class"], "weight_pct": 0.0, "holdings": 0})
        c["weight_pct"] += r["weight_pct"] or 0
        c["holdings"] += 1
    head = rows[0]
    holdings = [{k: r[k] for k in ("instrument", "isin", "asset_class", "sub_type", "weight_pct", "sector",
                                   "industry", "symbol", "market_cap")}
                for r in rows if not asset_class or (r["asset_class"] or "").lower() == asset_class.strip().lower()]
    if top:
        holdings = holdings[:max(1, int(top))]
    return {
        **f, "found": True, "requested": requested, "year": head["year"], "month": head["month"], "month_start": head["month_start"],
        "aum_cr": head["aum_cr"],
        "total_weight_pct": round(sum(r["weight_pct"] or 0 for r in rows), 4),
        "holding_count": len(rows),
        "by_asset_class": sorted(({**c, "weight_pct": round(c["weight_pct"], 4)} for c in by_class.values()),
                                 key=lambda c: -c["weight_pct"]),
        "holdings": holdings,
    }


@mcp.tool()
def sif_get_managers(fund: str = "", manager: str = "") -> dict:
    """SIF fund managers and their tenures.

    Give `fund` for a fund's managers, or `manager` for the SIF funds a person
    manages (case-insensitive partial name). Each tenure has a start date
    ('Oct-2025': the date printed in the factsheet, else the first factsheet
    month listing them) and an end date ('present' while still listed).

    Args:
        fund: sif_code (e.g. 'SIF-13') or fund name.
        manager: Manager name, e.g. 'Nilesh Saha'.
    """
    if not fund and not manager:
        raise ValueError("Give a fund or a manager.")
    params = {"code": _sif_fund(fund)["sif_code"] if fund else None,
              "manager": manager.strip().lower() if manager else None}
    rows, _ = _sif_read(
        """MATCH (m:SIF_FUND_MANAGER)-[r:SIF_MANAGES]->(f:SIF_FUND)
           WHERE ($code IS NULL OR f.sifCode = $code)
             AND ($manager IS NULL OR toLower(m.sifName) CONTAINS $manager)
           RETURN m.sifName AS manager, f.sifCode AS sif_code, f.sifFundName AS fund_name,
                  f.sifCategory AS category, r.sifStartDate AS start_date, r.sifEndDate AS end_date""",
        params)

    def start_key(r):
        try:
            return _dt.datetime.strptime(r["start_date"], "%b-%Y")
        except (TypeError, ValueError):
            return _dt.datetime.max

    rows.sort(key=lambda r: (r["fund_name"] or "", start_key(r), r["manager"] or ""))
    return {"fund": fund or None, "manager": manager or None, "total": len(rows), "tenures": rows}


@mcp.tool()
def sif_get_aum_history(fund: str) -> dict:
    """Month-by-month AUM (Rs crore) of a SIF fund, with each month's holding count and total weight.

    Args:
        fund: sif_code (e.g. 'SIF-13') or fund name.
    """
    f = _sif_fund(fund)
    rows, _ = _sif_read(
        """MATCH (s:SIF_MONTHLY_SNAPSHOT {sifCode: $code})
           OPTIONAL MATCH (s)-[:SIF_HAS_ASSET_CLASS]->()-[h:SIF_HAS_INSTRUMENT]->()
           RETURN s.sifYear AS year, s.sifMonth AS month, s.sifReportedDate AS month_start, s.sifAUM AS aum_cr,
                  count(h) AS holding_count, round(sum(h.sifHoldings), 4) AS total_weight_pct
           ORDER BY month_start""",
        {"code": f["sif_code"]})
    return {**f, "months": len(rows), "history": rows}


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
