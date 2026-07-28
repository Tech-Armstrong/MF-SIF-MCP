# MCP Server Build — Cursor Prompt Sequence

Target: expose the SIF analytics tools (`sif/scripts/agent/tools.py`) as an MCP
server that any MCP client (Cursor, Claude Desktop) can drive, and fold in the
correctness/scale fixes flagged during review.

**Approach — thin adapter.** `sif/mcp/server.py` imports the existing tool
functions and registers them as MCP tools. It does *not* reimplement the returns
math. Every fix below lands in `tools.py` and flows through to the server for
free. Nothing here touches the period-resolution logic or the per-fund anchoring
convention unless a prompt says so explicitly.

**Bands** (also the reason for the order):
- `[recon]` — look before touching. No code changes.
- `[work]` — a running, demoable server.
- `[capability]` / `[correct]` / `[fast]` — discrete additive hardening.
- `[verify]` — prove the numbers against a golden reference.

**How to run this.** Paste one prompt at a time. Do not move to the next until
its **Done when** holds. Prompt 2 gives you a working server, but **do not trust
the numbers until Prompts 4, 6, and 7 are done**, and **do not point it at the
full ~600-fund universe until Prompt 5 is done** (until then it's N+1 and will
crawl). Your `.cursor/rules/00-executor.mdc` constraints (no mock data, additive,
trace-first, diagnosis-before-rewriting) apply to all of these; the prompts
restate only the ones that bind hardest per step.

---

### Prompt 1 — Recon: connection semantics + schema ground truth  `[recon]`

Latent-bug hunt before we build. An MCP server is a long-lived process making
many calls; if `with _connect() as con:` is closing a shared handle, call #2
breaks. Confirm before wrapping.

```
Do not change any code in this prompt. Investigate and report only.

Read sif/config/duckdb_session.py and sif/scripts/agent/tools.py, then answer:

1. Does get_connection() return a module-level singleton, or a fresh connection
   each call? Trace the actual code path.
2. When tools.py does `with _connect() as con:`, does DuckDB's context-manager
   __exit__ close the connection in the version pinned here? If get_connection()
   is a singleton, does the second consecutive tool call get a closed handle?
3. get_category_returns opens a connection for the scheme_code list, then calls
   get_fund_returns which opens another. In the singleton case, does the inner
   `with` block close the connection out from under the outer one?
4. Confirm the real columns and types of nav_history and scheme_master by
   querying the parquet directly (DESCRIBE / a LIMIT 1 SELECT). I expect
   nav_history(scheme_code, nav_date, nav) and
   scheme_master(scheme_code, scheme_name, fund_house, category) — flag any drift.
5. How are the Blob-backed views attached, and what is the cost of re-attaching
   them per connection vs. holding one open for the server's lifetime?

Write findings to sif/docs/notes/mcp-connection-recon.md. Recommend one
connection lifecycle for a long-running server (single connection opened at
startup, or fresh-per-call) with a one-line justification. No code changes.
```

**Done when:** the note answers 1–5 and names the connection lifecycle we'll use in Prompt 2.

---

### Prompt 2 — Stand up the MCP server (thin adapter)  `[work]`

Wrap the four existing tools unchanged. Fix *only* the connection lifecycle per
Prompt 1. This is the demoable milestone.

```
Create an MCP server as a thin adapter over the existing tools. Additive only —
do NOT modify the returns math, period resolution, or anchoring logic in
sif/scripts/agent/tools.py.

- Create sif/mcp/server.py using the official MCP Python SDK (FastMCP). Check the
  current SDK docs for the exact API — it has changed across versions; do not
  assume signatures from memory.
- Register these four functions as MCP tools, importing them from
  sif.scripts.agent.tools: get_fund_returns, get_category_returns,
  list_categories, list_funds_in_category.
- Give each tool a precise description and typed parameters so a client picks the
  right one. Make the supported period strings (1W,2W,1M,3M,6M,9M,1Y,2Y,3Y,5Y,
  YTD,MTD,SI) explicit in the get_fund_returns / get_category_returns schemas.
- Apply the connection lifecycle recommended in
  sif/docs/notes/mcp-connection-recon.md. If it's a shared connection, ensure a
  single tool call does not close it for the next one, and that the nested
  category→fund_returns path does not double-open/close. If the current
  `with _connect()` pattern would break across calls, fix that pattern (in the
  connection helper, not the math).
- Transport: stdio (local use from Cursor / Claude Desktop). Do not build SSE/HTTP.
- Entry point: runnable as `python -m sif.mcp.server`.
- Pin the new deps (MCP SDK, duckdb, python-dateutil/dateutil) in the project's
  requirements file with exact versions.
- Add sif/mcp/README.md with: how to run it, and the exact registration snippet
  for BOTH Cursor (.cursor/mcp.json) and Claude Desktop (claude_desktop_config.json).

Test against real Blob data (no mock data):
- list_categories() returns the real category list.
- get_fund_returns(<a real scheme_code you find via list_funds_in_category on one
  category>, "1Y") returns a correct-looking result.
- Call two tools back-to-back in one server session to prove the connection
  survives call #2.
```

**Done when:** the server starts, both test calls succeed against real Blob data, and two consecutive calls in one session work (connection fix proven).

---

### Prompt 3 — Fund resolver: close the name→code gap  `[capability]`

The original premise was "input: fund name," but every tool takes `scheme_code`
and there's no way to get from one to the other. Add the resolver.

```
Additive. Add a fund-name resolver and expose it as an MCP tool.

- In sif/scripts/agent/tools.py add search_funds(query: str, limit: int = 10).
  Fuzzy-match query against scheme_master.scheme_name (case-insensitive; handle
  partial and word-order-insensitive matches). Return ranked candidates:
  [{scheme_code, scheme_name, fund_house, category, score}].
- Surface plan-variant disambiguation: if a matched name has Regular/Direct
  and/or Growth/IDCW variants, return them as distinct candidates rather than
  collapsing — the caller must be able to pick the exact scheme_code.
- Register search_funds as an MCP tool in sif/mcp/server.py.
- Test against real data (no mock data):
  - "HDFC Balanced Advantage" returns ranked candidates with scheme_codes.
  - A deliberately ambiguous stem (e.g. "SBI Bluechip") returns multiple variants.
  - A nonsense query returns an empty list, not an error.
```

**Done when:** a plain-English fund name resolves to ranked scheme_codes with variants distinguished.

---

### Prompt 4 — Returns metric: absolute for ≤1Y, CAGR for >1Y  `[correct]`

`_point_to_point_return` returns raw absolute for everything, so a 5Y number
like 78% isn't comparable across funds and won't match any published source.

```
Correctness fix. Additive to the output shape — keep return_pct, add cagr_pct.
Do not remove or rename existing fields.

- In sif/scripts/agent/tools.py, alongside the existing absolute return_pct,
  compute annualized CAGR using the ACTUAL day-count between the resolved
  start_nav_date and end_nav_date (not the nominal period label):
      years   = (end_nav_date - start_nav_date).days / 365.25
      cagr_pct = ((end_nav / start_nav) ** (1 / years) - 1) * 100   # rounded 4dp
- Add cagr_pct to each result dict in get_fund_returns. Set it to null when the
  window is <= 1 year (annualizing a sub-year period is misleading), and non-null
  for 2Y/3Y/5Y and for SI where the realized duration exceeds 1 year.
- get_category_returns should also surface cagr_pct per fund, and for periods
  where cagr_pct is the right comparison metric, allow sort_by="cagr_pct".
- Guard start_nav == 0 and years == 0 (return null, don't raise).

Verify against a golden reference (no mock data): pick one fund, compute 5Y, and
confirm cagr_pct matches its published 5Y CAGR (Value Research / ET Money / AMFI)
for the same as-of date, within a small tolerance. Confirm 1Y results are
unchanged (cagr_pct null, return_pct identical to before).
```

**Done when:** long-period CAGR matches a published figure within tolerance and short-period output is unchanged.

---

### Prompt 5 — Collapse the N+1 into set-based queries  `[fast]`

`get_fund_returns` loops per fund at ~4–5 queries each; a 40-fund category call
is ~200 sequential round-trips to Blob parquet, and the full universe is
thousands. This is the fix that makes ~600 funds usable.

```
Performance fix. The externally observable behavior and output shape must be
IDENTICAL to the current version — this is a rewrite of how, not what. Preserve
the per-fund anchoring convention exactly (each fund's window ends at its OWN
last available NAV, not a shared today), and preserve the trailing-vs-YTD/MTD/SI
start-snap rules.

Rewrite get_fund_returns internals so that for a list of scheme_codes it issues a
small CONSTANT number of queries regardless of list length:
1. One query for all metadata: SELECT ... FROM scheme_master WHERE scheme_code IN (...).
2. One grouped query for anchors: MAX(nav_date) per scheme_code (and MIN for SI).
3. Resolve start and end NAVs for all funds in one pass. Suggested approach: build
   a CTE of (scheme_code, start_target, end_target) derived from each fund's anchor,
   join to nav_history, and use window functions (ROW_NUMBER partitioned by
   scheme_code) to pick the on-or-after start and on-or-before end per fund. Deviate
   if you find something cleaner, but the query count must stay constant.

Trace-first: log the number of DuckDB queries issued per get_fund_returns call
before and after, and include both numbers in your summary.

Correctness gate (no mock data): before/after must produce byte-identical results
on a golden set — run get_category_returns on one real category with both the old
and new implementation and diff every field of every row. Only merge if the diff
is empty.
```

**Done when:** a multi-fund call issues a constant (not per-fund) query count and produces identical numbers to the pre-rewrite version on a real category.

---

### Prompt 6 — Category ranking: staleness guard  `[correct]`

Per-fund anchoring is right for a single fund, but `get_category_returns` then
ranks a fund whose last NAV is three months old against funds current to
yesterday, and `avg_return_pct` blends different as-of dates.

```
Correctness fix for aggregates. Additive — keep per-fund results intact.

- In get_category_returns, compute the category's reference as-of date as the
  MAX end_nav_date across its funds.
- Add per-fund fields: as_of_lag_days (reference as-of minus that fund's
  end_nav_date) and stale: bool. A fund is stale if as_of_lag_days exceeds a
  threshold parameter staleness_days (default 7; make it a function arg).
- avg_return_pct (and any avg_cagr) must EXCLUDE stale funds and error/None-return
  funds — never silently average across mismatched as-of dates. Report how many
  funds were excluded and why.
- Keep stale funds in the results list (flagged), do not drop them from output.
- Optional, only if it's a small addition: a common_as_of mode (arg) that ranks
  every fund to a single supplied/derived date instead of per-fund anchors. If it
  adds meaningful complexity, skip it and note that in the summary — the staleness
  guard is the required deliverable.

Test on a real category that contains at least one fund with an older last NAV:
confirm it's flagged stale, excluded from the average, and still present in
results.
```

**Done when:** a stale fund in a real category is flagged, kept in the list, and excluded from the average.

---

### Prompt 7 — Golden-reference verification of the boundary snap  `[verify]`

The trailing-period start snaps *strictly forward* past `anchor − period`, which
may skip the boundary NAV and undercount by a hair. The code comment claims it
matches ET Money / Value Research — verify against real published numbers rather
than trusting the comment.

```
Verification task. Do not change the math first — measure, then fix only if the
data says so.

- Write sif/scripts/verify_returns.py that, for 3–5 real funds, computes
  1Y / 3Y / 5Y (return_pct and cagr_pct) and prints them next to the figures
  published by a reference source (Value Research / ET Money / AMFI) for the SAME
  as-of date. IMPORTANT: align the as-of date before comparing — a mismatched
  as-of will look like a math bug when it isn't. Output a comparison table.
- If computed matches published within tolerance: done, record it.
- If trailing periods are consistently a touch low: the strict-forward start snap
  is the likely cause. Change the trailing-period start to snap to the first NAV
  on-or-after (anchor − period) INCLUSIVE (i.e. include the boundary day) rather
  than strictly after, re-run, and confirm the gap closes. Leave YTD/MTD/SI snap
  logic untouched. Document any deliberate remaining deviation.

No mock data — use real scheme_codes and real published references.
```

**Done when:** computed 1Y/3Y/5Y match a published reference within tolerance, with any deviation deliberately documented.

---

## After the sequence — keep context files honest

You've added a module (`sif/mcp/server.py`), new tools (`search_funds`), a new
output field (`cagr_pct`), and a run/registration path. Per your context
discipline, update `project-context.md` / `AGENTS.md` to register the MCP server
module, the run command, and the client-registration steps, so Cursor's baseline
doesn't drift. If you want, I can generate that drift-update block (with your
`<!-- DRIFT?: -->` markers) as its own snippet.
