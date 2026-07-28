"""
mcp_client_test.py — drive server.py over a real MCP stdio connection.

Everything in the probes so far called the tool functions in-process, which
skips the entire MCP layer. This spawns server.py as a subprocess and talks to
it the way Cursor / Claude Desktop do: handshake, tools/list, tools/call. It is
the only way to know the transport, the generated schemas, and the JSON
serialization of results actually work.

Reads .env for AZURE_STORAGE_CONNECTION_STRING and points the server at the SIF
blob URIs from sif/config/constants.py.

Run:
    python mcp_client_test.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

HERE = Path(__file__).resolve().parent

# The SIF blob layout, per sif/config/constants.py.
AZ_NAV_HISTORY = "az://sifnavdata/processed/nav_history/year=*/*.parquet"
AZ_SCHEME_MASTER = "az://sifnavdata/processed/scheme_master.parquet"


def load_env_file(path: Path) -> dict[str, str]:
    """Minimal .env reader — avoids depending on python-dotenv being present."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        out[key.strip()] = val.strip().strip('"').strip("'")
    return out


def build_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update(load_env_file(HERE / ".env"))
    env["NAV_HISTORY_PATH"] = AZ_NAV_HISTORY
    env["SCHEME_MASTER_PATH"] = AZ_SCHEME_MASTER
    if not env.get("AZURE_STORAGE_CONNECTION_STRING"):
        sys.exit("No AZURE_STORAGE_CONNECTION_STRING found (.env or environment).")
    return env


def dump_result(label: str, result) -> None:
    """Print what the SERVER actually sent back over the wire.

    Deliberately does NOT use json.dumps(default=str): the point is to see the
    real serialized payload, including how date objects came across.
    """
    print(f"\n--- {label} ---")
    if getattr(result, "isError", False):
        print("  isError: True")
    for block in result.content:
        text = getattr(block, "text", None)
        if text is None:
            print(f"  <{type(block).__name__}> {block!r}")
            continue
        try:
            print(json.dumps(json.loads(text), indent=2)[:1600])
        except json.JSONDecodeError:
            print(f"  (non-JSON text) {text[:600]}")


async def main() -> None:
    params = StdioServerParameters(
        command=sys.executable,
        args=[str(HERE / "server.py")],
        env=build_env(),
    )

    print("Spawning server.py over stdio and connecting...")
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print(f"Connected. Server: {init.serverInfo.name} v{init.serverInfo.version}")

            # 1) Schemas — what a client sees when deciding which tool to call.
            tools = await session.list_tools()
            print(f"\n=== tools/list -> {len(tools.tools)} tools ===")
            for t in tools.tools:
                props = (t.inputSchema or {}).get("properties", {})
                required = (t.inputSchema or {}).get("required", [])
                print(f"  {t.name}({', '.join(props)})  required={required}")

            # The Union[str, list[str]] hint on get_fund_returns is the schema
            # most likely to render oddly for a client — show it in full.
            gfr = next((t for t in tools.tools if t.name == "get_fund_returns"), None)
            if gfr:
                print("\n=== get_fund_returns inputSchema ===")
                print(json.dumps(gfr.inputSchema, indent=2))

            # 2) No-arg call.
            dump_result("list_categories()", await session.call_tool("list_categories", {}))

            # 3) Name resolution.
            dump_result(
                "search_funds('iSIF Equity Long-Short', limit=3)",
                await session.call_tool(
                    "search_funds", {"query": "iSIF Equity Long-Short", "limit": 3}
                ),
            )

            # 4) THE test: real returns with date objects in the payload.
            dump_result(
                "get_fund_returns('SIF-125', '1M')",
                await session.call_tool(
                    "get_fund_returns", {"scheme_codes": "SIF-125", "period": "1M"}
                ),
            )

            # 5) List form of the same union-typed arg.
            dump_result(
                "get_fund_returns(['SIF-125','SIF-124'], '1M')",
                await session.call_tool(
                    "get_fund_returns",
                    {"scheme_codes": ["SIF-125", "SIF-124"], "period": "1M"},
                ),
            )

            # 6) Insufficient history — SIF is a young asset class, so 1Y has no
            #    start NAV. Expect a clean error row, NOT a protocol error.
            dump_result(
                "get_fund_returns('SIF-125', '1Y')  [expect clean error row]",
                await session.call_tool(
                    "get_fund_returns", {"scheme_codes": "SIF-125", "period": "1Y"}
                ),
            )

            # 7) Invalid period — raises ValueError server-side. Expect the MCP
            #    layer to surface it as isError, not to kill the connection.
            dump_result(
                "get_fund_returns('SIF-125', '10Y')  [expect isError]",
                await session.call_tool(
                    "get_fund_returns", {"scheme_codes": "SIF-125", "period": "10Y"}
                ),
            )

            # 8) Category call — the heaviest path.
            dump_result(
                "get_category_returns('Equity Long-Short Fund', '1M')",
                await session.call_tool(
                    "get_category_returns",
                    {"category": "Equity Long-Short Fund", "period": "1M"},
                ),
            )

            # 9) Connection still alive after an error? Proves the shared DuckDB
            #    handle and the session both survived step 7.
            after = await session.call_tool("list_categories", {})
            ok = not getattr(after, "isError", False)
            print(f"\n=== connection alive after error: {ok} ===")

    print("\nDone — server exited cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
