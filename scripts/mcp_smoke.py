"""Smoke-test healthcare-data-mcp servers through the MCP client protocol.

This is intentionally small and CI-friendly. It is not a replacement for the
interactive MCP Inspector, but it exercises the same protocol path: initialize,
list tools/resources, and optionally call one read-only tool.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from shared.utils.server_registry import SERVER_BY_ID


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MCP protocol smoke checks against an hc-mcp stdio server.")
    parser.add_argument("--server", required=True, help="hc-mcp server id, e.g. discovery or gateway")
    parser.add_argument("--expect-tool", action="append", default=[], help="Tool name expected from tools/list")
    parser.add_argument("--expect-resource", action="append", default=[], help="Resource URI expected from resources/list")
    parser.add_argument("--call-tool", default="", help="Optional read-only tool to call after listing tools")
    parser.add_argument("--tool-args", default="{}", help="JSON object passed to --call-tool")
    parser.add_argument(
        "--expect-structured-key",
        action="append",
        default=[],
        help="Top-level structuredContent key expected after --call-tool. Repeat for multiple keys.",
    )
    parser.add_argument(
        "--expect-structured-path",
        action="append",
        default=[],
        help="Nested structuredContent path expected after --call-tool. Use [] for any list item, e.g. workflows[].identity_join_keys.",
    )
    parser.add_argument(
        "--expect-structured-path-all",
        action="append",
        default=[],
        help=(
            "Nested structuredContent path required for every item under [] list segments. "
            "Use this for policy catalogs where every advertised item must expose a field, "
            "e.g. tools[].allowed_scopes."
        ),
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Overall timeout in seconds")
    return parser.parse_args()


def structured_path_exists(value: Any, path: str) -> bool:
    """Return whether a dotted structuredContent path exists.

    Path segments ending in [] require a non-empty list and match when any item
    contains the remaining path. This keeps smoke assertions compact while
    avoiding assumptions about catalog ordering.
    """

    parts = [part for part in path.split(".") if part]
    if not parts:
        return False
    return _structured_path_exists(value, parts)


def structured_path_exists_for_all(value: Any, path: str) -> bool:
    """Return whether every item under [] list segments contains the path."""

    parts = [part for part in path.split(".") if part]
    if not parts:
        return False
    return _structured_path_exists_with_mode(value, parts, list_mode="all")


def _structured_path_exists(value: Any, parts: list[str]) -> bool:
    return _structured_path_exists_with_mode(value, parts, list_mode="any")


def _structured_path_exists_with_mode(value: Any, parts: list[str], *, list_mode: str) -> bool:
    if not parts:
        return True
    part, *rest = parts
    if part.endswith("[]"):
        key = part[:-2]
        if not isinstance(value, dict) or key not in value or not isinstance(value[key], list) or not value[key]:
            return False
        matches = [_structured_path_exists_with_mode(item, rest, list_mode=list_mode) for item in value[key]]
        return all(matches) if list_mode == "all" else any(matches)
    if not isinstance(value, dict) or part not in value:
        return False
    if not rest:
        return True
    return _structured_path_exists_with_mode(value[part], rest, list_mode=list_mode)


async def smoke(args: argparse.Namespace) -> dict[str, Any]:
    if args.server not in SERVER_BY_ID:
        raise SystemExit(f"Unknown server {args.server!r}; choose one of {', '.join(sorted(SERVER_BY_ID))}")

    spec = SERVER_BY_ID[args.server]
    env = os.environ.copy()
    env.setdefault("SEC_USER_AGENT", "healthcare-data-mcp smoke@example.com")
    env.setdefault("PYTHONWARNINGS", "ignore:.*found in sys.modules after import.*:RuntimeWarning")
    params = StdioServerParameters(
        command=sys.executable,
        args=["-W", "ignore::RuntimeWarning", "-m", spec.module],
        env=env,
        cwd=os.getcwd(),
    )

    async with stdio_client(params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools_result = await session.list_tools()
            tool_names = sorted(tool.name for tool in tools_result.tools)
            missing_tools = sorted(set(args.expect_tool) - set(tool_names))
            if missing_tools:
                raise SystemExit(f"{args.server} missing expected tool(s): {', '.join(missing_tools)}")

            resource_uris: list[str] = []
            if args.expect_resource:
                resources_result = await session.list_resources()
                resource_uris = sorted(str(resource.uri) for resource in resources_result.resources)
                missing_resources = sorted(set(args.expect_resource) - set(resource_uris))
                if missing_resources:
                    raise SystemExit(f"{args.server} missing expected resource(s): {', '.join(missing_resources)}")

            tool_call_result = None
            structured_content: Any = None
            if args.call_tool:
                tool_args = json.loads(args.tool_args)
                if not isinstance(tool_args, dict):
                    raise SystemExit("--tool-args must decode to a JSON object")
                tool_call_result = await session.call_tool(args.call_tool, tool_args)
                if bool(getattr(tool_call_result, "isError", False)):
                    error_text = " ".join(str(getattr(item, "text", "")) for item in tool_call_result.content)
                    raise SystemExit(f"{args.server} tool {args.call_tool} returned an MCP error: {error_text}")
                structured_content = getattr(tool_call_result, "structuredContent", None) or {}
                if args.expect_structured_key or args.expect_structured_path or args.expect_structured_path_all:
                    if not isinstance(structured_content, dict):
                        raise SystemExit(
                            f"{args.server} tool {args.call_tool} did not return object structuredContent"
                        )
                if args.expect_structured_key:
                    missing_keys = sorted(set(args.expect_structured_key) - set(structured_content))
                    if missing_keys:
                        raise SystemExit(
                            f"{args.server} tool {args.call_tool} missing structuredContent key(s): "
                            f"{', '.join(missing_keys)}"
                        )
                if args.expect_structured_path:
                    missing_paths = sorted(
                        path
                        for path in args.expect_structured_path
                        if not structured_path_exists(structured_content, path)
                    )
                    if missing_paths:
                        raise SystemExit(
                            f"{args.server} tool {args.call_tool} missing structuredContent path(s): "
                            f"{', '.join(missing_paths)}"
                        )
                if args.expect_structured_path_all:
                    missing_all_paths = sorted(
                        path
                        for path in args.expect_structured_path_all
                        if not structured_path_exists_for_all(structured_content, path)
                    )
                    if missing_all_paths:
                        raise SystemExit(
                            f"{args.server} tool {args.call_tool} missing structuredContent path(s) "
                            f"for one or more list items: {', '.join(missing_all_paths)}"
                        )

            return {
                "server": args.server,
                "tool_count": len(tool_names),
                "tools": tool_names,
                "resource_count": len(resource_uris),
                "resources": resource_uris,
                "called_tool": args.call_tool or "",
                "call_content_count": len(tool_call_result.content) if tool_call_result is not None else 0,
                "structured_keys": sorted(structured_content) if isinstance(structured_content, dict) else [],
                "structured_paths": sorted(args.expect_structured_path),
                "structured_paths_all": sorted(args.expect_structured_path_all),
            }


async def main_async() -> None:
    args = parse_args()
    result = await asyncio.wait_for(smoke(args), timeout=args.timeout)
    print(json.dumps(result, indent=2, sort_keys=True))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
