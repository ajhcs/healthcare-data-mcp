"""MCP design contract tests for agent-facing tool UX."""

from __future__ import annotations

import ast
from pathlib import Path

from shared.utils.tool_clusters import TOOL_CLUSTERS


REQUIRED_SECTIONS = (
    "Discovery",
    "When to use",
    "Parameters",
    "Returns",
    "Do / Don't",
    "Examples",
    "Common mistakes",
)


def _is_mcp_tool(node: ast.AST) -> bool:
    if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
        return False
    for dec in node.decorator_list:
        func = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id == "mcp" and func.attr == "tool":
                return True
    return False


def test_all_mcp_tool_docstrings_have_agent_contract_sections() -> None:
    missing: list[str] = []
    for path in sorted(Path("servers").rglob("server.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if _is_mcp_tool(node):
                doc = ast.get_docstring(node) or ""
                absent = [section for section in REQUIRED_SECTIONS if section not in doc]
                if absent:
                    missing.append(f"{path}:{node.lineno}:{node.name}: {', '.join(absent)}")
    assert not missing


def test_oversized_server_tool_surfaces_have_small_capability_clusters() -> None:
    assert {"public-records", "workforce-analytics", "financial-intelligence", "discovery", "research-trials"} <= set(
        TOOL_CLUSTERS
    )
    oversized = []
    for server_id, clusters in TOOL_CLUSTERS.items():
        for cluster_name, tools in clusters.items():
            if len(tools) > 7:
                oversized.append(f"{server_id}.{cluster_name}={len(tools)}")
    assert not oversized
