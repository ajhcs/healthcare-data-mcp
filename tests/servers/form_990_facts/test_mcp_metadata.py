from __future__ import annotations

import asyncio

from servers.form_990_facts import server


def test_query_tool_advertises_a_single_read_only_closed_world_operation() -> None:
    tools = asyncio.run(server.mcp.list_tools())

    assert [tool.name for tool in tools] == ["get_form_990_facts"]
    tool = tools[0]
    assert tool.inputSchema["required"] == ["ein", "tax_year"]
    assert tool.outputSchema is not None
    assert tool.annotations is not None
    assert tool.annotations.model_dump(exclude_none=True) == {
        "title": "Get exact Form 990 facts",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }


def test_server_instructions_keep_the_exact_filer_boundary_compact() -> None:
    assert server.mcp.instructions == (
        "Read-only lookup for pre-ingested official IRS Form 990 facts. "
        "Use only with a known legal-filer EIN and tax-period year; preserve "
        "returned provenance and do not infer brands, affiliates, or comparisons."
    )
