"""Tests for the remote metadata gateway tools."""

from __future__ import annotations

import pytest

from servers.gateway import server


@pytest.mark.asyncio
async def test_gateway_search_returns_structured_results() -> None:
    result = await server.search("CMS quality", max_results=3)

    assert result["query"] == "CMS quality"
    assert result["count"] <= 3
    assert result["results"]
    assert isinstance(result["results"][0]["metadata"]["tags"], list)


@pytest.mark.asyncio
async def test_gateway_fetch_returns_structured_document() -> None:
    result = await server.fetch("cms-facility")

    assert result["id"] == "cms-facility"
    assert "CMS Facility" in result["title"]
    assert result["metadata"]["server"] == "cms-facility"


@pytest.mark.asyncio
async def test_gateway_fetch_unknown_dataset_is_helpful() -> None:
    result = await server.fetch("missing")

    assert result["error"] == "dataset_not_found"
    assert "cms-facility" in result["available_ids"]


@pytest.mark.asyncio
async def test_gateway_fastmcp_tools_have_output_schemas() -> None:
    tools = await server.mcp.list_tools()
    by_name = {tool.name: tool for tool in tools}

    assert by_name["search"].outputSchema
    assert by_name["fetch"].outputSchema
