"""Tests for the remote metadata gateway tools."""

from __future__ import annotations

import importlib

import pytest

from servers._launcher import SERVERS
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


@pytest.mark.asyncio
async def test_gateway_advertises_only_registered_server_tools(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    for dataset in server.DATASETS:
        spec = SERVERS[dataset.server]
        module = importlib.import_module(spec.module)
        tools = await module.mcp.list_tools()
        registered = {tool.name for tool in tools}

        assert set(dataset.tools) <= registered, (
            f"{dataset.id} advertises missing tools for {dataset.server}: "
            f"{sorted(set(dataset.tools) - registered)}"
        )


def test_gateway_includes_april_2026_servers() -> None:
    dataset_ids = {dataset.id for dataset in server.DATASETS}
    workforce_dataset = next(dataset for dataset in server.DATASETS if dataset.id == "workforce-analytics")

    assert "service-area" in dataset_ids
    assert "provider-enrollment" in dataset_ids
    assert "community-health" in dataset_ids
    assert "research-trials" in dataset_ids
    assert "resolve_hospital_beds" in workforce_dataset.tools
