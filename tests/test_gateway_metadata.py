"""Tests for the remote metadata gateway tools."""

from __future__ import annotations

import importlib

import pytest

from servers._launcher import SERVERS
from servers.gateway import server
from shared.utils.server_registry import SERVER_BY_ID, SERVER_REGISTRY


@pytest.mark.asyncio
async def test_gateway_search_returns_structured_results() -> None:
    result = await server.search("CMS quality", max_results=3)

    assert result["query"] == "CMS quality"
    assert result["count"] <= 3
    assert result["results"]
    assert isinstance(result["results"][0]["metadata"]["tags"], list)
    assert "port" in result["results"][0]["metadata"]
    assert isinstance(result["results"][0]["metadata"]["profiles"], list)
    assert isinstance(result["results"][0]["metadata"]["workflow_roles"], list)
    assert isinstance(result["results"][0]["metadata"]["dataset_ids"], list)
    assert isinstance(result["results"][0]["metadata"]["cache_needs"], list)
    assert isinstance(result["results"][0]["metadata"]["safety_notes"], list)


@pytest.mark.asyncio
async def test_gateway_fetch_returns_structured_document() -> None:
    result = await server.fetch("cms-facility")

    assert result["id"] == "cms-facility"
    assert "CMS Facility" in result["title"]
    assert result["metadata"]["server"] == "cms-facility"
    assert result["metadata"]["server_capability"]["server_id"] == "cms-facility"
    assert result["metadata"]["server_capability"]["port"] == SERVER_BY_ID["cms-facility"].port
    assert result["metadata"]["server_capability"]["cache_needs"] == list(SERVER_BY_ID["cms-facility"].cache_needs)


@pytest.mark.asyncio
async def test_gateway_fetch_unknown_dataset_is_helpful() -> None:
    result = await server.fetch("missing")

    assert result["ok"] is False
    assert result["legacy_error"] == "dataset_not_found"
    assert result["message"] == "No dataset metadata found for 'missing'. Call search first and use a returned id."
    assert "cms-facility" in result["available_ids"]
    assert result["error"]["code"] == "dataset_not_found"
    assert result["error"]["type"] == "NOT_FOUND"
    assert "cms-facility" in result["error"]["data"]["available_options"]


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
        try:
            module = importlib.import_module(spec.module)
        except ModuleNotFoundError as exc:
            if exc.name in {"geopandas", "networkx", "osmnx", "aceso", "duckdb", "pyarrow", "polars"}:
                continue
            raise
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
    assert "discovery" in dataset_ids
    assert "gateway" in dataset_ids
    assert "provider-enrollment" in dataset_ids
    assert "community-health" in dataset_ids
    assert "research-trials" in dataset_ids
    assert "resolve_hospital_beds" in workforce_dataset.tools


def test_gateway_dataset_coverage_matches_registry_metadata_exposure() -> None:
    expected = {spec.server_id for spec in SERVER_REGISTRY if "metadata" in spec.gateway_exposure}
    dataset_ids = {dataset.id for dataset in server.DATASETS}
    dataset_servers = {dataset.server for dataset in server.DATASETS}

    assert dataset_ids == expected
    assert dataset_servers == expected
    assert "live-gateway" not in dataset_ids


def test_gateway_dataset_servers_are_registered_and_declared_for_metadata() -> None:
    for dataset in server.DATASETS:
        assert dataset.server in SERVER_BY_ID
        assert "metadata" in SERVER_BY_ID[dataset.server].gateway_exposure


def test_gateway_dataset_contracts_are_registry_backed_without_importing_servers() -> None:
    validation = server.validate_gateway_dataset_contracts()

    assert validation["status"] == "ok"
    assert validation["issue_count"] == 0
    assert validation["dataset_count"] == validation["expected_dataset_count"]
    assert validation["method"] == "registry_gateway_dataset_ast"
    assert validation["datasets"]["public-records"]["tool_count"] >= 1


@pytest.mark.asyncio
async def test_gateway_fetch_exposes_registry_capability_metadata_for_all_datasets() -> None:
    for dataset in server.DATASETS:
        result = await server.fetch(dataset.id)
        capability = result["metadata"]["server_capability"]
        spec = SERVER_BY_ID[dataset.server]

        assert capability["server_id"] == spec.server_id
        assert capability["module"] == spec.module
        assert capability["port"] == spec.port
        assert capability["zero_config"] is spec.zero_config
        assert capability["dataset_ids"] == list(spec.dataset_ids)
        assert capability["gateway_exposure"] == list(spec.gateway_exposure)
        assert capability["profiles"] == list(spec.profiles)
        assert capability["workflow_roles"] == list(spec.workflow_roles)
        assert capability["safety_notes"] == list(spec.safety_notes)


@pytest.mark.asyncio
async def test_gateway_search_exposes_registry_source_cache_and_safety_metadata_for_all_results() -> None:
    result = await server.search("", max_results=20)

    assert result["count"] == len(server.DATASETS)
    for row in result["results"]:
        spec = SERVER_BY_ID[row["metadata"]["server"]]
        assert row["metadata"]["dataset_ids"] == list(spec.dataset_ids)
        assert row["metadata"]["cache_needs"] == list(spec.cache_needs)
        assert row["metadata"]["safety_notes"] == list(spec.safety_notes)
