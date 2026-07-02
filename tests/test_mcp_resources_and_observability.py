"""Tests for standard per-server MCP resources and tooling metrics."""

from __future__ import annotations

import importlib

import pytest

from servers._launcher import SERVERS
from servers.gateway import server as gateway_server
from shared.utils.mcp_observability import observe_tool, tooling_metrics_payload


@pytest.mark.asyncio
async def test_standard_resources_registered_on_gateway() -> None:
    resources = await gateway_server.mcp.list_resources()
    uris = {str(resource.uri) for resource in resources}

    assert "healthcare-data://server/gateway/capabilities" in uris
    assert "healthcare-data://server/gateway/datasets" in uris
    assert "healthcare-data://server/gateway/examples" in uris
    assert "healthcare-data://server/gateway/identity-contract" in uris
    assert "healthcare-data://server/gateway/source-ledger" in uris
    assert "healthcare-data://server/gateway/tooling/metrics" in uris


@pytest.mark.asyncio
async def test_standard_resources_registered_on_all_server_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")
    expected_suffixes = {
        "capabilities",
        "datasets",
        "examples",
        "identity-contract",
        "source-ledger",
        "tooling/metrics",
    }

    for server_id, spec in SERVERS.items():
        try:
            module = importlib.import_module(spec.module)
        except ModuleNotFoundError as exc:
            if exc.name in {"geopandas", "networkx", "osmnx", "aceso", "duckdb", "pyarrow", "polars"}:
                continue
            raise
        resources = await module.mcp.list_resources()
        uris = {str(resource.uri) for resource in resources}

        for suffix in expected_suffixes:
            assert f"healthcare-data://server/{server_id}/{suffix}" in uris


def test_standard_capabilities_link_source_contract_and_ledger() -> None:
    from shared.utils.mcp_resources import server_capabilities_payload, server_source_ledger_payload

    capabilities = server_capabilities_payload("public-records")
    ledger = server_source_ledger_payload("public-records")

    assert capabilities["source_backed_contract"]["contract_uri"] == "docs/SOURCE_BACKED_RESULT_CONTRACT.md"
    assert capabilities["source_backed_contract"]["ledger_resource"] == "healthcare-data://server/public-records/source-ledger"
    assert capabilities["source_backed_contract"]["ledger_doc"] == "docs/SOURCE_CAPABILITY_LEDGER.md"
    assert "source-ledger" in capabilities["next_actions"][0]

    assert ledger["server_id"] == "public-records"
    assert ledger["ledger_doc"] == "docs/SOURCE_CAPABILITY_LEDGER.md"
    assert ledger["contract_doc"] == "docs/SOURCE_BACKED_RESULT_CONTRACT.md"
    assert ledger["dataset_ids"]
    assert ledger["gateway_exposure"] == ["metadata", "live"]
    assert ledger["operator_rules"]


@pytest.mark.asyncio
async def test_observe_tool_records_non_secret_metrics() -> None:
    @observe_tool("test-server", "demo")
    async def demo_tool() -> dict:
        return {"source_metadata": {"dataset_id": "demo_dataset", "cache_status": "ready"}}

    await demo_tool()
    payload = tooling_metrics_payload("test-server")

    assert payload["event_count"] >= 1
    event = payload["events"][-1]
    assert event["server"] == "test-server"
    assert event["tool"] == "demo"
    assert event["outcome"] == "ok"
    assert event["dataset_id"] == "demo_dataset"
    assert event["cache_status"] == "ready"
