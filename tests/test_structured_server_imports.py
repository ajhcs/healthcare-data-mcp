"""Import checks for servers migrated to FastMCP structured tool output."""

from __future__ import annotations

import importlib

import pytest


STRUCTURED_SERVER_MODULES = [
    "servers.claims-analytics.server",
    "servers.cms-facility.server",
    "servers.community_health.server",
    "servers.drive-time.server",
    "servers.financial-intelligence.server",
    "servers.geo-demographics.server",
    "servers.health-system-profiler.server",
    "servers.hospital-quality.server",
    "servers.live_gateway.server",
    "servers.physician-referral-network.server",
    "servers.price-transparency.server",
    "servers.provider_enrollment.server",
    "servers.public_records.server",
    "servers.research_trials.server",
    "servers.service-area.server",
    "servers.web_intelligence.server",
    "servers.workforce-analytics.server",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("module_name", STRUCTURED_SERVER_MODULES)
async def test_structured_server_tools_import_with_output_schemas(monkeypatch: pytest.MonkeyPatch, module_name: str) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name in {"geopandas", "networkx"}:
            pytest.skip(f"{exc.name} is not installed in this test environment")
        raise

    tools = await module.mcp.list_tools()

    assert tools
    assert all(tool.outputSchema for tool in tools)
