"""Fast smoke coverage for all 13 MCP server modules.

This is intentionally lightweight enough for CI:
- import each server module
- ensure a FastMCP instance is exposed
- verify the expected number of registered tools
"""

import importlib

import pytest

SERVER_SPECS = [
    ("cms-facility", "servers.cms_facility.server", 5),
    ("service-area", "servers.service_area.server", 4),
    ("geo-demographics", "servers.geo_demographics.server", 6),
    ("drive-time", "servers.drive_time.server", 5),
    ("hospital-quality", "servers.hospital_quality.server", 6),
    ("health-system-profiler", "servers.health_system_profiler.server", 3),
    ("financial-intelligence", "servers.financial_intelligence.server", 6),
    ("price-transparency", "servers.price_transparency.server", 5),
    ("physician-referral-network", "servers.physician_referral_network.server", 5),
    ("workforce-analytics", "servers.workforce_analytics.server", 7),
    ("claims-analytics", "servers.claims_analytics.server", 5),
    ("public-records", "servers.public_records.server", 6),
    ("web-intelligence", "servers.web_intelligence.server", 5),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("server_name", "module_name", "expected_tool_count"), SERVER_SPECS)
async def test_server_smoke(server_name: str, module_name: str, expected_tool_count: int):
    module = importlib.import_module(module_name)

    assert hasattr(module, "mcp"), f"{server_name} should expose an mcp server object"

    tools = await module.mcp.list_tools()
    tool_names = [tool.name for tool in tools]

    assert len(tool_names) == expected_tool_count, (
        f"{server_name} expected {expected_tool_count} tools, got {len(tool_names)}: {tool_names}"
    )
