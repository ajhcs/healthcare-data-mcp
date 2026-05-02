"""Tests for the authenticated live-data gateway."""

from __future__ import annotations

import pytest

from shared.utils.gateway_auth import GatewayAuthError, load_gateway_security_config
from servers.live_gateway import server


@pytest.mark.asyncio
async def test_live_gateway_registers_allowlisted_tools() -> None:
    tools = await server.mcp.list_tools()
    tool_names = {tool.name for tool in tools}
    allowlist_names = {spec.tool_name for spec in server.LIVE_TOOL_SPECS}

    assert "list_live_tools" in tool_names
    assert allowlist_names <= tool_names
    assert len(tool_names) == len(allowlist_names) + 1
    assert all(tool.outputSchema for tool in tools)


@pytest.mark.asyncio
async def test_list_live_tools_returns_inventory() -> None:
    result = await server.list_live_tools()
    names = {tool["name"] for tool in result["tools"]}

    assert result["gateway"] == "live-gateway"
    assert result["tool_count"] == len(server.LIVE_TOOL_SPECS)
    assert "search_provider_enrollment" in names
    assert "search_clinical_trials" in names
    assert "search_sam_exclusions" in names


def test_live_gateway_http_env_requires_credentials() -> None:
    mapped = server._live_gateway_env({}, require_auth=True)

    assert mapped["MCP_GATEWAY_AUTH_REQUIRED"] == "true"
    with pytest.raises(GatewayAuthError):
        load_gateway_security_config(mapped)


@pytest.mark.parametrize("disabled_value", ["0", "false", "no", "off"])
def test_live_gateway_http_env_rejects_explicit_auth_disable(disabled_value: str) -> None:
    with pytest.raises(GatewayAuthError, match="cannot disable auth"):
        server._live_gateway_env(
            {
                "MCP_LIVE_GATEWAY_AUTH_REQUIRED": disabled_value,
                "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            },
            require_auth=True,
        )


def test_live_gateway_stdio_env_can_disable_auth_without_credentials() -> None:
    mapped = server._live_gateway_env(
        {"MCP_LIVE_GATEWAY_AUTH_REQUIRED": "false"},
        require_auth=False,
    )
    config = load_gateway_security_config(mapped)

    assert mapped["MCP_GATEWAY_AUTH_REQUIRED"] == "false"
    assert config.auth_enabled is False
    assert config.bearer_tokens == ()


def test_live_gateway_env_prefix_maps_auth_settings() -> None:
    mapped = server._live_gateway_env(
        {
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "live.example.org",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "https://chatgpt.com",
            "MCP_LIVE_GATEWAY_PUBLIC_URL": "https://live.example.org/mcp",
        },
        require_auth=True,
    )
    config = load_gateway_security_config(mapped)

    assert config.auth_enabled is True
    assert config.bearer_tokens == ("this-is-a-long-live-token",)
    assert config.allowed_hosts == ("live.example.org",)
    assert config.allowed_origins == ("https://chatgpt.com",)
    assert config.public_url == "https://live.example.org/mcp"


@pytest.mark.asyncio
async def test_live_gateway_exposes_callable_tool_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"called": "search_provider_enrollment", "kwargs": kwargs}

    monkeypatch.setattr(server, "search_provider_enrollment", fake_search_provider_enrollment)

    result = await server.search_provider_enrollment(npi="1234567893", limit=1)

    assert result == {
        "called": "search_provider_enrollment",
        "kwargs": {"npi": "1234567893", "limit": 1},
    }
