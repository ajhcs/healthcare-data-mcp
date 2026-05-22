"""Tests for the authenticated live-data gateway."""

from __future__ import annotations

import json
import os

from mcp.server.auth.middleware.auth_context import auth_context_var
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken
from mcp.server.fastmcp.exceptions import ToolError
import pytest

os.environ.setdefault("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

from shared.utils.gateway_auth import GatewayAuthError, load_gateway_security_config, token_sha256
from shared.utils.mcp_response import evidence_receipt
from shared.utils.server_registry import SERVER_BY_ID
from servers.live_gateway import server


def _valid_evidence(match_basis: str = "unit_test") -> dict:
    return evidence_receipt(
        source_name="CMS Provider Enrollment",
        source_url="https://data.cms.gov/provider-enrollment",
        dataset_id="cms-provider-enrollment",
        source_period="current public file",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="hit",
        match_basis=match_basis,
        confidence="high",
        caveat="Public enrollment rows require source-system verification before operational decisions.",
        next_step="Review the returned row against the source record.",
    )


@pytest.mark.asyncio
async def test_live_gateway_registers_allowlisted_tools() -> None:
    tools = await server.mcp.list_tools()
    tool_names = {tool.name for tool in tools}
    allowlist_names = {spec.tool_name for spec in server.LIVE_TOOL_SPECS}

    assert "list_live_tools" in tool_names
    assert "get_live_gateway_audit_events" in tool_names
    assert allowlist_names <= tool_names
    assert len(tool_names) == len(allowlist_names) + 2
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
    assert "resolve_hospital_beds" in names
    first_tool = result["tools"][0]
    assert first_tool["allowed_scopes"] == ["mcp:read"]
    assert first_tool["request_size_limit_bytes"] == 32768
    assert first_tool["result_size_limit_bytes"] == 262144
    assert first_tool["result_limit"] == 100
    assert first_tool["auth_posture"] == "bearer_required_for_http_sse"
    assert first_tool["requires_provenance"] is True
    assert first_tool["dataset_ids"] == list(SERVER_BY_ID[first_tool["server"]].dataset_ids)
    assert first_tool["cache_needs"] == list(SERVER_BY_ID[first_tool["server"]].cache_needs)
    assert first_tool["server_safety_notes"] == list(SERVER_BY_ID[first_tool["server"]].safety_notes)
    bulk_tool = next(tool for tool in result["tools"] if tool["name"] == "screen_leie_batch")
    assert bulk_tool["allowed_scopes"] == ["mcp:read", "mcp:bulk"]
    assert bulk_tool["rate_limit_class"] == "bulk"
    assert bulk_tool["source_caveat_class"] == "exclusion_screening"
    assert bulk_tool["source_caveat"]
    assert "hhs_oig_leie" in bulk_tool["dataset_ids"]
    assert "hhs-oig-leie" in bulk_tool["cache_needs"]
    cyber_tool = next(tool for tool in result["tools"] if tool["name"] == "get_cyber_incident_profile")
    assert cyber_tool["source_caveat_class"] == "public_breach_or_state_record"
    assert result["policy"]["gateway_type"] == "live_policy_gateway"
    assert result["policy"]["allowed_tool_source"] == (
        "LIVE_TOOL_SPECS allowlist validated against registry gateway_exposure=live"
    )
    assert result["policy"]["allowed_scopes"] == ["mcp:bulk", "mcp:read"]
    assert result["policy"]["default_request_size_limit_bytes"] == 32768
    assert result["policy"]["default_result_size_limit_bytes"] == 262144
    assert result["policy"]["token_scope_overrides"]["env"] == "MCP_LIVE_GATEWAY_TOKEN_SCOPES"
    assert result["policy"]["token_scope_overrides"]["format"] == "<token_sha256>=mcp:read+mcp:bulk"
    assert result["policy"]["source_caveat_classes"]["exclusion_screening"]
    assert result["policy"]["audit_event_shape"]["event"] == "tool_call"
    assert result["policy"]["audit_event_shape"]["source_caveat_class"] == "<source_caveat_class>"
    assert result["policy"]["audit_event_shape"]["sensitive_argument_keys"] == "<redacted_key_names_when_blocked>"
    assert result["policy"]["audit_event_shape"]["invalid_evidence_paths"] == (
        "<paths_and_errors_when_nested_or_top_level_receipts_fail_validation>"
    )
    assert any("wildcard network binds" in note for note in result["policy"]["safe_defaults"])
    assert any("SSN/EIN/TIN" in note for note in result["policy"]["safe_defaults"])
    assert any("Nested request arrays" in note for note in result["policy"]["safe_defaults"])
    assert any("nested row receipts" in note for note in result["policy"]["safe_defaults"])
    assert any("empty upstream evidence receipts" in note for note in result["policy"]["safe_defaults"])
    assert any("Missing upstream evidence receipts" in note for note in result["policy"]["safe_defaults"])
    assert any("MCP_LIVE_GATEWAY_TOKEN_SCOPES" in note for note in result["policy"]["safe_defaults"])
    assert any("global mcp:bulk scope is rejected" in note for note in result["policy"]["safe_defaults"])
    assert any("SSN/EIN/TIN" in note for note in result["notes"])


def test_live_gateway_policy_specs_are_valid() -> None:
    server._validate_live_policy_specs()
    assert {
        server._effective_source_caveat_class(spec)
        for spec in server.LIVE_TOOL_SPECS
    } <= set(server.SOURCE_CAVEAT_CLASSES)


def test_live_gateway_allowlist_matches_registry_live_exposure() -> None:
    live_gateway_servers = {spec.server for spec in server.LIVE_TOOL_SPECS}

    for spec in server.LIVE_TOOL_SPECS:
        registry_spec = SERVER_BY_ID[spec.server]
        assert spec.module == registry_spec.module
        assert "live" in registry_spec.gateway_exposure

    registry_live_servers = {
        registry_spec.server_id
        for registry_spec in SERVER_BY_ID.values()
        if "live" in registry_spec.gateway_exposure and registry_spec.server_id != "live-gateway"
    }
    assert live_gateway_servers == registry_live_servers


def test_live_gateway_policy_validation_rejects_registry_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    bad_spec = server.LiveToolSpec(
        "web-intelligence",
        "servers.web_intelligence.server",
        "search_web",
        "web_intelligence",
    )
    monkeypatch.setattr(server, "LIVE_TOOL_SPECS", (*server.LIVE_TOOL_SPECS, bad_spec))

    with pytest.raises(RuntimeError, match="not marked gateway_exposure='live'"):
        server._validate_live_policy_specs()


def test_live_gateway_policy_validation_rejects_missing_allowlisted_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bad_spec = server.LiveToolSpec(
        "provider-enrollment",
        "servers.provider_enrollment.server",
        "does_not_exist",
        "provider_enrollment",
    )
    monkeypatch.setattr(server, "LIVE_TOOL_SPECS", (*server.LIVE_TOOL_SPECS, bad_spec))

    with pytest.raises(RuntimeError, match="does_not_exist is not defined in registry module"):
        server._validate_live_policy_specs()


def test_live_gateway_policy_validation_rejects_unknown_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bad_spec = server.LiveToolSpec(
        "provider-enrollment",
        "servers.provider_enrollment.server",
        "search_provider_enrollment",
        "provider_enrollment",
        scopes=("mcp:admin",),
    )
    monkeypatch.setattr(server, "LIVE_TOOL_SPECS", (bad_spec,))

    with pytest.raises(RuntimeError, match="unknown scope"):
        server._validate_live_policy_specs()


def test_live_gateway_policy_validation_requires_baseline_read_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bad_spec = server.LiveToolSpec(
        "public-records",
        "servers.public_records.server",
        "screen_leie_batch",
        "exclusions",
        scopes=("mcp:bulk",),
    )
    monkeypatch.setattr(server, "LIVE_TOOL_SPECS", (bad_spec,))

    with pytest.raises(RuntimeError, match="baseline mcp:read scope"):
        server._validate_live_policy_specs()


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


def test_live_gateway_rejects_wildcard_http_bind_without_explicit_opt_in() -> None:
    mapped = server._live_gateway_env(
        {"MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token"},
        require_auth=True,
    )
    config = load_gateway_security_config(mapped)

    with pytest.raises(GatewayAuthError, match="refuses to bind HTTP/SSE to a wildcard interface"):
        server._validate_live_transport_posture(
            transport="streamable-http",
            host="0.0.0.0",
            security_config=config,
            env={},
        )


def test_live_gateway_network_bind_opt_in_requires_https_public_url() -> None:
    mapped = server._live_gateway_env(
        {
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "live.example.org",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "https://chatgpt.com",
        },
        require_auth=True,
    )
    config = load_gateway_security_config(mapped)

    with pytest.raises(GatewayAuthError, match="requires MCP_LIVE_GATEWAY_PUBLIC_URL=https://"):
        server._validate_live_transport_posture(
            transport="streamable-http",
            host="0.0.0.0",
            security_config=config,
            env={"MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND": "true"},
        )


def test_live_gateway_network_bind_opt_in_accepts_locked_https_posture() -> None:
    mapped = server._live_gateway_env(
        {
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "live.example.org",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "https://chatgpt.com",
            "MCP_LIVE_GATEWAY_PUBLIC_URL": "https://live.example.org/mcp",
        },
        require_auth=True,
    )
    config = load_gateway_security_config(mapped)

    server._validate_live_transport_posture(
        transport="streamable-http",
        host="0.0.0.0",
        security_config=config,
        env={"MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND": "true"},
    )


def test_live_gateway_rejects_global_bulk_scope_without_explicit_opt_in() -> None:
    mapped = server._live_gateway_env(
        {
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            "MCP_LIVE_GATEWAY_REQUIRED_SCOPES": "mcp:read,mcp:bulk",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "127.0.0.1:*",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://127.0.0.1:*",
        },
        require_auth=True,
    )
    config = load_gateway_security_config(mapped)

    with pytest.raises(GatewayAuthError, match="global bulk scope is disabled by default"):
        server._validate_live_scope_posture(
            transport="streamable-http",
            security_config=config,
            env={},
        )


def test_live_gateway_global_bulk_scope_requires_explicit_opt_in() -> None:
    mapped = server._live_gateway_env(
        {
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            "MCP_LIVE_GATEWAY_REQUIRED_SCOPES": "mcp:read,mcp:bulk",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "127.0.0.1:*",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://127.0.0.1:*",
        },
        require_auth=True,
    )
    config = load_gateway_security_config(mapped)

    server._validate_live_scope_posture(
        transport="streamable-http",
        security_config=config,
        env={"MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE": "true"},
    )


def test_live_gateway_allows_compose_localhost_published_container_bind() -> None:
    mapped = server._live_gateway_env(
        {
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "localhost:8020,127.0.0.1:8020",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://localhost:*,http://127.0.0.1:*",
        },
        require_auth=True,
    )
    config = load_gateway_security_config(mapped)

    server._validate_live_transport_posture(
        transport="streamable-http",
        host="0.0.0.0",
        security_config=config,
        env={"MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND": "true"},
    )


@pytest.mark.parametrize(
    ("extra_env", "expected_reason"),
    [
        (
            {"MCP_LIVE_GATEWAY_PUBLIC_URL": "https://live.example.org/mcp"},
            "public_url",
        ),
        (
            {"MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "live.example.org"},
            "allowed_hosts",
        ),
        (
            {"MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "https://chatgpt.com"},
            "allowed_origins",
        ),
    ],
)
def test_live_gateway_container_local_bind_rejects_remote_posture(
    extra_env: dict[str, str],
    expected_reason: str,
) -> None:
    env = {
        "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
        "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "localhost:8020,127.0.0.1:8020",
        "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://localhost:*,http://127.0.0.1:*",
        **extra_env,
    }
    mapped = server._live_gateway_env(env, require_auth=True)
    config = load_gateway_security_config(mapped)

    assert server._container_local_bind_allowed(
        security_config=config,
        env={"MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND": "true"},
    ) is False
    if expected_reason == "public_url":
        assert config.public_url
    elif expected_reason == "allowed_hosts":
        assert "live.example.org" in config.allowed_hosts
    else:
        assert "https://chatgpt.com" in config.allowed_origins

    with pytest.raises(GatewayAuthError, match="refuses to bind HTTP/SSE to a wildcard interface"):
        server._validate_live_transport_posture(
            transport="streamable-http",
            host="0.0.0.0",
            security_config=config,
            env={"MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND": "true"},
        )


def test_live_gateway_env_prefix_maps_auth_settings() -> None:
    token_hash = token_sha256("bulk-live-token-123456789")
    mapped = server._live_gateway_env(
        {
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": "this-is-a-long-live-token",
            "MCP_LIVE_GATEWAY_TOKEN_SCOPES": f"{token_hash}=mcp:read+mcp:bulk",
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
    assert config.token_scope_overrides == {token_hash: ("mcp:read", "mcp:bulk")}


@pytest.mark.asyncio
async def test_live_gateway_exposes_callable_tool_alias_through_policy_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"called": "search_provider_enrollment", "kwargs": kwargs, "evidence": _valid_evidence()}

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    result = await server.search_provider_enrollment(npi="1234567893", limit=1)

    assert result["called"] == "search_provider_enrollment"
    assert result["kwargs"] == {"npi": "1234567893", "limit": 1}
    assert result["live_gateway_policy"]["tool"] == "search_provider_enrollment"
    assert result["live_gateway_policy"]["source_caveat_class"] == "provider_enrollment_public_record"
    assert result["live_gateway_policy"]["source_caveat"]
    assert result["live_gateway_policy"]["dataset_ids"] == list(SERVER_BY_ID["provider-enrollment"].dataset_ids)
    assert result["live_gateway_policy"]["cache_needs"] == list(SERVER_BY_ID["provider-enrollment"].cache_needs)
    assert result["live_gateway_policy"]["server_safety_notes"] == list(SERVER_BY_ID["provider-enrollment"].safety_notes)
    assert result["live_gateway_policy"]["request_size_limit_bytes"] == 32768
    assert result["live_gateway_policy"]["result_size_limit_bytes"] == 262144
    assert result["live_gateway_policy"]["result_limit"] == 100
    assert server._AUDIT_EVENTS[-1]["outcome"] == "allowed"
    assert server._AUDIT_EVENTS[-1]["source_caveat_class"] == "provider_enrollment_public_record"
    assert server._AUDIT_EVENTS[-1]["provenance_status"] == "evidence_receipt_valid"
    assert server._AUDIT_EVENTS[-1]["evidence_present"] is True


@pytest.mark.asyncio
async def test_live_gateway_policy_wrapper_uses_authenticated_token_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_screen_leie_batch(**kwargs):
        return {"called": "screen_leie_batch", "kwargs": kwargs, "evidence": _valid_evidence()}

    access_token = AccessToken(
        token="token-one-fingerprint",
        client_id="healthcare-data-mcp-gateway",
        scopes=["mcp:read", "mcp:bulk"],
    )
    context_token = auth_context_var.set(AuthenticatedUser(access_token))
    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "screen_leie_batch", fake_screen_leie_batch)

    try:
        result = await server.screen_leie_batch(records=[])
    finally:
        auth_context_var.reset(context_token)

    expected_subject = server._access_token_subject(access_token)
    assert result["called"] == "screen_leie_batch"
    assert result["live_gateway_policy"]["allowed_scopes"] == ["mcp:read", "mcp:bulk"]
    assert server._AUDIT_EVENTS[-1]["outcome"] == "allowed"
    assert server._AUDIT_EVENTS[-1]["subject"] == expected_subject
    assert f"bulk:screen_leie_batch:{expected_subject}" in server._RATE_LIMIT_WINDOWS


@pytest.mark.asyncio
async def test_live_gateway_policy_wrapper_does_not_let_call_arguments_grant_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_screen_leie_batch(**kwargs):
        return {"called": "screen_leie_batch", "kwargs": kwargs, "evidence": _valid_evidence()}

    access_token = AccessToken(
        token="token-two-fingerprint",
        client_id="healthcare-data-mcp-gateway",
        scopes=["mcp:read"],
    )
    context_token = auth_context_var.set(AuthenticatedUser(access_token))
    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "screen_leie_batch", fake_screen_leie_batch)

    try:
        with pytest.raises(ToolError, match="requires scope"):
            await server.screen_leie_batch(records=[], caller_scopes=["mcp:read", "mcp:bulk"])
    finally:
        auth_context_var.reset(context_token)

    audit_event = server._AUDIT_EVENTS[-1]
    assert audit_event["outcome"] == "blocked"
    assert audit_event["reason"] == "missing_scope"
    assert audit_event["missing_scopes"] == ["mcp:bulk"]
    assert audit_event["subject"] == server._access_token_subject(access_token)


@pytest.mark.asyncio
async def test_live_gateway_preserves_upstream_provenance_receipts(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {
            "results": [{"npi": kwargs["npi"], "provider_name": "Example Hospital"}],
            "source_metadata": {
                "source_name": "CMS Provider Enrollment",
                "dataset_id": "cms-provider-enrollment",
                "source_url": "https://data.cms.gov/provider-enrollment",
            },
            "evidence": evidence_receipt(
                source_name="CMS Provider Enrollment",
                source_url="https://data.cms.gov/provider-enrollment",
                dataset_id="cms-provider-enrollment",
                source_period="current public file",
                retrieved_at="2026-05-22T00:00:00Z",
                cache_status="hit",
                match_basis="npi_exact",
                confidence="high",
                caveat="Public enrollment rows require source-system verification before operational decisions.",
                next_step="Review the returned enrollment detail and ownership rows.",
            ),
            "identity": {
                "entity_type": "provider",
                "npi": kwargs["npi"],
                "match_basis": "npi_exact",
                "confidence": "high",
            },
        }

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    result = await server.call_live_tool("search_provider_enrollment", {"npi": "1234567893", "limit": 1})

    assert result["evidence"]["source_name"] == "CMS Provider Enrollment"
    assert result["source_metadata"]["dataset_id"] == "cms-provider-enrollment"
    assert result["identity"]["npi"] == "1234567893"
    assert result["live_gateway_policy"]["provenance_status"] == {
        "status": "evidence_receipt_valid",
        "evidence_present": True,
        "evidence_valid": True,
        "source_metadata_present": True,
        "identity_present": True,
    }
    audit_event = server._AUDIT_EVENTS[-1]
    assert audit_event["outcome"] == "allowed"
    assert audit_event["provenance_status"] == "evidence_receipt_valid"
    assert audit_event["evidence_present"] is True
    assert audit_event["source_metadata_present"] is True
    assert audit_event["identity_present"] is True
    assert "source_metadata" not in audit_event
    assert "evidence" not in audit_event


@pytest.mark.asyncio
async def test_live_gateway_blocks_invalid_upstream_evidence_receipts(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {
            "results": [{"npi": kwargs["npi"], "provider_name": "Example Hospital"}],
            "source_metadata": {
                "source_name": "CMS Provider Enrollment",
                "dataset_id": "cms-provider-enrollment",
                "source_url": "https://data.cms.gov/provider-enrollment",
            },
            "evidence": {"source_name": "CMS Provider Enrollment"},
        }

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="invalid evidence receipt"):
        await server.call_live_tool("search_provider_enrollment", {"npi": "1234567893", "limit": 1})

    audit_event = server._AUDIT_EVENTS[-1]
    assert audit_event["outcome"] == "blocked"
    assert audit_event["reason"] == "invalid_evidence_receipt"
    assert audit_event["provenance_status"] == "evidence_receipt_invalid"
    assert audit_event["evidence_present"] is True
    assert audit_event["source_metadata_present"] is True
    assert audit_event["identity_present"] is False
    assert "source_metadata" not in audit_event
    assert "evidence" not in audit_event


@pytest.mark.asyncio
async def test_live_gateway_blocks_invalid_nested_row_evidence_receipts(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {
            "results": [
                {
                    "npi": kwargs["npi"],
                    "provider_name": "Example Hospital",
                    "evidence": {"source_name": "CMS Provider Enrollment"},
                }
            ],
            "source_metadata": {
                "source_name": "CMS Provider Enrollment",
                "dataset_id": "cms-provider-enrollment",
                "source_url": "https://data.cms.gov/provider-enrollment",
            },
        }

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="invalid evidence receipt"):
        await server.call_live_tool("search_provider_enrollment", {"npi": "1234567893", "limit": 1})

    audit_event = server._AUDIT_EVENTS[-1]
    assert audit_event["outcome"] == "blocked"
    assert audit_event["reason"] == "invalid_evidence_receipt"
    assert audit_event["provenance_status"] == "evidence_receipt_invalid"
    assert audit_event["invalid_evidence_paths"][0]["path"] == "result.results[0].evidence"


@pytest.mark.asyncio
async def test_live_gateway_blocks_schema_valid_but_empty_evidence_receipts(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {
            "results": [{"npi": kwargs["npi"], "provider_name": "Example Hospital"}],
            "evidence": evidence_receipt(
                source_name="CMS Provider Enrollment",
                source_url="https://data.cms.gov/provider-enrollment",
                dataset_id="cms-provider-enrollment",
                source_period="current public file",
                retrieved_at="2026-05-22T00:00:00Z",
                cache_status="hit",
            ),
        }

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="invalid evidence receipt"):
        await server.call_live_tool("search_provider_enrollment", {"npi": "1234567893", "limit": 1})

    audit_event = server._AUDIT_EVENTS[-1]
    assert audit_event["outcome"] == "blocked"
    assert audit_event["reason"] == "invalid_evidence_receipt"
    assert audit_event["invalid_evidence_paths"][0]["path"] == "result.evidence"
    assert "missing required content" in audit_event["invalid_evidence_paths"][0]["error"]
    assert "match_basis" in audit_event["invalid_evidence_paths"][0]["error"]
    assert "caveat" in audit_event["invalid_evidence_paths"][0]["error"]
    assert "next_step" in audit_event["invalid_evidence_paths"][0]["error"]


@pytest.mark.asyncio
async def test_live_gateway_blocks_missing_upstream_evidence_receipts(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {
            "results": [{"npi": kwargs["npi"], "provider_name": "Example Hospital"}],
            "source_metadata": {
                "source_name": "CMS Provider Enrollment",
                "dataset_id": "cms-provider-enrollment",
                "source_url": "https://data.cms.gov/provider-enrollment",
            },
        }

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="no evidence receipt"):
        await server.call_live_tool("search_provider_enrollment", {"npi": "1234567893", "limit": 1})

    audit_event = server._AUDIT_EVENTS[-1]
    assert audit_event["outcome"] == "blocked"
    assert audit_event["reason"] == "missing_evidence_receipt"
    assert audit_event["provenance_status"] == "evidence_receipt_missing"
    assert audit_event["evidence_present"] is False
    assert audit_event["source_metadata_present"] is True


@pytest.mark.asyncio
async def test_live_gateway_accepts_nested_row_evidence_without_top_level_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {
            "results": [
                {
                    "npi": kwargs["npi"],
                    "provider_name": "Example Hospital",
                    "evidence": evidence_receipt(
                        source_name="CMS Provider Enrollment",
                        source_url="https://data.cms.gov/provider-enrollment",
                        dataset_id="cms-provider-enrollment",
                        source_period="current public file",
                        retrieved_at="2026-05-22T00:00:00Z",
                        cache_status="hit",
                        match_basis="npi_exact_row",
                        confidence="high",
                        caveat="Public enrollment rows require source-system verification before operational decisions.",
                        next_step="Review the returned enrollment detail and ownership rows.",
                    ),
                    "identity": {"npi": kwargs["npi"]},
                }
            ],
            "source_metadata": {
                "source_name": "CMS Provider Enrollment",
                "dataset_id": "cms-provider-enrollment",
                "source_url": "https://data.cms.gov/provider-enrollment",
            },
        }

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    result = await server.call_live_tool("search_provider_enrollment", {"npi": "1234567893", "limit": 1})

    assert result["live_gateway_policy"]["provenance_status"]["status"] == "evidence_receipt_valid"
    assert result["live_gateway_policy"]["provenance_status"]["evidence_present"] is True
    assert result["live_gateway_policy"]["provenance_status"]["identity_present"] is True
    assert server._AUDIT_EVENTS[-1]["provenance_status"] == "evidence_receipt_valid"


@pytest.mark.asyncio
async def test_live_gateway_overwrites_upstream_policy_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {
            "called": "search_provider_enrollment",
            "evidence": _valid_evidence(),
            "live_gateway_policy": {"gateway": "spoofed", "tool": "wrong"},
        }

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    result = await server.call_live_tool("search_provider_enrollment", {"limit": 1})

    assert result["live_gateway_policy"]["gateway"] == "live-gateway"
    assert result["live_gateway_policy"]["tool"] == "search_provider_enrollment"
    assert result["live_gateway_policy"]["request_size_limit_bytes"] == 32768
    assert result["live_gateway_policy"]["result_size_limit_bytes"] == 262144
    assert result["live_gateway_policy"]["source_caveat_class"] == "provider_enrollment_public_record"


@pytest.mark.asyncio
async def test_live_gateway_blocks_non_allowlisted_tools() -> None:
    with pytest.raises(ToolError, match="policy_denied"):
        await server.call_live_tool("not_a_real_tool", {})


@pytest.mark.asyncio
async def test_live_gateway_blocks_oversized_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"ok": True, "kwargs": kwargs}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="request is"):
        await server.call_live_tool("search_provider_enrollment", {"provider_name": "x" * 40000})

    assert server._AUDIT_EVENTS[-1]["reason"] == "request_size_limit_exceeded"


@pytest.mark.asyncio
async def test_live_gateway_rejects_sensitive_identifier_arguments_before_routing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    async def fake_search_provider_enrollment(**kwargs):
        calls["count"] += 1
        return {"ok": True, "kwargs": kwargs}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="sensitive identifier key"):
        await server.call_live_tool(
            "search_provider_enrollment",
            {"provider_name": "Example", "taxpayer_identification_number": "12-3456789"},
        )

    assert calls["count"] == 0
    assert server._AUDIT_EVENTS[-1]["reason"] == "sensitive_argument_key_rejected"
    assert server._AUDIT_EVENTS[-1]["sensitive_argument_keys"] == ["taxpayer_identification_number"]


@pytest.mark.asyncio
async def test_live_gateway_rejects_nested_sensitive_identifier_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    async def fake_screen_leie_batch(**kwargs):
        calls["count"] += 1
        return {"results": []}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "screen_leie_batch", fake_screen_leie_batch)

    with pytest.raises(ToolError, match="sensitive identifier key"):
        await server.call_live_tool(
            "screen_leie_batch",
            {"records": [{"candidate_id": "1", "ssn": "123-45-6789"}]},
            caller_scopes=("mcp:read", "mcp:bulk"),
        )

    assert calls["count"] == 0
    assert server._AUDIT_EVENTS[-1]["reason"] == "sensitive_argument_key_rejected"
    assert server._AUDIT_EVENTS[-1]["sensitive_argument_keys"] == ["records[0].ssn"]


@pytest.mark.asyncio
async def test_live_gateway_blocks_limit_arguments_above_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"ok": True, "kwargs": kwargs}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="exceeds live-gateway result_limit"):
        await server.call_live_tool("search_provider_enrollment", {"limit": 101})

    assert server._AUDIT_EVENTS[-1]["reason"] == "limit_argument_exceeds_result_limit"


@pytest.mark.asyncio
async def test_live_gateway_blocks_numeric_string_limit_arguments_above_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    async def fake_search_provider_enrollment(**kwargs):
        calls["count"] += 1
        return {"ok": True, "kwargs": kwargs}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="exceeds live-gateway result_limit"):
        await server.call_live_tool("search_provider_enrollment", {"limit": "101"})

    assert calls["count"] == 0
    assert server._AUDIT_EVENTS[-1]["reason"] == "limit_argument_exceeds_result_limit"


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_value", [0, -1, "0", "-1"])
async def test_live_gateway_blocks_non_positive_limit_arguments_before_routing(
    monkeypatch: pytest.MonkeyPatch,
    limit_value: int | str,
) -> None:
    calls = {"count": 0}

    async def fake_search_provider_enrollment(**kwargs):
        calls["count"] += 1
        return {"ok": True, "kwargs": kwargs}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="must be at least 1"):
        await server.call_live_tool("search_provider_enrollment", {"limit": limit_value})

    assert calls["count"] == 0
    assert server._AUDIT_EVENTS[-1]["reason"] == "limit_argument_below_minimum"


@pytest.mark.asyncio
async def test_live_gateway_blocks_nested_argument_lists_above_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    async def fake_search_provider_enrollment(**kwargs):
        calls["count"] += 1
        return {"ok": True, "kwargs": kwargs}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="filters.provider_ids has 101 items"):
        await server.call_live_tool(
            "search_provider_enrollment",
            {"filters": {"provider_ids": [str(index) for index in range(101)]}},
        )

    assert calls["count"] == 0
    audit_event = server._AUDIT_EVENTS[-1]
    assert audit_event["reason"] == "argument_list_exceeds_result_limit"
    assert audit_event["oversized_argument_lists"] == [
        {"path": "arguments.filters.provider_ids", "length": 101, "limit": 100}
    ]
    assert "provider_ids" in str(audit_event)
    assert "100" in str(audit_event)


@pytest.mark.asyncio
async def test_live_gateway_blocks_result_lists_above_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"results": [{"npi": str(index)} for index in range(101)]}

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="returned 101 rows/items"):
        await server.call_live_tool("search_provider_enrollment", {"limit": 100})

    assert server._AUDIT_EVENTS[-1]["reason"] == "result_limit_exceeded"


@pytest.mark.asyncio
async def test_live_gateway_blocks_result_bytes_above_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"summary": "x" * 1024}

    spec = server.LIVE_TOOL_BY_NAME["search_provider_enrollment"]
    narrow_spec = server.LiveToolSpec(
        spec.server,
        spec.module,
        spec.tool_name,
        spec.category,
        result_size_limit_bytes=128,
    )
    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server.LIVE_TOOL_BY_NAME, "search_provider_enrollment", narrow_spec)
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="result is"):
        await server.call_live_tool("search_provider_enrollment", {"limit": 1})

    assert server._AUDIT_EVENTS[-1]["reason"] == "result_size_limit_exceeded"
    assert server._AUDIT_EVENTS[-1]["source_caveat_class"] == "provider_enrollment_public_record"


@pytest.mark.asyncio
async def test_live_gateway_blocks_final_response_bytes_after_policy_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"summary": "x" * 16, "evidence": _valid_evidence()}

    spec = server.LIVE_TOOL_BY_NAME["search_provider_enrollment"]
    narrow_spec = server.LiveToolSpec(
        spec.server,
        spec.module,
        spec.tool_name,
        spec.category,
        result_size_limit_bytes=1200,
    )
    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server.LIVE_TOOL_BY_NAME, "search_provider_enrollment", narrow_spec)
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)

    with pytest.raises(ToolError, match="after live-gateway policy metadata"):
        await server.call_live_tool("search_provider_enrollment", {"limit": 1})

    assert server._AUDIT_EVENTS[-1]["reason"] == "response_size_limit_exceeded"


@pytest.mark.asyncio
async def test_live_gateway_enforces_bulk_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_screen_leie_batch(**kwargs):
        return {"results": [], "evidence": _valid_evidence()}

    server._AUDIT_EVENTS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "screen_leie_batch", fake_screen_leie_batch)

    with pytest.raises(ToolError, match="requires scope"):
        await server.call_live_tool("screen_leie_batch", {"records": []}, caller_scopes=("mcp:read",))

    result = await server.call_live_tool(
        "screen_leie_batch",
        {"records": []},
        caller_scopes=("mcp:read", "mcp:bulk"),
    )
    assert result["live_gateway_policy"]["allowed_scopes"] == ["mcp:read", "mcp:bulk"]
    assert server._AUDIT_EVENTS[-1]["outcome"] == "allowed"


@pytest.mark.asyncio
async def test_live_gateway_rate_limits_by_tool_class(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"results": [], "evidence": _valid_evidence()}

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)
    monkeypatch.setitem(server._RATE_LIMIT_POLICIES, "standard", (1, 60.0))

    await server.call_live_tool("search_provider_enrollment", {"limit": 1})
    with pytest.raises(ToolError, match="rate_limited"):
        await server.call_live_tool("search_provider_enrollment", {"limit": 1})

    assert server._AUDIT_EVENTS[-1]["reason"] == "rate_limit_exceeded"


@pytest.mark.asyncio
async def test_live_gateway_rate_limits_are_subject_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_search_provider_enrollment(**kwargs):
        return {"results": [], "evidence": _valid_evidence()}

    server._AUDIT_EVENTS.clear()
    server._RATE_LIMIT_WINDOWS.clear()
    monkeypatch.setitem(server._LIVE_TOOL_CALLABLES, "search_provider_enrollment", fake_search_provider_enrollment)
    monkeypatch.setitem(server._RATE_LIMIT_POLICIES, "standard", (1, 60.0))

    await server.call_live_tool("search_provider_enrollment", {"limit": 1}, subject="analyst-a")
    await server.call_live_tool("search_provider_enrollment", {"limit": 1}, subject="analyst-b")

    with pytest.raises(ToolError, match="rate_limited"):
        await server.call_live_tool("search_provider_enrollment", {"limit": 1}, subject="analyst-a")

    assert server._AUDIT_EVENTS[-1]["reason"] == "rate_limit_exceeded"
    assert server._AUDIT_EVENTS[-1]["subject"] == "analyst-a"


@pytest.mark.asyncio
async def test_live_gateway_audit_event_tool_returns_recent_non_secret_events() -> None:
    server._AUDIT_EVENTS.clear()
    server._record_audit(tool_name="example", outcome="blocked", reason="unit_test", subject="test-subject")

    result = await server.get_live_gateway_audit_events(limit=10)

    assert result["count"] == 1
    assert result["events"][0]["tool"] == "example"
    assert result["events"][0]["subject"] == "test-subject"


@pytest.mark.asyncio
async def test_live_gateway_writes_configured_audit_jsonl_without_sensitive_values(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_path = tmp_path / "audit" / "live-gateway.jsonl"
    monkeypatch.setenv("MCP_LIVE_GATEWAY_AUDIT_LOG_PATH", str(audit_path))
    server._AUDIT_EVENTS.clear()

    with pytest.raises(ToolError, match="sensitive identifier key"):
        await server.call_live_tool(
            "screen_leie_batch",
            {"records": [{"ssn": "123-45-6789", "npi": "1234567893"}]},
            caller_scopes=("mcp:read", "mcp:bulk"),
            subject="audit-test-subject",
        )

    lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["tool"] == "screen_leie_batch"
    assert event["outcome"] == "blocked"
    assert event["reason"] == "sensitive_argument_key_rejected"
    assert event["subject"] == "audit-test-subject"
    assert event["sensitive_argument_keys"] == ["records[0].ssn"]
    assert "123-45-6789" not in lines[0]

    result = await server.get_live_gateway_audit_events(limit=1)
    assert result["audit_log_path_configured"] is True
