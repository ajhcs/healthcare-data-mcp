"""Tests for remote gateway auth and Origin validation helpers."""

from __future__ import annotations

import pytest

from shared.utils.gateway_auth import (
    DEFAULT_LOCAL_ALLOWED_HOSTS,
    DEFAULT_LOCAL_ALLOWED_ORIGINS,
    GatewayAuthError,
    StaticBearerTokenVerifier,
    extract_bearer_token,
    is_host_allowed,
    is_origin_allowed,
    load_gateway_security_config,
    parse_token_scope_overrides,
    token_sha256,
)


def test_default_config_is_local_safe_without_credentials() -> None:
    config = load_gateway_security_config({})

    assert config.auth_enabled is False
    assert config.bearer_tokens == ()
    assert config.allowed_origins == DEFAULT_LOCAL_ALLOWED_ORIGINS
    assert config.allowed_hosts == DEFAULT_LOCAL_ALLOWED_HOSTS


def test_auth_required_needs_a_configured_credential() -> None:
    with pytest.raises(GatewayAuthError, match="requires a bearer token"):
        load_gateway_security_config({"MCP_GATEWAY_AUTH_REQUIRED": "true"})


def test_static_bearer_token_config_rejects_placeholders() -> None:
    with pytest.raises(GatewayAuthError, match="non-placeholder"):
        load_gateway_security_config({"MCP_GATEWAY_BEARER_TOKEN": "secret"})


@pytest.mark.asyncio
async def test_static_bearer_token_verifier_accepts_plain_or_hashed_token() -> None:
    token = "local-test-token-1234567890"
    verifier = StaticBearerTokenVerifier((), (token_sha256(token),), required_scopes=("mcp:read",))

    auth_info = await verifier.verify_token(token)
    assert auth_info is not None
    assert auth_info.client_id == "healthcare-data-mcp-gateway"
    assert await verifier.verify_token("wrong-token") is None


@pytest.mark.asyncio
async def test_static_bearer_token_verifier_applies_token_scope_overrides() -> None:
    read_token = "read-only-live-token-12345"
    bulk_token = "bulk-live-token-123456789"
    verifier = StaticBearerTokenVerifier(
        (read_token, bulk_token),
        (),
        required_scopes=("mcp:read",),
        token_scope_overrides={token_sha256(bulk_token): ("mcp:read", "mcp:bulk")},
    )

    read_auth = await verifier.verify_token(read_token)
    bulk_auth = await verifier.verify_token(bulk_token)

    assert read_auth is not None
    assert bulk_auth is not None
    assert read_auth.scopes == ["mcp:read"]
    assert bulk_auth.scopes == ["mcp:read", "mcp:bulk"]


def test_gateway_config_parses_token_scope_overrides() -> None:
    token_hash = token_sha256("bulk-live-token-123456789")
    config = load_gateway_security_config(
        {
            "MCP_GATEWAY_BEARER_TOKEN_SHA256": token_hash,
            "MCP_GATEWAY_TOKEN_SCOPES": f"{token_hash}=mcp:read+mcp:bulk",
        }
    )

    assert config.token_scope_overrides == {token_hash: ("mcp:read", "mcp:bulk")}


@pytest.mark.parametrize("value", ["not-a-hash=mcp:read", f"{token_sha256('token-with-empty-scope')}="])
def test_token_scope_overrides_reject_invalid_entries(value: str) -> None:
    with pytest.raises(GatewayAuthError):
        parse_token_scope_overrides(value)


def test_extract_bearer_token_is_case_insensitive() -> None:
    assert extract_bearer_token({"authorization": "Bearer abc123"}) == "abc123"
    assert extract_bearer_token({"Authorization": "bearer abc123"}) == "abc123"
    assert extract_bearer_token({"Authorization": "Basic abc123"}) is None
    assert extract_bearer_token({}) is None


def test_origin_validation_allows_missing_exact_and_wildcard_port() -> None:
    allowed = ("https://chatgpt.example.com", "http://localhost:*")

    assert is_origin_allowed(None, allowed) is True
    assert is_origin_allowed("https://chatgpt.example.com", allowed) is True
    assert is_origin_allowed("https://chatgpt.example.com:8443", allowed) is False
    assert is_origin_allowed("http://localhost:3000", allowed) is True
    assert is_origin_allowed("http://localhost", allowed) is True


def test_origin_validation_rejects_paths_unlisted_origins_and_broad_wildcard() -> None:
    allowed = ("https://chatgpt.example.com",)

    assert is_origin_allowed("https://chatgpt.example.com/mcp", allowed) is False
    assert is_origin_allowed("https://evil.example.com", allowed) is False
    assert is_origin_allowed("https://chatgpt.example.com", ("*",)) is False


def test_origin_validation_supports_subdomain_patterns() -> None:
    allowed = ("https://*.example.com",)

    assert is_origin_allowed("https://chatgpt.example.com", allowed) is True
    assert is_origin_allowed("https://nested.chatgpt.example.com", allowed) is True
    assert is_origin_allowed("https://example.com", allowed) is False


def test_host_validation_supports_exact_wildcard_port_and_subdomains() -> None:
    allowed = ("gateway.example.com", "localhost:*", "*.internal.example.com")

    assert is_host_allowed("gateway.example.com", allowed) is True
    assert is_host_allowed("localhost:8016", allowed) is True
    assert is_host_allowed("team.internal.example.com", allowed) is True
    assert is_host_allowed("internal.example.com", allowed) is False
    assert is_host_allowed("evil.example.com", allowed) is False
