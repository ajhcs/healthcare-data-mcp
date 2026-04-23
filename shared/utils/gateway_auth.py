"""Authentication and request-origin helpers for the remote MCP gateway."""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit

from mcp.server.auth.provider import AccessToken

DEFAULT_LOCAL_ALLOWED_HOSTS: tuple[str, ...] = ("127.0.0.1:*", "localhost:*")
DEFAULT_LOCAL_ALLOWED_ORIGINS: tuple[str, ...] = ("http://127.0.0.1:*", "http://localhost:*")
DEFAULT_REQUIRED_SCOPES: tuple[str, ...] = ("mcp:read",)

_WEAK_STATIC_TOKENS = frozenset(
    {
        "changeme",
        "change-me",
        "password",
        "secret",
        "token",
        "test-token",
        "dev-token",
        "bearer",
    }
)


class GatewayAuthError(ValueError):
    """Raised when gateway auth configuration is invalid."""


@dataclass(frozen=True)
class GatewaySecurityConfig:
    """Resolved remote gateway security settings.

    Authentication is enabled when bearer tokens or token hashes are configured,
    or when ``MCP_GATEWAY_AUTH_REQUIRED=true`` is set.
    """

    auth_enabled: bool
    bearer_tokens: tuple[str, ...]
    bearer_token_sha256: tuple[str, ...]
    required_scopes: tuple[str, ...]
    allowed_origins: tuple[str, ...]
    allowed_hosts: tuple[str, ...]
    public_url: str | None = None
    issuer_url: str | None = None


@dataclass(frozen=True)
class Origin:
    """Normalized browser Origin tuple."""

    scheme: str
    host: str
    port: int | None


@dataclass(frozen=True)
class OriginPattern:
    """Normalized Origin allow-list tuple."""

    scheme: str
    host: str
    port: int | None
    wildcard_port: bool = False


@dataclass(frozen=True)
class HostPattern:
    """Normalized Host header tuple."""

    host: str
    port: int | None
    wildcard_port: bool = False


def load_gateway_security_config(env: Mapping[str, str] | None = None) -> GatewaySecurityConfig:
    """Load gateway security settings from environment-style key/value data."""

    source = os.environ if env is None else env
    bearer_tokens = _split_csv(source.get("MCP_GATEWAY_BEARER_TOKENS")) + _split_csv(
        source.get("MCP_GATEWAY_BEARER_TOKEN")
    )
    bearer_hashes = _split_csv(source.get("MCP_GATEWAY_BEARER_TOKEN_SHA256")) + _split_csv(
        source.get("MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST")
    )

    for token in bearer_tokens:
        validate_static_token(token)
    for token_hash in bearer_hashes:
        validate_sha256_token_hash(token_hash)

    auth_required = _parse_bool(source.get("MCP_GATEWAY_AUTH_REQUIRED"))
    has_credentials = bool(bearer_tokens or bearer_hashes)
    if auth_required is True and not has_credentials:
        raise GatewayAuthError("MCP_GATEWAY_AUTH_REQUIRED=true requires a bearer token or SHA-256 token hash")

    auth_enabled = has_credentials if auth_required is None else auth_required
    required_scopes = _split_csv(source.get("MCP_GATEWAY_REQUIRED_SCOPES")) or DEFAULT_REQUIRED_SCOPES
    allowed_origins = _split_csv(source.get("MCP_GATEWAY_ALLOWED_ORIGINS")) or DEFAULT_LOCAL_ALLOWED_ORIGINS
    allowed_hosts = _split_csv(source.get("MCP_GATEWAY_ALLOWED_HOSTS")) or DEFAULT_LOCAL_ALLOWED_HOSTS

    for origin in allowed_origins:
        if _parse_origin_pattern(origin) is None:
            raise GatewayAuthError(f"Invalid origin allow-list entry: {origin!r}")
    for host in allowed_hosts:
        if _parse_host_pattern(host) is None:
            raise GatewayAuthError(f"Invalid host allow-list entry: {host!r}")

    return GatewaySecurityConfig(
        auth_enabled=auth_enabled,
        bearer_tokens=bearer_tokens,
        bearer_token_sha256=bearer_hashes,
        required_scopes=required_scopes,
        allowed_origins=allowed_origins,
        allowed_hosts=allowed_hosts,
        public_url=_clean_optional(source.get("MCP_GATEWAY_PUBLIC_URL")),
        issuer_url=_clean_optional(source.get("MCP_GATEWAY_ISSUER_URL")),
    )


def extract_bearer_token(headers: Mapping[str, str]) -> str | None:
    """Extract a bearer token from case-insensitive HTTP headers."""

    auth_header = None
    for name, value in headers.items():
        if name.lower() == "authorization":
            auth_header = value.strip()
            break

    if not auth_header:
        return None

    scheme, _, token = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def validate_static_token(token: str) -> None:
    """Reject empty, placeholder, or trivially short static bearer tokens."""

    cleaned = token.strip()
    if not cleaned:
        raise GatewayAuthError("Bearer tokens must not be empty")
    if cleaned.lower() in _WEAK_STATIC_TOKENS or len(cleaned) < 16:
        raise GatewayAuthError("Static gateway bearer tokens must be non-placeholder values at least 16 chars long")


def validate_sha256_token_hash(token_hash: str) -> None:
    """Validate a hex-encoded SHA-256 bearer token hash."""

    cleaned = token_hash.strip().lower()
    if len(cleaned) != 64:
        raise GatewayAuthError("SHA-256 token hashes must be 64 lowercase or uppercase hex characters")
    try:
        int(cleaned, 16)
    except ValueError as exc:
        raise GatewayAuthError("SHA-256 token hashes must contain only hex characters") from exc


def token_sha256(token: str) -> str:
    """Return the hex SHA-256 digest for a bearer token."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def token_fingerprint(token: str) -> str:
    """Return a short non-secret token identifier for logs and auth context."""

    return token_sha256(token)[:12]


def is_origin_allowed(origin: str | None, allowed_origins: Sequence[str]) -> bool:
    """Return whether a browser Origin header is allowed.

    Missing Origin is allowed because non-browser MCP clients usually omit it.
    Invalid or unlisted origins are rejected.
    """

    if not origin:
        return True

    parsed = normalize_origin(origin)
    if parsed is None:
        return False

    for allowed in allowed_origins:
        pattern = _parse_origin_pattern(allowed)
        if pattern is not None and _origin_matches(parsed, pattern):
            return True
    return False


def normalize_origin(origin: str) -> Origin | None:
    """Normalize an HTTP(S) Origin value, rejecting paths, queries, and fragments."""

    try:
        parts = urlsplit(origin.strip())
    except ValueError:
        return None
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return None
    if parts.path not in {"", "/"} or parts.query or parts.fragment:
        return None
    try:
        port = parts.port
    except ValueError:
        return None
    return Origin(scheme=parts.scheme.lower(), host=parts.hostname.lower(), port=port)


def is_host_allowed(host: str | None, allowed_hosts: Sequence[str]) -> bool:
    """Return whether an HTTP Host header is allowed."""

    if not host:
        return False

    parsed = _parse_host_header(host)
    if parsed is None:
        return False

    for allowed in allowed_hosts:
        pattern = _parse_host_pattern(allowed)
        if pattern is not None and _host_matches(parsed, pattern):
            return True
    return False


def build_transport_security_settings(config: GatewaySecurityConfig):
    """Build FastMCP transport security settings from gateway config."""

    from mcp.server.transport_security import TransportSecuritySettings

    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(config.allowed_hosts),
        allowed_origins=list(config.allowed_origins),
    )


class StaticBearerTokenVerifier:
    """FastMCP token verifier for static bearer tokens or SHA-256 token hashes."""

    def __init__(
        self,
        tokens: Sequence[str],
        token_sha256_hashes: Sequence[str],
        *,
        required_scopes: Sequence[str] = DEFAULT_REQUIRED_SCOPES,
        client_id: str = "healthcare-data-mcp-gateway",
        resource: str | None = None,
    ) -> None:
        self._tokens = tuple(token.strip() for token in tokens if token.strip())
        self._hashes = tuple(token_hash.strip().lower() for token_hash in token_sha256_hashes if token_hash.strip())
        self._required_scopes = tuple(required_scopes)
        self._client_id = client_id
        self._resource = resource

    async def verify_token(self, token: str) -> AccessToken | None:
        """Verify a bearer token for FastMCP's auth middleware."""

        if not token:
            return None

        token_digest = token_sha256(token)
        for expected in self._tokens:
            if hmac.compare_digest(token, expected):
                return self._access_token(token)
        for expected_hash in self._hashes:
            if hmac.compare_digest(token_digest, expected_hash):
                return self._access_token(token)
        return None

    def _access_token(self, token: str) -> AccessToken:
        return AccessToken(
            token=token_fingerprint(token),
            client_id=self._client_id,
            scopes=list(self._required_scopes),
            resource=self._resource,
        )


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _parse_bool(value: str | None) -> bool | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise GatewayAuthError(f"Expected boolean value, got {value!r}")


def _parse_origin_pattern(value: str) -> OriginPattern | None:
    if value == "*":
        return None
    if value.endswith(":*"):
        base = value[:-2]
        parsed = normalize_origin(base)
        if parsed is None:
            return None
        return OriginPattern(parsed.scheme, parsed.host, None, wildcard_port=True)
    parsed = normalize_origin(value)
    if parsed is None:
        return None
    return OriginPattern(parsed.scheme, parsed.host, parsed.port)


def _origin_matches(origin: Origin, pattern: OriginPattern) -> bool:
    if origin.scheme != pattern.scheme:
        return False
    if pattern.host.startswith("*."):
        suffix = pattern.host[1:]
        if not origin.host.endswith(suffix) or origin.host == pattern.host[2:]:
            return False
    elif origin.host != pattern.host:
        return False
    return pattern.wildcard_port or origin.port == pattern.port


def _parse_host_pattern(value: str) -> HostPattern | None:
    if value == "*":
        return None
    if value.endswith(":*"):
        parsed = _parse_host_header(value[:-2])
        if parsed is None:
            return None
        return HostPattern(host=parsed.host, port=None, wildcard_port=True)

    parsed = _parse_host_header(value)
    if parsed is None:
        return None
    return HostPattern(host=parsed.host, port=parsed.port)


def _parse_host_header(value: str) -> HostPattern | None:
    cleaned = value.strip().lower()
    if not cleaned or "/" in cleaned or "@" in cleaned:
        return None
    host = cleaned
    port = None
    if cleaned.startswith("["):
        end = cleaned.find("]")
        if end == -1:
            return None
        host = cleaned[1:end]
        remainder = cleaned[end + 1 :]
        if remainder:
            if not remainder.startswith(":"):
                return None
            port = _parse_port(remainder[1:])
    elif ":" in cleaned:
        host, raw_port = cleaned.rsplit(":", 1)
        port = _parse_port(raw_port)
    if not host or port == -1:
        return None
    return HostPattern(host=host, port=port)


def _parse_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError:
        return -1
    if not 0 < port < 65536:
        return -1
    return port


def _host_matches(host: HostPattern, pattern: HostPattern) -> bool:
    if pattern.host.startswith("*."):
        suffix = pattern.host[1:]
        if not host.host.endswith(suffix) or host.host == pattern.host[2:]:
            return False
    elif host.host != pattern.host:
        return False
    return pattern.wildcard_port or pattern.port == host.port
