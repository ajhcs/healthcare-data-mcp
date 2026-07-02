"""Authenticated live-data gateway for approved healthcare-data-mcp tools."""

from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
import json
import logging
import os
import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from functools import wraps
from pathlib import Path
from typing import Any

from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources

from shared.utils.gateway_auth import (
    GatewayAuthError,
    StaticBearerTokenVerifier,
    build_transport_security_settings,
    load_gateway_security_config,
    token_fingerprint,
)
from shared.utils.mcp_response import raise_tool_error, to_structured
from shared.utils.server_registry import SERVER_BY_ID
from servers.live_gateway.policy_runner import (
    SOURCE_CAVEAT_CLASSES,
    LiveToolSpec,
    attach_gateway_policy,
    audit_provenance_fields,
    effective_source_caveat_class,
    evaluate_provenance_status,
    source_caveat,
)

logger = logging.getLogger(__name__)


LIVE_TOOL_SPECS: tuple[LiveToolSpec, ...] = (
    # Provider enrollment and ownership
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "search_provider_enrollment", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "get_provider_enrollment_detail", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "get_facility_ownership", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "trace_owner_network", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "search_change_of_ownership", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "profile_provider_control", "provider_enrollment"),
    # Hospital quality
    LiveToolSpec("hospital-quality", "servers.hospital_quality.server", "get_quality_scores", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital_quality.server", "get_readmission_data", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital_quality.server", "get_safety_scores", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital_quality.server", "get_patient_experience", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital_quality.server", "get_financial_profile", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital_quality.server", "get_quality_measure_rows", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital_quality.server", "compare_hospitals", "hospital_quality"),
    # Claims analytics
    LiveToolSpec("claims-analytics", "servers.claims_analytics.server", "get_inpatient_volumes", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims_analytics.server", "get_outpatient_volumes", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims_analytics.server", "trend_service_lines", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims_analytics.server", "compute_case_mix", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims_analytics.server", "analyze_market_volumes", "claims_analytics"),
    # LEIE and SAM.gov Exclusions
    LiveToolSpec("public-records", "servers.public_records.server", "check_leie_npi", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "search_leie_individual", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "search_leie_entity", "exclusions"),
    LiveToolSpec(
        "public-records",
        "servers.public_records.server",
        "screen_leie_batch",
        "exclusions",
        scopes=("mcp:read", "mcp:bulk"),
        request_size_limit_bytes=65_536,
        result_size_limit_bytes=524_288,
        result_limit=500,
        rate_limit_class="bulk",
    ),
    LiveToolSpec("public-records", "servers.public_records.server", "get_leie_metadata", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "search_sam_exclusions", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "check_sam_exclusion_identifier", "exclusions"),
    LiveToolSpec(
        "public-records",
        "servers.public_records.server",
        "screen_sam_exclusions_batch",
        "exclusions",
        scopes=("mcp:read", "mcp:bulk"),
        request_size_limit_bytes=65_536,
        result_size_limit_bytes=524_288,
        result_limit=500,
        rate_limit_class="bulk",
    ),
    LiveToolSpec("public-records", "servers.public_records.server", "get_sam_exclusions_metadata", "exclusions"),
    # Public state-health records, PHC4, and cyber enrichment
    LiveToolSpec("public-records", "servers.public_records.server", "search_phc4_public_reports", "public_records"),
    LiveToolSpec("public-records", "servers.public_records.server", "get_phc4_hospital_performance", "public_records"),
    LiveToolSpec("public-records", "servers.public_records.server", "get_phc4_financial_analysis", "public_records"),
    LiveToolSpec("public-records", "servers.public_records.server", "get_phc4_common_procedure_profile", "public_records"),
    LiveToolSpec("public-records", "servers.public_records.server", "get_cyber_incident_profile", "public_records"),
    # Public financial health / community benefit
    LiveToolSpec("financial-intelligence", "servers.financial_intelligence.server", "get_public_financial_health_profile", "financial_intelligence"),
    LiveToolSpec("financial-intelligence", "servers.financial_intelligence.server", "get_uncompensated_care_profile", "financial_intelligence"),
    LiveToolSpec("financial-intelligence", "servers.financial_intelligence.server", "get_charity_care_profile", "financial_intelligence"),
    LiveToolSpec("financial-intelligence", "servers.financial_intelligence.server", "get_bad_debt_profile", "financial_intelligence"),
    # Staffing productivity and public throughput
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "resolve_hospital_beds", "operations"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "get_hospital_staffing_productivity", "workforce"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "compare_hospital_staffing_productivity", "workforce"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "get_snf_nursing_hprd", "workforce"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "get_teaching_intensity", "workforce"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "get_public_throughput_profile", "operations"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "compare_public_throughput", "operations"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "get_ed_volume_profile", "operations"),
    LiveToolSpec("workforce-analytics", "servers.workforce_analytics.server", "get_or_procedure_volume_profile", "operations"),
    # CDC PLACES
    LiveToolSpec("community-health", "servers.community_health.server", "list_places_measures", "community_health"),
    LiveToolSpec("community-health", "servers.community_health.server", "search_places", "community_health"),
    LiveToolSpec("community-health", "servers.community_health.server", "get_places_profile", "community_health"),
    LiveToolSpec("community-health", "servers.community_health.server", "compare_places", "community_health"),
    LiveToolSpec("community-health", "servers.community_health.server", "get_market_community_profile", "community_health"),
    # NIH RePORTER and ClinicalTrials.gov
    LiveToolSpec("research-trials", "servers.research_trials.server", "search_nih_projects", "research_trials"),
    LiveToolSpec("research-trials", "servers.research_trials.server", "get_nih_project", "research_trials"),
    LiveToolSpec("research-trials", "servers.research_trials.server", "profile_research_funding", "research_trials"),
    LiveToolSpec("research-trials", "servers.research_trials.server", "search_clinical_trials", "research_trials"),
    LiveToolSpec("research-trials", "servers.research_trials.server", "get_clinical_trial", "research_trials"),
    LiveToolSpec("research-trials", "servers.research_trials.server", "profile_research_activity", "research_trials"),
)

LIVE_TOOL_BY_NAME: dict[str, LiveToolSpec] = {spec.tool_name: spec for spec in LIVE_TOOL_SPECS}
_LIVE_TOOL_CALLABLES: dict[str, Callable[..., Any]] = {}
_AUDIT_EVENTS: deque[dict[str, Any]] = deque(maxlen=500)
_RATE_LIMIT_WINDOWS: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_POLICIES: dict[str, tuple[int, float]] = {
    "standard": (60, 60.0),
    "bulk": (10, 60.0),
}
_ALLOWED_LIVE_SCOPES = {"mcp:read", "mcp:bulk"}
_SENSITIVE_ARGUMENT_KEYS = {
    "ssn",
    "social_security_number",
    "social_security_num",
    "social_security",
    "ein",
    "fein",
    "tin",
    "tax_id",
    "tax_identifier",
    "taxpayer_id",
    "taxpayer_identifier",
    "taxpayer_identification_number",
    "employer_identification_number",
    "federal_tax_id",
    "federal_tax_identifier",
}
def _validate_live_policy_specs() -> None:
    seen: set[str] = set()
    module_functions: dict[str, set[str]] = {}
    for spec in LIVE_TOOL_SPECS:
        if spec.tool_name in seen:
            raise RuntimeError(f"Duplicate live-gateway tool spec: {spec.tool_name}")
        seen.add(spec.tool_name)
        registry_spec = SERVER_BY_ID.get(spec.server)
        if registry_spec is None:
            raise RuntimeError(f"Live-gateway tool {spec.tool_name} references unknown registry server: {spec.server}")
        if spec.module != registry_spec.module:
            raise RuntimeError(
                f"Live-gateway tool {spec.tool_name} module {spec.module!r} does not match "
                f"registry module {registry_spec.module!r} for {spec.server}"
            )
        functions = module_functions.get(spec.module)
        if functions is None:
            functions = _module_function_names(spec.module)
            module_functions[spec.module] = functions
        if spec.tool_name not in functions:
            raise RuntimeError(
                f"Live-gateway tool {spec.tool_name} is not defined in registry module {spec.module}"
            )
        if "live" not in registry_spec.gateway_exposure:
            raise RuntimeError(
                f"Live-gateway tool {spec.tool_name} references {spec.server}, "
                "but that server is not marked gateway_exposure='live' in the registry"
            )
        if spec.rate_limit_class not in _RATE_LIMIT_POLICIES:
            raise RuntimeError(f"Unknown live-gateway rate limit class for {spec.tool_name}: {spec.rate_limit_class}")
        caveat_class = effective_source_caveat_class(spec)
        if caveat_class not in SOURCE_CAVEAT_CLASSES:
            raise RuntimeError(f"Unknown live-gateway source caveat class for {spec.tool_name}: {caveat_class}")
        if not spec.scopes:
            raise RuntimeError(f"Live-gateway tool {spec.tool_name} must declare at least one scope")
        unknown_scopes = sorted(set(spec.scopes) - _ALLOWED_LIVE_SCOPES)
        if unknown_scopes:
            raise RuntimeError(
                f"Live-gateway tool {spec.tool_name} declares unknown scope(s): {', '.join(unknown_scopes)}"
            )
        if "mcp:read" not in spec.scopes:
            raise RuntimeError(f"Live-gateway tool {spec.tool_name} must include baseline mcp:read scope")


def _module_function_names(module_name: str) -> set[str]:
    spec = importlib.util.find_spec(module_name)
    if spec is None or not spec.origin:
        return set()
    module_path = Path(spec.origin)
    if not module_path.exists():
        return set()
    try:
        tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    except (OSError, SyntaxError):
        return set()
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _live_gateway_env(env: Mapping[str, str], *, require_auth: bool) -> dict[str, str]:
    """Map MCP_LIVE_GATEWAY_* variables onto the shared gateway auth loader shape."""

    mapped = {key: value for key, value in env.items() if not key.startswith("MCP_GATEWAY_")}
    live_auth_required = env.get("MCP_LIVE_GATEWAY_AUTH_REQUIRED")
    if (
        require_auth
        and live_auth_required is not None
        and live_auth_required.strip().lower() in {"0", "false", "no", "off"}
    ):
        raise GatewayAuthError(
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED cannot disable auth for HTTP/SSE live-gateway transports"
        )
    aliases = {
        "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "MCP_GATEWAY_AUTH_REQUIRED",
        "MCP_LIVE_GATEWAY_BEARER_TOKEN": "MCP_GATEWAY_BEARER_TOKEN",
        "MCP_LIVE_GATEWAY_BEARER_TOKENS": "MCP_GATEWAY_BEARER_TOKENS",
        "MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256": "MCP_GATEWAY_BEARER_TOKEN_SHA256",
        "MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256_LIST": "MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST",
        "MCP_LIVE_GATEWAY_REQUIRED_SCOPES": "MCP_GATEWAY_REQUIRED_SCOPES",
        "MCP_LIVE_GATEWAY_TOKEN_SCOPES": "MCP_GATEWAY_TOKEN_SCOPES",
        "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "MCP_GATEWAY_ALLOWED_HOSTS",
        "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "MCP_GATEWAY_ALLOWED_ORIGINS",
        "MCP_LIVE_GATEWAY_PUBLIC_URL": "MCP_GATEWAY_PUBLIC_URL",
        "MCP_LIVE_GATEWAY_ISSUER_URL": "MCP_GATEWAY_ISSUER_URL",
    }
    for source, target in aliases.items():
        if source in env:
            mapped[target] = env[source]
    if require_auth and "MCP_LIVE_GATEWAY_AUTH_REQUIRED" not in env:
        mapped["MCP_GATEWAY_AUTH_REQUIRED"] = "true"
    return mapped


def _is_wildcard_bind(host: str) -> bool:
    return host.strip().lower() in {"0.0.0.0", "::", "[::]", ""}


def _parse_live_bool(value: str | None) -> bool:
    return bool(value and value.strip().lower() in {"1", "true", "yes", "on"})


def _validate_live_transport_posture(
    *,
    transport: str,
    host: str,
    security_config: Any,
    env: Mapping[str, str],
) -> None:
    """Reject unsafe HTTP/SSE exposure unless explicitly configured for deployment."""

    if transport not in {"sse", "streamable-http"} or not _is_wildcard_bind(host):
        return

    if _container_local_bind_allowed(security_config=security_config, env=env):
        return

    if not _parse_live_bool(env.get("MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND")):
        raise GatewayAuthError(
            "live-gateway refuses to bind HTTP/SSE to a wildcard interface by default; "
            "keep MCP_HOST=127.0.0.1 behind a trusted reverse proxy, or set "
            "MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND=true with HTTPS public URL and locked host/origin allow-lists"
        )

    public_url = security_config.public_url or ""
    if not public_url.startswith("https://"):
        raise GatewayAuthError(
            "MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND=true requires MCP_LIVE_GATEWAY_PUBLIC_URL=https://..."
        )
    if "*" in security_config.allowed_hosts or "*" in security_config.allowed_origins:
        raise GatewayAuthError(
            "MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND=true requires explicit allowed hosts and origins, not wildcard entries"
        )


def _validate_live_scope_posture(
    *,
    transport: str,
    security_config: Any,
    env: Mapping[str, str],
) -> None:
    """Reject broad HTTP/SSE bulk scope unless the deployment opts in explicitly."""

    if transport not in {"sse", "streamable-http"}:
        return
    if "mcp:bulk" not in security_config.required_scopes:
        return
    if _parse_live_bool(env.get("MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE")):
        return
    raise GatewayAuthError(
        "MCP_LIVE_GATEWAY_REQUIRED_SCOPES includes mcp:bulk. Static live-gateway bearer auth applies "
        "configured scopes to every valid token, so global bulk scope is disabled by default. Set "
        "MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE=true only for deployments where every live-gateway "
        "principal may run bulk screening tools."
    )


def _container_local_bind_allowed(*, security_config: Any, env: Mapping[str, str]) -> bool:
    if not _parse_live_bool(env.get("MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND")):
        return False
    if security_config.public_url:
        return False
    local_hosts = {"localhost", "127.0.0.1", "localhost:8020", "127.0.0.1:8020"}
    if not all(host in local_hosts for host in security_config.allowed_hosts):
        return False
    if not all(origin.startswith("http://localhost") or origin.startswith("http://127.0.0.1") for origin in security_config.allowed_origins):
        return False
    return True


_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_port = int(os.environ.get("MCP_PORT", "8020"))
_host = os.environ.get("MCP_HOST", "127.0.0.1")
_security_config = load_gateway_security_config(_live_gateway_env(os.environ, require_auth=_transport in {"sse", "streamable-http"}))
_validate_live_transport_posture(
    transport=_transport,
    host=_host,
    security_config=_security_config,
    env=os.environ,
)
_validate_live_scope_posture(
    transport=_transport,
    security_config=_security_config,
    env=os.environ,
)

_mcp_kwargs: dict[str, Any] = {
    "name": "healthcare-data-live-gateway",
    "instructions": (
        "Authenticated router for approved live healthcare-data-mcp tools. "
        "Use list_live_tools before calling domain tools. This gateway exposes public-source healthcare intelligence, "
        "not PHI or deployment secrets."
    ),
    "transport_security": build_transport_security_settings(_security_config),
}
if _transport in {"sse", "streamable-http"}:
    _mcp_kwargs["host"] = _host
    _mcp_kwargs["port"] = _port

if _security_config.auth_enabled:
    public_url = _security_config.public_url or f"http://{_host}:{_port}/mcp"
    issuer_url = _security_config.issuer_url or public_url
    _mcp_kwargs["token_verifier"] = StaticBearerTokenVerifier(
        _security_config.bearer_tokens,
        _security_config.bearer_token_sha256,
        required_scopes=_security_config.required_scopes,
        token_scope_overrides=_security_config.token_scope_overrides,
        resource=public_url,
    )
    _mcp_kwargs["auth"] = AuthSettings(
        issuer_url=issuer_url,
        resource_server_url=public_url,
        required_scopes=list(_security_config.required_scopes),
    )

mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "live-gateway")


def live_tool_inventory() -> list[dict[str, Any]]:
    """Return the approved live tool inventory."""

    inventory: list[dict[str, Any]] = []
    for spec in LIVE_TOOL_SPECS:
        registry_spec = SERVER_BY_ID[spec.server]
        inventory.append(
            {
                "name": spec.tool_name,
                "server": spec.server,
                "category": spec.category,
                "dataset_ids": list(registry_spec.dataset_ids),
                "cache_needs": list(registry_spec.cache_needs),
                "server_safety_notes": list(registry_spec.safety_notes),
                "allowed_scopes": list(spec.scopes),
                "request_size_limit_bytes": spec.request_size_limit_bytes,
                "result_size_limit_bytes": spec.result_size_limit_bytes,
                "result_limit": spec.result_limit,
                "rate_limit_class": spec.rate_limit_class,
                "auth_posture": "bearer_required_for_http_sse",
                "source_caveat_class": effective_source_caveat_class(spec),
                "source_caveat": source_caveat(spec),
                "requires_provenance": spec.require_provenance,
                "audit_event": "tool_call",
            }
        )
    return inventory


@mcp.tool(structured_output=True)
@observe_tool("live-gateway")
async def list_live_tools() -> dict[str, Any]:
    """List approved tools exposed by this authenticated live gateway.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"list_live_tools","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    tools = live_tool_inventory()
    return {
        "gateway": "live-gateway",
        "tool_count": len(tools),
        "tools": tools,
        "notes": [
            "Tool response shapes are preserved from the owning servers.",
            "Use domain-specific identifiers and source caveats returned by each tool.",
            "HTTP/SSE deployments require bearer-token authentication; stdio is local-process only.",
            "Requests containing SSN/EIN/TIN-style argument keys are rejected before routing.",
        ],
        "policy": {
            "gateway_type": "live_policy_gateway",
            "allowed_tool_source": "LIVE_TOOL_SPECS allowlist validated against registry gateway_exposure=live",
            "default_scope": "mcp:read",
            "allowed_scopes": sorted(_ALLOWED_LIVE_SCOPES),
            "token_scope_overrides": {
                "env": "MCP_LIVE_GATEWAY_TOKEN_SCOPES",
                "format": "<token_sha256>=mcp:read+mcp:bulk",
                "configured_count": len(_security_config.token_scope_overrides),
            },
            "default_request_size_limit_bytes": 32768,
            "default_result_size_limit_bytes": 262144,
            "default_result_limit": 100,
            "rate_limit_classes": {
                name: {"calls": calls, "window_seconds": int(window)}
                for name, (calls, window) in _RATE_LIMIT_POLICIES.items()
            },
            "source_caveat_classes": SOURCE_CAVEAT_CLASSES,
            "audit_event_shape": {
                "event": "tool_call",
                "gateway": "live-gateway",
                "tool": "<tool_name>",
                "server": "<owning_server>",
                "category": "<category>",
                "rate_limit_class": "<rate_limit_class>",
                "source_caveat_class": "<source_caveat_class>",
                "provenance_status": (
                    "evidence_receipt_valid|evidence_receipt_invalid|"
                    "evidence_receipt_missing|source_claim_paths_invalid|non_object_result"
                ),
                "evidence_present": "<bool>",
                "source_metadata_present": "<bool>",
                "identity_present": "<bool>",
                "source_claim_paths_status": "source_claim_paths_valid|source_claim_paths_invalid",
                "source_claim_paths_valid": "<bool>",
                "source_claim_path_issues": "<path validation defects when source-claim traceability fails>",
                "sensitive_argument_keys": "<redacted_key_names_when_blocked>",
                "invalid_evidence_paths": "<paths_and_errors_when_nested_or_top_level_receipts_fail_validation>",
                "subject": "<caller_identity_when_auth_available>",
                "outcome": "allowed|blocked|error",
            },
            "safe_defaults": [
                "No wildcard tool proxying.",
                "No PHI handling claims.",
                "No SSN/EIN/TIN-style sensitive identifier routing.",
                "Nested request arrays are bounded by the tool result_limit before routing.",
                "Malformed upstream evidence receipts are blocked before results leave live-gateway, including nested row receipts.",
                "Structurally valid but empty upstream evidence receipts are blocked; live routed receipts must include source identity, match basis, confidence, caveat, and next step.",
                "Missing upstream evidence receipts are blocked for live tools that require provenance.",
                "Live-routed results must pass strict source-claim-path validation before leaving live-gateway.",
                "HTTP/SSE auth cannot be disabled for live-gateway startup.",
                "HTTP/SSE wildcard network binds require explicit opt-in, HTTPS public URL, and locked Host/Origin allow-lists.",
                "Batch screening tools require the additional mcp:bulk scope.",
                "Use MCP_LIVE_GATEWAY_TOKEN_SCOPES to grant mcp:bulk only to selected static-token principals.",
                "HTTP/SSE global mcp:bulk scope is rejected unless MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE=true.",
            ],
        },
    }


@mcp.tool(structured_output=True)
@observe_tool("live-gateway")
async def get_live_gateway_audit_events(limit: int = 50) -> dict[str, Any]:
    """Return recent non-secret live-gateway audit events for local operations review.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_live_gateway_audit_events","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    bounded_limit = max(1, min(int(limit or 50), 100))
    return {
        "gateway": "live-gateway",
        "count": min(len(_AUDIT_EVENTS), bounded_limit),
        "events": list(_AUDIT_EVENTS)[-bounded_limit:],
        "audit_log_path_configured": bool(_audit_log_path()),
        "caveat": "Audit events intentionally omit request payload values and secrets.",
    }


async def call_live_tool(
    tool_name: str,
    arguments: Mapping[str, Any] | None = None,
    *,
    caller_scopes: Sequence[str] | None = None,
    subject: str = "configured_gateway_principal",
) -> Any:
    """Call an allowlisted live tool through the gateway policy enforcement path."""

    spec = LIVE_TOOL_BY_NAME.get(tool_name)
    if spec is None:
        _record_audit(tool_name=tool_name, outcome="blocked", reason="tool_not_allowlisted", subject=subject)
        raise_tool_error(f"{tool_name!r} is not exposed by live-gateway", code="policy_denied")

    kwargs = dict(arguments or {})
    sensitive_keys = _find_sensitive_argument_keys(kwargs)
    if sensitive_keys:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="sensitive_argument_key_rejected",
            subject=subject,
            sensitive_argument_keys=sensitive_keys,
        )
        raise_tool_error(
            (
                f"{spec.tool_name} request contains sensitive identifier key(s): "
                f"{', '.join(sensitive_keys)}. Live-gateway accepts public identifiers only."
            ),
            code="policy_denied",
            detail={"sensitive_argument_keys": sensitive_keys},
        )

    request_size = _json_size(kwargs)
    if request_size > spec.request_size_limit_bytes:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="request_size_limit_exceeded",
            subject=subject,
            request_size_bytes=request_size,
        )
        raise_tool_error(
            f"{spec.tool_name} request is {request_size} bytes; limit is {spec.request_size_limit_bytes}",
            code="policy_denied",
        )

    _enforce_argument_limits(spec, kwargs, subject=subject)
    _enforce_scopes(spec, caller_scopes, subject=subject)
    _enforce_rate_limit(spec, subject=subject)

    tool = _LIVE_TOOL_CALLABLES[spec.tool_name]
    try:
        raw_result = tool(**kwargs)
        result = await raw_result if inspect.isawaitable(raw_result) else raw_result
    except Exception:
        _record_audit(spec=spec, outcome="error", reason="owning_tool_error", subject=subject, request_size_bytes=request_size)
        raise

    structured = to_structured(result)
    result_count = _max_list_length(structured)
    if result_count > spec.result_limit:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="result_limit_exceeded",
            subject=subject,
            request_size_bytes=request_size,
            result_count=result_count,
        )
        raise_tool_error(
            f"{spec.tool_name} returned {result_count} rows/items; live-gateway limit is {spec.result_limit}",
            code="policy_denied",
        )

    result_size = _json_size(structured)
    if result_size > spec.result_size_limit_bytes:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="result_size_limit_exceeded",
            subject=subject,
            request_size_bytes=request_size,
            result_size_bytes=result_size,
        )
        raise_tool_error(
            f"{spec.tool_name} result is {result_size} bytes; limit is {spec.result_size_limit_bytes}",
            code="policy_denied",
        )

    provenance_status = evaluate_provenance_status(structured)
    if provenance_status.get("status") == "evidence_receipt_invalid":
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="invalid_evidence_receipt",
            subject=subject,
            request_size_bytes=request_size,
            result_size_bytes=result_size,
            result_count=result_count,
            **audit_provenance_fields(provenance_status),
        )
        raise_tool_error(
            f"{spec.tool_name} returned an invalid evidence receipt; live-gateway requires valid provenance when evidence is present",
            code="policy_denied",
        )
    if provenance_status.get("status") == "source_claim_paths_invalid":
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="invalid_source_claim_paths",
            subject=subject,
            request_size_bytes=request_size,
            result_size_bytes=result_size,
            result_count=result_count,
            **audit_provenance_fields(provenance_status),
        )
        raise_tool_error(
            f"{spec.tool_name} returned invalid source claim paths; live-gateway requires boundary traceability",
            code="policy_denied",
        )
    if spec.require_provenance and provenance_status.get("status") in {"evidence_receipt_missing", "non_object_result"}:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="missing_evidence_receipt",
            subject=subject,
            request_size_bytes=request_size,
            result_size_bytes=result_size,
            result_count=result_count,
            **audit_provenance_fields(provenance_status),
        )
        raise_tool_error(
            f"{spec.tool_name} returned no evidence receipt; live-gateway requires source provenance before routing results",
            code="policy_denied",
        )
    response = attach_gateway_policy(spec, structured, provenance_status=provenance_status)
    response_size = _json_size(response)
    if response_size > spec.result_size_limit_bytes:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="response_size_limit_exceeded",
            subject=subject,
            request_size_bytes=request_size,
            result_size_bytes=response_size,
            result_count=result_count,
            **audit_provenance_fields(provenance_status),
        )
        raise_tool_error(
            f"{spec.tool_name} response is {response_size} bytes after live-gateway policy metadata; "
            f"limit is {spec.result_size_limit_bytes}",
            code="policy_denied",
        )
    _record_audit(
        spec=spec,
        outcome="allowed",
        reason="policy_passed",
        subject=subject,
        request_size_bytes=request_size,
        result_size_bytes=response_size,
        result_count=result_count,
        **audit_provenance_fields(provenance_status),
    )
    return response


def _enforce_argument_limits(spec: LiveToolSpec, arguments: Mapping[str, Any], *, subject: str) -> None:
    for key in ("limit", "size", "page_size", "max_results"):
        value = arguments.get(key)
        requested_limit = _coerce_result_limit_argument(value)
        if requested_limit is not None and requested_limit < 1:
            _record_audit(
                spec=spec,
                outcome="blocked",
                reason=f"{key}_argument_below_minimum",
                subject=subject,
            )
            raise_tool_error(
                f"{spec.tool_name} argument {key}={value!r} must be at least 1",
                code="policy_denied",
            )
        if requested_limit is not None and requested_limit > spec.result_limit:
            _record_audit(
                spec=spec,
                outcome="blocked",
                reason=f"{key}_argument_exceeds_result_limit",
                subject=subject,
            )
            raise_tool_error(
                f"{spec.tool_name} argument {key}={value!r} exceeds live-gateway result_limit={spec.result_limit}",
                code="policy_denied",
            )

    oversized_lists = _find_oversized_argument_lists(arguments, spec.result_limit)
    if oversized_lists:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="argument_list_exceeds_result_limit",
            subject=subject,
            oversized_argument_lists=oversized_lists,
        )
        first = oversized_lists[0]
        raise_tool_error(
            f"{spec.tool_name} argument {first['path']} has {first['length']} items; limit is {spec.result_limit}",
            code="policy_denied",
            detail={"oversized_argument_lists": oversized_lists},
        )


def _coerce_result_limit_argument(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
            return int(stripped)
    return None


def _find_oversized_argument_lists(value: Any, limit: int, *, path: str = "arguments") -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}" if path else key_text
            matches.extend(_find_oversized_argument_lists(child, limit, path=child_path))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if len(value) > limit:
            matches.append({"path": path, "length": len(value), "limit": limit})
        for index, child in enumerate(value):
            matches.extend(_find_oversized_argument_lists(child, limit, path=f"{path}[{index}]"))
    return matches


def _find_sensitive_argument_keys(value: Any, *, path: str = "") -> list[str]:
    matches: list[str] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}" if path else key_text
            if _normalize_argument_key(key_text) in _SENSITIVE_ARGUMENT_KEYS:
                matches.append(child_path)
            matches.extend(_find_sensitive_argument_keys(child, path=child_path))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, child in enumerate(value):
            matches.extend(_find_sensitive_argument_keys(child, path=f"{path}[{index}]"))
    return sorted(dict.fromkeys(matches))


def _normalize_argument_key(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "_" for character in value).strip("_")


def _enforce_scopes(spec: LiveToolSpec, caller_scopes: Sequence[str] | None, *, subject: str) -> None:
    granted = set(caller_scopes if caller_scopes is not None else _security_config.required_scopes)
    missing = sorted(set(spec.scopes) - granted)
    if missing:
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="missing_scope",
            subject=subject,
            missing_scopes=missing,
        )
        raise_tool_error(
            f"{spec.tool_name} requires scope(s): {', '.join(missing)}",
            code="policy_denied",
            detail={"required_scopes": list(spec.scopes), "granted_scopes": sorted(granted)},
        )


def _enforce_rate_limit(spec: LiveToolSpec, *, subject: str) -> None:
    calls, window_seconds = _RATE_LIMIT_POLICIES.get(spec.rate_limit_class, _RATE_LIMIT_POLICIES["standard"])
    now = time.monotonic()
    key = f"{spec.rate_limit_class}:{spec.tool_name}:{_rate_limit_subject(subject)}"
    window = _RATE_LIMIT_WINDOWS[key]
    while window and now - window[0] >= window_seconds:
        window.popleft()
    if len(window) >= calls:
        retry_after = max(1, int(window_seconds - (now - window[0])))
        _record_audit(
            spec=spec,
            outcome="blocked",
            reason="rate_limit_exceeded",
            subject=subject,
            retry_after_seconds=retry_after,
        )
        raise_tool_error(
            f"{spec.tool_name} exceeded {calls} calls per {int(window_seconds)} seconds",
            code="rate_limited",
            detail={"retry_after_seconds": retry_after, "rate_limit_class": spec.rate_limit_class},
        )
    window.append(now)


def _rate_limit_subject(subject: str) -> str:
    cleaned = (subject or "anonymous").strip() or "anonymous"
    return cleaned[:128]


def _authenticated_policy_context() -> tuple[Sequence[str] | None, str]:
    """Return non-secret caller policy context from FastMCP auth middleware."""

    access_token = get_access_token()
    if access_token is None:
        return None, "configured_gateway_principal"
    return _access_token_scopes(access_token), _access_token_subject(access_token)


def _access_token_scopes(access_token: AccessToken) -> tuple[str, ...]:
    return tuple(str(scope) for scope in (access_token.scopes or []) if str(scope).strip())


def _access_token_subject(access_token: AccessToken) -> str:
    client_id = str(access_token.client_id or "authenticated_principal").strip() or "authenticated_principal"
    token_id = str(access_token.token or "").strip()
    if not token_id:
        return client_id[:128]
    return f"{client_id}:{token_fingerprint(token_id)}"[:128]


def _json_size(value: Any) -> int:
    return len(json.dumps(to_structured(value), separators=(",", ":"), sort_keys=True, default=str).encode("utf-8"))


def _max_list_length(value: Any) -> int:
    if isinstance(value, list):
        child_lengths = [_max_list_length(item) for item in value]
        return max([len(value), *child_lengths], default=len(value))
    if isinstance(value, dict):
        return max((_max_list_length(item) for item in value.values()), default=0)
    return 0


def _record_audit(
    *,
    spec: LiveToolSpec | None = None,
    tool_name: str | None = None,
    outcome: str,
    reason: str,
    subject: str = "configured_gateway_principal",
    **fields: Any,
) -> None:
    event = {
        "event": "tool_call",
        "gateway": "live-gateway",
        "tool": spec.tool_name if spec else tool_name or "",
        "server": spec.server if spec else "",
        "category": spec.category if spec else "",
        "rate_limit_class": spec.rate_limit_class if spec else "",
        "source_caveat_class": effective_source_caveat_class(spec) if spec else "",
        "subject": subject,
        "outcome": outcome,
        "reason": reason,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    event.update({key: value for key, value in fields.items() if value not in (None, "")})
    _AUDIT_EVENTS.append(event)
    _append_audit_log(event)
    logger.info("live_gateway_audit %s", json.dumps(event, sort_keys=True, default=str))


def _audit_log_path() -> Path | None:
    path = os.environ.get("MCP_LIVE_GATEWAY_AUDIT_LOG_PATH", "").strip()
    return Path(path).expanduser() if path else None


def _append_audit_log(event: Mapping[str, Any]) -> None:
    path = _audit_log_path()
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(to_structured(dict(event)), sort_keys=True, separators=(",", ":"), default=str))
            handle.write("\n")
    except OSError:
        logger.exception("Failed to append live-gateway audit event to %s", path)


def _make_live_tool_wrapper(spec: LiveToolSpec, tool: Callable[..., Any]) -> Callable[..., Awaitable[Any]]:
    @wraps(tool)
    async def wrapper(**kwargs: Any) -> Any:
        caller_scopes, subject = _authenticated_policy_context()
        return await call_live_tool(spec.tool_name, kwargs, caller_scopes=caller_scopes, subject=subject)

    wrapper.__name__ = spec.tool_name
    wrapper.__qualname__ = spec.tool_name
    wrapper.__doc__ = tool.__doc__
    wrapper.__signature__ = inspect.signature(tool)  # type: ignore[attr-defined]
    return wrapper


def _register_live_tools() -> None:
    for spec in LIVE_TOOL_SPECS:
        module = importlib.import_module(spec.module)
        tool = getattr(module, spec.tool_name)
        _LIVE_TOOL_CALLABLES[spec.tool_name] = tool
        wrapper = _make_live_tool_wrapper(spec, tool)
        globals()[spec.tool_name] = wrapper
        mcp.tool(name=spec.tool_name, structured_output=True)(wrapper)


_validate_live_policy_specs()
_register_live_tools()


if __name__ == "__main__":
    mcp.run(transport=_transport)
