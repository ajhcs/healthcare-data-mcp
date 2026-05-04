"""Authenticated live-data gateway for approved healthcare-data-mcp tools."""

from __future__ import annotations

import importlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP

from shared.utils.gateway_auth import (
    GatewayAuthError,
    StaticBearerTokenVerifier,
    build_transport_security_settings,
    load_gateway_security_config,
)


@dataclass(frozen=True)
class LiveToolSpec:
    """One approved live tool exposed through the live gateway."""

    server: str
    module: str
    tool_name: str
    category: str


LIVE_TOOL_SPECS: tuple[LiveToolSpec, ...] = (
    # Provider enrollment and ownership
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "search_provider_enrollment", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "get_provider_enrollment_detail", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "get_facility_ownership", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "trace_owner_network", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "search_change_of_ownership", "provider_enrollment"),
    LiveToolSpec("provider-enrollment", "servers.provider_enrollment.server", "profile_provider_control", "provider_enrollment"),
    # Hospital quality
    LiveToolSpec("hospital-quality", "servers.hospital-quality.server", "get_quality_scores", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital-quality.server", "get_readmission_data", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital-quality.server", "get_safety_scores", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital-quality.server", "get_patient_experience", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital-quality.server", "get_financial_profile", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital-quality.server", "get_quality_measure_rows", "hospital_quality"),
    LiveToolSpec("hospital-quality", "servers.hospital-quality.server", "compare_hospitals", "hospital_quality"),
    # Claims analytics
    LiveToolSpec("claims-analytics", "servers.claims-analytics.server", "get_inpatient_volumes", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims-analytics.server", "get_outpatient_volumes", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims-analytics.server", "trend_service_lines", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims-analytics.server", "compute_case_mix", "claims_analytics"),
    LiveToolSpec("claims-analytics", "servers.claims-analytics.server", "analyze_market_volumes", "claims_analytics"),
    # LEIE and SAM.gov Exclusions
    LiveToolSpec("public-records", "servers.public_records.server", "check_leie_npi", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "search_leie_individual", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "search_leie_entity", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "screen_leie_batch", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "get_leie_metadata", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "search_sam_exclusions", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "check_sam_exclusion_identifier", "exclusions"),
    LiveToolSpec("public-records", "servers.public_records.server", "screen_sam_exclusions_batch", "exclusions"),
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


_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_port = int(os.environ.get("MCP_PORT", "8020"))
_host = os.environ.get("MCP_HOST", "127.0.0.1")
_security_config = load_gateway_security_config(_live_gateway_env(os.environ, require_auth=_transport in {"sse", "streamable-http"}))

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
        resource=public_url,
    )
    _mcp_kwargs["auth"] = AuthSettings(
        issuer_url=issuer_url,
        resource_server_url=public_url,
        required_scopes=list(_security_config.required_scopes),
    )

mcp = FastMCP(**_mcp_kwargs)


def live_tool_inventory() -> list[dict[str, str]]:
    """Return the approved live tool inventory."""

    return [
        {
            "name": spec.tool_name,
            "server": spec.server,
            "category": spec.category,
        }
        for spec in LIVE_TOOL_SPECS
    ]


@mcp.tool(structured_output=True)
async def list_live_tools() -> dict[str, Any]:
    """List approved tools exposed by this authenticated live gateway."""

    tools = live_tool_inventory()
    return {
        "gateway": "live-gateway",
        "tool_count": len(tools),
        "tools": tools,
        "notes": [
            "Tool response shapes are preserved from the owning servers.",
            "Use domain-specific identifiers and source caveats returned by each tool.",
        ],
    }


def _register_live_tools() -> None:
    for spec in LIVE_TOOL_SPECS:
        module = importlib.import_module(spec.module)
        tool = getattr(module, spec.tool_name)
        globals()[spec.tool_name] = tool
        mcp.tool(structured_output=True)(tool)


_register_live_tools()


if __name__ == "__main__":
    mcp.run(transport=_transport)
