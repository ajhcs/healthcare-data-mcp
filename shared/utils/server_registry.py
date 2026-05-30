"""Canonical server and capability registry for healthcare-data-mcp."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class EnvKey:
    """Environment variable used by one or more server capabilities."""

    name: str
    required: bool = False
    description: str = ""


@dataclass(frozen=True, slots=True)
class ServerCapability:
    """One MCP server's user-facing and deployment metadata."""

    server_id: str
    module: str
    port: int
    description: str
    required_env: tuple[EnvKey, ...] = ()
    optional_env: tuple[EnvKey, ...] = ()
    cache_needs: tuple[str, ...] = ()
    zero_config: bool = True
    gateway_exposure: tuple[str, ...] = ()
    profiles: tuple[str, ...] = ()
    safety_notes: tuple[str, ...] = ()
    workflow_roles: tuple[str, ...] = ()
    dataset_ids: tuple[str, ...] = ()
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def all_env_names(self) -> tuple[str, ...]:
        return tuple(key.name for key in (*self.required_env, *self.optional_env))


def env(name: str, description: str = "", *, required: bool = False) -> EnvKey:
    return EnvKey(name=name, required=required, description=description)


@dataclass(frozen=True, slots=True)
class CuratedPreset:
    """Task-first install/use profile built from registry server IDs."""

    preset_id: str
    title: str
    description: str
    server_ids: tuple[str, ...]
    workflow_ids: tuple[str, ...] = ()
    safety_notes: tuple[str, ...] = ()


SERVER_REGISTRY: tuple[ServerCapability, ...] = (
    ServerCapability(
        "service-area",
        "servers.service_area.server",
        8002,
        "CMS hospital service areas and market share",
        cache_needs=("cms-hsaf",),
        profiles=("market", "competitive"),
        gateway_exposure=("metadata",),
        workflow_roles=("facility_profile", "market_community_health_scan"),
        dataset_ids=("cms_hospital_general_info", "cms_hsaf", "dartmouth_hsa_hrr"),
    ),
    ServerCapability(
        "geo-demographics",
        "servers.geo_demographics.server",
        8003,
        "Census, ZCTA, Medicare, and HUD geography",
        optional_env=(
            env("CENSUS_API_KEY", "Optional Census key for higher API limits."),
            env("HUD_API_TOKEN", "Optional HUD USPS ZIP crosswalk API token."),
        ),
        profiles=("market", "community"),
        gateway_exposure=("metadata",),
        workflow_roles=("market_community_health_scan",),
        dataset_ids=("census_acs", "cms_geographic_variation"),
    ),
    ServerCapability(
        "drive-time",
        "servers.drive_time.server",
        8004,
        "Routing, drive-time matrices, and access scoring",
        optional_env=(
            env("ORS_API_KEY", "Optional OpenRouteService key for isochrones."),
            env("OSRM_BASE_URL", "Optional OSRM endpoint; defaults to the public demo server."),
        ),
        profiles=("market", "access"),
        gateway_exposure=("metadata",),
        workflow_roles=("market_community_health_scan", "referral_leakage_readiness"),
        dataset_ids=("cms_hospital_general_info",),
        safety_notes=("Public routing services can be rate-limited; self-host OSRM for heavy use.",),
    ),
    ServerCapability(
        "hospital-quality",
        "servers.hospital_quality.server",
        8005,
        "CMS quality, readmission, and safety data",
        cache_needs=("cms-provider-data-quality-files",),
        profiles=("quality", "competitive"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("quality_profile", "quality_measure_lookup", "finance_profile", "hospital_competitive_profile"),
        dataset_ids=("cms_cost_report", "cms_hospital_general_info", "cms_hospital_quality"),
        safety_notes=("Adjacent HRRP/HAC summaries are not substitutes for exact CMS measure rows.",),
    ),
    ServerCapability(
        "cms-facility",
        "servers.cms_facility.server",
        8006,
        "CMS facility master data and NPPES lookup",
        cache_needs=("cms-provider-of-services", "nppes"),
        profiles=("identity", "facility"),
        gateway_exposure=("metadata",),
        workflow_roles=(
            "facility_profile",
            "quality_profile",
            "hospital_competitive_profile",
            "ownership_chow_trace",
            "system_reconciliation",
        ),
        dataset_ids=("cms_hospital_general_info", "cms_provider_of_services", "nppes_registry"),
    ),
    ServerCapability(
        "health-system-profiler",
        "servers.health_system_profiler.server",
        8007,
        "Health system discovery and facility enrichment",
        cache_needs=("ahrq-compendium", "cms-provider-of-services"),
        profiles=("identity", "competitive"),
        gateway_exposure=("metadata",),
        workflow_roles=("facility_profile", "hospital_competitive_profile", "ownership_chow_trace", "system_reconciliation"),
        dataset_ids=("ahrq_health_system_compendium", "cms_provider_of_services", "nppes_registry"),
    ),
    ServerCapability(
        "financial-intelligence",
        "servers.financial_intelligence.server",
        8008,
        "IRS 990, SEC EDGAR, and nonprofit finance intelligence",
        required_env=(env("SEC_USER_AGENT", "Required for SEC EDGAR-backed tools.", required=True),),
        cache_needs=("irs-990", "cms-hcris", "ahrq-hfmd"),
        zero_config=False,
        profiles=("finance", "competitive"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("finance_profile", "hospital_competitive_profile"),
        dataset_ids=("ahrq_hfmd", "nj_hospital_public_data", "state_health_data"),
        safety_notes=("Public financial records vary by filing period and organization type.",),
    ),
    ServerCapability(
        "price-transparency",
        "servers.price_transparency.server",
        8009,
        "Hospital MRF and benchmark pricing",
        optional_env=(
            env("MRF_MAX_DOWNLOAD_BYTES", "Maximum hospital MRF download size in bytes."),
            env("MRF_MIN_FREE_BYTES", "Minimum free disk bytes required before MRF downloads."),
            env("MRF_DOWNLOAD_PROGRESS_INTERVAL_BYTES", "Progress logging interval for large MRF downloads."),
        ),
        cache_needs=("hospital-mrf-index",),
        profiles=("pricing", "competitive"),
        gateway_exposure=("metadata",),
        dataset_ids=("cms_price_transparency_mrf",),
        safety_notes=("Hospital MRFs can be very large; respect configured download and disk limits.",),
    ),
    ServerCapability(
        "physician-referral-network",
        "servers.physician_referral_network.server",
        8010,
        "NPPES, physician mix, referral network, and leakage analysis",
        optional_env=(env("DOCGRAPH_CSV_PATH", "Optional licensed CareSet DocGraph import path."),),
        cache_needs=("nppes", "docgraph-import"),
        profiles=("referral", "market"),
        gateway_exposure=("metadata",),
        workflow_roles=("referral_leakage_readiness",),
        dataset_ids=("dartmouth_hsa_hrr", "docgraph_referrals", "nppes_registry", "physician_compare_utilization"),
        safety_notes=("DocGraph/CareSet data is separately licensed and import-only.",),
    ),
    ServerCapability(
        "workforce-analytics",
        "servers.workforce_analytics.server",
        8011,
        "BLS and ACGME workforce analytics",
        optional_env=(
            env("BLS_API_KEY", "Optional BLS key for higher API limits."),
            env("ACGME_PROGRAMS_CSV", "Optional normalized ACGME Program Search export path."),
        ),
        cache_needs=("cms-hcris", "acgme-programs", "state-hospital-reports"),
        profiles=("workforce", "operations"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("facility_profile", "finance_profile", "hospital_competitive_profile"),
        dataset_ids=(
            "cms_cost_report",
            "de_hospital_discharge",
            "nj_hospital_public_data",
            "pa_hospital_reports",
            "state_health_data",
            "workforce_labor",
        ),
    ),
    ServerCapability(
        "claims-analytics",
        "servers.claims_analytics.server",
        8012,
        "DRG, service-line, and claims analytics",
        cache_needs=("cms-claims-reference",),
        profiles=("claims", "service-line"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("hospital_competitive_profile", "referral_leakage_readiness"),
        dataset_ids=("cms_medicare_claims_pufs",),
        safety_notes=("This project exposes public/reference claims data, not patient-level PHI.",),
    ),
    ServerCapability(
        "public-records",
        "servers.public_records.server",
        8013,
        "SAM.gov, USAspending, CHPL, accreditation, and exclusion screening",
        optional_env=(
            env("SAM_GOV_API_KEY", "Required for SAM.gov opportunity and Exclusions API tools."),
            env("CHPL_API_KEY", "Optional ONC CHPL enrichment key."),
        ),
        cache_needs=("hhs-oig-leie", "hipaa-breaches", "phc4-public-reports", "state-breach-notices"),
        profiles=("compliance", "public-records"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("compliance_exclusion_screening", "ownership_chow_trace"),
        dataset_ids=(
            "cms_provider_of_services",
            "hhs_oig_leie",
            "phc4_public_reports",
            "public_records",
            "sam_gov_exclusions",
            "state_health_data",
        ),
        safety_notes=("Exclusion screening supports follow-up; it is not final SSN/EIN identity verification or legal clearance.",),
    ),
    ServerCapability(
        "web-intelligence",
        "servers.web_intelligence.server",
        8014,
        "Web search and health system OSINT",
        optional_env=(
            env("GOOGLE_CSE_API_KEY", "Optional Google Custom Search API key."),
            env("GOOGLE_CSE_ID", "Optional Google Custom Search Engine ID."),
            env("GOOGLE_CSE_CACHE_TTL_SECONDS", "Google Custom Search cache TTL in seconds."),
            env("GOOGLE_CSE_DAILY_LIMIT", "Google Custom Search daily request guardrail."),
            env("GOOGLE_CSE_SESSION_LIMIT", "Google Custom Search per-session request guardrail."),
            env("PROXYCURL_API_KEY", "Optional Proxycurl enrichment key."),
        ),
        zero_config=False,
        profiles=("web", "osint"),
        gateway_exposure=("metadata",),
        workflow_roles=("system_reconciliation",),
        dataset_ids=("web_intelligence",),
        safety_notes=("Fetched web content is untrusted and should be validated against source pages.",),
    ),
    ServerCapability(
        "discovery",
        "servers.discovery.server",
        8015,
        "Dataset catalog resources, cache status, and prompts",
        profiles=("metadata", "onboarding"),
        gateway_exposure=("metadata",),
        workflow_roles=("quality_measure_lookup",),
        dataset_ids=("mcp_metadata_surfaces",),
    ),
    ServerCapability(
        "gateway",
        "servers.gateway.server",
        8016,
        "Remote-safe metadata gateway with search/fetch",
        optional_env=(
            env("MCP_GATEWAY_AUTH_REQUIRED", "Whether metadata gateway HTTP/SSE auth is required."),
            env("MCP_GATEWAY_BEARER_TOKEN", "Optional local bearer token."),
            env("MCP_GATEWAY_BEARER_TOKENS", "Comma-separated metadata gateway bearer tokens."),
            env("MCP_GATEWAY_BEARER_TOKEN_SHA256", "Recommended token hash for remote deployments."),
            env("MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST", "Comma-separated metadata gateway bearer token SHA-256 hashes."),
            env("MCP_GATEWAY_REQUIRED_SCOPES", "Required metadata gateway auth scopes; defaults to mcp:read."),
            env("MCP_GATEWAY_TOKEN_SCOPES", "Optional semicolon-separated SHA-256 token-hash scope overrides."),
            env("MCP_GATEWAY_ALLOWED_HOSTS", "Allowed Host headers for metadata gateway HTTP/SSE."),
            env("MCP_GATEWAY_ALLOWED_ORIGINS", "Allowed Origin headers for metadata gateway HTTP/SSE."),
            env("MCP_GATEWAY_PUBLIC_URL", "Public HTTPS MCP URL for metadata gateway deployments."),
            env("MCP_GATEWAY_ISSUER_URL", "OAuth/OIDC issuer URL advertised by the metadata gateway."),
        ),
        profiles=("remote", "metadata"),
        gateway_exposure=("metadata",),
        dataset_ids=("mcp_metadata_surfaces",),
        safety_notes=("Metadata gateway does not proxy live healthcare tools.",),
    ),
    ServerCapability(
        "provider-enrollment",
        "servers.provider_enrollment.server",
        8017,
        "CMS PECOS-derived provider enrollment, ownership, and CHOW",
        cache_needs=("cms-pecos-enrollment", "cms-pecos-ownership", "cms-pecos-chow"),
        profiles=("ownership", "identity", "compliance"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("ownership_chow_trace", "compliance_exclusion_screening", "system_reconciliation"),
        dataset_ids=(
            "cms_pecos_hospital_chow",
            "cms_pecos_hospital_enrollments",
            "cms_pecos_hospital_owners",
            "cms_pecos_public_provider_enrollment",
            "cms_pecos_snf_chow",
            "cms_pecos_snf_enrollments",
            "cms_pecos_snf_owners",
        ),
    ),
    ServerCapability(
        "community-health",
        "servers.community_health.server",
        8018,
        "CDC PLACES community-health estimates for counties, places, tracts, and ZCTAs",
        optional_env=(env("PLACES_CACHE_DIR", "Optional CDC PLACES cache directory override."),),
        cache_needs=("cdc-places",),
        profiles=("community", "market"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("market_community_health_scan",),
        dataset_ids=("cdc_places",),
        safety_notes=("CDC PLACES values are modeled community estimates, not facility-specific outcomes.",),
    ),
    ServerCapability(
        "research-trials",
        "servers.research_trials.server",
        8019,
        "NIH RePORTER funding and ClinicalTrials.gov study activity",
        optional_env=(env("CLINICAL_TRIALS_INVENTORY_HARD_MAX", "Maximum ClinicalTrials.gov records scanned by inventory tools."),),
        profiles=("research", "trials"),
        gateway_exposure=("metadata", "live"),
        workflow_roles=("research_trials_activity_profile",),
        dataset_ids=("clinicaltrials_gov", "nih_reporter_projects"),
    ),
    ServerCapability(
        "live-gateway",
        "servers.live_gateway.server",
        8020,
        "Authenticated live router for approved provider, quality, claims, compliance, community, and research tools",
        optional_env=(
            env("MCP_LIVE_GATEWAY_AUTH_REQUIRED", "Whether live-gateway HTTP/SSE auth is required; cannot disable remote auth."),
            env("MCP_LIVE_GATEWAY_BEARER_TOKEN", "Bearer token for live HTTP/SSE deployments."),
            env("MCP_LIVE_GATEWAY_BEARER_TOKENS", "Comma-separated live-gateway bearer tokens."),
            env("MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256", "Recommended token hash for live HTTP/SSE deployments."),
            env("MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256_LIST", "Comma-separated live-gateway bearer token SHA-256 hashes."),
            env("MCP_LIVE_GATEWAY_REQUIRED_SCOPES", "Required auth scopes; defaults to mcp:read."),
            env("MCP_LIVE_GATEWAY_TOKEN_SCOPES", "Optional semicolon-separated SHA-256 token-hash scope overrides; use this to grant mcp:bulk to selected tokens."),
            env("MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE", "Explicit opt-in to grant mcp:bulk to every valid live-gateway token."),
            env("MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND", "Docker-only marker for container wildcard binds published to localhost."),
            env("MCP_LIVE_GATEWAY_ALLOWED_HOSTS", "Allowed Host headers for live-gateway HTTP/SSE."),
            env("MCP_LIVE_GATEWAY_ALLOWED_ORIGINS", "Allowed Origin headers for live-gateway HTTP/SSE."),
            env("MCP_LIVE_GATEWAY_PUBLIC_URL", "Public HTTPS MCP URL for live-gateway deployments."),
            env("MCP_LIVE_GATEWAY_ISSUER_URL", "OAuth/OIDC issuer URL advertised by live-gateway."),
            env("MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND", "Explicit opt-in for live-gateway wildcard network binds behind HTTPS and locked allow-lists."),
            env("MCP_LIVE_GATEWAY_AUDIT_LOG_PATH", "Optional JSONL file path for non-secret live-gateway audit events."),
        ),
        zero_config=False,
        profiles=("remote", "live"),
        gateway_exposure=("live",),
        workflow_roles=("compliance_exclusion_screening",),
        safety_notes=("HTTP/SSE live-gateway requires auth and should be deployed behind HTTPS.",),
    ),
    ServerCapability(
        "cache-manager",
        "servers.cache_manager.server",
        8021,
        "Local-safe cache inspection, planning, validation, refresh, promotion, rollback, and lineage control plane",
        optional_env=(
            env("HC_MCP_CACHE_ROOT", "Optional cache root override for cache-manager operations."),
            env("HC_MCP_CACHE_MANAGER_ALLOW_REMOTE_MUTATIONS", "Explicit opt-in for mutating HTTP deployments."),
        ),
        profiles=("cache", "operations"),
        gateway_exposure=(),
        dataset_ids=(),
        safety_notes=(
            "Read-only inspection and planning are safe by default; mutating tools are dataset allowlisted and cache-root scoped.",
            "Remote metadata gateway does not expose cache-manager mutations.",
        ),
    ),
)

SERVER_BY_ID: dict[str, ServerCapability] = {spec.server_id: spec for spec in SERVER_REGISTRY}

WORKFLOW_PRESETS: dict[str, tuple[str, ...]] = {
    "compliance_exclusion_screening": ("public-records", "provider-enrollment", "live-gateway"),
    "facility_profile": ("cms-facility", "health-system-profiler", "service-area", "workforce-analytics"),
    "quality_profile": ("hospital-quality", "cms-facility"),
    "finance_profile": ("financial-intelligence", "hospital-quality", "workforce-analytics"),
    "hospital_competitive_profile": (
        "cms-facility",
        "health-system-profiler",
        "hospital-quality",
        "financial-intelligence",
        "workforce-analytics",
        "claims-analytics",
    ),
    "ownership_chow_trace": ("provider-enrollment", "cms-facility", "health-system-profiler", "public-records"),
    "market_community_health_scan": ("geo-demographics", "community-health", "service-area", "drive-time"),
    "quality_measure_lookup": ("hospital-quality", "discovery"),
    "research_trials_activity_profile": ("research-trials",),
    "referral_leakage_readiness": ("physician-referral-network", "claims-analytics", "drive-time"),
    "system_reconciliation": ("health-system-profiler", "cms-facility", "provider-enrollment", "web-intelligence"),
}

CURATED_PRESETS: dict[str, CuratedPreset] = {
    "compliance": CuratedPreset(
        preset_id="compliance",
        title="Compliance Screening",
        description="Public exclusion, enrollment, ownership, and controlled live-gateway screening surfaces.",
        server_ids=("public-records", "provider-enrollment", "discovery", "live-gateway"),
        workflow_ids=("compliance_exclusion_screening", "ownership_chow_trace"),
        safety_notes=("Screening outputs support follow-up; they are not legal clearance or PHI/HIPAA tooling.",),
    ),
    "market-strategy": CuratedPreset(
        preset_id="market-strategy",
        title="Market Strategy",
        description="Facility, quality, finance, workforce, claims, service-area, access, and community context.",
        server_ids=(
            "cms-facility",
            "health-system-profiler",
            "hospital-quality",
            "financial-intelligence",
            "workforce-analytics",
            "claims-analytics",
            "service-area",
            "geo-demographics",
            "community-health",
            "drive-time",
        ),
        workflow_ids=(
            "facility_profile",
            "quality_profile",
            "finance_profile",
            "hospital_competitive_profile",
            "system_reconciliation",
            "market_community_health_scan",
            "referral_leakage_readiness",
        ),
        safety_notes=("Public data varies by source period; preserve evidence receipts for every report fact.",),
    ),
    "research": CuratedPreset(
        preset_id="research",
        title="Research And Trials",
        description="NIH RePORTER, ClinicalTrials.gov, web intelligence, and organization context.",
        server_ids=("research-trials", "web-intelligence", "discovery"),
        workflow_ids=("research_trials_activity_profile",),
        safety_notes=("Public sponsor/site matching is conservative and not a complete internal research portfolio.",),
    ),
    "metadata-only": CuratedPreset(
        preset_id="metadata-only",
        title="Metadata Only",
        description="Remote-safe dataset discovery without live healthcare tool proxying.",
        server_ids=("discovery", "gateway"),
        workflow_ids=("quality_measure_lookup",),
        safety_notes=("The metadata gateway exposes search/fetch only; live calls belong on authenticated live-gateway.",),
    ),
}


def server_ids() -> tuple[str, ...]:
    """Return registered server IDs in port order."""

    return tuple(spec.server_id for spec in SERVER_REGISTRY)


def get_server(server_id: str) -> ServerCapability:
    """Return one registered server or raise KeyError."""

    return SERVER_BY_ID[server_id]


__all__ = [
    "CURATED_PRESETS",
    "CuratedPreset",
    "EnvKey",
    "SERVER_BY_ID",
    "SERVER_REGISTRY",
    "ServerCapability",
    "WORKFLOW_PRESETS",
    "get_server",
    "server_ids",
]
