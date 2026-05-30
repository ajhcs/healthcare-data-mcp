"""Remote MCP gateway exposing dataset metadata via OpenAI-compatible tools."""

from __future__ import annotations

import ast
import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources

from shared.utils.gateway_auth import (
    StaticBearerTokenVerifier,
    build_transport_security_settings,
    load_gateway_security_config,
)
from shared.utils.mcp_response import not_found_response
from shared.utils.mistake_detection import fuzzy_options
from shared.utils.server_registry import SERVER_BY_ID, ServerCapability


@dataclass(frozen=True)
class DatasetDocument:
    """Static metadata document exposed through search/fetch."""

    id: str
    title: str
    description: str
    server: str
    tools: tuple[str, ...]
    tags: tuple[str, ...]
    source: str
    access_notes: str

    @property
    def text(self) -> str:
        tools = ", ".join(self.tools)
        tags = ", ".join(self.tags)
        return (
            f"{self.title}\n\n"
            f"{self.description}\n\n"
            f"Server: {self.server}\n"
            f"Tools: {tools}\n"
            f"Tags: {tags}\n"
            f"Source: {self.source}\n"
            f"Access notes: {self.access_notes}"
        )


DATASETS: tuple[DatasetDocument, ...] = (
    DatasetDocument(
        id="cms-facility",
        title="CMS Facility Master Data",
        description="Hospital General Info, NPPES organization lookup, and cost report facility financial fields.",
        server="cms-facility",
        tools=("search_facilities", "get_facility", "search_npi", "get_facility_financials"),
        tags=("cms", "facility", "nppes", "cost-report", "hospital"),
        source="CMS public provider datasets and NPPES public registry",
        access_notes="Static gateway metadata only; use the underlying local server for live facility records.",
    ),
    DatasetDocument(
        id="health-system-profiler",
        title="Health System Profiler",
        description="AHRQ health system discovery with CMS Provider of Services enrichment and outpatient discovery.",
        server="health-system-profiler",
        tools=("search_health_systems", "get_system_profile", "get_system_facilities"),
        tags=("ahrq", "health-system", "provider-of-services", "outpatient", "profile"),
        source="AHRQ Compendium, CMS Provider of Services, and NPPES public registry",
        access_notes="Metadata describes available profiling workflows; no patient-level data is exposed.",
    ),
    DatasetDocument(
        id="hospital-quality",
        title="CMS Hospital Quality",
        description="CMS quality, readmission, mortality, safety, and patient experience measures for hospitals.",
        server="hospital-quality",
        tools=(
            "get_quality_scores",
            "compare_hospitals",
            "get_readmission_data",
            "get_safety_scores",
            "get_patient_experience",
            "get_financial_profile",
            "get_quality_measure_rows",
        ),
        tags=("cms", "quality", "readmission", "safety", "stars", "measure-rows"),
        source="CMS Care Compare public quality files",
        access_notes="Exact CMS measure rows are exposed through get_quality_measure_rows; HRRP/HAC summary tools are adjacent context and not substitutes for exact IDs.",
    ),
    DatasetDocument(
        id="geo-demographics",
        title="Geography and Demographics",
        description="Census, ZCTA, Medicare geography, and HUD crosswalk context for healthcare markets.",
        server="geo-demographics",
        tools=(
            "get_zcta_demographics",
            "get_zcta_demographics_batch",
            "get_zcta_adjacency",
            "get_medicare_enrollment",
            "get_geographic_variation",
            "crosswalk_zip",
        ),
        tags=("census", "zcta", "demographics", "geography", "market"),
        source="US Census, HUD, and public Medicare geographic references",
        access_notes="Use for discovering available geographic context before calling local analysis tools.",
    ),
    DatasetDocument(
        id="service-area",
        title="CMS Hospital Service Area",
        description="CMS hospital service areas, market share, and Dartmouth HSA/HRR comparison metadata.",
        server="service-area",
        tools=("compute_service_area", "get_market_share", "get_hsa_hrr_mapping", "compare_to_dartmouth"),
        tags=("cms", "service-area", "market-share", "hsaf", "dartmouth"),
        source="CMS Hospital Service Area Files and Dartmouth Atlas geography references",
        access_notes="Remote gateway does not proxy market-share computation; use the local service-area server.",
    ),
    DatasetDocument(
        id="drive-time",
        title="Drive Time and Access",
        description="Routing, drive-time matrices, catchment accessibility, and market access scoring.",
        server="drive-time",
        tools=(
            "compute_drive_time",
            "compute_drive_time_matrix",
            "generate_isochrone",
            "find_competing_facilities",
            "compute_accessibility_score",
        ),
        tags=("routing", "drive-time", "access", "catchment", "osrm"),
        source="OpenStreetMap-derived routing engines and facility coordinates",
        access_notes="Remote gateway does not proxy route calculations; it only advertises dataset/tool metadata.",
    ),
    DatasetDocument(
        id="price-transparency",
        title="Hospital Price Transparency",
        description="Hospital machine-readable-file registry and benchmark price transparency analysis metadata.",
        server="price-transparency",
        tools=(
            "search_mrf_index",
            "get_negotiated_rates",
            "compute_rate_dispersion",
            "compare_rates_system",
            "benchmark_rates",
        ),
        tags=("price-transparency", "mrf", "payer", "contract", "benchmark"),
        source="Hospital price transparency machine-readable files and public registry data",
        access_notes="Gateway responses avoid large MRF payloads and expose retrievable knowledge metadata only.",
    ),
    DatasetDocument(
        id="financial-intelligence",
        title="Financial Intelligence",
        description="IRS 990, ProPublica nonprofit, SEC EDGAR, and high-confidence public financial health metadata.",
        server="financial-intelligence",
        tools=(
            "search_form990",
            "get_form990_details",
            "search_sec_filings",
            "get_sec_filing",
            "search_muni_bonds",
            "get_muni_bond_details",
            "parse_audited_financial_pdf",
            "get_public_financial_health_profile",
            "get_uncompensated_care_profile",
            "get_charity_care_profile",
            "get_bad_debt_profile",
        ),
        tags=("irs-990", "edgar", "nonprofit", "finance", "community-benefit", "hcris"),
        source="IRS e-file data, ProPublica Nonprofit Explorer, SEC EDGAR public filings, CMS HCRIS, and AHRQ HFMD",
        access_notes="Metadata only; live filings remain in the dedicated financial intelligence server.",
    ),
    DatasetDocument(
        id="physician-referral-network",
        title="Physician Referral Network",
        description="NPPES physician mix, referral network, leakage, and specialty alignment analysis.",
        server="physician-referral-network",
        tools=(
            "search_physicians",
            "get_physician_profile",
            "load_docgraph_cache",
            "map_referral_network",
            "analyze_physician_mix",
            "detect_leakage",
        ),
        tags=("physician", "nppes", "referral", "leakage", "specialty"),
        source="NPPES public registry and referral-network reference datasets",
        access_notes="No PHI or patient-level claims are available through the gateway.",
    ),
    DatasetDocument(
        id="workforce-analytics",
        title="Healthcare Workforce Analytics",
        description="BLS, ACGME, HCRIS, PBJ, staffing productivity, and public throughput metadata.",
        server="workforce-analytics",
        tools=(
            "get_bls_employment",
            "get_hrsa_workforce",
            "get_gme_profile",
            "get_residency_programs",
            "get_acgme_source_status",
            "get_acgme_program",
            "search_acgme_programs",
            "search_union_activity",
            "get_staffing_benchmarks",
            "get_cost_report_staffing",
            "resolve_hospital_beds",
            "get_hospital_staffing_productivity",
            "compare_hospital_staffing_productivity",
            "get_snf_nursing_hprd",
            "get_teaching_intensity",
            "get_public_throughput_profile",
            "compare_public_throughput",
            "get_ed_volume_profile",
            "get_or_procedure_volume_profile",
        ),
        tags=("bls", "acgme", "workforce", "labor", "occupation", "hcris", "pbj", "throughput"),
        source="BLS public labor datasets, ACGME public training references, CMS HCRIS, CMS POS, CMS PBJ, PA DOH public hospital reports, and AHRQ linkage",
        access_notes="ACGME program inventory requires a ready imported public export; check get_acgme_source_status before asserting completeness.",
    ),
    DatasetDocument(
        id="claims-analytics",
        title="Claims Analytics Reference Data",
        description="DRG weights, service-line mapping, and claims analytics reference metadata.",
        server="claims-analytics",
        tools=(
            "get_inpatient_volumes",
            "get_outpatient_volumes",
            "trend_service_lines",
            "compute_case_mix",
            "analyze_market_volumes",
        ),
        tags=("drg", "claims", "service-line", "medicare", "weights"),
        source="CMS public DRG reference files and repository-maintained service-line mappings",
        access_notes="Gateway does not expose claims records; only public reference dataset descriptions.",
    ),
    DatasetDocument(
        id="public-records",
        title="Public Records Intelligence",
        description="SAM.gov, USAspending, accreditation, PHC4 public reports, cyber incidents, and public contracting metadata.",
        server="public-records",
        tools=(
            "search_usaspending",
            "search_sam_gov",
            "get_breach_history",
            "search_phc4_public_reports",
            "get_phc4_hospital_performance",
            "get_phc4_financial_analysis",
            "get_phc4_common_procedure_profile",
            "get_cyber_incident_profile",
            "get_cyber_attestation_source_status",
            "get_state_ag_breach_notice_sources",
            "search_state_breach_notices",
            "get_cisa_kev_context_status",
            "get_accreditation",
            "get_interop_status",
            "check_leie_npi",
            "search_leie_individual",
            "search_leie_entity",
            "screen_leie_batch",
            "get_leie_metadata",
            "search_sam_exclusions",
            "check_sam_exclusion_identifier",
            "screen_sam_exclusions_batch",
            "get_sam_exclusions_metadata",
        ),
        tags=("sam", "usaspending", "chpl", "accreditation", "contracts", "340b", "phc4", "cyber"),
        source="US government public records APIs, PHC4 public reports, OCR breaches, and bundled accreditation references",
        access_notes="Cybersecurity attestation status is unsupported unless a reviewed public attestation source is configured; adjacent OCR/SEC/state/CISA sources are labeled separately.",
    ),
    DatasetDocument(
        id="web-intelligence",
        title="Healthcare Web Intelligence",
        description="Web search, executive discovery, GPO directory, and health system OSINT workflows.",
        server="web-intelligence",
        tools=(
            "scrape_system_profile",
            "detect_ehr_vendor",
            "get_executive_profiles",
            "monitor_newsroom",
            "detect_gpo_affiliation",
            "search_web",
            "fetch_web_page",
        ),
        tags=("web", "osint", "executives", "gpo", "search"),
        source="Public web pages, search APIs, and bundled GPO directory data",
        access_notes="Treat fetched web content as untrusted; validate before relying on generated summaries.",
    ),
    DatasetDocument(
        id="discovery",
        title="Healthcare Data Discovery Catalog",
        description="Dataset catalog, cache readiness, runbooks, workflow plans, and source metadata resources.",
        server="discovery",
        tools=(
            "list_datasets",
            "get_dataset",
            "get_dataset_schema",
            "get_dataset_source",
            "get_cache_status",
            "list_runbooks",
            "get_runbook",
            "list_workflows",
            "get_workflow_plan",
        ),
        tags=("metadata", "catalog", "cache", "workflow", "runbook"),
        source="healthcare-data-mcp registry, discovery catalog, and local cache metadata",
        access_notes="Metadata-only surface; it reports readiness and workflow plans without fetching live healthcare records.",
    ),
    DatasetDocument(
        id="gateway",
        title="Remote Metadata Gateway",
        description="Remote-safe search and fetch for healthcare-data-mcp dataset and capability metadata.",
        server="gateway",
        tools=("search", "fetch"),
        tags=("metadata", "remote", "gateway", "search", "fetch"),
        source="healthcare-data-mcp canonical server registry and gateway dataset documents",
        access_notes="Does not proxy live healthcare tools; deploy remote access with bearer auth and HTTPS.",
    ),
    DatasetDocument(
        id="provider-enrollment",
        title="CMS PECOS Provider Enrollment and Ownership",
        description="Medicare FFS provider enrollment, hospital/SNF ownership, and change-of-ownership history.",
        server="provider-enrollment",
        tools=(
            "search_provider_enrollment",
            "get_provider_enrollment_detail",
            "get_facility_ownership",
            "trace_owner_network",
            "search_change_of_ownership",
            "profile_provider_control",
        ),
        tags=("cms", "pecos", "provider-enrollment", "ownership", "chow"),
        source="CMS Medicare FFS public provider enrollment and provider ownership datasets",
        access_notes="Remote gateway metadata only; live ownership queries belong in provider-enrollment or live-gateway.",
    ),
    DatasetDocument(
        id="community-health",
        title="CDC PLACES Community Health",
        description="CDC PLACES community estimates for counties, places, tracts, and ZCTAs.",
        server="community-health",
        tools=(
            "list_places_measures",
            "search_places",
            "get_places_profile",
            "compare_places",
            "get_market_community_profile",
        ),
        tags=("cdc", "places", "community-health", "zcta", "county"),
        source="CDC PLACES public Socrata datasets",
        access_notes="Values are community estimates; use community-health or live-gateway for live PLACES calls.",
    ),
    DatasetDocument(
        id="research-trials",
        title="NIH and ClinicalTrials Research Activity",
        description="NIH RePORTER funding and ClinicalTrials.gov study activity for organizations and markets.",
        server="research-trials",
        tools=(
            "search_nih_projects",
            "get_nih_project",
            "profile_research_funding",
            "search_clinical_trials",
            "get_clinical_trial",
            "inventory_clinical_trial_sponsors",
            "inventory_clinical_trial_sites",
            "profile_research_activity",
        ),
        tags=("nih", "reporter", "clinicaltrials", "research", "trials"),
        source="NIH RePORTER and ClinicalTrials.gov public APIs",
        access_notes="Sponsor/site inventories expose conservative matching contracts; raw sponsor/location search is not a deduped entity inventory.",
    ),
)

_DATASET_BY_ID = {dataset.id: dataset for dataset in DATASETS}
_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_port = int(os.environ.get("MCP_PORT", "8016"))
_host = os.environ.get("MCP_HOST", "127.0.0.1")
_security_config = load_gateway_security_config()

_mcp_kwargs: dict[str, Any] = {
    "name": "healthcare-data-gateway",
    "instructions": (
        "Search and fetch static metadata for healthcare-data-mcp public datasets. "
        "Use search before fetch. This gateway does not expose PHI or live deployment secrets."
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
register_standard_resources(mcp, "gateway")


@mcp.tool(structured_output=True)
@observe_tool("gateway")
async def search(query: str, max_results: int = 10) -> dict[str, Any]:
    """Search healthcare-data-mcp dataset metadata.

    Args:
        query: Natural-language search terms such as "CMS quality", "NPPES", or "price transparency".
        max_results: Maximum result count. Values outside 1-20 are clamped.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    normalized_query = query.strip()
    limit = max(1, min(max_results, 20))
    scored = sorted(
        ((dataset, _score_dataset(dataset, normalized_query)) for dataset in DATASETS),
        key=lambda item: (-item[1], item[0].title),
    )
    matches = [item for item in scored if item[1] > 0] if normalized_query else scored
    results = [_search_result(dataset, score) for dataset, score in matches[:limit]]
    return {"query": normalized_query, "count": len(results), "results": results}


@mcp.tool(structured_output=True)
@observe_tool("gateway")
async def fetch(id: str) -> dict[str, Any]:
    """Fetch one dataset metadata document by ID from search results.

    Args:
        id: Dataset ID returned by search, for example "cms-facility" or "price-transparency".

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"fetch","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    dataset_id = id.strip().lower()
    dataset = _DATASET_BY_ID.get(dataset_id)
    if dataset is None:
        suggestions = fuzzy_options(dataset_id, _DATASET_BY_ID)
        available_ids = sorted(_DATASET_BY_ID)
        message = f"No dataset metadata found for {id!r}. Call search first and use a returned id."
        response = not_found_response(
            f"No dataset metadata found for {id!r}.",
            fix_hint="Call search first and use a returned id.",
            available_options=available_ids,
            suggested_tool_calls=[
                {
                    "tool": "search",
                    "arguments": {"query": id, "max_results": 5},
                    "reason": "Find matching dataset metadata IDs before calling fetch.",
                }
            ],
            suggestions=suggestions,
        )
        response["error"]["code"] = "dataset_not_found"
        response["legacy_error"] = "dataset_not_found"
        response["message"] = message
        response["available_ids"] = available_ids
        return response
    return _fetch_result(dataset)


def _search_result(dataset: DatasetDocument, score: int) -> dict[str, Any]:
    spec = SERVER_BY_ID[dataset.server]
    return {
        "id": dataset.id,
        "title": dataset.title,
        "text": dataset.description,
        "url": f"healthcare-data-mcp://datasets/{dataset.id}",
        "metadata": {
            "server": dataset.server,
            "port": spec.port,
            "dataset_ids": list(spec.dataset_ids),
            "cache_needs": list(spec.cache_needs),
            "safety_notes": list(spec.safety_notes),
            "profiles": list(spec.profiles),
            "gateway_exposure": list(spec.gateway_exposure),
            "workflow_roles": list(spec.workflow_roles),
            "tags": list(dataset.tags),
            "score": score,
        },
    }


def _fetch_result(dataset: DatasetDocument) -> dict[str, Any]:
    spec = SERVER_BY_ID[dataset.server]
    return {
        "id": dataset.id,
        "title": dataset.title,
        "text": dataset.text,
        "url": f"healthcare-data-mcp://datasets/{dataset.id}",
        "metadata": {
            "server": dataset.server,
            "server_capability": _server_capability_metadata(spec),
            "tools": list(dataset.tools),
            "tags": list(dataset.tags),
            "source": dataset.source,
            "access_notes": dataset.access_notes,
        },
    }


def validate_gateway_dataset_contracts() -> dict[str, Any]:
    """Validate metadata gateway dataset documents against the server registry.

    This checker avoids importing server modules. It parses registered module
    source files so CI can catch gateway metadata drift without optional API keys
    or runtime side effects.
    """

    expected_ids = {spec.server_id for spec in SERVER_BY_ID.values() if "metadata" in spec.gateway_exposure}
    dataset_ids = {dataset.id for dataset in DATASETS}
    issues: list[dict[str, Any]] = []
    dataset_statuses: dict[str, dict[str, Any]] = {}
    module_functions: dict[str, set[str]] = {}

    for missing_id in sorted(expected_ids - dataset_ids):
        issues.append(
            {
                "dataset_id": missing_id,
                "status": "missing_metadata_document",
                "message": "Registry metadata-exposed server is missing a gateway dataset document.",
            }
        )
    for extra_id in sorted(dataset_ids - expected_ids):
        issues.append(
            {
                "dataset_id": extra_id,
                "status": "non_registry_metadata_document",
                "message": "Gateway dataset document is not backed by registry gateway_exposure='metadata'.",
            }
        )

    seen_ids: set[str] = set()
    for dataset in DATASETS:
        dataset_issues: list[dict[str, Any]] = []
        if dataset.id in seen_ids:
            dataset_issues.append(
                {
                    "dataset_id": dataset.id,
                    "status": "duplicate_dataset_id",
                    "message": "Gateway dataset ids must be unique.",
                }
            )
        seen_ids.add(dataset.id)

        spec = SERVER_BY_ID.get(dataset.server)
        if spec is None:
            dataset_issues.append(
                {
                    "dataset_id": dataset.id,
                    "status": "server_not_registered",
                    "message": f"{dataset.server} is not in the canonical server registry.",
                }
            )
        else:
            if dataset.id != spec.server_id:
                dataset_issues.append(
                    {
                        "dataset_id": dataset.id,
                        "status": "dataset_id_server_mismatch",
                        "message": f"Dataset id {dataset.id!r} must match owning server id {spec.server_id!r}.",
                    }
                )
            if "metadata" not in spec.gateway_exposure:
                dataset_issues.append(
                    {
                        "dataset_id": dataset.id,
                        "status": "server_not_metadata_exposed",
                        "message": f"{dataset.server} is not marked gateway_exposure='metadata'.",
                    }
                )

            functions = module_functions.get(spec.module)
            if functions is None:
                functions = _module_function_names(spec.module)
                module_functions[spec.module] = functions
            missing_tools = sorted(set(dataset.tools) - functions)
            if missing_tools:
                dataset_issues.append(
                    {
                        "dataset_id": dataset.id,
                        "status": "advertised_tool_not_found",
                        "message": "Gateway metadata advertises tool(s) not found in the registry module: "
                        + ", ".join(missing_tools),
                    }
                )

        if not dataset.tools:
            dataset_issues.append(
                {
                    "dataset_id": dataset.id,
                    "status": "missing_tools",
                    "message": "Gateway dataset documents must advertise at least one tool.",
                }
            )
        if not dataset.tags:
            dataset_issues.append(
                {
                    "dataset_id": dataset.id,
                    "status": "missing_tags",
                    "message": "Gateway dataset documents must include searchable tags.",
                }
            )

        dataset_statuses[dataset.id] = {
            "status": "ok" if not dataset_issues else "issues_found",
            "issue_count": len(dataset_issues),
            "server": dataset.server,
            "tool_count": len(dataset.tools),
        }
        issues.extend(dataset_issues)

    return {
        "status": "ok" if not issues else "issues_found",
        "dataset_count": len(DATASETS),
        "expected_dataset_count": len(expected_ids),
        "issue_count": len(issues),
        "issues": issues,
        "datasets": dataset_statuses,
        "method": "registry_gateway_dataset_ast",
    }


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


def _server_capability_metadata(spec: ServerCapability) -> dict[str, Any]:
    return {
        "server_id": spec.server_id,
        "module": spec.module,
        "port": spec.port,
        "description": spec.description,
        "required_env": [
            {"name": item.name, "description": item.description}
            for item in spec.required_env
        ],
        "optional_env": [
            {"name": item.name, "description": item.description}
            for item in spec.optional_env
        ],
        "cache_needs": list(spec.cache_needs),
        "dataset_ids": list(spec.dataset_ids),
        "zero_config": spec.zero_config,
        "gateway_exposure": list(spec.gateway_exposure),
        "profiles": list(spec.profiles),
        "workflow_roles": list(spec.workflow_roles),
        "safety_notes": list(spec.safety_notes),
    }


def _score_dataset(dataset: DatasetDocument, query: str) -> int:
    if not query:
        return 1

    haystack = " ".join(
        (
            dataset.id,
            dataset.title,
            dataset.description,
            dataset.server,
            " ".join(dataset.tools),
            " ".join(dataset.tags),
            dataset.source,
        )
    ).lower()
    terms = [term for term in _tokenize(query) if term]
    if not terms:
        return 1

    score = 0
    for term in terms:
        if term in dataset.id.lower():
            score += 8
        if term in dataset.title.lower():
            score += 5
        if term in dataset.tags:
            score += 4
        if term in haystack:
            score += 1
    return score


def _tokenize(value: str) -> list[str]:
    return [token.strip(".,:/()[]{}").lower() for token in value.split()]


if __name__ == "__main__":
    mcp.run(transport=_transport)
