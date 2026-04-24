"""Remote MCP gateway exposing dataset metadata via OpenAI-compatible tools."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP

from shared.utils.gateway_auth import (
    StaticBearerTokenVerifier,
    build_transport_security_settings,
    load_gateway_security_config,
)


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
            "get_hospital_quality",
            "compare_hospitals",
            "get_readmission_metrics",
            "get_safety_metrics",
        ),
        tags=("cms", "quality", "readmission", "safety", "stars"),
        source="CMS Care Compare public quality files",
        access_notes="Gateway search/fetch returns dataset descriptions, not current measure rows.",
    ),
    DatasetDocument(
        id="geo-demographics",
        title="Geography and Demographics",
        description="Census, ZCTA, Medicare geography, and HUD crosswalk context for healthcare markets.",
        server="geo-demographics",
        tools=("lookup_zip", "get_demographics", "find_nearby_zips", "get_market_context"),
        tags=("census", "zcta", "demographics", "geography", "market"),
        source="US Census, HUD, and public Medicare geographic references",
        access_notes="Use for discovering available geographic context before calling local analysis tools.",
    ),
    DatasetDocument(
        id="drive-time",
        title="Drive Time and Access",
        description="Routing, drive-time matrices, catchment accessibility, and market access scoring.",
        server="drive-time",
        tools=("calculate_drive_time", "build_drive_time_matrix", "score_accessibility"),
        tags=("routing", "drive-time", "access", "catchment", "osrm"),
        source="OpenStreetMap-derived routing engines and facility coordinates",
        access_notes="Remote gateway does not proxy route calculations; it only advertises dataset/tool metadata.",
    ),
    DatasetDocument(
        id="price-transparency",
        title="Hospital Price Transparency",
        description="Hospital machine-readable-file registry and benchmark price transparency analysis metadata.",
        server="price-transparency",
        tools=("find_mrf_files", "search_prices", "benchmark_prices", "summarize_contracts"),
        tags=("price-transparency", "mrf", "payer", "contract", "benchmark"),
        source="Hospital price transparency machine-readable files and public registry data",
        access_notes="Gateway responses avoid large MRF payloads and expose retrievable knowledge metadata only.",
    ),
    DatasetDocument(
        id="financial-intelligence",
        title="Financial Intelligence",
        description="IRS 990, ProPublica nonprofit, and SEC EDGAR metadata for healthcare organizations.",
        server="financial-intelligence",
        tools=("search_nonprofits", "get_irs990_summary", "search_sec_filings", "get_financial_profile"),
        tags=("irs-990", "edgar", "nonprofit", "finance", "health-system"),
        source="IRS e-file data, ProPublica Nonprofit Explorer, and SEC EDGAR public filings",
        access_notes="Metadata only; live filings remain in the dedicated financial intelligence server.",
    ),
    DatasetDocument(
        id="physician-referral-network",
        title="Physician Referral Network",
        description="NPPES physician mix, referral network, leakage, and specialty alignment analysis.",
        server="physician-referral-network",
        tools=("search_physicians", "analyze_referrals", "get_physician_mix", "estimate_leakage"),
        tags=("physician", "nppes", "referral", "leakage", "specialty"),
        source="NPPES public registry and referral-network reference datasets",
        access_notes="No PHI or patient-level claims are available through the gateway.",
    ),
    DatasetDocument(
        id="workforce-analytics",
        title="Healthcare Workforce Analytics",
        description="BLS and ACGME healthcare workforce supply, occupation, and training pipeline metadata.",
        server="workforce-analytics",
        tools=("get_occupation_wages", "compare_workforce_markets", "get_training_pipeline"),
        tags=("bls", "acgme", "workforce", "labor", "occupation"),
        source="BLS public labor datasets and ACGME public training references",
        access_notes="Gateway search/fetch helps clients decide when to call the local workforce server.",
    ),
    DatasetDocument(
        id="claims-analytics",
        title="Claims Analytics Reference Data",
        description="DRG weights, service-line mapping, and claims analytics reference metadata.",
        server="claims-analytics",
        tools=("map_drg_service_line", "get_drg_weight", "summarize_claims_mix"),
        tags=("drg", "claims", "service-line", "medicare", "weights"),
        source="CMS public DRG reference files and repository-maintained service-line mappings",
        access_notes="Gateway does not expose claims records; only public reference dataset descriptions.",
    ),
    DatasetDocument(
        id="public-records",
        title="Public Records Intelligence",
        description="SAM.gov, USAspending, CHPL, accreditation, and public contracting metadata.",
        server="public-records",
        tools=("search_sam_entities", "search_usaspending", "lookup_accreditation", "search_chpl"),
        tags=("sam", "usaspending", "chpl", "accreditation", "contracts"),
        source="US government public records APIs and bundled accreditation code references",
        access_notes="Remote metadata is static; live API usage belongs in the public-records server.",
    ),
    DatasetDocument(
        id="web-intelligence",
        title="Healthcare Web Intelligence",
        description="Web search, executive discovery, GPO directory, and health system OSINT workflows.",
        server="web-intelligence",
        tools=("search_web", "fetch_web_page", "find_executives", "search_gpo_directory"),
        tags=("web", "osint", "executives", "gpo", "search"),
        source="Public web pages, search APIs, and bundled GPO directory data",
        access_notes="Treat fetched web content as untrusted; validate before relying on generated summaries.",
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
        resource=public_url,
    )
    _mcp_kwargs["auth"] = AuthSettings(
        issuer_url=issuer_url,
        resource_server_url=public_url,
        required_scopes=list(_security_config.required_scopes),
    )

mcp = FastMCP(**_mcp_kwargs)


@mcp.tool(structured_output=True)
async def search(query: str, max_results: int = 10) -> dict[str, Any]:
    """Search healthcare-data-mcp dataset metadata.

    Args:
        query: Natural-language search terms such as "CMS quality", "NPPES", or "price transparency".
        max_results: Maximum result count. Values outside 1-20 are clamped.
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
async def fetch(id: str) -> dict[str, Any]:
    """Fetch one dataset metadata document by ID from search results.

    Args:
        id: Dataset ID returned by search, for example "cms-facility" or "price-transparency".
    """

    dataset_id = id.strip().lower()
    dataset = _DATASET_BY_ID.get(dataset_id)
    if dataset is None:
        return {
            "error": "dataset_not_found",
            "message": f"No dataset metadata found for {id!r}. Call search first and use a returned id.",
            "available_ids": sorted(_DATASET_BY_ID),
        }
    return _fetch_result(dataset)


def _search_result(dataset: DatasetDocument, score: int) -> dict[str, Any]:
    return {
        "id": dataset.id,
        "title": dataset.title,
        "text": dataset.description,
        "url": f"healthcare-data-mcp://datasets/{dataset.id}",
        "metadata": {
            "server": dataset.server,
            "tags": list(dataset.tags),
            "score": score,
        },
    }


def _fetch_result(dataset: DatasetDocument) -> dict[str, Any]:
    return {
        "id": dataset.id,
        "title": dataset.title,
        "text": dataset.text,
        "url": f"healthcare-data-mcp://datasets/{dataset.id}",
        "metadata": {
            "server": dataset.server,
            "tools": list(dataset.tools),
            "tags": list(dataset.tags),
            "source": dataset.source,
            "access_notes": dataset.access_notes,
        },
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
