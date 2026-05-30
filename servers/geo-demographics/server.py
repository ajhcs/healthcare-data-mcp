"""Geographic Demographics MCP Server.

Provides Census ACS demographics, ZCTA geography/adjacency,
Medicare enrollment data, and ZIP-to-geography crosswalks.
"""

from typing import Any
import logging
import os

import httpx

from shared.utils.http_client import resilient_request
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import collection_response, error_response, evidence_receipt, to_structured

from . import data_loaders as gv_loaders
from . import geography as geo_shapes
from .census_client import CENSUS_BASE, get_demographics_batch, get_demographics_for_zcta
from .geography import get_adjacent_zctas
from .models import (
    CrosswalkResponse,
    CrosswalkResult,
    GeographicVariation,
    MedicareEnrollment,
    ZctaAdjacency,
    ZctaDemographics,
)

logger = logging.getLogger(__name__)

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "geo-demographics"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8003"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "geo-demographics")

# HUD USPS Crosswalk API
HUD_CROSSWALK_BASE = "https://www.huduser.gov/hudapi/public/usps"
HUD_CROSSWALK_TYPES = {"tract": 1, "county": 2, "cbsa": 3, "cd": 4}


def _census_source_metadata(year: int) -> dict[str, Any]:
    return {
        "source_name": "U.S. Census Bureau ACS 5-Year API",
        "source_url": f"{CENSUS_BASE}/{year}/acs/acs5",
        "dataset_id": "census_acs5_zcta_demographics",
        "source_period": str(year),
        "landing_page": "https://www.census.gov/data/developers/data-sets/acs-5year.html",
        "cache_status": "live_api",
        "cache_freshness": "queried live via Census API",
        "source_caveat": "ACS estimates are geography-level survey estimates with margins and sampling limits; they are not patient-level facts.",
    }


def _gv_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "CMS Medicare Geographic Variation Public Use File",
        "source_url": gv_loaders.GV_CSV_URL,
        "dataset_id": "cms_medicare_geographic_variation_puf",
        "source_period": "2014-2023",
        "landing_page": "https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-geographic-comparisons/medicare-geographic-variation-by-national-state-county",
        "cache_status": "ready" if gv_loaders._GV_PARQUET.exists() else "download_on_demand",
        "cache_key": str(gv_loaders._GV_PARQUET),
        "source_caveat": "CMS Geographic Variation PUF is aggregate Medicare FFS geography data, not all-payer utilization or facility performance.",
    }


def _tiger_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "U.S. Census TIGER/Line ZCTA Shapefile",
        "source_url": geo_shapes.TIGER_ZCTA_URL,
        "dataset_id": "census_tiger_zcta_adjacency",
        "source_period": "2024",
        "landing_page": "https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html",
        "cache_status": "ready" if geo_shapes.ADJACENCY_CACHE.exists() else "download_on_demand",
        "cache_key": str(geo_shapes.ADJACENCY_CACHE),
        "source_caveat": "ZCTA adjacency is geography topology context; adjacency does not imply market membership, patient flow, or access.",
    }


def _hud_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "HUD USPS ZIP Crosswalk API",
        "source_url": HUD_CROSSWALK_BASE,
        "dataset_id": "hud_usps_zip_crosswalk",
        "source_period": "live API query",
        "landing_page": "https://www.huduser.gov/portal/dataset/uspszip-api.html",
        "cache_status": "live_api",
        "cache_freshness": "queried live via HUD API",
        "source_caveat": "HUD ZIP crosswalk ratios allocate ZIPs to geographies and are not exact patient, facility, or market facts.",
    }


def _geo_evidence(
    source_metadata: dict[str, Any],
    *,
    dataset_id: str = "",
    entity_scope: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_metadata=source_metadata,
        dataset_id=dataset_id,
        entity_scope=entity_scope,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _geo_row_evidence(
    source_metadata: dict[str, Any],
    *,
    entity_scope: str,
    parent_query: dict[str, Any],
    row_query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
) -> dict[str, Any]:
    query = {
        **{key: value for key, value in parent_query.items() if value not in ("", None, [])},
        **{key: value for key, value in row_query.items() if value not in ("", None, [])},
    }
    return _geo_evidence(
        source_metadata,
        entity_scope=entity_scope,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _geography_identity(
    *,
    code: str,
    geography_type: str,
    name: str = "",
    source_name: str,
    source_url: str,
) -> dict[str, Any]:
    clean_code = str(code or "").strip()
    label = name or f"{geography_type.upper()} {clean_code}"
    identity = identity_from_public_record(
        name=label,
        entity_type=f"{geography_type}_geography",
        source_name=source_name,
        source_url=source_url,
    ).to_dict()
    if geography_type in {"zcta", "zip"}:
        identity["zip_code"] = clean_code.zfill(5)
    identity["unresolved_identifiers"].append({"type": geography_type, "value": clean_code})
    return identity


def _geography_identity_map(entities: list[dict[str, Any]], *, match_basis: str) -> dict[str, Any]:
    return {
        "entities": entities,
        "match_basis": match_basis,
        "conflict_policy": "Join geography records by exact geography type and code; names and allocation ratios are context only.",
    }


@mcp.tool(structured_output=True)
@observe_tool("geo-demographics")
async def get_zcta_demographics(zcta: str, year: int = 2023) -> dict[str, Any]:
    """Get Census ACS demographics for a single ZCTA (ZIP Code Tabulation Area).

    Returns population, age distribution, median income, and health insurance coverage.

    Args:
        zcta: 5-digit ZCTA code (e.g., "60614")
        year: ACS 5-Year data year (default 2023)

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_zcta_demographics","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    zcta = zcta.strip().zfill(5)
    try:
        data = await get_demographics_for_zcta(zcta, year)
        result = ZctaDemographics(**data)
        payload = to_structured(result.model_dump())
        source_metadata = _census_source_metadata(year)
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _geo_evidence(
            source_metadata,
            entity_scope="zcta_demographics",
            query={"zcta": zcta, "year": year},
            match_basis="zcta_exact_acs5_api_row",
            confidence="source_backed_geography_estimate",
            caveat=source_metadata["source_caveat"],
            next_step="Preserve ACS year and ZCTA code when using demographics in market or community-health scans.",
        )
        payload["identity"] = _geography_identity(
            code=zcta,
            geography_type="zcta",
            source_name=source_metadata["source_name"],
            source_url=source_metadata["source_url"],
        )
        return payload
    except httpx.HTTPStatusError as e:
        return error_response(f"Census API error: {e.response.status_code}", detail=str(e))
    except Exception as e:
        return error_response(str(e))


@mcp.tool(structured_output=True)
@observe_tool("geo-demographics")
async def get_zcta_demographics_batch(zctas: list[str], year: int = 2023) -> dict[str, Any] | list[dict[str, Any]]:
    """Get Census ACS demographics for multiple ZCTAs in a single efficient query.

    Args:
        zctas: List of 5-digit ZCTA codes (e.g., ["60614", "60657", "60613"])
        year: ACS 5-Year data year (default 2023)

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_zcta_demographics_batch","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    zctas = [z.strip().zfill(5) for z in zctas]
    try:
        data_list = await get_demographics_batch(zctas, year)
        results = [ZctaDemographics(**d) for d in data_list]
        source_metadata = _census_source_metadata(year)
        rows = [r.model_dump() for r in results]
        for row in rows:
            row["evidence"] = _geo_row_evidence(
                source_metadata,
                entity_scope="zcta_demographics",
                parent_query={"zctas": zctas, "year": year},
                row_query={"zcta": row.get("zcta"), "year": row.get("year")},
                match_basis="zcta_exact_acs5_batch_row",
                confidence="source_backed_geography_estimate",
                caveat=source_metadata["source_caveat"],
                next_step="Preserve this row receipt with the exact ZCTA and ACS year before citing demographics.",
            )
        return collection_response(
            rows,
            limit=len(zctas),
            meta={"source": source_metadata},
            source_metadata=source_metadata,
            evidence=_geo_evidence(
                source_metadata,
                entity_scope="zcta_demographics_batch",
                query={"zctas": zctas, "year": year},
                match_basis="zcta_exact_batch_acs5_api_rows",
                confidence="source_backed_geography_estimates",
                caveat=source_metadata["source_caveat"],
                next_step="Treat missing ZCTAs as unresolved and preserve ACS year with each fact row.",
            ),
            identity_map=_geography_identity_map(
                [
                    _geography_identity(
                        code=row["zcta"],
                        geography_type="zcta",
                        source_name=source_metadata["source_name"],
                        source_url=source_metadata["source_url"],
                    )
                    for row in rows
                ],
                match_basis="acs_zcta_exact_batch",
            ),
        )
    except httpx.HTTPStatusError as e:
        return error_response(f"Census API error: {e.response.status_code}", detail=str(e))
    except Exception as e:
        return error_response(str(e))


@mcp.tool(structured_output=True)
@observe_tool("geo-demographics")
async def get_zcta_adjacency(zcta: str) -> dict[str, Any]:
    """Find all ZCTAs geographically adjacent to the given ZCTA.

    Uses TIGER/Line ZCTA shapefiles. The adjacency graph is computed once
    and cached for subsequent calls.

    NOTE: First call requires downloading the ZCTA shapefile (~800MB) and
    computing adjacency, which may take several minutes.

    Args:
        zcta: 5-digit ZCTA code (e.g., "60614")

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_zcta_adjacency","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    zcta = zcta.strip().zfill(5)
    try:
        neighbors = await get_adjacent_zctas(zcta)
        result = ZctaAdjacency(zcta=zcta, adjacent_zctas=neighbors, count=len(neighbors))
        payload = to_structured(result.model_dump())
        source_metadata = _tiger_source_metadata()
        payload["adjacent_zcta_rows"] = [
            {
                "zcta": zcta,
                "adjacent_zcta": neighbor,
                "evidence": _geo_row_evidence(
                    source_metadata,
                    entity_scope="zcta_adjacency_row",
                    parent_query={"zcta": zcta},
                    row_query={"zcta": zcta, "adjacent_zcta": neighbor},
                    match_basis="tiger_zcta_adjacency_neighbor_row",
                    confidence="source_backed_geography_topology_row",
                    caveat=source_metadata["source_caveat"],
                    next_step="Use this only as topology context; confirm market membership, access, and service-area facts separately.",
                ),
            }
            for neighbor in neighbors
        ]
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _geo_evidence(
            source_metadata,
            entity_scope="zcta_adjacency",
            query={"zcta": zcta},
            match_basis="zcta_exact_tiger_adjacency_cache",
            confidence="source_backed_geography_topology",
            caveat=source_metadata["source_caveat"],
            next_step="Use adjacency only as geography context and review market/service-area facts separately.",
        )
        payload["identity"] = _geography_identity(
            code=zcta,
            geography_type="zcta",
            source_name=source_metadata["source_name"],
            source_url=source_metadata["source_url"],
        )
        payload["identity_map"] = _geography_identity_map(
            [
                _geography_identity(
                    code=neighbor,
                    geography_type="zcta",
                    source_name=source_metadata["source_name"],
                    source_url=source_metadata["source_url"],
                )
                for neighbor in neighbors
            ],
            match_basis="tiger_adjacent_zcta_exact_codes",
        )
        return payload
    except FileNotFoundError as e:
        return error_response(f"Shapefile not available: {e}")
    except Exception as e:
        return error_response(str(e))


@mcp.tool(structured_output=True)
@observe_tool("geo-demographics")
async def get_medicare_enrollment(state: str | None = None, county_fips: str | None = None) -> dict[str, Any]:
    """Get Medicare enrollment and spending data from the CMS Geographic Variation PUF.

    Provide either a state abbreviation or county FIPS code.

    Args:
        state: Two-letter state abbreviation (e.g., "IL"). Returns state-level data.
        county_fips: 5-digit county FIPS code (e.g., "17031" for Cook County, IL). Returns county-level data.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_medicare_enrollment","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    if not state and not county_fips:
        return error_response("Provide either 'state' or 'county_fips'")

    try:
        await gv_loaders.ensure_gv_cached()

        if county_fips:
            data = gv_loaders.query_gv("County", county_fips)
            geo_type, geo_code = "county", county_fips
        else:
            data = gv_loaders.query_gv("State", state.upper())
            geo_type, geo_code = "state", state.upper()

        if not data:
            return error_response(f"No Medicare data found for {geo_type} {geo_code}")

        result = MedicareEnrollment(
            geography_type=geo_type,
            geography_code=geo_code,
            geography_name=data.get("geo_desc", ""),
            total_beneficiaries=data.get("total_beneficiaries"),
            ma_penetration_pct=data.get("ma_penetration_pct"),
            avg_age=data.get("avg_age"),
            pct_female=data.get("pct_female"),
            pct_dual_eligible=data.get("pct_dual_eligible"),
            pct_a_b_coverage=None,
            per_capita_spending=data.get("per_capita_spending"),
        )
        payload = to_structured(result.model_dump())
        source_metadata = _gv_source_metadata()
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _geo_evidence(
            source_metadata,
            entity_scope="medicare_geography_enrollment",
            query={"state": state, "county_fips": county_fips},
            match_basis="geography_code_exact_cms_gv_latest_year",
            confidence="source_backed_medicare_ffs_geography_aggregate",
            caveat=source_metadata["source_caveat"],
            next_step="Use geography_code and source period when combining Medicare enrollment with community or service-area facts.",
        )
        payload["identity"] = _geography_identity(
            code=geo_code,
            geography_type=geo_type,
            name=payload.get("geography_name", ""),
            source_name=source_metadata["source_name"],
            source_url=source_metadata["source_url"],
        )
        return payload

    except Exception as e:
        return error_response(str(e))


@mcp.tool(structured_output=True)
@observe_tool("geo-demographics")
async def get_geographic_variation(geography_type: str = "county", geography_code: str | None = None) -> dict[str, Any]:
    """Get CMS Geographic Variation PUF data including spending and utilization.

    Returns demographics, per-capita spending breakdown (IP, OP, physician, SNF),
    utilization rates (discharges, ER visits per 1000), and readmission rate.

    Args:
        geography_type: "county" (FIPS code) or "state" (abbreviation)
        geography_code: County FIPS (e.g., "17031") or state abbreviation (e.g., "IL")

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_geographic_variation","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    if not geography_code:
        return error_response("geography_code is required")

    try:
        await gv_loaders.ensure_gv_cached()

        geo_level = "County" if geography_type == "county" else "State"
        code = geography_code.upper() if geography_type != "county" else geography_code
        data = gv_loaders.query_gv(geo_level, code)

        if not data:
            return error_response(f"No data found for {geography_type} {geography_code}")

        result = GeographicVariation(
            geography_type=geography_type,
            geography_code=geography_code,
            geography_name=data.get("geo_desc", ""),
            total_beneficiaries=data.get("total_beneficiaries"),
            avg_age=data.get("avg_age"),
            pct_female=data.get("pct_female"),
            pct_dual_eligible=data.get("pct_dual_eligible"),
            per_capita_spending=data.get("per_capita_spending"),
            ip_spending_per_capita=data.get("ip_spending_per_capita"),
            op_spending_per_capita=data.get("op_spending_per_capita"),
            physician_spending_per_capita=data.get("physician_spending_per_capita"),
            snf_spending_per_capita=data.get("snf_spending_per_capita"),
            discharges_per_1000=data.get("discharges_per_1000"),
            er_visits_per_1000=data.get("er_visits_per_1000"),
            readmission_rate=data.get("readmission_rate"),
        )
        payload = to_structured(result.model_dump())
        source_metadata = _gv_source_metadata()
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _geo_evidence(
            source_metadata,
            entity_scope="medicare_geographic_variation",
            query={"geography_type": geography_type, "geography_code": geography_code},
            match_basis="geography_code_exact_cms_gv_latest_year",
            confidence="source_backed_medicare_ffs_geography_aggregate",
            caveat=source_metadata["source_caveat"],
            next_step="Do not cite Medicare FFS geographic utilization as facility-specific performance or all-payer demand.",
        )
        payload["identity"] = _geography_identity(
            code=geography_code,
            geography_type=geography_type,
            name=payload.get("geography_name", ""),
            source_name=source_metadata["source_name"],
            source_url=source_metadata["source_url"],
        )
        return payload

    except Exception as e:
        return error_response(str(e))


@mcp.tool(structured_output=True)
@observe_tool("geo-demographics")
async def crosswalk_zip(zip_code: str, target: str = "county") -> dict[str, Any]:
    """Crosswalk a ZIP code to another geography using the HUD USPS Crosswalk API.

    Maps ZIP codes to Census tracts, counties, or CBSAs with allocation ratios.

    Requires HUD_API_TOKEN environment variable.

    Args:
        zip_code: 5-digit ZIP code (e.g., "60614")
        target: Target geography — "tract", "county", or "cbsa" (default "county")

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"crosswalk_zip","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    hud_token = os.environ.get("HUD_API_TOKEN")
    if not hud_token:
        return error_response("HUD_API_TOKEN environment variable not set")

    target_lower = target.lower()
    type_code = HUD_CROSSWALK_TYPES.get(target_lower)
    if type_code is None:
        return error_response(f"Invalid target '{target}'. Must be one of: {', '.join(HUD_CROSSWALK_TYPES.keys())}")

    zip_code = zip_code.strip().zfill(5)

    try:
        url = HUD_CROSSWALK_BASE
        params = {"type": type_code, "query": zip_code}
        headers = {"Authorization": f"Bearer {hud_token}"}

        resp = await resilient_request("GET", url, params=params, headers=headers, timeout=30.0)
        data = resp.json()

        results_data = data.get("data", {}).get("results", data) if isinstance(data, dict) else data

        # HUD API returns a list of crosswalk records
        if isinstance(results_data, dict):
            results_data = results_data.get("results", [results_data])

        crosswalk_results = []
        for item in results_data if isinstance(results_data, list) else [results_data]:
            # HUD response field names vary by target type
            if target_lower == "county":
                target_code = item.get("county", item.get("geoid", ""))
            elif target_lower == "tract":
                target_code = item.get("tract", item.get("geoid", ""))
            elif target_lower == "cbsa":
                target_code = item.get("cbsa", item.get("geoid", ""))
            else:
                target_code = item.get("geoid", "")

            crosswalk_results.append(CrosswalkResult(
                zip_code=zip_code,
                target_type=target_lower,
                target_code=str(target_code),
                residential_ratio=_to_float(item.get("res_ratio")),
                business_ratio=_to_float(item.get("bus_ratio")),
                other_ratio=_to_float(item.get("oth_ratio")),
                total_ratio=_to_float(item.get("tot_ratio")),
            ))

        response = CrosswalkResponse(
            zip_code=zip_code,
            target_type=target_lower,
            results=crosswalk_results,
        )
        payload = to_structured(response.model_dump())
        source_metadata = _hud_source_metadata()
        for row in payload.get("results", []):
            if not isinstance(row, dict):
                continue
            row["evidence"] = _geo_row_evidence(
                source_metadata,
                entity_scope="zip_geography_crosswalk",
                parent_query={"zip_code": zip_code, "target": target_lower},
                row_query={
                    "target_type": row.get("target_type"),
                    "target_code": row.get("target_code"),
                    "residential_ratio": row.get("residential_ratio"),
                    "business_ratio": row.get("business_ratio"),
                    "other_ratio": row.get("other_ratio"),
                    "total_ratio": row.get("total_ratio"),
                },
                match_basis="hud_zip_crosswalk_allocation_row",
                confidence="source_backed_crosswalk_allocation",
                caveat=source_metadata["source_caveat"],
                next_step="Use this row's allocation ratios when rolling ZIP facts into target geographies; do not treat it as an exact market fact.",
            )
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _geo_evidence(
            source_metadata,
            entity_scope="zip_geography_crosswalk",
            query={"zip_code": zip_code, "target": target_lower},
            match_basis="zip_exact_hud_usps_crosswalk",
            confidence="source_backed_crosswalk_allocations",
            caveat=source_metadata["source_caveat"],
            next_step="Use allocation ratios when rolling ZIP facts into counties, tracts, CBSAs, or congressional districts.",
        )
        payload["identity"] = _geography_identity(
            code=zip_code,
            geography_type="zip",
            source_name=source_metadata["source_name"],
            source_url=source_metadata["source_url"],
        )
        payload["identity_map"] = _geography_identity_map(
            [
                _geography_identity(
                    code=row["target_code"],
                    geography_type=target_lower,
                    source_name=source_metadata["source_name"],
                    source_url=source_metadata["source_url"],
                )
                for row in payload.get("results", [])
                if row.get("target_code")
            ],
            match_basis="hud_zip_crosswalk_target_codes",
        )
        return payload

    except httpx.HTTPStatusError as e:
        return error_response(f"HUD API error: {e.response.status_code}", detail=str(e))
    except Exception as e:
        return error_response(str(e))


# --- Utility functions ---

def _to_int(value) -> int | None:
    """Safely convert a value to int."""
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _to_float(value) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    mcp.run(transport=_transport)
