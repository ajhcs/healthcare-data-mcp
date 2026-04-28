"""Health System Profiler MCP Server.

Returns complete health system profiles in 1-3 tool calls by combining
AHRQ Compendium, CMS Provider of Services, NPPES, and HSAF data.
"""

from typing import Any
import logging
import os as _os
import sys
from pathlib import Path

import pandas as pd
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_response import error_response, to_structured

# Support running both as a package and as a standalone script
try:
    from .data_loaders import (
        load_ahrq_hospital_linkage,
        load_ahrq_systems,
        load_pos,
        search_nppes,
    )
    from .facility_enrichment import aggregate_off_site, enrich_facility
    from .graph_expansion import expand_related_providers
    from .jefferson_resolver import (
        JEFFERSON_SLUG,
        build_combined_system_profile,
        reconcile_system_facilities as reconcile_jefferson_facilities,
        resolve_combined_system_slug,
    )
    from .models import (
        BedBreakdown,
        FacilitySummary,
        HealthSystemSummary,
        SystemProfileResponse,
    )
    from .outpatient_discovery import build_search_patterns, parse_nppes_results
    from .system_discovery import fuzzy_search_systems, resolve_system_ccns
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from data_loaders import (
        load_ahrq_hospital_linkage,
        load_ahrq_systems,
        load_pos,
        search_nppes,
    )
    from facility_enrichment import aggregate_off_site, enrich_facility
    from graph_expansion import expand_related_providers
    from jefferson_resolver import (
        JEFFERSON_SLUG,
        build_combined_system_profile,
        reconcile_system_facilities as reconcile_jefferson_facilities,
        resolve_combined_system_slug,
    )
    from models import (
        BedBreakdown,
        FacilitySummary,
        HealthSystemSummary,
        SystemProfileResponse,
    )
    from outpatient_discovery import build_search_patterns, parse_nppes_results
    from system_discovery import fuzzy_search_systems, resolve_system_ccns

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "health-system-profiler"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8007"))
mcp = FastMCP(**_mcp_kwargs)


# ---- Internal loader wrappers (mockable for tests) ----

async def _load_ahrq_systems() -> pd.DataFrame:
    return await load_ahrq_systems()

async def _load_ahrq_hospitals() -> pd.DataFrame:
    return await load_ahrq_hospital_linkage()

async def _load_pos() -> pd.DataFrame:
    return await load_pos()

async def _search_nppes(**kwargs) -> list[dict]:
    return await search_nppes(**kwargs)


# ---- MCP Tools ----

@mcp.tool(structured_output=True)
async def search_health_systems(query: str, limit: int = 10) -> dict[str, Any]:
    """Search for health systems by name using AHRQ Compendium.

    Performs fuzzy matching against ~700 US health system names.

    Args:
        query: System name to search for (e.g. "Jefferson Health", "LVHN", "Penn Medicine").
        limit: Maximum results to return (default 10).
    """
    systems_df = await _load_ahrq_systems()
    results = fuzzy_search_systems(query, systems_df, limit=limit)
    return to_structured({"count": len(results), "results": results})


@mcp.tool(structured_output=True)
async def get_system_profile(
    system_id: str | None = None,
    system_name: str | None = None,
    edition_date: str | None = None,
    include_outpatient: bool = True,
) -> dict[str, Any]:
    """Get a complete health system profile in one call.

    Combines AHRQ Compendium (system to hospitals), CMS POS (beds, services,
    staffing), NPPES (outpatient sites), and related provider graph expansion.

    Provide either system_id (from search_health_systems) or system_name
    (auto-resolved via fuzzy search, takes the top match).

    Args:
        system_id: AHRQ system ID (e.g. "SYS_001"). Preferred.
        system_name: System name for auto-resolution (e.g. "Jefferson Health").
        edition_date: Profile edition/as-of date. Jefferson Health uses this to apply the
            post-2024 LVHN combined-system resolver.
        include_outpatient: Include NPPES outpatient site discovery (default True).
    """
    if not system_id and system_name and resolve_combined_system_slug(system_name=system_name) == JEFFERSON_SLUG:
        profile = build_combined_system_profile(system_name, edition_date=edition_date)
        if profile is not None:
            return to_structured(profile)

    systems_df = await _load_ahrq_systems()
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    # Resolve system_id if only name provided
    if not system_id and system_name:
        matches = fuzzy_search_systems(system_name, systems_df, limit=1)
        if not matches:
            return error_response(f"No health system found matching '{system_name}'")
        system_id = matches[0]["system_id"]

    if not system_id:
        return error_response("Provide either system_id or system_name")

    # Get system info
    sys_row = systems_df[systems_df["health_sys_id"] == system_id]
    if sys_row.empty:
        return error_response(f"System ID '{system_id}' not found in AHRQ Compendium")

    sys_info = sys_row.iloc[0]
    sys_name = str(sys_info.get("health_sys_name", ""))
    sys_city = str(sys_info.get("health_sys_city", ""))
    sys_state = str(sys_info.get("health_sys_state", ""))

    # Resolve CCNs
    ccns = resolve_system_ccns(system_id, hospitals_df)

    # Enrich each facility from POS
    facilities: list[FacilitySummary] = []
    total_beds = 0
    for ccn in ccns:
        facility = enrich_facility(ccn, pos_df)
        if facility:
            total_beds += facility.beds.total
            facilities.append(facility)
        else:
            # Fallback: use AHRQ data if POS has no match
            ahrq_row = hospitals_df[hospitals_df["ccn"] == ccn]
            if not ahrq_row.empty:
                r = ahrq_row.iloc[0]
                beds = int(r.get("hos_beds", 0) or 0)
                total_beds += beds
                facilities.append(FacilitySummary(
                    ccn=ccn,
                    name=str(r.get("hospital_name", "")),
                    city=str(r.get("hosp_city", "")),
                    state=str(r.get("hosp_state", "")),
                    zip_code=str(r.get("hosp_zip", "")),
                    beds=BedBreakdown(total=beds),
                ))

    # Graph expansion — find sub-entities
    sub_entities = expand_related_providers(ccns, pos_df)

    # Aggregate off-site counts
    off_site = aggregate_off_site(ccns, pos_df)

    # NPPES outpatient discovery
    outpatient_sites = []
    if include_outpatient and sys_state:
        patterns = build_search_patterns(sys_name, sys_state)
        for params in patterns:
            try:
                raw = await _search_nppes(**params)
                outpatient_sites.extend(parse_nppes_results(raw))
            except Exception as e:
                logger.warning("NPPES search failed for %s: %s", params, e)

        # Deduplicate by NPI
        seen_npis: set[str] = set()
        unique_sites = []
        for site in outpatient_sites:
            if site.npi not in seen_npis:
                seen_npis.add(site.npi)
                unique_sites.append(site)
        outpatient_sites = unique_sites

    # Compute total discharges from AHRQ linkage
    sys_hospitals = hospitals_df[hospitals_df["health_sys_id"] == system_id]
    total_dsch = int(sys_hospitals["hos_dsch"].sum()) if "hos_dsch" in sys_hospitals.columns else 0

    # Build response
    profile = SystemProfileResponse(
        system=HealthSystemSummary(
            system_id=system_id,
            name=sys_name,
            hq_city=sys_city,
            hq_state=sys_state,
            hospital_count=len(ccns),
            total_beds=total_beds,
            total_discharges=total_dsch,
            physician_group_count=int(sys_info.get("phys_grp_count", 0) or 0),
        ),
        inpatient_facilities=[f.model_dump() for f in facilities],
        sub_entities=[s.model_dump() for s in sub_entities],
        outpatient_sites=[o.model_dump() for o in outpatient_sites],
        off_site_summary=off_site.model_dump(),
    )
    return to_structured(profile.model_dump())


@mcp.tool(structured_output=True)
async def reconcile_system_facilities(
    system_slug: str,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    """Return a canonical facility ledger for deterministic system resolvers.

    Jefferson Health is reconciled as a combined post-merger system by merging the
    legacy Jefferson, Einstein, and LVHN rosters rather than relying on AHRQ 2023 alone.

    Args:
        system_slug: Deterministic system slug, currently "jefferson-health".
        as_of_date: Ledger as-of date. Jefferson/LVHN is valid on or after 2024-08-01.
    """
    result = reconcile_jefferson_facilities(system_slug, as_of_date=as_of_date)
    if "error" in result:
        return error_response(result["error"])
    return to_structured(result)


@mcp.tool(structured_output=True)
async def get_system_facilities(
    system_id: str,
    facility_type: str = "all",
) -> dict[str, Any]:
    """Get detailed facility data for a health system with full POS enrichment.

    Args:
        system_id: AHRQ system ID (from search_health_systems).
        facility_type: Filter: "inpatient", "outpatient", "rehab", "behavioral_health", "all" (default).
    """
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    ccns = resolve_system_ccns(system_id, hospitals_df)
    if not ccns:
        return error_response(f"No hospitals found for system ID '{system_id}'")

    facilities = []
    for ccn in ccns:
        facility = enrich_facility(ccn, pos_df)
        if facility:
            facilities.append(facility)

    # Include sub-entities if not filtered to inpatient-only
    sub_entities = []
    if facility_type in ("all", "rehab", "behavioral_health"):
        sub_entities = expand_related_providers(ccns, pos_df)

    result = {
        "system_id": system_id,
        "facility_count": len(facilities) + len(sub_entities),
        "inpatient_facilities": [f.model_dump() for f in facilities],
    }
    if sub_entities:
        result["sub_entities"] = [s.model_dump() for s in sub_entities]

    return to_structured(result)


if __name__ == "__main__":
    mcp.run(transport=_transport)
