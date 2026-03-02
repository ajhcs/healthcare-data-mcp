"""Health System Profiler MCP Server.

Returns complete health system profiles in 1-3 tool calls by combining
AHRQ Compendium, CMS Provider of Services, NPPES, and HSAF data.
"""

import json
import logging
import os as _os
import sys
from pathlib import Path

import pandas as pd
from mcp.server.fastmcp import FastMCP

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
    _mcp_kwargs["host"] = "0.0.0.0"
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

@mcp.tool()
async def search_health_systems(query: str, limit: int = 10) -> str:
    """Search for health systems by name using AHRQ Compendium.

    Performs fuzzy matching against ~700 US health system names.

    Args:
        query: System name to search for (e.g. "Jefferson Health", "LVHN", "Penn Medicine").
        limit: Maximum results to return (default 10).
    """
    systems_df = await _load_ahrq_systems()
    results = fuzzy_search_systems(query, systems_df, limit=limit)
    return json.dumps({"count": len(results), "results": results})


@mcp.tool()
async def get_system_profile(
    system_id: str | None = None,
    system_name: str | None = None,
    include_outpatient: bool = True,
) -> str:
    """Get a complete health system profile in one call.

    Combines AHRQ Compendium (system to hospitals), CMS POS (beds, services,
    staffing), NPPES (outpatient sites), and related provider graph expansion.

    Provide either system_id (from search_health_systems) or system_name
    (auto-resolved via fuzzy search, takes the top match).

    Args:
        system_id: AHRQ system ID (e.g. "SYS_001"). Preferred.
        system_name: System name for auto-resolution (e.g. "Jefferson Health").
        include_outpatient: Include NPPES outpatient site discovery (default True).
    """
    systems_df = await _load_ahrq_systems()
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    # Resolve system_id if only name provided
    if not system_id and system_name:
        matches = fuzzy_search_systems(system_name, systems_df, limit=1)
        if not matches:
            return json.dumps({"error": f"No health system found matching '{system_name}'"})
        system_id = matches[0]["system_id"]

    if not system_id:
        return json.dumps({"error": "Provide either system_id or system_name"})

    # Get system info
    sys_row = systems_df[systems_df["health_sys_id"] == system_id]
    if sys_row.empty:
        return json.dumps({"error": f"System ID '{system_id}' not found in AHRQ Compendium"})

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
    return json.dumps(profile.model_dump(), indent=2)


@mcp.tool()
async def get_system_facilities(
    system_id: str,
    facility_type: str = "all",
) -> str:
    """Get detailed facility data for a health system with full POS enrichment.

    Args:
        system_id: AHRQ system ID (from search_health_systems).
        facility_type: Filter: "inpatient", "outpatient", "rehab", "behavioral_health", "all" (default).
    """
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    ccns = resolve_system_ccns(system_id, hospitals_df)
    if not ccns:
        return json.dumps({"error": f"No hospitals found for system ID '{system_id}'"})

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

    return json.dumps(result, indent=2)


if __name__ == "__main__":
    mcp.run(transport=_transport)
