"""Physician & Referral Network MCP Server.

Provides tools for physician search, profiles with Medicare utilization,
referral network mapping, health system employment mix analysis,
and referral leakage detection.
"""

import asyncio
import json
import logging
import os as _os

from mcp.server.fastmcp import FastMCP

from . import nppes_client, referral_network, physician_mix
from .models import (
    LeakageDestination,
    LeakageResponse,
    PhysicianClassification,
    PhysicianMixResponse,
    PhysicianProfile,
    PhysicianSearchResponse,
    PhysicianSummary,
    QualityInfo,
    ReferralEdge,
    ReferralNetworkResponse,
    ReferralNode,
    SpecialtyLeakage,
    UtilizationSummary,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "physician-referral-network"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8010"))
mcp = FastMCP(**_mcp_kwargs)


async def _build_system_service_area_zips(facility_zips: set[str]) -> set[str]:
    """Expand system facility ZIPs into the union of their Dartmouth HSAs."""
    if not facility_zips:
        return set()

    service_area_zips = set(facility_zips)
    if not await referral_network.ensure_hsa_crosswalk_cached():
        return service_area_zips

    expanded_zips: set[str] = set()
    for zip_code in facility_zips:
        hsa_number = referral_network.get_hsa_for_zip(zip_code)
        if not hsa_number:
            continue
        expanded_zips.update(referral_network.get_zips_for_hsa(hsa_number))

    return expanded_zips or service_area_zips


async def _lookup_destination_details(npis: set[str]) -> dict[str, dict]:
    """Fetch NPPES destination metadata keyed by NPI."""
    if not npis:
        return {}

    semaphore = asyncio.Semaphore(10)

    async def _lookup(npi: str) -> tuple[str, dict]:
        async with semaphore:
            try:
                physicians = await nppes_client.search_physicians(npi, limit=1)
            except Exception:
                physicians = []
            return npi, physicians[0] if physicians else {}

    pairs = await asyncio.gather(*(_lookup(npi) for npi in sorted(npis)))
    return {npi: details for npi, details in pairs if details}


# ---------------------------------------------------------------------------
# Tool 1: search_physicians
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_physicians(
    query: str, specialty: str = "", state: str = "", limit: int = 25
) -> str:
    """Search for physicians by name, NPI, or specialty in the NPPES registry.

    Returns matching physicians with NPI, specialty, practice location,
    and organization affiliation.

    Args:
        query: Physician name (e.g. "John Smith"), NPI number, or last name.
        specialty: Specialty filter (e.g. "Cardiology", "Orthopedic Surgery").
        state: Two-letter state code filter (e.g. "PA").
        limit: Maximum results (1-200).
    """
    try:
        physicians = await nppes_client.search_physicians(query, specialty, state, limit)

        response = PhysicianSearchResponse(
            total_results=len(physicians),
            physicians=[PhysicianSummary(**p) for p in physicians],
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("search_physicians failed")
        return json.dumps({"error": f"search_physicians failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_physician_profile
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_physician_profile(npi: str) -> str:
    """Get a full physician profile including specialties, affiliations,
    Medicare utilization, and quality scores.

    Combines NPPES registry data with CMS Physician Compare and
    Medicare Provider Utilization datasets.

    Args:
        npi: 10-digit National Provider Identifier.
    """
    try:
        # Ensure bulk datasets are cached
        await nppes_client.ensure_physician_compare_cached()
        await nppes_client.ensure_utilization_cached()

        profile_data = await nppes_client.get_physician_detail(npi)
        if not profile_data:
            return json.dumps({"error": f"No physician found for NPI: {npi}"})

        # Build response model
        utilization = None
        if profile_data.get("utilization"):
            utilization = UtilizationSummary(**profile_data["utilization"])

        quality = None
        if profile_data.get("quality"):
            quality = QualityInfo(**profile_data["quality"])

        response = PhysicianProfile(
            npi=profile_data["npi"],
            first_name=profile_data.get("first_name", ""),
            last_name=profile_data.get("last_name", ""),
            credential=profile_data.get("credential", ""),
            specialties=profile_data.get("specialties", []),
            practice_locations=profile_data.get("practice_locations", []),
            org_affiliations=profile_data.get("org_affiliations", []),
            gender=profile_data.get("gender", ""),
            enumeration_date=profile_data.get("enumeration_date", ""),
            utilization=utilization,
            quality=quality,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_physician_profile failed")
        return json.dumps({"error": f"get_physician_profile failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: map_referral_network
# ---------------------------------------------------------------------------
@mcp.tool()
async def map_referral_network(
    npi: str, depth: int = 1, min_shared: int = 11
) -> str:
    """Build a referral network graph centered on a physician using
    DocGraph shared patient data (2014-2020 Medicare claims).

    Returns nodes (physicians) and edges (shared patient counts) for
    graph visualization and network analysis.

    Args:
        npi: Center physician NPI.
        depth: Network depth (1=direct connections, 2=include second-hop).
        min_shared: Minimum shared patient count to include an edge (default 11).
    """
    try:
        if not referral_network.is_docgraph_cached():
            return json.dumps({
                "error": "DocGraph shared patient data not cached. "
                         "Download from https://careset.com/datasets/ and load with "
                         "the load_docgraph_csv() function."
            })

        result = referral_network.get_referral_network(npi, depth=depth, min_shared=min_shared)

        if "error" in result:
            return json.dumps(result)

        # Enrich nodes with NPPES data (batch lookup)
        enriched_nodes = []
        for node in result.get("nodes", []):
            try:
                physicians = await nppes_client.search_physicians(node["npi"], limit=1)
                if physicians:
                    p = physicians[0]
                    enriched_nodes.append(ReferralNode(
                        npi=node["npi"],
                        name=f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                        specialty=p.get("specialty", ""),
                        city=p.get("city", ""),
                        state=p.get("state", ""),
                    ))
                else:
                    enriched_nodes.append(ReferralNode(npi=node["npi"]))
            except Exception:
                enriched_nodes.append(ReferralNode(npi=node["npi"]))

        center_name = ""
        for n in enriched_nodes:
            if n.npi == npi:
                center_name = n.name
                break

        response = ReferralNetworkResponse(
            center_npi=npi,
            center_name=center_name,
            nodes=enriched_nodes,
            edges=[ReferralEdge(**e) for e in result.get("edges", [])],
            total_connections=result.get("total_connections", 0),
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("map_referral_network failed")
        return json.dumps({"error": f"map_referral_network failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: analyze_physician_mix
# ---------------------------------------------------------------------------
@mcp.tool()
async def analyze_physician_mix(system_name: str, state: str = "") -> str:
    """Analyze the employed vs. affiliated vs. independent physician mix
    for a health system.

    Cross-references NPPES physician records with AHRQ Health System
    Compendium and CMS Provider of Services data to classify physicians.

    Args:
        system_name: Health system name (e.g. "Penn Medicine", "HCA Healthcare").
        state: Two-letter state code filter.
    """
    try:
        result = await physician_mix.analyze_system_mix(system_name, state)

        if "error" in result:
            return json.dumps(result)

        response = PhysicianMixResponse(
            system_name=result.get("system_name", system_name),
            total_physicians=result.get("total_physicians", 0),
            employed=result.get("employed", 0),
            affiliated=result.get("affiliated", 0),
            independent=result.get("independent", 0),
            employed_pct=result.get("employed_pct", 0),
            affiliated_pct=result.get("affiliated_pct", 0),
            independent_pct=result.get("independent_pct", 0),
            by_specialty=result.get("by_specialty", []),
            sample_physicians=[
                PhysicianClassification(**c) for c in result.get("sample_physicians", [])
            ],
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("analyze_physician_mix failed")
        return json.dumps({"error": f"analyze_physician_mix failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: detect_leakage
# ---------------------------------------------------------------------------
@mcp.tool()
async def detect_leakage(
    system_name: str, state: str = "", specialty: str = ""
) -> str:
    """Detect out-of-network referral leakage for a health system using
    DocGraph shared patient data (2014-2020).

    Identifies physicians who share patients with the system but are not
    affiliated, grouped by specialty and geographic area.

    Args:
        system_name: Health system name (e.g. "Cleveland Clinic").
        state: Two-letter state code filter.
        specialty: Optional specialty filter to focus leakage analysis.
    """
    try:
        if not referral_network.is_docgraph_cached():
            return json.dumps({
                "error": "DocGraph shared patient data not cached. "
                         "Download from https://careset.com/datasets/ and load first."
            })

        # Get system's physician NPIs
        mix_result = await physician_mix.analyze_system_mix(system_name, state)
        if "error" in mix_result:
            return json.dumps(mix_result)

        system_npis = set()
        for p in mix_result.get("sample_physicians", []):
            if p.get("status") in ("employed", "affiliated"):
                system_npis.add(p["npi"])

        facility_zips = {
            str(zip_code).strip()[:5]
            for zip_code in mix_result.get("facility_zips", [])
            if str(zip_code).strip()
        }
        system_zips = await _build_system_service_area_zips(facility_zips)

        outbound = referral_network._get_outbound_referrals(system_npis, min_shared=11)
        if outbound is None:
            return json.dumps({"error": "DocGraph data not cached."})
        destination_npis = {
            str(npi)
            for npi in outbound["npi_to"].tolist()
            if str(npi) not in system_npis
        }
        destination_details = await _lookup_destination_details(destination_npis)
        destination_zip_by_npi = {
            npi: str(details.get("zip_code", "")).strip()[:5]
            for npi, details in destination_details.items()
            if str(details.get("zip_code", "")).strip()
        }

        # Run leakage detection
        leakage = referral_network.detect_leakage(
            system_npis=system_npis,
            system_zips=system_zips,
            destination_zip_by_npi=destination_zip_by_npi,
            min_shared=11,
        )

        if "error" in leakage:
            return json.dumps(leakage)

        # Enrich top destinations with NPPES data, filter by specialty if requested
        enriched_destinations = []
        specialty_lower = specialty.strip().lower() if specialty else ""
        for dest in leakage.get("top_leakage_destinations", [])[:25]:
            try:
                p = destination_details.get(dest["npi"], {})
                if p:
                    dest_specialty = p.get("specialty", "")
                    if specialty_lower and specialty_lower not in dest_specialty.lower():
                        continue
                    enriched_destinations.append(LeakageDestination(
                        npi=dest["npi"],
                        name=f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                        specialty=dest_specialty,
                        shared_count=dest.get("shared_count", 0),
                        city=p.get("city", ""),
                        state=p.get("state", ""),
                        classification=dest.get("classification", "out_of_network"),
                    ))
                else:
                    enriched_destinations.append(LeakageDestination(**dest))
            except Exception:
                enriched_destinations.append(LeakageDestination(**dest))

        response = LeakageResponse(
            system_name=mix_result.get("system_name", system_name),
            total_referrals=leakage.get("total_referrals", 0),
            in_network_pct=leakage.get("in_network_pct", 0),
            out_of_network_in_area_pct=leakage.get("out_of_network_in_area_pct", 0),
            out_of_area_pct=leakage.get("out_of_area_pct", 0),
            top_leakage_destinations=enriched_destinations,
            specialty_breakdown=[
                SpecialtyLeakage(**s) for s in leakage.get("specialty_breakdown", [])
            ],
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("detect_leakage failed")
        return json.dumps({"error": f"detect_leakage failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
