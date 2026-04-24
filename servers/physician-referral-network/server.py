"""Physician & Referral Network MCP Server.

Provides tools for physician search, profiles with Medicare utilization,
referral network mapping, health system employment mix analysis,
and referral leakage detection.
"""

from typing import Any
import asyncio
import logging
import os as _os

from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_response import error_response, to_structured

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
_DOCGRAPH_DOWNLOAD_URL = "https://careset.com/datasets/"

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "physician-referral-network"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8010"))
mcp = FastMCP(**_mcp_kwargs)


def _docgraph_setup_message() -> str:
    """Return a user-facing setup message for DocGraph-backed tools."""
    return (
        "DocGraph shared patient data is not cached. Download the CareSet "
        f"DocGraph CSV from {_DOCGRAPH_DOWNLOAD_URL} and run "
        "load_docgraph_cache(csv_path='/path/to/docgraph.csv'), or set "
        "DOCGRAPH_CSV_PATH and call load_docgraph_cache()."
    )


# ---------------------------------------------------------------------------
# Tool 1: search_physicians
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_physicians(
    query: str, specialty: str = "", state: str = "", limit: int = 25
) -> dict[str, Any]:
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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("search_physicians failed")
        return error_response(f"search_physicians failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: get_physician_profile
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_physician_profile(npi: str) -> dict[str, Any]:
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
            return error_response(f"No physician found for NPI: {npi}")

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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_physician_profile failed")
        return error_response(f"get_physician_profile failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: load_docgraph_cache
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def load_docgraph_cache(csv_path: str = "") -> dict[str, Any]:
    """Import a local DocGraph CSV into the shared Parquet cache used by
    referral network and leakage tools.

    Args:
        csv_path: Absolute or relative path to a downloaded CareSet DocGraph CSV.
            If omitted, the server will use DOCGRAPH_CSV_PATH from the environment.
    """
    try:
        resolved_path = csv_path.strip() or _os.environ.get("DOCGRAPH_CSV_PATH", "").strip()
        if not resolved_path:
            return error_response(
                _docgraph_setup_message(),
                download_url=_DOCGRAPH_DOWNLOAD_URL,
                cache_path=referral_network.get_docgraph_cache_path(),
            )

        rows_loaded = await asyncio.to_thread(referral_network.load_docgraph_csv, resolved_path)
        return to_structured({
            "status": "loaded",
            "csv_path": resolved_path,
            "cache_path": referral_network.get_docgraph_cache_path(),
            "rows_loaded": rows_loaded,
        })
    except Exception as e:
        logger.exception("load_docgraph_cache failed")
        return error_response(f"load_docgraph_cache failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: map_referral_network
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def map_referral_network(
    npi: str, depth: int = 1, min_shared: int = 11
) -> dict[str, Any]:
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
            return error_response(_docgraph_setup_message())

        result = referral_network.get_referral_network(npi, depth=depth, min_shared=min_shared)

        if "error" in result:
            return error_response(str(result["error"]))

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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("map_referral_network failed")
        return error_response(f"map_referral_network failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: analyze_physician_mix
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def analyze_physician_mix(system_name: str, state: str = "") -> dict[str, Any]:
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
            return to_structured(result)

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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("analyze_physician_mix failed")
        return error_response(f"analyze_physician_mix failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: detect_leakage
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def detect_leakage(
    system_name: str, state: str = "", specialty: str = ""
) -> dict[str, Any]:
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
            return error_response(_docgraph_setup_message())

        # Get system's physician NPIs
        mix_result = await physician_mix.analyze_system_mix(system_name, state)
        if "error" in mix_result:
            return error_response(str(mix_result["error"]))

        system_npis = set()
        for p in mix_result.get("sample_physicians", []):
            if p.get("status") in ("employed", "affiliated"):
                system_npis.add(p["npi"])

        # Get system's service area ZIPs
        system_zips: set[str] = set()

        # Run leakage detection
        leakage = referral_network.detect_leakage(
            system_npis=system_npis,
            system_zips=system_zips,
            min_shared=11,
        )

        if "error" in leakage:
            return error_response(str(leakage["error"]))

        # Enrich top destinations with NPPES data, filter by specialty if requested
        enriched_destinations = []
        specialty_lower = specialty.strip().lower() if specialty else ""
        for dest in leakage.get("top_leakage_destinations", [])[:25]:
            try:
                physicians = await nppes_client.search_physicians(dest["npi"], limit=1)
                if physicians:
                    p = physicians[0]
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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("detect_leakage failed")
        return error_response(f"detect_leakage failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)
