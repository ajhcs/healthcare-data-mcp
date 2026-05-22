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
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured

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
        "DocGraph shared patient data is unavailable because a licensed CareSet/DocGraph "
        "source file is not loaded. If you have access, download the CareSet "
        f"DocGraph CSV from {_DOCGRAPH_DOWNLOAD_URL} and run "
        "load_docgraph_cache(csv_path='/path/to/docgraph.csv'), or set "
        "DOCGRAPH_CSV_PATH and call load_docgraph_cache()."
    )


def _nppes_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "NPPES NPI Registry",
        "source_url": nppes_client.NPPES_API_URL,
        "dataset_id": "nppes_npi_registry",
        "source_period": "live registry query",
        "cache_status": "live_api",
        "cache_freshness": "queried live via NPPES API",
        "source_caveat": "NPPES is a public provider registry; names, specialties, and practice locations can be stale or self-reported.",
    }


def _docgraph_source_metadata(*, cache_status: str | None = None) -> dict[str, Any]:
    status = cache_status or ("ready" if referral_network.is_docgraph_cached() else "missing")
    return {
        "source_name": "CareSet DocGraph Hop Teaming",
        "source_url": _DOCGRAPH_DOWNLOAD_URL,
        "dataset_id": "careset_docgraph_shared_patient_counts",
        "source_period": "2014-2020",
        "cache_status": status,
        "cache_freshness": "local imported cache; source release date not encoded",
        "cache_key": referral_network.get_docgraph_cache_path(),
        "source_caveat": (
            "DocGraph shared-patient counts are imported licensed Medicare claims-derived network data; "
            "they are directional context and not a complete referral, leakage, or network-adequacy source."
        ),
    }


def _physician_compare_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "CMS Physician Compare",
        "source_url": nppes_client.PHYSICIAN_COMPARE_CSV_URL,
        "dataset_id": "cms_physician_compare_public_file",
        "source_period": "latest cached public file",
        "cache_status": "ready" if nppes_client._PHYSICIAN_COMPARE_CACHE.exists() else "missing",
        "cache_key": str(nppes_client._PHYSICIAN_COMPARE_CACHE),
        "source_caveat": "Cached Physician Compare/public affiliation data may lag current practice relationships.",
    }


def _utilization_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "CMS Medicare Physician & Other Practitioners Utilization",
        "source_url": nppes_client.UTILIZATION_DATASET_URL,
        "dataset_id": "cms_medicare_physician_utilization_public_use_file",
        "source_period": "latest cached public file",
        "cache_status": "ready" if nppes_client._UTILIZATION_CACHE.exists() else "missing",
        "cache_key": str(nppes_client._UTILIZATION_CACHE),
        "source_caveat": "Medicare utilization public-use files are aggregate historical Medicare facts, not complete all-payer activity.",
    }


def _physician_evidence(
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


def _physician_row_evidence(
    source_metadata: dict[str, Any],
    *,
    dataset_id: str = "",
    entity_scope: str,
    parent_query: dict[str, Any],
    row: dict[str, Any],
    row_kind: str,
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
) -> dict[str, Any]:
    row_query = {
        **parent_query,
        "row_kind": row_kind,
        "row_npi": row.get("npi") or "",
        "row_npi_from": row.get("npi_from") or "",
        "row_npi_to": row.get("npi_to") or "",
        "row_name": row.get("name") or f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
        "row_specialty": row.get("specialty") or "",
        "row_state": row.get("state") or "",
        "row_status": row.get("status") or row.get("classification") or "",
    }
    return _physician_evidence(
        source_metadata,
        dataset_id=dataset_id,
        entity_scope=entity_scope,
        query=row_query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _physician_identity(record: dict[str, Any]) -> dict[str, Any]:
    name = str(record.get("name") or f"{record.get('first_name', '')} {record.get('last_name', '')}").strip()
    location = {}
    practice_locations = record.get("practice_locations")
    if isinstance(practice_locations, list) and practice_locations:
        first_location = practice_locations[0]
        if isinstance(first_location, dict):
            location = first_location
    identity = identity_from_public_record(
        name=name,
        entity_type="physician",
        npi=record.get("npi") or "",
        address=location.get("address_1") or "",
        zip_code=record.get("zip_code") or location.get("postal_code") or "",
        source_name="NPPES NPI Registry",
        source_url=nppes_client.NPPES_API_URL,
    ).to_dict()
    city = record.get("city") or location.get("city")
    state = record.get("state") or location.get("state")
    if city:
        identity["city"] = str(city)
    if state:
        identity["state"] = str(state).upper()
    specialty = record.get("specialty") or (record.get("specialties") or [""])[0]
    if specialty:
        identity["specialty"] = str(specialty)
    return identity


def _physician_identity_map(records: list[dict[str, Any]], *, match_basis: str) -> dict[str, Any]:
    return {
        "entities": [_physician_identity(record) for record in records if record.get("npi")],
        "match_basis": match_basis,
        "conflict_policy": "Join physicians by exact NPI first; names, specialty, city, and state are labels or candidate context only.",
    }


def _system_identity(system_name: str, *, state: str = "", match_basis: str) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=system_name,
        entity_type="health_system",
        source_name="AHRQ Compendium / NPPES public workflow",
    ).to_dict()
    if state:
        identity["state"] = state.strip().upper()
    identity["match_decisions"].append({
        "basis": match_basis,
        "confidence": "candidate_system_name_match",
        "decided_at": "",
        "notes": "System-level referral readiness uses public name/geography matching unless a workflow supplies exact system identifiers.",
    })
    return identity


def _docgraph_unavailable_response() -> dict[str, Any]:
    """Return deterministic unavailability metadata for DocGraph-backed tools."""
    source_metadata = _docgraph_source_metadata(cache_status="missing")
    return error_response(
        _docgraph_setup_message(),
        code="data_unavailable",
        data_unavailable="licensed_source_missing",
        download_url=_DOCGRAPH_DOWNLOAD_URL,
        cache_path=referral_network.get_docgraph_cache_path(),
        source_metadata=source_metadata,
        evidence=_physician_evidence(
            source_metadata,
            entity_scope="physician_referral_network",
            query={"cache_path": referral_network.get_docgraph_cache_path()},
            match_basis="docgraph_cache_readiness_check",
            confidence="data_unavailable_until_imported",
            caveat=source_metadata["source_caveat"],
            next_step="Import a licensed DocGraph CSV with load_docgraph_cache before running referral network or leakage tools.",
        ),
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
        payload = to_structured(response.model_dump())
        source_metadata = _nppes_source_metadata()
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _physician_evidence(
            source_metadata,
            entity_scope="physician_search",
            query={"query": query, "specialty": specialty, "state": state, "limit": min(limit, 200)},
            match_basis="npi_exact" if query.strip().isdigit() and len(query.strip()) == 10 else "nppes_name_taxonomy_state_search",
            confidence="high_for_exact_npi" if query.strip().isdigit() and len(query.strip()) == 10 else "candidate_registry_matches_require_review",
            caveat=source_metadata["source_caveat"],
            next_step="Use exact NPI for profile, enrollment, referral, or report joins; treat name-only matches as candidates.",
        )
        for physician in payload["physicians"]:
            physician["evidence"] = _physician_row_evidence(
                source_metadata,
                entity_scope="physician_search",
                parent_query=payload["evidence"]["query"],
                row=physician,
                row_kind="nppes_physician_search_result",
                match_basis="nppes_physician_search_result_row",
                confidence="source_registry_candidate_row",
                caveat=source_metadata["source_caveat"],
                next_step="Use exact NPI before joining this physician row to profile, enrollment, referral, or report facts.",
            )
        payload["identity_map"] = _physician_identity_map(physicians, match_basis="nppes_search_result_npi_identity")
        return payload
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
        payload = to_structured(response.model_dump())
        source_metadata = {
            "sources": [_nppes_source_metadata(), _physician_compare_source_metadata(), _utilization_source_metadata()]
        }
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _physician_evidence(
            _nppes_source_metadata(),
            dataset_id="public_physician_profile",
            entity_scope="physician_profile",
            query={"npi": npi},
            match_basis="npi_exact_public_profile",
            confidence="high_for_exact_npi_registry_record",
            caveat=(
                "Physician profile combines NPPES live registry data with optional cached CMS Physician Compare "
                "and Medicare utilization public files; missing enrichment caches are not zero activity."
            ),
            next_step="Check source_metadata cache_status values before citing quality, affiliation, or utilization fields.",
        )
        payload["identity"] = _physician_identity(payload)
        return payload
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
            return _docgraph_unavailable_response()

        rows_loaded = await asyncio.to_thread(referral_network.load_docgraph_csv, resolved_path)
        source_metadata = _docgraph_source_metadata(cache_status="ready")
        payload = to_structured({
            "status": "loaded",
            "csv_path": resolved_path,
            "cache_path": referral_network.get_docgraph_cache_path(),
            "rows_loaded": rows_loaded,
        })
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _physician_evidence(
            source_metadata,
            entity_scope="docgraph_cache_import",
            query={"csv_path": resolved_path},
            match_basis="operator_supplied_docgraph_csv_import",
            confidence="local_import_completed",
            caveat=source_metadata["source_caveat"],
            next_step="Run map_referral_network or detect_leakage and keep the imported source file/version with reports.",
        )
        return payload
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
            return _docgraph_unavailable_response()

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
        payload = to_structured(response.model_dump())
        source_metadata = _docgraph_source_metadata(cache_status="ready")
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _physician_evidence(
            source_metadata,
            entity_scope="physician_referral_network",
            query={"npi": npi, "depth": depth, "min_shared": min_shared},
            match_basis="docgraph_exact_center_npi_shared_patient_edges",
            confidence="source_backed_shared_patient_edges_with_nppes_enrichment",
            caveat=source_metadata["source_caveat"],
            next_step="Review NPI identities and shared_count thresholds before treating graph edges as referral opportunities.",
        )
        payload["identity"] = _physician_identity({"npi": npi, "name": center_name})
        payload["identity_map"] = _physician_identity_map(payload.get("nodes", []), match_basis="docgraph_network_node_npis")
        for node in payload["nodes"]:
            node["evidence"] = _physician_row_evidence(
                source_metadata,
                entity_scope="physician_referral_network",
                parent_query=payload["evidence"]["query"],
                row=node,
                row_kind="docgraph_referral_network_node",
                match_basis="docgraph_referral_network_node_row",
                confidence="source_backed_node_with_optional_nppes_enrichment",
                caveat=source_metadata["source_caveat"],
                next_step="Review the node NPI and NPPES enrichment before treating this physician as a network destination.",
            )
        for edge in payload["edges"]:
            edge["evidence"] = _physician_row_evidence(
                source_metadata,
                entity_scope="physician_referral_network",
                parent_query=payload["evidence"]["query"],
                row=edge,
                row_kind="docgraph_shared_patient_edge",
                match_basis="docgraph_shared_patient_edge_row",
                confidence="source_backed_shared_patient_edge",
                caveat=source_metadata["source_caveat"],
                next_step="Preserve both NPIs, shared_count threshold, and source vintage before citing this edge.",
            )
        return payload
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
        payload = to_structured(response.model_dump())
        source_metadata = {"sources": [_nppes_source_metadata(), {"source_name": "AHRQ Compendium", "dataset_id": "ahrq_compendium_public_files"}]}
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _physician_evidence(
            _nppes_source_metadata(),
            dataset_id="physician_mix_public_workflow",
            entity_scope="health_system_physician_mix",
            query={"system_name": system_name, "state": state},
            match_basis="ahrq_system_name_resolution_plus_nppes_organization_search",
            confidence="heuristic_public_registry_classification",
            caveat=(
                "Physician mix uses public NPPES organization/geography signals and AHRQ facility geography; "
                "classification is analytical context, not employment verification."
            ),
            next_step="Review sample_physicians evidence and supply exact system/facility identifiers for report-grade reconciliation.",
        )
        payload["identity"] = _system_identity(payload.get("system_name", system_name), state=state, match_basis="ahrq_system_name_resolution_plus_nppes_organization_search")
        payload["identity_map"] = _physician_identity_map(payload.get("sample_physicians", []), match_basis="physician_mix_sample_npis")
        for physician in payload["sample_physicians"]:
            physician["classification_evidence"] = physician.get("evidence", [])
            physician["evidence"] = _physician_row_evidence(
                _nppes_source_metadata(),
                dataset_id="physician_mix_public_workflow",
                entity_scope="health_system_physician_mix",
                parent_query=payload["evidence"]["query"],
                row=physician,
                row_kind="physician_mix_sample_classification",
                match_basis="physician_mix_sample_classification_row",
                confidence=str(physician.get("confidence") or "heuristic_public_registry_classification"),
                caveat=payload["evidence"]["caveat"],
                next_step="Review classification_evidence, NPI, specialty, and source organization context before citing employment or affiliation mix.",
            )
        return payload
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
            return _docgraph_unavailable_response()

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
        payload = to_structured(response.model_dump())
        source_metadata = _docgraph_source_metadata(cache_status="ready")
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _physician_evidence(
            source_metadata,
            entity_scope="health_system_referral_leakage_readiness",
            query={"system_name": system_name, "state": state, "specialty": specialty, "min_shared": 11},
            match_basis="physician_mix_system_npis_plus_docgraph_outbound_shared_patient_counts",
            confidence="readiness_analysis_requires_source_review",
            caveat=(
                "Leakage output depends on heuristic system physician classification and imported DocGraph counts; "
                "it is readiness context, not complete leakage measurement or network adequacy evidence."
            ),
            next_step="Validate system NPIs, destination identities, service-area ZIPs, and source vintage before reporting leakage claims.",
        )
        payload["identity"] = _system_identity(payload.get("system_name", system_name), state=state, match_basis="physician_mix_system_npis")
        payload["identity_map"] = _physician_identity_map(
            payload.get("top_leakage_destinations", []),
            match_basis="docgraph_leakage_destination_npis",
        )
        for destination in payload["top_leakage_destinations"]:
            destination["evidence"] = _physician_row_evidence(
                source_metadata,
                entity_scope="health_system_referral_leakage_readiness",
                parent_query=payload["evidence"]["query"],
                row=destination,
                row_kind="docgraph_leakage_destination",
                match_basis="docgraph_leakage_destination_row",
                confidence="readiness_destination_requires_source_review",
                caveat=payload["evidence"]["caveat"],
                next_step="Validate destination NPI, classification, specialty filter, service area, and source vintage before citing leakage facts.",
            )
        for specialty_row in payload["specialty_breakdown"]:
            specialty_row["evidence"] = _physician_row_evidence(
                source_metadata,
                entity_scope="health_system_referral_leakage_readiness",
                parent_query=payload["evidence"]["query"],
                row=specialty_row,
                row_kind="docgraph_leakage_specialty_breakdown",
                match_basis="docgraph_leakage_specialty_breakdown_row",
                confidence="readiness_specialty_summary_requires_source_review",
                caveat=payload["evidence"]["caveat"],
                next_step="Validate specialty grouping, system NPIs, and source vintage before citing this leakage summary.",
            )
        return payload
    except Exception as e:
        logger.exception("detect_leakage failed")
        return error_response(f"detect_leakage failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)
