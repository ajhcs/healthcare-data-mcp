"""Service Area Derivation MCP Server.

Derives Primary Service Areas (PSA) and Secondary Service Areas (SSA)
from public CMS Hospital Service Area File data, and provides Dartmouth
Atlas HSA/HRR crosswalk lookups.
"""

from typing import Any
import logging
import os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared.utils.source_backed_result import source_claim

# Support running both as a package and as a standalone script
try:
    from . import data_loaders as sa_loaders
    from .data_loaders import download_dartmouth_crosswalk, download_hsaf, load_hospital_names
    from .models import DartmouthOverlap, HsaHrrMapping, MarketShareResult, ServiceAreaResult
    from .service_area_engine import compute_market_share, derive_service_area
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    import data_loaders as sa_loaders
    from data_loaders import download_dartmouth_crosswalk, download_hsaf, load_hospital_names
    from models import DartmouthOverlap, HsaHrrMapping, MarketShareResult, ServiceAreaResult
    from service_area_engine import compute_market_share, derive_service_area

logger = logging.getLogger(__name__)

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "service-area"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8002"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "service-area")


def _hsaf_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "CMS Hospital Service Area File",
        "source_url": sa_loaders.HSAF_CSV_URL,
        "dataset_id": "cms_hospital_service_area_file",
        "source_period": "2024",
        "landing_page": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area",
        "cache_status": "ready" if sa_loaders.HSAF_CACHE_PATH.exists() else "download_on_demand",
        "cache_key": str(sa_loaders.HSAF_CACHE_PATH),
        "source_caveat": (
            "CMS HSAF uses Medicare inpatient discharge counts by hospital and beneficiary ZIP; "
            "it is not all-payer service-area capture or a current network adequacy source."
        ),
    }


def _dartmouth_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "Dartmouth Atlas ZIP-HSA-HRR Crosswalk",
        "source_url": sa_loaders.DARTMOUTH_CROSSWALK_URL,
        "dataset_id": "dartmouth_atlas_zip_hsa_hrr_crosswalk",
        "source_period": "2019 ZIP crosswalk",
        "landing_page": "https://data.dartmouthatlas.org/downloads/geography/",
        "cache_status": "ready" if sa_loaders.DARTMOUTH_CACHE_PATH.exists() else "download_on_demand",
        "cache_key": str(sa_loaders.DARTMOUTH_CACHE_PATH),
        "source_caveat": (
            "Dartmouth HSA/HRR assignments are public geography crosswalks; they are benchmark geographies, "
            "not facility-defined service areas."
        ),
    }


def _service_area_evidence(
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


def _service_area_row_evidence(
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
    return _service_area_evidence(
        source_metadata,
        entity_scope=entity_scope,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _facility_identity(ccn: str, facility_name: str = "") -> dict[str, Any]:
    return identity_from_public_record(
        name=facility_name,
        entity_type="facility",
        ccn=ccn,
        source_name="CMS Hospital Service Area File",
        source_url=sa_loaders.HSAF_CSV_URL,
    ).to_dict()


def _zip_identity(zip_code: str, *, source_name: str, source_url: str) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=f"ZIP {str(zip_code).strip().zfill(5)}",
        entity_type="zip_geography",
        source_name=source_name,
        source_url=source_url,
    ).to_dict()
    identity["zip_code"] = str(zip_code).strip().zfill(5)
    identity["unresolved_identifiers"].append({"type": "zip_code", "value": identity["zip_code"]})
    return identity


def _service_area_source_claim(
    source_metadata: dict[str, Any],
    *,
    collection: str = "",
    row_evidence_paths: tuple[str, ...] = (),
    match_policy: str,
) -> dict[str, Any]:
    return source_claim(
        collection=collection or str(source_metadata.get("dataset_id") or "service_area"),
        source_name=str(source_metadata.get("source_name") or ""),
        source_url=str(source_metadata.get("source_url") or ""),
        evidence_path="evidence",
        source_metadata_path="source_metadata",
        row_evidence_paths=row_evidence_paths,
        match_policy=match_policy,
    )


def _hsa_hrr_identity_map(mapping: HsaHrrMapping) -> dict[str, Any]:
    source = _dartmouth_source_metadata()
    zip_identity = _zip_identity(mapping.zip_code, source_name=source["source_name"], source_url=source["source_url"])
    zip_identity["hsa_number"] = str(mapping.hsa_number)
    zip_identity["hrr_number"] = str(mapping.hrr_number)
    return {
        "entities": [zip_identity],
        "match_basis": "dartmouth_zip_exact_crosswalk",
        "source_claims": [
            _service_area_source_claim(source, match_policy="dartmouth_zip_exact_crosswalk")
        ],
        "conflict_policy": "Join ZIP geographies by exact five-digit ZIP and crosswalk vintage; HSA/HRR numbers are geography labels.",
    }


def _facility_identity_map(
    hospitals: list[dict[str, Any]],
    *,
    match_basis: str,
    source_metadata: dict[str, Any] | None = None,
    row_evidence_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    source = source_metadata or _hsaf_source_metadata()
    return {
        "entities": [_facility_identity(str(row.get("ccn", "")), str(row.get("facility_name", ""))) for row in hospitals],
        "match_basis": match_basis,
        "source_claims": [
            _service_area_source_claim(
                source,
                row_evidence_paths=row_evidence_paths,
                match_policy=match_basis,
            )
        ],
        "conflict_policy": "Join hospitals by exact CCN; facility names from HSAF/name lookup are labels.",
    }


@mcp.tool(structured_output=True)
@observe_tool("service-area")
async def compute_service_area(
    ccn: str,
    psa_threshold: float = 0.75,
    ssa_threshold: float = 0.95,
    use_contiguity: bool = False,
) -> dict[str, Any]:
    """Compute Primary and Secondary Service Areas for a hospital.

    Downloads the CMS Hospital Service Area File if not cached, filters to the
    given hospital CCN, ranks ZIP codes by discharge volume, and computes
    cumulative percentage cutoffs for PSA and SSA.

    Args:
        ccn: CMS Certification Number (6-digit, zero-padded).
        psa_threshold: Cumulative discharge fraction for PSA (default 0.75 = 75%).
        ssa_threshold: Cumulative discharge fraction for SSA boundary (default 0.95 = 95%).
        use_contiguity: If True, enforce geographic contiguity on PSA ZIPs (requires adjacency data).

    Returns:
        JSON with facility info, PSA/SSA ZIP lists, discharge counts, and percentages.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"compute_service_area","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    ccn = str(ccn).strip().zfill(6)

    hsaf = await download_hsaf()
    facility_df = hsaf[hsaf["ccn"] == ccn]

    if facility_df.empty:
        return error_response(f"No data found for CCN {ccn} in HSAF")

    facility_name = facility_df["facility_name"].iloc[0]

    # Build discharge data for the engine
    discharge_data = facility_df[["zip_code", "discharges"]].copy()

    # Contiguity graph placeholder — adjacency data is large and optional
    adjacency_graph = None
    if use_contiguity:
        logger.warning(
            "Contiguity enforcement requested but ZCTA adjacency data is not bundled. "
            "Proceeding without contiguity filter. Provide a ZCTA adjacency file to enable."
        )

    result = derive_service_area(
        discharge_data,
        psa_threshold=psa_threshold,
        ssa_threshold=ssa_threshold,
        adjacency_graph=adjacency_graph,
    )

    output = ServiceAreaResult(
        facility_ccn=ccn,
        facility_name=facility_name,
        total_discharges=result["total_discharges"],
        psa_zips=result["psa_zips"],
        psa_discharge_count=result["psa_discharge_count"],
        psa_pct=result["psa_pct"],
        ssa_zips=result["ssa_zips"],
        ssa_discharge_count=result["ssa_discharge_count"],
        ssa_pct=result["ssa_pct"],
        remaining_zips_count=result["remaining_zips_count"],
    )
    payload = to_structured(output.model_dump())
    source_metadata = _hsaf_source_metadata()
    payload["source_metadata"] = source_metadata
    payload["evidence"] = _service_area_evidence(
        source_metadata,
        entity_scope="hospital_service_area",
        query={
            "ccn": ccn,
            "psa_threshold": psa_threshold,
            "ssa_threshold": ssa_threshold,
            "use_contiguity": use_contiguity,
        },
        match_basis="ccn_exact_hsaf_zip_discharge_rows",
        confidence="source_backed_medicare_inpatient_service_area",
        caveat=source_metadata["source_caveat"],
        next_step="Use PSA/SSA ZIPs as Medicare inpatient service-area context and preserve thresholds with cited facts.",
    )
    payload["identity"] = _facility_identity(ccn, facility_name)
    payload["identity_map"] = {
        "entities": [payload["identity"], *[_zip_identity(zip_code, source_name=source_metadata["source_name"], source_url=source_metadata["source_url"]) for zip_code in payload.get("psa_zips", []) + payload.get("ssa_zips", [])]],
        "match_basis": "ccn_exact_with_hsaf_zip_rows",
        "source_claims": [
            _service_area_source_claim(source_metadata, match_policy="ccn_exact_with_hsaf_zip_rows")
        ],
        "conflict_policy": "Keep facility CCN identity separate from ZIP geography identities; ZIPs are service-area components, not facilities.",
    }
    return payload


@mcp.tool(structured_output=True)
@observe_tool("service-area")
async def get_market_share(zip_code: str, limit: int = 20) -> dict[str, Any]:
    """Get hospital market share for a ZIP code.

    Finds all hospitals serving patients from this ZIP and computes each
    hospital's share of total discharges.

    Args:
        zip_code: 5-digit beneficiary ZIP code.
        limit: Maximum number of hospitals to return (default 20).

    Returns:
        JSON with list of hospitals sorted by market share descending.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_market_share","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    zip_code = str(zip_code).strip().zfill(5)

    hsaf = await download_hsaf()
    name_lookup = await load_hospital_names()
    result = compute_market_share(hsaf, zip_code, limit=limit, name_lookup=name_lookup)

    output = MarketShareResult(**result)
    payload = to_structured(output.model_dump())
    source_metadata = _hsaf_source_metadata()
    for hospital in payload.get("hospitals", []):
        if not isinstance(hospital, dict):
            continue
        hospital["evidence"] = _service_area_row_evidence(
            source_metadata,
            entity_scope="zip_hospital_market_share",
            parent_query={"zip_code": zip_code, "limit": limit},
            row_query={
                "ccn": hospital.get("ccn"),
                "facility_name": hospital.get("facility_name"),
                "discharges": hospital.get("discharges"),
                "market_share_pct": hospital.get("market_share_pct"),
            },
            match_basis="hsaf_zip_hospital_market_share_row",
            confidence="source_backed_medicare_inpatient_market_share_row",
            caveat=source_metadata["source_caveat"],
            next_step="Use the hospital CCN for follow-up and do not generalize this Medicare inpatient row to all-payer market share.",
        )
    payload["source_metadata"] = source_metadata
    payload["evidence"] = _service_area_evidence(
        source_metadata,
        entity_scope="zip_hospital_market_share",
        query={"zip_code": zip_code, "limit": limit},
        match_basis="zip_exact_hsaf_discharge_rows",
        confidence="source_backed_medicare_inpatient_market_share",
        caveat=source_metadata["source_caveat"],
        next_step="Use hospital CCNs for follow-up and do not generalize Medicare inpatient share to all-payer market share.",
    )
    payload["identity"] = _zip_identity(zip_code, source_name=source_metadata["source_name"], source_url=source_metadata["source_url"])
    payload["identity_map"] = _facility_identity_map(
        payload.get("hospitals", []),
        match_basis="hsaf_market_share_hospital_ccns",
        source_metadata=source_metadata,
        row_evidence_paths=("hospitals[].evidence",),
    )
    return payload


@mcp.tool(structured_output=True)
@observe_tool("service-area")
async def get_hsa_hrr_mapping(zip_code: str) -> dict[str, Any]:
    """Look up Dartmouth Atlas HSA and HRR assignment for a ZIP code.

    Downloads the Dartmouth crosswalk file on first use, then returns the
    Hospital Service Area and Hospital Referral Region for the given ZIP.

    Args:
        zip_code: 5-digit ZIP code.

    Returns:
        JSON with HSA/HRR number, city, and state.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_hsa_hrr_mapping","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    zip_code = str(zip_code).strip().zfill(5)

    crosswalk = await download_dartmouth_crosswalk()
    row = crosswalk[crosswalk["zip_code"] == zip_code]

    if row.empty:
        return error_response(f"ZIP {zip_code} not found in Dartmouth crosswalk")

    r = row.iloc[0]
    output = HsaHrrMapping(
        zip_code=zip_code,
        hsa_number=int(r.get("hsanum", 0) or 0),
        hsa_city=str(r.get("hsacity", "")),
        hsa_state=str(r.get("hsastate", "")),
        hrr_number=int(r.get("hrrnum", 0) or 0),
        hrr_city=str(r.get("hrrcity", "")),
        hrr_state=str(r.get("hrrstate", "")),
    )
    payload = to_structured(output.model_dump())
    source_metadata = _dartmouth_source_metadata()
    payload["source_metadata"] = source_metadata
    payload["evidence"] = _service_area_evidence(
        source_metadata,
        entity_scope="zip_hsa_hrr_crosswalk",
        query={"zip_code": zip_code},
        match_basis="zip_exact_dartmouth_crosswalk_row",
        confidence="high_for_exact_zip_crosswalk_row",
        caveat=source_metadata["source_caveat"],
        next_step="Use HSA/HRR as geography context and preserve the crosswalk vintage in reports.",
    )
    payload["identity"] = _zip_identity(zip_code, source_name=source_metadata["source_name"], source_url=source_metadata["source_url"])
    payload["identity_map"] = _hsa_hrr_identity_map(output)
    return payload


@mcp.tool(structured_output=True)
@observe_tool("service-area")
async def compare_to_dartmouth(ccn: str) -> dict[str, Any]:
    """Compare a hospital's computed PSA to its Dartmouth Atlas HSA.

    Computes the PSA for the hospital, looks up which HSA the hospital's
    primary ZIP belongs to, then calculates overlap: how many PSA ZIPs fall
    within the same HSA vs. outside.

    Args:
        ccn: CMS Certification Number (6-digit, zero-padded).

    Returns:
        JSON with overlap statistics and lists of ZIPs that differ.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"compare_to_dartmouth","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    ccn = str(ccn).strip().zfill(6)

    hsaf = await download_hsaf()
    crosswalk = await download_dartmouth_crosswalk()

    facility_df = hsaf[hsaf["ccn"] == ccn]
    if facility_df.empty:
        return error_response(f"No data found for CCN {ccn} in HSAF")

    facility_name = facility_df["facility_name"].iloc[0]

    # Compute PSA
    discharge_data = facility_df[["zip_code", "discharges"]].copy()
    sa = derive_service_area(discharge_data)
    psa_zips = set(sa["psa_zips"])

    # Find the hospital's own ZIP (highest-volume ZIP as proxy, or first record)
    top_zip = discharge_data.sort_values("discharges", ascending=False).iloc[0]["zip_code"]

    # Look up HSA for this ZIP
    hosp_hsa_row = crosswalk[crosswalk["zip_code"] == top_zip]
    if hosp_hsa_row.empty:
        return error_response(
            f"Hospital top ZIP {top_zip} not found in Dartmouth crosswalk",
            facility_ccn=ccn,
            facility_name=facility_name,
        )

    hsa_num = int(hosp_hsa_row.iloc[0].get("hsanum", 0) or 0)
    hsa_city = str(hosp_hsa_row.iloc[0].get("hsacity", ""))
    hsa_state = str(hosp_hsa_row.iloc[0].get("hsastate", ""))

    # Find all ZIPs in the same HSA
    hsa_zips = set(crosswalk[crosswalk["hsanum"] == str(hsa_num)]["zip_code"].tolist())

    # Compute overlap
    in_both = psa_zips & hsa_zips
    only_psa = psa_zips - hsa_zips
    only_hsa = hsa_zips - psa_zips

    overlap_pct = round(len(in_both) / len(psa_zips) * 100, 2) if psa_zips else 0.0

    output = DartmouthOverlap(
        facility_ccn=ccn,
        facility_name=facility_name,
        facility_zip=top_zip,
        hsa_number=hsa_num,
        hsa_city=hsa_city,
        hsa_state=hsa_state,
        psa_zip_count=len(psa_zips),
        zips_in_hsa=len(in_both),
        zips_outside_hsa=len(only_psa),
        overlap_pct=overlap_pct,
        zips_only_in_psa=sorted(only_psa),
        zips_only_in_hsa=sorted(only_hsa),
    )
    payload = to_structured(output.model_dump())
    hsaf_source = _hsaf_source_metadata()
    dartmouth_source = _dartmouth_source_metadata()
    payload["source_metadata"] = {"sources": [hsaf_source, dartmouth_source]}
    payload["evidence"] = _service_area_evidence(
        hsaf_source,
        dataset_id="service_area_dartmouth_overlap",
        entity_scope="hospital_service_area_geography_overlap",
        query={"ccn": ccn},
        match_basis="ccn_exact_hsaf_rows_plus_top_zip_dartmouth_crosswalk",
        confidence="source_backed_overlap_context",
        caveat=(
            "Overlap compares a CMS HSAF-derived Medicare inpatient PSA to Dartmouth benchmark geography; "
            "differences are context, not errors or network adequacy conclusions."
        ),
        next_step="Review facility ZIP proxy, PSA thresholds, and HSA/HRR vintage before using overlap in reports.",
    )
    payload["identity"] = _facility_identity(ccn, facility_name)
    payload["identity_map"] = {
        "entities": [
            payload["identity"],
            _zip_identity(top_zip, source_name=dartmouth_source["source_name"], source_url=dartmouth_source["source_url"]),
        ],
        "match_basis": "facility_ccn_exact_plus_top_zip_crosswalk",
        "source_claims": [
            _service_area_source_claim(
                hsaf_source,
                collection="service_area_dartmouth_overlap",
                match_policy="facility_ccn_exact_plus_top_zip_crosswalk",
            )
        ],
        "conflict_policy": "Keep facility and geography identities separate; top ZIP is a service-area proxy from discharge volume.",
    }
    return payload


if __name__ == "__main__":
    mcp.run(transport=_transport)
