"""Service Area Derivation MCP Server.

Derives Primary Service Areas (PSA) and Secondary Service Areas (SSA)
from public CMS Hospital Service Area File data, and provides Dartmouth
Atlas HSA/HRR crosswalk lookups.
"""

import json
import logging
import os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

# Support running both as a package and as a standalone script
try:
    from .data_loaders import download_dartmouth_crosswalk, download_hsaf, load_hospital_names
    from .models import DartmouthOverlap, HsaHrrMapping, MarketShareResult, ServiceAreaResult
    from .service_area_engine import compute_market_share, derive_service_area
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from data_loaders import download_dartmouth_crosswalk, download_hsaf, load_hospital_names
    from models import DartmouthOverlap, HsaHrrMapping, MarketShareResult, ServiceAreaResult
    from service_area_engine import compute_market_share, derive_service_area

logger = logging.getLogger(__name__)

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "service-area"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8002"))
mcp = FastMCP(**_mcp_kwargs)


@mcp.tool()
async def compute_service_area(
    ccn: str,
    psa_threshold: float = 0.75,
    ssa_threshold: float = 0.95,
    use_contiguity: bool = False,
) -> str:
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
    """
    ccn = str(ccn).strip().zfill(6)

    hsaf = await download_hsaf()
    facility_df = hsaf[hsaf["ccn"] == ccn]

    if facility_df.empty:
        return json.dumps({"error": f"No data found for CCN {ccn} in HSAF"})

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
    return json.dumps(output.model_dump(), indent=2)


@mcp.tool()
async def get_market_share(zip_code: str, limit: int = 20) -> str:
    """Get hospital market share for a ZIP code.

    Finds all hospitals serving patients from this ZIP and computes each
    hospital's share of total discharges.

    Args:
        zip_code: 5-digit beneficiary ZIP code.
        limit: Maximum number of hospitals to return (default 20).

    Returns:
        JSON with list of hospitals sorted by market share descending.
    """
    zip_code = str(zip_code).strip().zfill(5)

    hsaf = await download_hsaf()
    name_lookup = await load_hospital_names()
    result = compute_market_share(hsaf, zip_code, limit=limit, name_lookup=name_lookup)

    output = MarketShareResult(**result)
    return json.dumps(output.model_dump(), indent=2)


@mcp.tool()
async def get_hsa_hrr_mapping(zip_code: str) -> str:
    """Look up Dartmouth Atlas HSA and HRR assignment for a ZIP code.

    Downloads the Dartmouth crosswalk file on first use, then returns the
    Hospital Service Area and Hospital Referral Region for the given ZIP.

    Args:
        zip_code: 5-digit ZIP code.

    Returns:
        JSON with HSA/HRR number, city, and state.
    """
    zip_code = str(zip_code).strip().zfill(5)

    crosswalk = await download_dartmouth_crosswalk()
    row = crosswalk[crosswalk["zip_code"] == zip_code]

    if row.empty:
        return json.dumps({"error": f"ZIP {zip_code} not found in Dartmouth crosswalk"})

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
    return json.dumps(output.model_dump(), indent=2)


@mcp.tool()
async def compare_to_dartmouth(ccn: str) -> str:
    """Compare a hospital's computed PSA to its Dartmouth Atlas HSA.

    Computes the PSA for the hospital, looks up which HSA the hospital's
    primary ZIP belongs to, then calculates overlap: how many PSA ZIPs fall
    within the same HSA vs. outside.

    Args:
        ccn: CMS Certification Number (6-digit, zero-padded).

    Returns:
        JSON with overlap statistics and lists of ZIPs that differ.
    """
    ccn = str(ccn).strip().zfill(6)

    hsaf = await download_hsaf()
    crosswalk = await download_dartmouth_crosswalk()

    facility_df = hsaf[hsaf["ccn"] == ccn]
    if facility_df.empty:
        return json.dumps({"error": f"No data found for CCN {ccn} in HSAF"})

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
        return json.dumps({
            "error": f"Hospital top ZIP {top_zip} not found in Dartmouth crosswalk",
            "facility_ccn": ccn,
            "facility_name": facility_name,
        })

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
    return json.dumps(output.model_dump(), indent=2)


if __name__ == "__main__":
    mcp.run(transport=_transport)
