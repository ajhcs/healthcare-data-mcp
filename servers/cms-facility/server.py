"""CMS Facility Master Data MCP Server.

Provides tools for looking up healthcare facility data from public CMS sources
including Hospital General Info, NPPES NPI Registry, and Cost Report PUF.
"""

import json
import logging
import os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

# Support running both as a package and as a standalone script
try:
    from . import data_loaders
    from .models import Facility, FinancialProfile, NPIResult
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    import data_loaders
    from models import Facility, FinancialProfile, NPIResult

logger = logging.getLogger(__name__)

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "cms-facility"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8006"))
mcp = FastMCP(**_mcp_kwargs)


def _col(df, *candidates, default=""):
    """Find the first matching column name in a DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return default


def _row_to_facility(row) -> Facility:
    """Map a Hospital General Info row to a Facility model."""
    def val(key, *alts):
        for k in (key, *alts):
            if k in row.index and row[k]:
                return row[k]
        return ""

    emergency = val("emergency_services", "emergency_service")
    emergency_bool = None
    if emergency:
        emergency_bool = emergency.strip().lower() in ("yes", "true", "1", "y")

    beds_val = val("hospital_bed_count", "beds", "number_of_beds", "total_beds")
    beds = None
    if beds_val:
        try:
            beds = int(float(beds_val))
        except (ValueError, TypeError):
            pass

    return Facility(
        ccn=val("facility_id", "ccn", "provider_id", "cms_certification_number", "provider_number"),
        facility_name=val("facility_name", "hospital_name", "provider_name", "name"),
        address=val("address", "address_line_1", "street_address"),
        city=val("city", "city/town"),
        state=val("state"),
        zip_code=val("zip_code", "zip", "postal_code"),
        county=val("county_name", "county"),
        phone=val("phone_number", "phone", "telephone_number"),
        hospital_type=val("hospital_type", "facility_type", "provider_type"),
        ownership=val("hospital_ownership", "ownership", "ownership_type"),
        emergency_services=emergency_bool,
        beds=beds,
        overall_rating=val("hospital_overall_rating", "overall_rating", "overall_quality_star_rating"),
        mortality_rating=val("mortality_national_comparison", "mortality_rating"),
        safety_rating=val("safety_of_care_national_comparison", "safety_rating"),
        readmission_rating=val("readmission_national_comparison", "readmission_rating"),
        patient_experience_rating=val("patient_experience_national_comparison", "patient_experience_rating"),
    )


@mcp.tool()
async def search_facilities(
    name: str | None = None,
    state: str | None = None,
    facility_type: str | None = None,
    city: str | None = None,
    limit: int = 50,
) -> str:
    """Search CMS Hospital General Info for healthcare facilities.

    Args:
        name: Facility name (partial/contains match, case-insensitive).
        state: Two-letter state code (e.g. "CA", "NY").
        facility_type: Hospital type filter (e.g. "Acute Care", "Critical Access").
        city: City name filter.
        limit: Max results to return (default 50).
    """
    df = await data_loaders.load_hospital_info()
    if df.empty:
        return json.dumps({"error": "Hospital data not available", "results": []})

    mask = df.index >= 0  # start with all True

    name_col = _col(df, "facility_name", "hospital_name", "provider_name", "name")
    state_col = _col(df, "state")
    type_col = _col(df, "hospital_type", "facility_type", "provider_type")
    city_col = _col(df, "city", "city/town")

    if name and name_col:
        mask = mask & df[name_col].str.contains(name, case=False, na=False)
    if state and state_col:
        mask = mask & (df[state_col].str.upper() == state.upper())
    if facility_type and type_col:
        mask = mask & df[type_col].str.contains(facility_type, case=False, na=False)
    if city and city_col:
        mask = mask & df[city_col].str.contains(city, case=False, na=False)

    results = df[mask].head(limit)
    facilities = [_row_to_facility(row).model_dump() for _, row in results.iterrows()]
    return json.dumps({"count": len(facilities), "results": facilities})


@mcp.tool()
async def get_facility(ccn: str) -> str:
    """Get full facility details by CMS Certification Number (CCN).

    Returns Hospital General Information including quality ratings (overall,
    mortality, safety, readmission, patient experience) for the facility.
    Use this tool for any hospital info lookup by CCN.

    Args:
        ccn: The 6-character CMS Certification Number.
    """
    df = await data_loaders.load_hospital_info()
    if df.empty:
        return json.dumps({"error": "Hospital data not available"})

    ccn_col = _col(df, "facility_id", "ccn", "provider_id", "cms_certification_number", "provider_number")
    if not ccn_col:
        return json.dumps({"error": "Cannot identify CCN column in dataset"})

    matches = df[df[ccn_col].str.strip() == ccn.strip()]
    if matches.empty:
        return json.dumps({"error": f"No facility found with CCN: {ccn}"})

    facility = _row_to_facility(matches.iloc[0])
    return json.dumps(facility.model_dump())


@mcp.tool()
async def search_npi(
    npi: str | None = None,
    organization_name: str | None = None,
    state: str | None = None,
    taxonomy_description: str | None = None,
    enumeration_type: str = "NPI-2",
    limit: int = 50,
) -> str:
    """Search the NPPES NPI Registry for provider/organization records.

    Args:
        npi: Exact NPI number to look up.
        organization_name: Organization name (partial match supported by API).
        state: Two-letter state code.
        taxonomy_description: Provider taxonomy/specialty description.
        enumeration_type: "NPI-1" for individuals, "NPI-2" for organizations (default NPI-2).
        limit: Max results (default 50, API max 200).
    """
    try:
        raw_results = await data_loaders.search_nppes(
            npi=npi,
            organization_name=organization_name,
            state=state,
            taxonomy_description=taxonomy_description,
            enumeration_type=enumeration_type,
            limit=limit,
        )
    except Exception as e:
        return json.dumps({"error": f"NPPES API error: {e}", "results": []})

    parsed = []
    for r in raw_results:
        basic = r.get("basic", {})
        enum_type = r.get("enumeration_type", "")

        if enum_type == "NPI-2":
            display_name = basic.get("organization_name", "")
        else:
            first = basic.get("first_name", "")
            last = basic.get("last_name", "")
            display_name = f"{first} {last}".strip()

        npi_result = NPIResult(
            npi=str(r.get("number", "")),
            enumeration_type=enum_type,
            name=display_name,
            first_name=basic.get("first_name", ""),
            last_name=basic.get("last_name", ""),
            organization_name=basic.get("organization_name", ""),
            addresses=r.get("addresses", []),
            taxonomies=r.get("taxonomies", []),
            other_names=r.get("other_names", []),
        )
        parsed.append(npi_result.model_dump())

    return json.dumps({"count": len(parsed), "results": parsed})


@mcp.tool()
async def get_facility_financials(ccn: str) -> str:
    """Get financial data for a facility from the CMS Hospital Cost Report PUF.

    Args:
        ccn: The CMS Certification Number of the facility.
    """
    df = await data_loaders.load_cost_report()
    if df.empty:
        return json.dumps({"error": "Cost report data not available"})

    # Identify CCN column
    ccn_col = _col(df, "provider_ccn", "provider_number", "ccn", "provider_id", "prvdr_num")
    if not ccn_col:
        return json.dumps({"error": "Cannot identify CCN column in cost report dataset"})

    matches = df[df[ccn_col].str.strip() == ccn.strip()]
    if matches.empty:
        return json.dumps({"error": f"No cost report data found for CCN: {ccn}"})

    # Take the most recent row if multiple years exist
    fy_col = _col(df, "fiscal_year_end", "fy_end", "fiscal_year_end_date", "fy_end_dt")
    if fy_col and fy_col in matches.columns:
        matches = matches.sort_values(fy_col, ascending=False)

    row = matches.iloc[0]

    def num(col_name, *alts):
        for c in (col_name, *alts):
            if c in row.index and row[c]:
                try:
                    return float(str(row[c]).replace(",", ""))
                except (ValueError, TypeError):
                    pass
        return None

    def intval(col_name, *alts):
        v = num(col_name, *alts)
        return int(v) if v is not None else None

    profile = FinancialProfile(
        ccn=ccn,
        fiscal_year_end=str(row.get(fy_col, "")) if fy_col else "",
        total_beds=intval("total_bed_days_available", "beds", "total_beds", "bed_size"),
        total_discharges=intval("total_discharges", "discharges", "tot_dschrgs"),
        total_patient_days=intval("total_days", "total_patient_days", "patient_days", "ip_days"),
        net_patient_revenue=num("net_patient_revenue", "net_revenue", "net_pat_rev"),
        total_costs=num("total_costs", "tot_costs", "total_operating_costs"),
        fte_employees=num("fte_employees", "fte", "total_fte"),
    )
    return json.dumps(profile.model_dump())



if __name__ == "__main__":
    mcp.run(transport=_transport)
