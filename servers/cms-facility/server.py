"""CMS Facility Master Data MCP Server.

Provides tools for looking up healthcare facility data from public CMS sources
including Hospital General Info, NPPES NPI Registry, and Cost Report PUF.
"""

from typing import Any
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared.utils.bed_resolver import resolve_hospital_bed_source

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
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8006"))
mcp = FastMCP(**_mcp_kwargs)

_HOSPITAL_INFO_DATASET_ID = "cms_hospital_general_info"
_HOSPITAL_INFO_SOURCE_NAME = "CMS Hospital General Information"
_HOSPITAL_INFO_CACHE_FILE = "hospital_general_info.csv"
_HOSPITAL_INFO_LANDING_PAGE = "https://data.cms.gov/provider-data/dataset/xubh-q36u"
_COST_REPORT_DATASET_ID = "cms_cost_report"
_COST_REPORT_SOURCE_NAME = "CMS Hospital Cost Report PUF"
_COST_REPORT_CACHE_FILE = "hospital_cost_report.csv"
_COST_REPORT_SOURCE_URL = (
    "https://data.cms.gov/sites/default/files/2026-01/"
    "3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv"
)
_COST_REPORT_LANDING_PAGE = "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report"
_NPPES_DATASET_ID = "nppes_npi_registry"


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


def _cache_metadata(
    *,
    dataset_id: str,
    source_name: str,
    source_url: str,
    cache_file: str = "",
    landing_page: str = "",
    source_period: str = "",
) -> dict[str, Any]:
    """Return source/cache metadata for a CMS public dataset."""

    metadata: dict[str, Any] = {
        "source_name": source_name,
        "source_url": source_url,
        "dataset_id": dataset_id,
        "source_period": source_period,
        "landing_page": landing_page,
        "cache_status": "live_api" if not cache_file else "missing",
        "cache_freshness": "live_api" if not cache_file else "missing",
    }
    if not cache_file:
        return metadata

    path = data_loaders.CACHE_DIR / cache_file
    metadata["cache_key"] = str(path)
    if path.exists():
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        age_days = (datetime.now(timezone.utc) - modified).total_seconds() / 86400
        metadata.update(
            {
                "cache_status": "ready",
                "cache_freshness": f"ready; age_days={age_days:.1f}",
                "source_modified": modified.isoformat(),
                "cache_age_days": round(age_days, 1),
            }
        )
    return metadata


def _hospital_info_metadata() -> dict[str, Any]:
    return _cache_metadata(
        dataset_id=_HOSPITAL_INFO_DATASET_ID,
        source_name=_HOSPITAL_INFO_SOURCE_NAME,
        source_url=data_loaders.HOSPITAL_INFO_URL,
        cache_file=_HOSPITAL_INFO_CACHE_FILE,
        landing_page=_HOSPITAL_INFO_LANDING_PAGE,
        source_period="current CMS Provider Data Hospital General Information export",
    )


def _cost_report_metadata(source_period: str = "") -> dict[str, Any]:
    return _cache_metadata(
        dataset_id=_COST_REPORT_DATASET_ID,
        source_name=_COST_REPORT_SOURCE_NAME,
        source_url=_COST_REPORT_SOURCE_URL,
        cache_file=_COST_REPORT_CACHE_FILE,
        landing_page=_COST_REPORT_LANDING_PAGE,
        source_period=source_period,
    )


def _nppes_metadata() -> dict[str, Any]:
    return _cache_metadata(
        dataset_id=_NPPES_DATASET_ID,
        source_name="NPPES NPI Registry",
        source_url=data_loaders.NPPES_API_URL,
        landing_page="https://npiregistry.cms.hhs.gov/search",
        source_period="live NPPES NPI Registry API response",
    )


def _facility_identity(facility: Facility, *, source_metadata: dict[str, Any]) -> dict[str, Any]:
    return identity_from_public_record(
        name=facility.facility_name,
        entity_type="facility",
        ccn=facility.ccn,
        address=facility.address,
        zip_code=facility.zip_code,
        source_name=str(source_metadata.get("source_name") or ""),
        source_url=str(source_metadata.get("source_url") or ""),
    ).to_dict()


def _npi_identity(result: NPIResult, *, source_metadata: dict[str, Any]) -> dict[str, Any]:
    return identity_from_public_record(
        name=result.organization_name or result.name,
        entity_type="organization" if result.enumeration_type == "NPI-2" else "individual",
        npi=result.npi,
        source_name=str(source_metadata.get("source_name") or ""),
        source_url=str(source_metadata.get("source_url") or ""),
    ).to_dict()


def _cms_facility_identity_map(
    *,
    identities: list[dict[str, Any]],
    source_metadata: dict[str, Any],
    match_basis: str,
    identity_paths: tuple[str, ...],
    evidence_path: str = "evidence",
    row_evidence_path: str = "",
) -> dict[str, Any]:
    dataset_id = str(source_metadata.get("dataset_id") or "")
    source_name = str(source_metadata.get("source_name") or "")
    source_url = str(source_metadata.get("source_url") or "")
    fields = ("ccn", "npi", "canonical_name", "address", "zip_code")
    join_keys = []
    for field in fields:
        values = sorted(
            {
                str(identity.get(field) or "").strip()
                for identity in identities
                if str(identity.get(field) or "").strip()
            }
        )
        join_keys.append(
            {
                "field": field,
                "values": values,
                "status": "provided" if values else "missing",
                "used_by": [dataset_id] if values and dataset_id else [],
            }
        )

    source_claim = {
        "collection": dataset_id,
        "source_name": source_name,
        "source_url": source_url,
        "identity_paths": list(identity_paths),
        "evidence_path": evidence_path,
        "match_policy": match_basis,
    }
    if row_evidence_path:
        source_claim["row_evidence_path"] = row_evidence_path

    return {
        "entity_scope": "cms_facility_or_provider_public_identity",
        "join_keys": join_keys,
        "source_claims": [source_claim],
        "conflict_policy": [
            "Use exact CCN for CMS hospital facility joins when available.",
            "Use exact NPI for NPPES provider or organization joins when available.",
            "Treat names, addresses, and ZIP codes as source-specific aliases unless exact public identifiers agree.",
        ],
        "missing_data_policy": (
            "Missing CMS Facility or NPPES rows identify the queried public-source scope only; "
            "they are not proof that a facility, provider, ownership relationship, quality measure, or enrollment record does not exist."
        ),
    }


def _receipt(
    *,
    source_metadata: dict[str, Any],
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_metadata=source_metadata,
        entity_scope="facility_or_provider",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


@mcp.tool(structured_output=True)
async def search_facilities(
    name: str | None = None,
    state: str | None = None,
    facility_type: str | None = None,
    city: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
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
        return error_response("Hospital data not available", results=[])

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
    source_metadata = _hospital_info_metadata()
    facilities = []
    for _, row in results.iterrows():
        facility = _row_to_facility(row)
        facility_payload = facility.model_dump()
        facility_payload["identity"] = _facility_identity(facility, source_metadata=source_metadata)
        facility_payload["evidence"] = _receipt(
            source_metadata=source_metadata,
            query={
                "ccn": facility.ccn,
                "name": name,
                "state": state,
                "facility_type": facility_type,
                "city": city,
            },
            match_basis="cms_hospital_general_info_search_row",
            confidence="candidate_facility_row_requires_exact_ccn_review",
            caveat=(
                "This is a candidate CMS Hospital General Information row returned by search filters; "
                "use exact CCN lookup before citing facility-specific facts."
            ),
            next_step=(
                "Call get_facility with this row's CCN and preserve the exact-row evidence receipt "
                "for report facts."
            ),
        )
        facilities.append(facility_payload)
    query = {"name": name, "state": state, "facility_type": facility_type, "city": city, "limit": limit}
    identities = [item["identity"] for item in facilities if item.get("identity")]
    return to_structured(
        {
            "count": len(facilities),
            "results": facilities,
            "source_metadata": source_metadata,
            "identity_map": _cms_facility_identity_map(
                identities=identities,
                source_metadata=source_metadata,
                match_basis="facility_search_filters",
                identity_paths=(
                    "results[].identity.ccn",
                    "results[].identity.canonical_name",
                    "results[].identity.address",
                    "results[].identity.zip_code",
                ),
                row_evidence_path="results[].evidence",
            ),
            "evidence": _receipt(
                source_metadata=source_metadata,
                query=query,
                match_basis="facility_search_filters",
                confidence="candidate_facility_matches_require_review",
                caveat=(
                    "Search results are public CMS hospital records filtered by supplied criteria; "
                    "use exact CCN lookup before citing facility-specific facts."
                ),
                next_step="Call get_facility with an exact CCN and preserve that evidence receipt for report facts.",
            ),
        }
    )


@mcp.tool(structured_output=True)
async def get_facility(ccn: str) -> dict[str, Any]:
    """Get full facility details by CMS Certification Number (CCN).

    Args:
        ccn: The 6-character CMS Certification Number.
    """
    df = await data_loaders.load_hospital_info()
    if df.empty:
        return error_response("Hospital data not available")

    ccn_col = _col(df, "facility_id", "ccn", "provider_id", "cms_certification_number", "provider_number")
    if not ccn_col:
        return error_response("Cannot identify CCN column in dataset")

    matches = df[df[ccn_col].str.strip() == ccn.strip()]
    if matches.empty:
        return error_response(f"No facility found with CCN: {ccn}")

    source_metadata = _hospital_info_metadata()
    facility = _row_to_facility(matches.iloc[0])
    payload = facility.model_dump()
    payload["identity"] = _facility_identity(facility, source_metadata=source_metadata)
    payload["source_metadata"] = source_metadata
    payload["identity_map"] = _cms_facility_identity_map(
        identities=[payload["identity"]],
        source_metadata=source_metadata,
        match_basis="ccn_exact",
        identity_paths=("identity.ccn", "identity.canonical_name", "identity.address", "identity.zip_code"),
    )
    payload["evidence"] = _receipt(
        source_metadata=source_metadata,
        query={"ccn": ccn},
        match_basis="ccn_exact",
        confidence="high_for_cms_hospital_general_info_row",
        caveat=(
            "CMS Hospital General Information is a public facility master record. "
            "Use source-specific tools for quality, ownership, enrollment, or financial assertions."
        ),
        next_step="Use this CCN as the cross-server facility identity key for quality, finance, workforce, and ownership workflows.",
    )
    return to_structured(payload)


@mcp.tool(structured_output=True)
async def search_npi(
    npi: str | None = None,
    organization_name: str | None = None,
    state: str | None = None,
    taxonomy_description: str | None = None,
    enumeration_type: str = "NPI-2",
    limit: int = 50,
) -> dict[str, Any]:
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
        return error_response(f"NPPES API error: {e}", results=[])

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
        source_metadata = _nppes_metadata()
        result_payload = npi_result.model_dump()
        result_payload["identity"] = _npi_identity(npi_result, source_metadata=source_metadata)
        result_payload["evidence"] = _receipt(
            source_metadata=source_metadata,
            query={
                "npi": npi_result.npi,
                "organization_name": organization_name,
                "state": state,
                "taxonomy_description": taxonomy_description,
                "enumeration_type": enumeration_type,
            },
            match_basis="nppes_result_row",
            confidence="high_for_exact_npi" if npi and str(npi_result.npi) == str(npi).strip() else "candidate_nppes_row_requires_review",
            caveat="NPPES result rows are public registry records; search-filter rows are candidate identity matches unless the NPI is exact.",
            next_step="Use exact NPI for provider identity joins and preserve names, addresses, and taxonomies as source-scoped aliases.",
        )
        parsed.append(result_payload)

    source_metadata = _nppes_metadata()
    query = {
        "npi": npi,
        "organization_name": organization_name,
        "state": state,
        "taxonomy_description": taxonomy_description,
        "enumeration_type": enumeration_type,
        "limit": limit,
    }
    identities = [item["identity"] for item in parsed if item.get("identity")]
    return to_structured(
        {
            "count": len(parsed),
            "results": parsed,
            "source_metadata": source_metadata,
            "identity_map": _cms_facility_identity_map(
                identities=identities,
                source_metadata=source_metadata,
                match_basis="npi_exact" if npi else "nppes_search_filters",
                identity_paths=(
                    "results[].identity.npi",
                    "results[].identity.canonical_name",
                    "results[].identity.address",
                    "results[].identity.zip_code",
                ),
                row_evidence_path="results[].evidence",
            ),
            "evidence": _receipt(
                source_metadata=source_metadata,
                query=query,
                match_basis="npi_exact" if npi else "nppes_search_filters",
                confidence="high_for_exact_npi" if npi else "candidate_nppes_matches_require_review",
                caveat="NPPES is a public registry. Search-filter matches are candidates unless an exact NPI is used.",
                next_step="Use exact NPI values as provider identity keys; preserve names and addresses as aliases/context.",
            ),
        }
    )


@mcp.tool(structured_output=True)
async def get_facility_financials(ccn: str) -> dict[str, Any]:
    """Get financial data for a facility from the CMS Hospital Cost Report PUF.

    Args:
        ccn: The CMS Certification Number of the facility.
    """
    df = await data_loaders.load_cost_report()
    if df.empty:
        return error_response("Cost report data not available")

    # Identify CCN column
    ccn_col = _col(df, "provider_ccn", "provider_number", "ccn", "provider_id", "prvdr_num")
    if not ccn_col:
        return error_response("Cannot identify CCN column in cost report dataset")

    matches = df[df[ccn_col].str.strip() == ccn.strip()]
    if matches.empty:
        return error_response(f"No cost report data found for CCN: {ccn}")

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

    bed_source = resolve_hospital_bed_source(ccn=ccn, hcris_row=row, target_scope="ccn")
    profile = FinancialProfile(
        ccn=ccn,
        fiscal_year_end=str(row.get(fy_col, "")) if fy_col else "",
        total_beds=bed_source.get("selected_bed_count"),
        total_bed_source=bed_source,
        total_discharges=intval("total_discharges", "discharges", "tot_dschrgs"),
        total_patient_days=intval("total_days", "total_patient_days", "patient_days", "ip_days"),
        net_patient_revenue=num("net_patient_revenue", "net_revenue", "net_pat_rev"),
        total_costs=num("total_costs", "tot_costs", "total_operating_costs"),
        fte_employees=num("fte_employees", "fte", "total_fte"),
    )
    source_period = profile.fiscal_year_end
    source_metadata = _cost_report_metadata(source_period)
    payload = profile.model_dump()
    payload["identity"] = identity_from_public_record(
        name=str(row.get("facility_name", "") or row.get("provider_name", "")),
        entity_type="facility",
        ccn=ccn,
        source_name=str(source_metadata.get("source_name") or ""),
        source_url=str(source_metadata.get("source_url") or ""),
    ).to_dict()
    payload["source_metadata"] = source_metadata
    payload["identity_map"] = _cms_facility_identity_map(
        identities=[payload["identity"]],
        source_metadata=source_metadata,
        match_basis="ccn_exact_cost_report_row",
        identity_paths=("identity.ccn", "identity.canonical_name"),
    )
    payload["evidence"] = _receipt(
        source_metadata=source_metadata,
        query={"ccn": ccn},
        match_basis="ccn_exact_cost_report_row",
        confidence="high_for_cms_cost_report_public_row",
        caveat="CMS Cost Report PUF values are public reported financial/utilization fields and may lag current operations.",
        next_step="Compare fiscal periods before combining with current facility, workforce, or quality facts.",
    )
    return to_structured(payload)


@mcp.tool(structured_output=True)
async def get_hospital_info(ccn: str) -> dict[str, Any]:
    """Get Hospital General Information including quality ratings for a facility.

    Args:
        ccn: The CMS Certification Number of the hospital.
    """
    df = await data_loaders.load_hospital_info()
    if df.empty:
        return error_response("Hospital data not available")

    ccn_col = _col(df, "facility_id", "ccn", "provider_id", "cms_certification_number", "provider_number")
    if not ccn_col:
        return error_response("Cannot identify CCN column in dataset")

    matches = df[df[ccn_col].str.strip() == ccn.strip()]
    if matches.empty:
        return error_response(f"No hospital found with CCN: {ccn}")

    source_metadata = _hospital_info_metadata()
    facility = _row_to_facility(matches.iloc[0])
    payload = facility.model_dump()
    payload["identity"] = _facility_identity(facility, source_metadata=source_metadata)
    payload["source_metadata"] = source_metadata
    payload["identity_map"] = _cms_facility_identity_map(
        identities=[payload["identity"]],
        source_metadata=source_metadata,
        match_basis="ccn_exact",
        identity_paths=("identity.ccn", "identity.canonical_name", "identity.address", "identity.zip_code"),
    )
    payload["evidence"] = _receipt(
        source_metadata=source_metadata,
        query={"ccn": ccn},
        match_basis="ccn_exact",
        confidence="high_for_cms_hospital_general_info_row",
        caveat=(
            "CMS Hospital General Information includes summary rating/context fields. "
            "Use hospital-quality exact measure tools for measure-specific assertions."
        ),
        next_step="Call hospital-quality get_quality_measure_rows for reportable measure-specific facts.",
    )
    return to_structured(payload)


if __name__ == "__main__":
    mcp.run(transport=_transport)
