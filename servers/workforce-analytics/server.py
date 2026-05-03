"""Workforce & Labor Analytics MCP Server.

Provides tools for BLS employment data, HRSA shortage areas, CMS GME profiles,
ACGME residency programs, NLRB union activity, staffing benchmarks, and
HCRIS cost report staffing analysis.
"""

from typing import Any
import logging
import os as _os
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_response import error_response, to_structured
from shared.utils import ahrq_data
from shared.utils.cost_report import load_cost_report_row

from . import bls_client, labor_data, operations_data, workforce_data  # pyright: ignore[reportAttributeAccessIssue]
from servers.hospital_quality import data_loaders as hospital_quality_data_loaders
from .models import (
    BLSEmploymentResponse,
    CostReportStaffingResponse,
    DepartmentStaffing,
    GMEProfileResponse,
    HPSARecord,
    HRSAWorkforceResponse,
    NLRBElection,
    ResidencyProgram,
    ResidencyProgramsResponse,
    StaffingBenchmarksResponse,
    UnionActivityResponse,
    WorkStoppage,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "workforce-analytics"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8011"))
mcp = FastMCP(**_mcp_kwargs)


async def _ahrq_hospital_row(ccn: str) -> dict[str, Any]:
    if not ccn:
        return {}
    try:
        df = await ahrq_data.load_ahrq_hospital_linkage()
        if df.empty or "ccn" not in df.columns:
            return {}
        matches = df[df["ccn"].astype(str).str.zfill(6) == ccn.strip().zfill(6)]
        if matches.empty:
            return {}
        return {str(k): v for k, v in matches.iloc[0].to_dict().items()}
    except Exception:
        logger.debug("AHRQ hospital linkage lookup failed", exc_info=True)
        return {}


async def _cost_report_row(ccn: str, year: int = 0) -> Any | None:
    if not ccn:
        return None
    row, error = await load_cost_report_row(hospital_quality_data_loaders, ccn, year=year)
    if error:
        logger.debug("Cost report row unavailable for %s: %s", ccn, error)
        return None
    return row


async def _productivity_profile(ccn: str, year: int = 0) -> dict[str, Any]:
    await workforce_data.ensure_hcris_cached()
    staffing = workforce_data.query_hcris_staffing(ccn, year=year) or {}
    gme = workforce_data.query_hcris_gme(ccn, year=year) or {}
    ahrq_row = await _ahrq_hospital_row(ccn)
    cost_row = await _cost_report_row(ccn, year=year)
    total_ftes = operations_data.dict_float(staffing, "total_ftes")
    bed_days_available = operations_data.series_float(cost_row, "bed_days_available", "total_bed_days_available")
    beds = operations_data.dict_float(ahrq_row, "hos_beds", "beds") or operations_data.series_float(
        cost_row,
        "beds",
        "total_beds",
    )
    if beds is None and bed_days_available is not None:
        beds = operations_data.ratio(bed_days_available, 365)
    discharges = operations_data.dict_float(ahrq_row, "hos_dsch", "discharges") or operations_data.series_float(
        cost_row,
        "total_discharges",
        "discharges",
        "total_hospital_discharges",
        "medicare_discharges",
    )
    patient_days = operations_data.series_float(
        cost_row,
        "total_inpatient_days",
        "inpatient_days",
        "days_of_care",
        "total_patient_days",
    )
    occupied_beds = operations_data.ratio(patient_days, 365) if patient_days is not None else None
    adjusted_patient_days = operations_data.series_float(cost_row, "adjusted_patient_days", "adj_patient_days")
    cmi = operations_data.series_float(cost_row, "case_mix_index", "cmi", "casemix_index")
    resident_fte = operations_data.dict_float(gme, "total_resident_ftes")
    case_mix_adjusted_discharges = discharges * cmi if discharges is not None and cmi is not None else None
    peer_group_metadata = _peer_group_metadata(ahrq_row, beds=beds, resident_fte=resident_fte)
    return {
        "ccn": ccn,
        "year": year or 0,
        "source": "CMS HCRIS Worksheet S-3 with AHRQ hospital linkage where available",
        "source_confidence": "high_for_reported_hcris_fields",
        "total_ftes": total_ftes,
        "beds": beds,
        "discharges": discharges,
        "patient_days": patient_days,
        "occupied_beds": occupied_beds,
        "case_mix_index": cmi,
        "fte_per_occupied_bed": operations_data.ratio(total_ftes, occupied_beds),
        "fte_per_adjusted_patient_day": operations_data.ratio(total_ftes, adjusted_patient_days),
        "fte_per_bed": operations_data.ratio(total_ftes, beds),
        "fte_per_discharge": operations_data.ratio(total_ftes, discharges),
        "resident_fte": resident_fte,
        "resident_to_bed_ratio": operations_data.ratio(resident_fte, beds),
        "case_mix_adjusted_discharges_per_fte": operations_data.ratio(case_mix_adjusted_discharges, total_ftes),
        "optional_metric_caveat": "case_mix_adjusted_discharges_per_fte is only populated when public CMI and discharge fields are present.",
        "departments": staffing.get("departments", []),
        "peer_group_metadata": peer_group_metadata,
    }


async def _throughput_profile(ccn: str = "", state_facility_id: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    return await operations_data.throughput_profile(
        ccn=ccn,
        state_facility_id=state_facility_id,
        state=state,
        year=year,
        hospital_row_loader=_ahrq_hospital_row,
        cost_report_row_loader=_cost_report_row,
    )


def _peer_group_metadata(row: dict[str, Any], *, beds: float | None, resident_fte: float | None) -> dict[str, Any]:
    state = str(row.get("hosp_state", "") or row.get("state", "")).upper()
    rural_urban = _rural_urban_value(row)
    attributes = {
        "state": state,
        "bed_size": _bed_size_group(beds),
        "teaching": "teaching" if (resident_fte or 0) > 0 else "non_teaching",
        "rural_urban": rural_urban,
    }
    available = [key for key, value in attributes.items() if value not in ("", None)]
    return {
        "attributes": attributes,
        "available_dimensions": available,
        "logic": "Peer grouping can combine state, bed_size, teaching, and rural_urban when those attributes are present.",
    }


def _bed_size_group(beds: float | None) -> str:
    if beds is None:
        return ""
    if beds < 25:
        return "under_25"
    if beds < 100:
        return "25_99"
    if beds < 300:
        return "100_299"
    if beds < 500:
        return "300_499"
    return "500_plus"


def _rural_urban_value(row: dict[str, Any]) -> str:
    for key in ("urban_rural", "urban_rural_indicator", "rural_urban", "cbsa_urban_rural", "ruralurban"):
        value = str(row.get(key, "")).strip().lower()
        if not value:
            continue
        if "rural" in value:
            return "rural"
        if "urban" in value:
            return "urban"
        return value
    return ""


# ---------------------------------------------------------------------------
# Tool 1: get_bls_employment
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_bls_employment(
    occupation: str, area_code: str = "", state: str = "",
    include_projections: bool = True,  # noqa: ARG001 — exposed in MCP schema
) -> dict[str, Any]:
    """Get occupation-level employment counts, wages, and projections by MSA or state.

    Uses BLS OES (Occupational Employment and Wage Statistics) API v2.

    Args:
        occupation: Occupation name (e.g. "Registered Nurses") or SOC code (e.g. "29-1141").
        area_code: BLS area code (MSA FIPS). Leave empty for state or national.
        state: Two-letter state code (e.g. "PA"). Leave empty for national.
        include_projections: Include 10-year employment projections.
    """
    try:
        result = await bls_client.get_oes_data(occupation, area_code, state)
        if not result:
            return error_response("No data returned from BLS API")
        if "error" in result:
            return to_structured(result)

        response = BLSEmploymentResponse(
            occupation_title=result.get("occupation_title", ""),
            soc_code=result.get("soc_code", ""),
            area_name=result.get("area_name", state or "National"),
            employment=result.get("employment", 0),
            mean_wage=result.get("mean_wage", 0),
            median_wage=result.get("median_wage", 0),
            pct_10_wage=result.get("pct_10_wage", 0),
            pct_90_wage=result.get("pct_90_wage", 0),
            data_year=result.get("data_year", ""),
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_bls_employment failed")
        return error_response(f"get_bls_employment failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: get_hrsa_workforce
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_hrsa_workforce(
    state: str, county_fips: str = "", discipline: str = "",
) -> dict[str, Any]:
    """Get health workforce shortage areas (HPSAs) and supply data for a state.

    Uses HRSA Data Warehouse HPSA data and Area Health Resource File.

    Args:
        state: Two-letter state code (e.g. "PA").
        county_fips: 5-digit county FIPS code for county-level detail.
        discipline: Filter by discipline ("Primary Care", "Dental", "Mental Health").
    """
    try:
        await workforce_data.ensure_hpsa_cached()

        hpsas = workforce_data.query_hpsas(state, discipline, county_fips)

        response = HRSAWorkforceResponse(
            state=state.upper(),
            total_hpsas=len(hpsas),
            hpsas=[HPSARecord(**h) for h in hpsas if "error" not in h],
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_hrsa_workforce failed")
        return error_response(f"get_hrsa_workforce failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: get_gme_profile
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_gme_profile(
    hospital_name: str = "", ccn: str = "",
) -> dict[str, Any]:
    """Get graduate medical education profile for a teaching hospital.

    Uses CMS HCRIS Worksheet S-2 for resident FTEs, IME/DGME payments,
    teaching status, and bed count.

    Args:
        hospital_name: Hospital name (fuzzy search).
        ccn: 6-digit CMS Certification Number (preferred, exact match).
    """
    try:
        await workforce_data.ensure_hcris_cached()

        if not ccn and hospital_name:
            return error_response("CCN required for HCRIS lookup. Use hospital_name with CMS facility search to find the CCN first.")

        result = workforce_data.query_hcris_gme(ccn)
        if not result:
            return error_response(f"No GME data found for CCN: {ccn}")

        response = GMEProfileResponse(**result)
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_gme_profile failed")
        return error_response(f"get_gme_profile failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: get_residency_programs
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_acgme_source_status() -> dict[str, Any]:
    """Return ACGME public export/import status for residency program inventory."""
    try:
        return to_structured(workforce_data.get_acgme_source_status())
    except Exception as e:
        logger.exception("get_acgme_source_status failed")
        return error_response(f"get_acgme_source_status failed: {e}")


@mcp.tool(structured_output=True)
async def get_acgme_program(program_id: str) -> dict[str, Any]:
    """Return one exact ACGME program by 10-digit Program Code."""
    try:
        status = workforce_data.get_acgme_source_status()
        if status["status"] != "ready":
            return to_structured(status)
        result = workforce_data.get_acgme_program(program_id)
        if result is None:
            return to_structured(
                {
                    "status": "exact_program_not_found",
                    "program_id": program_id,
                    "source_status": status,
                    "next_step": "Verify the 10-digit ACGME Program Code against the imported public export.",
                }
            )
        return to_structured(
            {
                "status": "ready",
                "source_status": status,
                "program": result,
            }
        )
    except ValueError as e:
        return error_response(str(e), code="invalid_params")
    except Exception as e:
        logger.exception("get_acgme_program failed")
        return error_response(f"get_acgme_program failed: {e}")


@mcp.tool(structured_output=True)
async def search_acgme_programs(
    institution: str = "", specialty: str = "", state: str = "",
) -> dict[str, Any]:
    """Search imported ACGME program inventory with explicit match-basis fields."""
    try:
        status = workforce_data.get_acgme_source_status()
        if status["status"] != "ready":
            return to_structured(status)
        programs = workforce_data.query_acgme_programs(institution, specialty, state)
        return to_structured(
            {
                "status": "ready",
                "source_status": status,
                "total_programs": len(programs),
                "programs": programs,
            }
        )
    except Exception as e:
        logger.exception("search_acgme_programs failed")
        return error_response(f"search_acgme_programs failed: {e}")


@mcp.tool(structured_output=True)
async def get_residency_programs(
    institution: str = "", specialty: str = "", state: str = "",
) -> dict[str, Any]:
    """Search residency and fellowship programs from ACGME data.

    Uses a static extract of the ACGME Data Resource Book with program-level
    data including specialty, positions, and accreditation status.

    Args:
        institution: Institution name to search (e.g. "Johns Hopkins").
        specialty: Specialty filter (e.g. "Internal Medicine", "Surgery").
        state: Two-letter state code.
    """
    try:
        programs = workforce_data.query_acgme_programs(institution, specialty, state)

        if programs and "error" in programs[0]:
            return to_structured(programs[0])

        response = ResidencyProgramsResponse(
            total_programs=len(programs),
            programs=[ResidencyProgram(**p) for p in programs],
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_residency_programs failed")
        return error_response(f"get_residency_programs failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: search_union_activity
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_union_activity(
    employer_name: str = "", state: str = "",
    year_start: int = 2015, year_end: int = 2026,
) -> dict[str, Any]:
    """Search NLRB union election records and BLS work stoppages for healthcare employers.

    Uses the labordata/nlrb-data database (daily refreshed from NLRB.gov)
    and BLS work stoppage data for strikes and lockouts.

    Args:
        employer_name: Employer or health system name to search.
        state: Two-letter state code filter.
        year_start: Start year (default 2015).
        year_end: End year (default 2026).
    """
    try:
        await labor_data.ensure_nlrb_cached()
        await labor_data.ensure_stoppages_cached()

        elections = labor_data.search_nlrb_elections(
            employer_name, state, year_start, year_end
        )
        stoppages = labor_data.query_work_stoppages(year_start, year_end)

        response = UnionActivityResponse(
            total_elections=len(elections),
            total_stoppages=len(stoppages),
            elections=[NLRBElection(**e) for e in elections],
            work_stoppages=[WorkStoppage(**s) for s in stoppages if isinstance(s, dict) and "employer" in s],
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("search_union_activity failed")
        return error_response(f"search_union_activity failed: {e}")


# ---------------------------------------------------------------------------
# Tool 6: get_staffing_benchmarks
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_staffing_benchmarks(
    ccn: str = "", state: str = "", facility_type: str = "hospital",
) -> dict[str, Any]:
    """Get staffing benchmarks for a hospital or nursing home.

    Uses CMS PBJ (Payroll-Based Journal) for nursing homes and CMS HCRIS
    Worksheet S-3 for hospitals. Computes peer percentile rankings.

    Args:
        ccn: CMS Certification Number for a specific facility.
        state: State code for state-level benchmarks.
        facility_type: "hospital" (uses HCRIS) or "nursing_home" (uses PBJ).
    """
    try:
        if facility_type == "nursing_home":
            records = await workforce_data.query_pbj_staffing(ccn=ccn, state=state)
            if not records:
                return error_response("No PBJ staffing data found")

            # Average across dates for the facility
            if ccn and len(records) > 1:
                avg_rn = sum(r["rn_hprd"] for r in records) / len(records)
                avg_lpn = sum(r["lpn_hprd"] for r in records) / len(records)
                avg_cna = sum(r["cna_hprd"] for r in records) / len(records)
                avg_total = sum(r["total_nurse_hprd"] for r in records) / len(records)
                response = StaffingBenchmarksResponse(
                    facility_name=records[0]["facility_name"],
                    ccn=ccn,
                    facility_type="nursing_home",
                    rn_hprd=round(avg_rn, 2),
                    lpn_hprd=round(avg_lpn, 2),
                    cna_hprd=round(avg_cna, 2),
                    total_nurse_hprd=round(avg_total, 2),
                    data_source="CMS_PBJ",
                    data_period=records[0].get("date", ""),
                )
            else:
                r = records[0]
                response = StaffingBenchmarksResponse(
                    facility_name=r["facility_name"],
                    ccn=r.get("ccn", ccn),
                    facility_type="nursing_home",
                    rn_hprd=r["rn_hprd"],
                    lpn_hprd=r["lpn_hprd"],
                    cna_hprd=r["cna_hprd"],
                    total_nurse_hprd=r["total_nurse_hprd"],
                    data_source="CMS_PBJ",
                    data_period=r.get("date", ""),
                )
            return to_structured(response.model_dump())

        else:  # hospital
            await workforce_data.ensure_hcris_cached()
            result = workforce_data.query_hcris_staffing(ccn)
            if not result:
                return error_response(f"No HCRIS staffing data found for CCN: {ccn}")

            response = StaffingBenchmarksResponse(
                facility_name="",
                ccn=ccn,
                facility_type="hospital",
                data_source="CMS_HCRIS",
                total_nurse_hprd=None,
            )
            return to_structured(response.model_dump())

    except Exception as e:
        logger.exception("get_staffing_benchmarks failed")
        return error_response(f"get_staffing_benchmarks failed: {e}")


# ---------------------------------------------------------------------------
# Tool 7: get_cost_report_staffing
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_cost_report_staffing(ccn: str, year: int = 0) -> dict[str, Any]:
    """Get FTE breakdowns by department from CMS Cost Reports (Worksheet S-3).

    Extracts staffing data from the Healthcare Cost Report Information System
    (HCRIS) for a specific hospital.

    Args:
        ccn: 6-digit CMS Certification Number.
        year: Fiscal year (0 for most recent available).
    """
    try:
        await workforce_data.ensure_hcris_cached()

        result = workforce_data.query_hcris_staffing(ccn, year=year)
        if not result:
            return error_response(f"No cost report staffing data found for CCN: {ccn}")

        response = CostReportStaffingResponse(
            hospital_name="",
            ccn=ccn,
            fiscal_year=str(year) if year else "most_recent",
            departments=[DepartmentStaffing(**d) for d in result.get("departments", [])],
            total_ftes=result.get("total_ftes", 0),
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_cost_report_staffing failed")
        return error_response(f"get_cost_report_staffing failed: {e}")


@mcp.tool(structured_output=True)
async def get_hospital_staffing_productivity(ccn: str, year: int = 0) -> dict[str, Any]:
    """Get high-confidence hospital staffing productivity metrics from public HCRIS/PBJ-adjacent sources."""
    try:
        if not ccn:
            return error_response("ccn is required.", code="invalid_params")
        return to_structured(await _productivity_profile(ccn, year=year))
    except Exception as e:
        logger.exception("get_hospital_staffing_productivity failed")
        return error_response(f"get_hospital_staffing_productivity failed: {e}")


@mcp.tool(structured_output=True)
async def compare_hospital_staffing_productivity(state: str, year: int = 0, peer_group: str = "") -> dict[str, Any]:
    """Compare public staffing productivity profiles for hospitals in a state."""
    try:
        df = await ahrq_data.load_ahrq_hospital_linkage()
        if df.empty:
            return error_response("AHRQ hospital linkage data not available")
        state_col = "hosp_state" if "hosp_state" in df.columns else "state"
        ccn_col = "ccn" if "ccn" in df.columns else ""
        if not state_col or not ccn_col:
            return error_response("Cannot identify state/CCN columns in AHRQ hospital linkage data")
        matches = df[df[state_col].astype(str).str.upper() == state.upper()].head(50)
        profiles = [await _productivity_profile(str(row[ccn_col]).zfill(6), year=year) for _, row in matches.iterrows()]
        requested_peer_dimensions = _requested_peer_dimensions(peer_group)
        return to_structured(
            {
                "state": state.upper(),
                "year": year or 0,
                "peer_group": peer_group,
                "peer_group_logic": {
                    "requested_dimensions": requested_peer_dimensions,
                    "supported_dimensions": ["state", "bed_size", "teaching", "rural_urban"],
                    "note": "Profiles include peer_group_metadata attributes; callers can bucket peers by any available requested dimension.",
                },
                "total_results": len(profiles),
                "profiles": profiles,
                "confidence": "high_for_reported_public_fields",
            }
        )
    except Exception as e:
        logger.exception("compare_hospital_staffing_productivity failed")
        return error_response(f"compare_hospital_staffing_productivity failed: {e}")


def _requested_peer_dimensions(peer_group: str) -> list[str]:
    supported = {"state", "bed_size", "teaching", "rural_urban"}
    requested = [
        token.strip().lower().replace("-", "_")
        for token in peer_group.replace(";", ",").split(",")
        if token.strip()
    ]
    return [token for token in requested if token in supported] or ["state"]


@mcp.tool(structured_output=True)
async def get_snf_nursing_hprd(ccn: str = "", state: str = "", quarter: str = "") -> dict[str, Any]:
    """Get SNF nursing hours per resident day from CMS PBJ public data."""
    try:
        records = await workforce_data.query_pbj_staffing(ccn=ccn, state=state)
        if quarter:
            records = [record for record in records if quarter.lower() in str(record.get("date", "")).lower()]
        return to_structured(
            {
                "ccn": ccn,
                "state": state.upper() if state else "",
                "quarter": quarter,
                "source": "CMS Payroll-Based Journal Daily Nurse Staffing",
                "total_results": len(records),
                "records": records[:200],
                "confidence": "high_for_pbj_reported_hours_and_census",
            }
        )
    except Exception as e:
        logger.exception("get_snf_nursing_hprd failed")
        return error_response(f"get_snf_nursing_hprd failed: {e}")


@mcp.tool(structured_output=True)
async def get_teaching_intensity(ccn: str, year: int = 0) -> dict[str, Any]:
    """Get resident FTE, resident-to-bed ratio, and teaching status from HCRIS."""
    try:
        if not ccn:
            return error_response("ccn is required.", code="invalid_params")
        profile = await _productivity_profile(ccn, year=year)
        return to_structured(
            {
                "ccn": ccn,
                "year": year or 0,
                "teaching_status": "Teaching" if (profile.get("resident_fte") or 0) > 0 else "Non-Teaching",
                "resident_fte": profile.get("resident_fte"),
                "beds": profile.get("beds"),
                "resident_to_bed_ratio": profile.get("resident_to_bed_ratio"),
                "source": "CMS HCRIS Worksheet S-2",
                "confidence": "high_for_reported_hcris_fields",
            }
        )
    except Exception as e:
        logger.exception("get_teaching_intensity failed")
        return error_response(f"get_teaching_intensity failed: {e}")


@mcp.tool(structured_output=True)
async def get_public_throughput_profile(ccn: str = "", state_facility_id: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    """Get public hospital throughput metrics where public source fields exist."""
    try:
        if not ccn and not state_facility_id:
            return error_response("ccn or state_facility_id is required.", code="invalid_params")
        return to_structured(await _throughput_profile(ccn=ccn, state_facility_id=state_facility_id, state=state, year=year))
    except Exception as e:
        logger.exception("get_public_throughput_profile failed")
        return error_response(f"get_public_throughput_profile failed: {e}")


@mcp.tool(structured_output=True)
async def compare_public_throughput(state: str, year: int = 0) -> dict[str, Any]:
    """Compare public throughput metrics for hospitals in a state."""
    try:
        df = await ahrq_data.load_ahrq_hospital_linkage()
        if df.empty:
            return error_response("AHRQ hospital linkage data not available")
        state_col = "hosp_state" if "hosp_state" in df.columns else "state"
        ccn_col = "ccn" if "ccn" in df.columns else ""
        matches = df[df[state_col].astype(str).str.upper() == state.upper()].head(100)
        profiles = [await _throughput_profile(ccn=str(row[ccn_col]).zfill(6), state=state, year=year) for _, row in matches.iterrows()]
        return to_structured({"state": state.upper(), "year": year or 0, "total_results": len(profiles), "profiles": profiles})
    except Exception as e:
        logger.exception("compare_public_throughput failed")
        return error_response(f"compare_public_throughput failed: {e}")


@mcp.tool(structured_output=True)
async def get_ed_volume_profile(ccn: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    """Return ED visit and admissions-from-ED fields where public sources provide them."""
    try:
        profile = await _throughput_profile(ccn=ccn, state=state, year=year)
        return to_structured(
            {
                "ccn": ccn,
                "year": year or 0,
                "state": state.upper() if state else profile.get("state", ""),
                "ed_visits": profile.get("ed_visits"),
                "inpatient_admissions_from_ed": profile.get("inpatient_admissions_from_ed"),
                "source": profile.get("source"),
                "confidence": profile.get("confidence"),
            }
        )
    except Exception as e:
        logger.exception("get_ed_volume_profile failed")
        return error_response(f"get_ed_volume_profile failed: {e}")


@mcp.tool(structured_output=True)
async def get_or_procedure_volume_profile(ccn: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    """Return OR/procedure volume fields where public state sources provide them."""
    try:
        profile = await _throughput_profile(ccn=ccn, state=state, year=year)
        return to_structured(
            {
                "ccn": ccn,
                "year": year or 0,
                "state": state.upper() if state else profile.get("state", ""),
                "or_procedure_volumes": profile.get("or_procedure_volumes"),
                "ct_mri_cath_open_heart_volumes": profile.get("ct_mri_cath_open_heart_volumes"),
                "source": profile.get("source"),
                "confidence": profile.get("confidence"),
            }
        )
    except Exception as e:
        logger.exception("get_or_procedure_volume_profile failed")
        return error_response(f"get_or_procedure_volume_profile failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
