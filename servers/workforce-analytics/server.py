"""Workforce & Labor Analytics MCP Server.

Provides tools for BLS employment data, HRSA shortage areas, CMS GME profiles,
ACGME residency programs, NLRB union activity, staffing benchmarks, and
HCRIS cost report staffing analysis.
"""

import json
import logging
import os as _os
from mcp.server.fastmcp import FastMCP

from . import bls_client, labor_data, workforce_data  # pyright: ignore[reportAttributeAccessIssue]
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
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8011"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Tool 1: get_bls_employment
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_bls_employment(
    occupation: str, area_code: str = "", state: str = "",
) -> str:
    """Get occupation-level employment counts and wages by MSA or state.

    Uses BLS OES (Occupational Employment and Wage Statistics) API v2.

    Args:
        occupation: Occupation name (e.g. "Registered Nurses") or SOC code (e.g. "29-1141").
        area_code: BLS area code (MSA FIPS). Leave empty for state or national.
        state: Two-letter state code (e.g. "PA"). Leave empty for national.
    """
    try:
        result = await bls_client.get_oes_data(occupation, area_code, state)
        if not result:
            return json.dumps({"error": "No data returned from BLS API"})
        if "error" in result:
            return json.dumps(result)

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
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_bls_employment failed")
        return json.dumps({"error": f"get_bls_employment failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_hrsa_workforce
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_hrsa_workforce(
    state: str, county_fips: str = "", discipline: str = "",
) -> str:
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
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_hrsa_workforce failed")
        return json.dumps({"error": f"get_hrsa_workforce failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: get_gme_profile
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_gme_profile(
    hospital_name: str = "", ccn: str = "",
) -> str:
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
            return json.dumps({"error": "CCN required for HCRIS lookup. Use hospital_name with CMS facility search to find the CCN first."})

        result = workforce_data.query_hcris_gme(ccn)
        if not result:
            return json.dumps({"error": f"No GME data found for CCN: {ccn}"})

        response = GMEProfileResponse(**result)
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_gme_profile failed")
        return json.dumps({"error": f"get_gme_profile failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: get_residency_programs
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_residency_programs(
    institution: str = "", specialty: str = "", state: str = "",
) -> str:
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
            return json.dumps(programs[0])

        response = ResidencyProgramsResponse(
            total_programs=len(programs),
            programs=[ResidencyProgram(**p) for p in programs],
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_residency_programs failed")
        return json.dumps({"error": f"get_residency_programs failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: search_union_activity
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_union_activity(
    employer_name: str = "", state: str = "",
    year_start: int = 2015, year_end: int = 2026,
) -> str:
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
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("search_union_activity failed")
        return json.dumps({"error": f"search_union_activity failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 6: get_staffing_benchmarks
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_staffing_benchmarks(
    ccn: str = "", state: str = "", facility_type: str = "hospital",
) -> str:
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
                return json.dumps({"error": "No PBJ staffing data found"})

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
            return json.dumps(response.model_dump())

        else:  # hospital
            await workforce_data.ensure_hcris_cached()
            result = workforce_data.query_hcris_staffing(ccn)
            if not result:
                return json.dumps({"error": f"No HCRIS staffing data found for CCN: {ccn}"})

            response = StaffingBenchmarksResponse(
                facility_name="",
                ccn=ccn,
                facility_type="hospital",
                data_source="CMS_HCRIS",
                total_nurse_hprd=None,
            )
            return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("get_staffing_benchmarks failed")
        return json.dumps({"error": f"get_staffing_benchmarks failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 7: get_cost_report_staffing
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_cost_report_staffing(ccn: str, year: int = 0) -> str:
    """Get FTE breakdowns by department from CMS Cost Reports (Worksheet S-3).

    Extracts staffing data from the Healthcare Cost Report Information System
    (HCRIS) for a specific hospital.

    Args:
        ccn: 6-digit CMS Certification Number.
        year: Fiscal year (0 for most recent available).
    """
    try:
        await workforce_data.ensure_hcris_cached()

        result = workforce_data.query_hcris_staffing(ccn)
        if not result:
            return json.dumps({"error": f"No cost report staffing data found for CCN: {ccn}"})

        response = CostReportStaffingResponse(
            hospital_name="",
            ccn=ccn,
            fiscal_year=str(year) if year else "most_recent",
            departments=[DepartmentStaffing(**d) for d in result.get("departments", [])],
            total_ftes=result.get("total_ftes", 0),
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_cost_report_staffing failed")
        return json.dumps({"error": f"get_cost_report_staffing failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
