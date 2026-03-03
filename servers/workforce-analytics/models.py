"""Pydantic models for workforce & labor analytics server."""

from pydantic import BaseModel, Field


# --- Tool 1: get_bls_employment ---

class BLSEmploymentResponse(BaseModel):
    """Employment and wage data from BLS OES."""

    occupation_title: str = ""
    soc_code: str = ""
    area_name: str = ""
    employment: int = 0
    mean_wage: float = 0.0
    median_wage: float = 0.0
    pct_10_wage: float = 0.0
    pct_90_wage: float = 0.0
    employment_change_pct: float | None = None
    annual_openings: int | None = None
    data_year: str = ""


# --- Tool 2: get_hrsa_workforce ---

class HPSARecord(BaseModel):
    """Health Professional Shortage Area record."""

    hpsa_name: str = ""
    hpsa_id: str = ""
    hpsa_score: int = 0
    designation_type: str = ""
    discipline: str = ""
    designation_date: str = ""
    provider_ratio: str = ""
    est_underserved_pop: int = 0
    state: str = ""
    county: str = ""


class CountyWorkforceStats(BaseModel):
    """County-level workforce counts from AHRF."""

    county_name: str = ""
    fips: str = ""
    total_mds: int = 0
    total_dos: int = 0
    total_rns: int = 0
    total_dentists: int = 0
    total_pharmacists: int = 0


class HRSAWorkforceResponse(BaseModel):
    """Response from get_hrsa_workforce."""

    state: str = ""
    total_hpsas: int = 0
    hpsas: list[HPSARecord] = Field(default_factory=list)
    county_stats: CountyWorkforceStats | None = None


# --- Tool 3: get_gme_profile ---

class GMEProfileResponse(BaseModel):
    """Graduate medical education profile from HCRIS."""

    hospital_name: str = ""
    ccn: str = ""
    teaching_status: str = ""
    total_resident_ftes: float = 0.0
    primary_care_ftes: float = 0.0
    total_intern_ftes: float = 0.0
    ime_payment: float | None = None
    dgme_payment: float | None = None
    beds: int = 0
    fiscal_year: str = ""


# --- Tool 4: get_residency_programs ---

class ResidencyProgram(BaseModel):
    """A single residency/fellowship program."""

    program_id: str = ""
    specialty: str = ""
    institution: str = ""
    city: str = ""
    state: str = ""
    total_positions: int = 0
    filled_positions: int = 0
    accreditation_status: str = ""


class ResidencyProgramsResponse(BaseModel):
    """Response from get_residency_programs."""

    total_programs: int = 0
    programs: list[ResidencyProgram] = Field(default_factory=list)


# --- Tool 5: search_union_activity ---

class NLRBElection(BaseModel):
    """An NLRB union election record."""

    case_number: str = ""
    employer: str = ""
    union: str = ""
    date: str = ""
    result: str = ""
    unit_size: int = 0
    city: str = ""
    state: str = ""


class WorkStoppage(BaseModel):
    """A work stoppage (strike/lockout) record."""

    employer: str = ""
    union: str = ""
    start_date: str = ""
    end_date: str = ""
    workers_involved: int = 0
    duration_days: int = 0


class UnionActivityResponse(BaseModel):
    """Response from search_union_activity."""

    total_elections: int = 0
    total_stoppages: int = 0
    elections: list[NLRBElection] = Field(default_factory=list)
    work_stoppages: list[WorkStoppage] = Field(default_factory=list)


# --- Tool 6: get_staffing_benchmarks ---

class StaffingBenchmarksResponse(BaseModel):
    """Staffing benchmarks for a facility."""

    facility_name: str = ""
    ccn: str = ""
    facility_type: str = ""
    rn_hprd: float | None = None
    lpn_hprd: float | None = None
    cna_hprd: float | None = None
    total_nurse_hprd: float | None = None
    peer_median_rn_hprd: float | None = None
    peer_pct_rank: float | None = None
    data_source: str = ""
    data_period: str = ""


# --- Tool 7: get_cost_report_staffing ---

class DepartmentStaffing(BaseModel):
    """FTE breakdown for one department."""

    dept_name: str = ""
    total_ftes: float = 0.0
    rn_ftes: float = 0.0
    lpn_ftes: float = 0.0
    aide_ftes: float = 0.0
    salary_expense: float | None = None
    benefits_expense: float | None = None


class CostReportStaffingResponse(BaseModel):
    """Response from get_cost_report_staffing."""

    hospital_name: str = ""
    ccn: str = ""
    fiscal_year: str = ""
    departments: list[DepartmentStaffing] = Field(default_factory=list)
    total_ftes: float = 0.0
    total_salary_expense: float | None = None
