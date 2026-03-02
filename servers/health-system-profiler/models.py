"""Pydantic models for health system profiler responses."""

from pydantic import BaseModel, Field


class SystemSearchResult(BaseModel):
    """A health system found by search."""
    system_id: str = Field(description="AHRQ health system ID")
    name: str = ""
    hq_city: str = ""
    hq_state: str = ""
    hospital_count: int = 0
    total_beds: int = 0


class BedBreakdown(BaseModel):
    """Bed counts by type from POS file."""
    total: int = 0
    certified: int = 0
    psychiatric: int = 0
    rehabilitation: int = 0
    hospice: int = 0
    ventilator: int = 0
    aids: int = 0
    alzheimer: int = 0
    dialysis: int = 0


class ServiceCapabilities(BaseModel):
    """Clinical service flags from POS file."""
    cardiac_catheterization: bool = False
    open_heart_surgery: bool = False
    mri: bool = False
    ct_scanner: bool = False
    pet_scanner: bool = False
    nuclear_medicine: bool = False
    trauma_center: bool = False
    trauma_level: str = ""
    burn_care: bool = False
    neonatal_icu: bool = False
    obstetrics: bool = False
    transplant: bool = False
    emergency_department: bool = False
    operating_rooms: int = 0
    endoscopy_rooms: int = 0
    cardiac_cath_rooms: int = 0


class StaffingCounts(BaseModel):
    """Staffing counts from POS file."""
    rn: int = 0
    lpn: int = 0
    physicians: int = 0
    pharmacists: int = 0
    therapists: int = 0
    total_fte: float = 0.0


class ServiceArea(BaseModel):
    """PSA/SSA for a facility."""
    psa_zips: list[str] = Field(default_factory=list)
    psa_discharge_count: int = 0
    psa_pct: float = 0.0
    ssa_zips: list[str] = Field(default_factory=list)
    ssa_discharge_count: int = 0
    ssa_pct: float = 0.0
    total_discharges: int = 0


class FacilitySummary(BaseModel):
    """A single inpatient facility within the system."""
    ccn: str = Field(description="CMS Certification Number")
    name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    county: str = ""
    phone: str = ""
    hospital_type: str = ""
    ownership: str = ""
    teaching_status: str = ""
    beds: BedBreakdown = Field(default_factory=BedBreakdown)
    services: ServiceCapabilities = Field(default_factory=ServiceCapabilities)
    staffing: StaffingCounts = Field(default_factory=StaffingCounts)
    overall_quality_rating: str = ""
    service_area: ServiceArea | None = None


class SubEntity(BaseModel):
    """A related sub-entity linked via RELATED_PROVIDER_NUMBER."""
    ccn: str = ""
    name: str = ""
    parent_ccn: str = ""
    facility_type: str = ""
    city: str = ""
    state: str = ""
    beds: int = 0


class OutpatientSite(BaseModel):
    """An outpatient site discovered via NPPES."""
    npi: str = ""
    name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    phone: str = ""
    taxonomy_code: str = ""
    taxonomy_description: str = ""
    category: str = ""


class OffSiteSummary(BaseModel):
    """Aggregated off-site location counts from POS."""
    emergency_departments: int = 0
    urgent_care_centers: int = 0
    psychiatric_units: int = 0
    rehabilitation_hospitals: int = 0
    total_off_site: int = 0


class HealthSystemSummary(BaseModel):
    """System-level aggregated summary."""
    system_id: str = ""
    name: str = ""
    hq_city: str = ""
    hq_state: str = ""
    hospital_count: int = 0
    total_beds: int = 0
    total_discharges: int = 0
    physician_group_count: int = 0


class SystemProfileResponse(BaseModel):
    """Complete system profile -- the main response type."""
    system: HealthSystemSummary = Field(default_factory=HealthSystemSummary)
    inpatient_facilities: list[FacilitySummary] = Field(default_factory=list)
    sub_entities: list[SubEntity] = Field(default_factory=list)
    outpatient_sites: list[OutpatientSite] = Field(default_factory=list)
    off_site_summary: OffSiteSummary = Field(default_factory=OffSiteSummary)
