"""Pydantic models for CMS facility data."""

from pydantic import BaseModel, Field


class Facility(BaseModel):
    """A healthcare facility from CMS Provider of Services / Hospital General Info."""

    ccn: str = Field(description="CMS Certification Number")
    facility_name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    county: str = ""
    phone: str = ""
    hospital_type: str = ""
    ownership: str = ""
    emergency_services: bool | None = None
    beds: int | None = None
    overall_rating: str = ""
    mortality_rating: str = ""
    safety_rating: str = ""
    readmission_rating: str = ""
    patient_experience_rating: str = ""


class NPIResult(BaseModel):
    """A result from the NPPES NPI Registry."""

    npi: str = Field(description="National Provider Identifier")
    enumeration_type: str = ""
    name: str = ""
    first_name: str = ""
    last_name: str = ""
    organization_name: str = ""
    addresses: list[dict] = Field(default_factory=list)
    taxonomies: list[dict] = Field(default_factory=list)
    other_names: list[dict] = Field(default_factory=list)


class FinancialProfile(BaseModel):
    """Financial data from CMS Hospital Cost Report PUF."""

    ccn: str = Field(description="CMS Certification Number")
    fiscal_year_end: str = ""
    total_beds: int | None = None
    total_discharges: int | None = None
    total_patient_days: int | None = None
    net_patient_revenue: float | None = None
    total_costs: float | None = None
    fte_employees: float | None = None
