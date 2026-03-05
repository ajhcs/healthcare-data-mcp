"""Pydantic response models for the public-records MCP server.

Covers 6 tools:
  1. search_usaspending   — USAspending federal award data
  2. search_sam_gov        — SAM.gov contract opportunities
  3. get_340b_status       — 340B Drug Pricing Program covered entities
  4. get_breach_history    — HHS HIPAA breach portal
  5. get_accreditation     — CMS Provider-of-Services accreditation
  6. get_interop_status    — CMS Promoting Interoperability attestation
"""

from pydantic import BaseModel, Field


# --- Tool 1: search_usaspending ---


class USAspendingAward(BaseModel):
    """A single federal award from USAspending.gov."""

    award_id: str = ""
    recipient_name: str = ""
    awarding_agency: str = ""
    awarding_sub_agency: str = ""
    award_type: str = ""
    total_obligation: float = 0.0
    description: str = ""
    start_date: str = ""
    end_date: str = ""
    naics_code: str = ""
    naics_description: str = ""


class USAspendingResponse(BaseModel):
    """Response from search_usaspending."""

    recipient_search: str = ""
    fiscal_year: str = ""
    total_awards: int = 0
    total_obligation: float = 0.0
    awards: list[USAspendingAward] = Field(default_factory=list)


# --- Tool 2: search_sam_gov ---


class SAMOpportunity(BaseModel):
    """A single contract opportunity from SAM.gov."""

    notice_id: str = ""
    title: str = ""
    solicitation_number: str = ""
    department: str = ""
    sub_tier: str = ""
    posted_date: str = ""
    response_deadline: str = ""
    naics_code: str = ""
    set_aside_type: str = ""
    description: str = ""
    active: bool = True


class SAMResponse(BaseModel):
    """Response from search_sam_gov."""

    keyword: str = ""
    total_results: int = 0
    opportunities: list[SAMOpportunity] = Field(default_factory=list)


# --- Tool 3: get_340b_status ---


class CoveredEntity340B(BaseModel):
    """A single 340B Drug Pricing Program covered entity."""

    entity_id: str = ""
    entity_name: str = ""
    entity_type: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    grant_number: str = ""
    participating: bool = True
    contract_pharmacy_count: int = 0


class Status340BResponse(BaseModel):
    """Response from get_340b_status."""

    search_term: str = ""
    total_results: int = 0
    entities: list[CoveredEntity340B] = Field(default_factory=list)


# --- Tool 4: get_breach_history ---


class BreachRecord(BaseModel):
    """A single HIPAA breach record from the HHS breach portal."""

    entity_name: str = ""
    state: str = ""
    covered_entity_type: str = ""
    individuals_affected: int = 0
    breach_submission_date: str = ""
    breach_type: str = ""
    location_of_breached_info: str = ""
    business_associate_present: str = ""
    web_description: str = ""


class BreachHistoryResponse(BaseModel):
    """Response from get_breach_history."""

    search_entity: str = ""
    total_breaches: int = 0
    total_individuals_affected: int = 0
    breaches: list[BreachRecord] = Field(default_factory=list)


# --- Tool 5: get_accreditation ---


class AccreditationRecord(BaseModel):
    """A single provider accreditation record from CMS POS."""

    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    city: str = ""
    accreditation_org: str = ""
    accreditation_type_code: str = ""
    accreditation_effective_date: str = ""
    accreditation_expiration_date: str = ""
    certification_date: str = ""
    ownership_type: str = ""
    bed_count: int = 0
    medicare_medicaid: str = ""
    compliance_status: str = ""


class AccreditationResponse(BaseModel):
    """Response from get_accreditation."""

    search_term: str = ""
    total_results: int = 0
    providers: list[AccreditationRecord] = Field(default_factory=list)


# --- Tool 6: get_interop_status ---


class InteropRecord(BaseModel):
    """A single Promoting Interoperability attestation record."""

    facility_name: str = ""
    ccn: str = ""
    state: str = ""
    city: str = ""
    meets_pi_criteria: str = ""
    cehrt_id: str = ""
    reporting_period_start: str = ""
    reporting_period_end: str = ""
    ehr_product_name: str = ""
    ehr_developer: str = ""


class InteropResponse(BaseModel):
    """Response from get_interop_status."""

    search_term: str = ""
    total_results: int = 0
    records: list[InteropRecord] = Field(default_factory=list)
