"""Pydantic response models for the public-records MCP server.

Covers public-records tools:
  1. search_usaspending   — USAspending federal award data
  2. search_sam_gov        — SAM.gov contract opportunities
  3. get_340b_status       — 340B Drug Pricing Program covered entities
  4. get_breach_history    — HHS HIPAA breach portal
  5. get_accreditation     — CMS Provider-of-Services accreditation
  6. get_interop_status    — CMS Promoting Interoperability attestation
  7. LEIE screening tools  — HHS OIG current exclusions
  8. SAM Exclusions tools  — SAM.gov active federal exclusions
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


# --- HHS OIG LEIE exclusion screening ---


OIG_LEIE_CAVEAT = (
    "HHS OIG LEIE downloadable data is a screening source for currently "
    "excluded individuals and entities. Name matches are potential matches; "
    "use OIG's online searchable database and documented follow-up process "
    "when SSN/EIN-level identity verification is required."
)


class LEIESourceMetadata(BaseModel):
    """Source/version metadata for the cached HHS OIG LEIE file."""

    source_name: str = "HHS OIG LEIE"
    source_url: str = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
    landing_page_url: str = "https://oig.hhs.gov/exclusions/exclusions_list.asp"
    record_layout_url: str = "https://www.oig.hhs.gov/exclusions/files/leie_record_layout.pdf"
    downloaded_at: str = ""
    source_last_modified: str = ""
    source_etag: str = ""
    record_count: int = 0
    cache_path: str = ""
    csv_path: str = ""
    cache_status: str = "missing"
    cache_age_days: float | None = None
    layout_columns: list[str] = Field(default_factory=list)
    last_error: str = ""


class LEIEExclusionRecord(BaseModel):
    """A single current HHS OIG LEIE exclusion record."""

    entity_type: str = ""
    display_name: str = ""
    last_name: str = ""
    first_name: str = ""
    middle_name: str = ""
    business_name: str = ""
    general_category: str = ""
    specialty: str = ""
    upin: str = ""
    npi: str = ""
    dob: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    exclusion_type: str = ""
    exclusion_date: str = ""
    reinstatement_date: str = ""
    waiver_date: str = ""
    waiver_state: str = ""
    match_basis: str = ""
    match_score: int = 0
    verification_status: str = "potential_match"


class LEIESearchResponse(BaseModel):
    """Response from a single LEIE search tool."""

    search_type: str = ""
    query: dict[str, str] = Field(default_factory=dict)
    status: str = "no_current_leie_match_found"
    total_results: int = 0
    records: list[LEIEExclusionRecord] = Field(default_factory=list)
    source_metadata: LEIESourceMetadata = Field(default_factory=LEIESourceMetadata)
    oig_verification_caveat: str = OIG_LEIE_CAVEAT


class LEIEBatchCandidate(BaseModel):
    """A caller-supplied candidate for LEIE batch screening."""

    candidate_id: str = ""
    entity_type: str = ""
    npi: str = ""
    first_name: str = ""
    last_name: str = ""
    entity_name: str = ""
    state: str = ""
    dob: str = ""


class LEIEBatchResult(BaseModel):
    """LEIE screening result for one candidate."""

    candidate: LEIEBatchCandidate = Field(default_factory=LEIEBatchCandidate)
    status: str = "no_current_leie_match_found"
    match_count: int = 0
    best_match_score: int = 0
    matches: list[LEIEExclusionRecord] = Field(default_factory=list)
    screened_at: str = ""
    source_metadata: LEIESourceMetadata = Field(default_factory=LEIESourceMetadata)
    oig_verification_caveat: str = OIG_LEIE_CAVEAT


class LEIEBatchResponse(BaseModel):
    """Response from screen_leie_batch."""

    total_candidates: int = 0
    results: list[LEIEBatchResult] = Field(default_factory=list)
    source_metadata: LEIESourceMetadata = Field(default_factory=LEIESourceMetadata)
    oig_verification_caveat: str = OIG_LEIE_CAVEAT


# --- SAM.gov Exclusions screening ---


SAM_EXCLUSIONS_CAVEAT = (
    "SAM.gov Exclusions is an official federal screening source for active "
    "exclusion records. Name matches are potential matches; verify against "
    "the full SAM.gov record and agency guidance before making eligibility "
    "or contracting decisions."
)


class SAMExclusionsSourceMetadata(BaseModel):
    """Source/query metadata for a SAM.gov Exclusions API response."""

    source_name: str = "SAM.gov Exclusions"
    source_url: str = "https://api.sam.gov/entity-information/v4/exclusions"
    docs_url: str = "https://open.gsa.gov/api/exclusions-api/"
    api_version: str = "v4"
    queried_at: str = ""
    query: dict[str, str | int | bool] = Field(default_factory=dict)
    total_records: int = 0
    returned_records: int = 0
    limit: int = 0
    page_count: int = 0
    has_more: bool = False
    api_key_configured: bool = False
    last_error: str = ""


class SAMExclusionRecord(BaseModel):
    """A normalized active exclusion record from SAM.gov Exclusions."""

    classification: str = ""
    exclusion_type: str = ""
    exclusion_program: str = ""
    excluding_agency_code: str = ""
    excluding_agency_name: str = ""
    uei: str = ""
    cage_code: str = ""
    npi: str = ""
    prefix: str = ""
    first_name: str = ""
    middle_name: str = ""
    last_name: str = ""
    suffix: str = ""
    entity_name: str = ""
    display_name: str = ""
    address_line_1: str = ""
    address_line_2: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    zip_code_plus_4: str = ""
    country: str = ""
    create_date: str = ""
    update_date: str = ""
    activation_date: str = ""
    termination_date: str = ""
    termination_type: str = ""
    record_status: str = ""
    ct_code: str = ""
    fascsa_order: str = ""
    additional_comments: str = ""
    references: list[dict[str, str]] = Field(default_factory=list)
    match_basis: str = ""
    match_score: int = 0
    verification_status: str = "potential_match"


class SAMExclusionSearchResponse(BaseModel):
    """Response from a SAM.gov Exclusions search tool."""

    search_type: str = ""
    query: dict[str, str] = Field(default_factory=dict)
    status: str = "no_current_sam_exclusion_found"
    total_results: int = 0
    records: list[SAMExclusionRecord] = Field(default_factory=list)
    source_metadata: SAMExclusionsSourceMetadata = Field(
        default_factory=SAMExclusionsSourceMetadata,
    )
    sam_verification_caveat: str = SAM_EXCLUSIONS_CAVEAT


class SAMExclusionBatchCandidate(BaseModel):
    """A caller-supplied candidate for SAM.gov Exclusions batch screening."""

    candidate_id: str = ""
    entity_name: str = ""
    first_name: str = ""
    last_name: str = ""
    uei: str = ""
    cage_code: str = ""
    npi: str = ""
    state: str = ""
    country: str = ""
    classification: str = ""


class SAMExclusionBatchResult(BaseModel):
    """SAM.gov Exclusions screening result for one candidate."""

    candidate: SAMExclusionBatchCandidate = Field(default_factory=SAMExclusionBatchCandidate)
    status: str = "no_current_sam_exclusion_found"
    match_count: int = 0
    matches: list[SAMExclusionRecord] = Field(default_factory=list)
    match_basis: str = ""
    best_match_score: int = 0
    screened_at: str = ""
    source_metadata: SAMExclusionsSourceMetadata = Field(
        default_factory=SAMExclusionsSourceMetadata,
    )
    sam_verification_caveat: str = SAM_EXCLUSIONS_CAVEAT


class SAMExclusionBatchResponse(BaseModel):
    """Response from screen_sam_exclusions_batch."""

    total_candidates: int = 0
    results: list[SAMExclusionBatchResult] = Field(default_factory=list)
    source_metadata: SAMExclusionsSourceMetadata = Field(
        default_factory=SAMExclusionsSourceMetadata,
    )
    sam_verification_caveat: str = SAM_EXCLUSIONS_CAVEAT
