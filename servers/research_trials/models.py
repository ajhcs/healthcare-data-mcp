"""Pydantic response models for the research-trials MCP server."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SourceMetadata(BaseModel):
    """Common source/version metadata for API-backed tools."""

    source_name: str = ""
    source_url: str = ""
    api_version: str = ""
    data_timestamp: str = ""
    retrieved_at: str = ""
    search_id: str = ""
    source_detail_url: str = ""


class NIHOrganization(BaseModel):
    """Funded organization details from NIH RePORTER."""

    name: str = ""
    department: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    zip_code: str = ""
    uei: str = ""
    duns: str = ""
    ipf_code: str = ""


class NIHPrincipalInvestigator(BaseModel):
    """Principal investigator from NIH RePORTER."""

    profile_id: str = ""
    full_name: str = ""
    first_name: str = ""
    middle_name: str = ""
    last_name: str = ""
    title: str = ""
    is_contact_pi: bool = False


class NIHFundingInstitute(BaseModel):
    """Institute/center funding row from NIH RePORTER."""

    fiscal_year: int | None = None
    code: str = ""
    name: str = ""
    abbreviation: str = ""
    total_cost: float = 0.0
    direct_cost: float = 0.0
    indirect_cost: float = 0.0


class NIHProject(BaseModel):
    """Normalized NIH RePORTER project result."""

    appl_id: str = ""
    project_num: str = ""
    core_project_num: str = ""
    title: str = ""
    abstract: str = ""
    public_health_relevance: str = ""
    fiscal_year: int | None = None
    award_amount: float = 0.0
    activity_code: str = ""
    agency_code: str = ""
    funding_mechanism: str = ""
    award_notice_date: str = ""
    project_start_date: str = ""
    project_end_date: str = ""
    project_detail_url: str = ""
    organization: NIHOrganization = Field(default_factory=NIHOrganization)
    principal_investigators: list[NIHPrincipalInvestigator] = Field(default_factory=list)
    institute_fundings: list[NIHFundingInstitute] = Field(default_factory=list)
    terms: list[str] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)


class NIHPublication(BaseModel):
    """Selected publication linked to a RePORTER project."""

    pmid: str = ""
    title: str = ""
    journal: str = ""
    publication_year: str = ""
    core_project_num: str = ""
    appl_id: str = ""
    evidence: dict[str, Any] = Field(default_factory=dict)


class NIHProjectSearchResponse(BaseModel):
    """Response from search_nih_projects."""

    total_results: int = 0
    limit: int = 0
    offset: int = 0
    next_offset: int | None = None
    projects: list[NIHProject] = Field(default_factory=list)
    metadata: SourceMetadata = Field(default_factory=SourceMetadata)


class NIHProjectDetailResponse(BaseModel):
    """Response from get_nih_project."""

    project: NIHProject | None = None
    publications: list[NIHPublication] = Field(default_factory=list)
    metadata: SourceMetadata = Field(default_factory=SourceMetadata)


class FundingProfileResponse(BaseModel):
    """Aggregated NIH funding activity profile."""

    organization_search: str = ""
    org_uei: str = ""
    years: list[int] = Field(default_factory=list)
    total_projects: int = 0
    total_award_amount: float = 0.0
    by_fiscal_year: list[dict[str, Any]] = Field(default_factory=list)
    by_institute: list[dict[str, Any]] = Field(default_factory=list)
    by_pi: list[dict[str, Any]] = Field(default_factory=list)
    by_activity_code: list[dict[str, Any]] = Field(default_factory=list)
    top_terms: list[dict[str, Any]] = Field(default_factory=list)
    projects: list[NIHProject] = Field(default_factory=list)
    metadata: SourceMetadata = Field(default_factory=SourceMetadata)


class ClinicalTrialsMetadata(SourceMetadata):
    """ClinicalTrials.gov response metadata."""

    next_page_token: str = ""
    page_size: int = 0


class ClinicalTrialSponsor(BaseModel):
    """ClinicalTrials.gov sponsor/collaborator."""

    name: str = ""
    sponsor_class: str = ""
    role: str = ""


class ClinicalTrialLocation(BaseModel):
    """ClinicalTrials.gov recruiting/contact location."""

    facility: str = ""
    status: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    zip_code: str = ""


class ClinicalTrial(BaseModel):
    """Normalized ClinicalTrials.gov v2 study."""

    nct_id: str = ""
    brief_title: str = ""
    official_title: str = ""
    organization: str = ""
    overall_status: str = ""
    study_type: str = ""
    phases: list[str] = Field(default_factory=list)
    conditions: list[str] = Field(default_factory=list)
    interventions: list[str] = Field(default_factory=list)
    lead_sponsor: ClinicalTrialSponsor = Field(default_factory=ClinicalTrialSponsor)
    collaborators: list[ClinicalTrialSponsor] = Field(default_factory=list)
    locations: list[ClinicalTrialLocation] = Field(default_factory=list)
    overall_officials: list[dict[str, str]] = Field(default_factory=list)
    start_date: str = ""
    primary_completion_date: str = ""
    completion_date: str = ""
    last_update_posted: str = ""
    enrollment: int | None = None
    version_holder: str = ""
    url: str = ""
    evidence: dict[str, Any] = Field(default_factory=dict)


class ClinicalTrialSearchResponse(BaseModel):
    """Response from search_clinical_trials."""

    total_results: int = 0
    trials: list[ClinicalTrial] = Field(default_factory=list)
    metadata: ClinicalTrialsMetadata = Field(default_factory=ClinicalTrialsMetadata)


class ClinicalTrialDetailResponse(BaseModel):
    """Response from get_clinical_trial."""

    trial: ClinicalTrial | None = None
    metadata: ClinicalTrialsMetadata = Field(default_factory=ClinicalTrialsMetadata)


class TrialSponsorInventoryRecord(BaseModel):
    normalized_sponsor_name: str = ""
    display_names: list[str] = Field(default_factory=list)
    role_counts: dict[str, int] = Field(default_factory=dict)
    lead_sponsor_count: int = 0
    collaborator_count: int = 0
    org_full_name_count: int = 0
    nct_ids: list[str] = Field(default_factory=list)
    classes: list[str] = Field(default_factory=list)
    first_study_date: str = ""
    last_study_date: str = ""
    active_recruiting_count: int = 0
    match_basis: str = ""
    match_confidence: str = ""


class TrialSiteInventoryRecord(BaseModel):
    normalized_facility_name: str = ""
    display_names: list[str] = Field(default_factory=list)
    city: str = ""
    state: str = ""
    country: str = ""
    zip_code: str = ""
    location_status_counts: dict[str, int] = Field(default_factory=dict)
    nct_ids: list[str] = Field(default_factory=list)
    first_update_date: str = ""
    last_update_date: str = ""
    match_basis: str = ""
    match_confidence: str = ""


class TrialInventoryResponse(BaseModel):
    query: str = ""
    filters: dict[str, Any] = Field(default_factory=dict)
    metadata: ClinicalTrialsMetadata = Field(default_factory=ClinicalTrialsMetadata)
    total_studies_scanned: int = 0
    unique_sponsor_count: int = 0
    unique_site_count: int = 0
    records: list[dict[str, Any]] = Field(default_factory=list)
    unresolved_location_count: int = 0
    truncated: bool = False
    next_page_token: str = ""
    warnings: list[str] = Field(default_factory=list)


class OrganizationMatchDecision(BaseModel):
    """Conservative organization matching decision."""

    status: str = "unmatched"
    query_name: str = ""
    query_uei: str = ""
    matched_name: str = ""
    matched_uei: str = ""
    confidence: str = "none"
    rationale: str = ""
    ambiguous_candidates: list[str] = Field(default_factory=list)


class ResearchActivityProfileResponse(BaseModel):
    """Combined NIH funding and ClinicalTrials.gov activity profile."""

    organization_name: str = ""
    uei: str = ""
    facility_name: str = ""
    state: str = ""
    years: list[int] = Field(default_factory=list)
    match_decision: OrganizationMatchDecision = Field(default_factory=OrganizationMatchDecision)
    funding: FundingProfileResponse = Field(default_factory=FundingProfileResponse)
    trials: ClinicalTrialSearchResponse = Field(default_factory=ClinicalTrialSearchResponse)
    combined_summary: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
