"""Pydantic response models for the web-intelligence MCP server.

Covers 5 tools:
  1. scrape_system_profile  — Health system website profile extraction
  2. detect_ehr_vendor      — EHR vendor identification
  3. get_executive_profiles — Executive bios from websites + LinkedIn
  4. monitor_newsroom       — Press releases and news mentions
  5. detect_gpo_affiliation — Group Purchasing Organization matching
"""

from pydantic import BaseModel, Field


# --- Tool 1: scrape_system_profile ---


class LocationEntry(BaseModel):
    """A single facility/location extracted from a health system website."""

    name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    location_type: str = ""


class SystemProfileResponse(BaseModel):
    """Response from scrape_system_profile."""

    system_name: str = ""
    domain: str = ""
    mission: str = ""
    vision: str = ""
    values: str = ""
    tagline: str = ""
    location_count: int = 0
    locations: list[LocationEntry] = Field(default_factory=list)
    source_urls: list[str] = Field(default_factory=list)
    data_quality: str = ""  # "full_parse" | "meta_tags_only" | "snippets_only"


# --- Tool 2: detect_ehr_vendor ---


class EhrDetectionResponse(BaseModel):
    """Response from detect_ehr_vendor."""

    system_name: str = ""
    vendor_name: str = ""
    product_name: str = ""
    confidence: str = ""  # "PI_DATA" | "CAREER_PAGE" | "NEWS_MENTION" | "NOT_FOUND"
    evidence_summary: str = ""
    source_url: str = ""
    cehrt_id: str = ""


# --- Tool 3: get_executive_profiles ---


class LinkedInData(BaseModel):
    """LinkedIn enrichment data from Proxycurl (optional)."""

    headline: str = ""
    summary: str = ""
    education: str = ""
    linkedin_url: str = ""


class ExecutiveProfile(BaseModel):
    """A single executive profile."""

    name: str = ""
    title: str = ""
    bio_snippet: str = ""
    source_url: str = ""
    linkedin_url: str = ""
    linkedin_data: LinkedInData | None = None


class ExecutiveProfilesResponse(BaseModel):
    """Response from get_executive_profiles."""

    system_name: str = ""
    total_results: int = 0
    executives: list[ExecutiveProfile] = Field(default_factory=list)
    source_urls: list[str] = Field(default_factory=list)


# --- Tool 4: monitor_newsroom ---


class NewsItem(BaseModel):
    """A single news item or press release."""

    headline: str = ""
    source: str = ""
    date: str = ""
    snippet: str = ""
    url: str = ""


class NewsroomResponse(BaseModel):
    """Response from monitor_newsroom."""

    system_name: str = ""
    days_back: int = 0
    total_results: int = 0
    items: list[NewsItem] = Field(default_factory=list)


# --- Tool 5: detect_gpo_affiliation ---


class GpoMatch(BaseModel):
    """A single GPO match with supporting evidence."""

    gpo_name: str = ""
    confidence: str = ""  # "strong" | "moderate" | "weak"
    evidence_snippet: str = ""
    evidence_url: str = ""


class GpoAffiliationResponse(BaseModel):
    """Response from detect_gpo_affiliation."""

    system_name: str = ""
    matches: list[GpoMatch] = Field(default_factory=list)
    search_terms_used: str = ""
