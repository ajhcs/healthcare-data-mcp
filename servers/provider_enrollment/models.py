"""Pydantic response models for provider enrollment and ownership tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SourceMetadata(BaseModel):
    """Cached public-source metadata for provider-enrollment data."""

    source_name: str = "CMS Provider Enrollment"
    source_url: str = ""
    landing_page: str = ""
    dataset_id: str = ""
    dataset_key: str = ""
    title: str = ""
    modified: str = ""
    fetched_at: str = ""
    retrieved_at: str = ""
    source_modified: str = ""
    record_count: int | None = None
    checksum: str = ""
    etag: str = ""
    last_modified: str = ""
    cache_path: str = ""
    entity_scope: str = ""
    query: dict[str, Any] = Field(default_factory=dict)
    cache_key: str = ""
    confidence: str = ""


class EnrollmentRecord(BaseModel):
    """Normalized CMS provider enrollment row with original CMS values retained."""

    dataset_key: str = ""
    provider_category: str = ""
    npi: str = ""
    pac_id: str = ""
    enrollment_id: str = ""
    associate_id: str = ""
    ccn: str = ""
    state: str = ""
    provider_type: str = ""
    provider_name: str = ""
    facility_name: str = ""
    source_name: str = ""
    source_url: str = ""
    landing_page: str = ""
    retrieved_at: str = ""
    source_modified: str = ""
    entity_scope: str = ""
    query: dict[str, Any] = Field(default_factory=dict)
    cache_key: str = ""
    confidence: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


class OwnershipRecord(BaseModel):
    """Normalized CMS owner or managing-control relationship."""

    dataset_key: str = ""
    provider_category: str = ""
    enrollment_id: str = ""
    ccn: str = ""
    facility_name: str = ""
    state: str = ""
    owner_name: str = ""
    owner_associate_id: str = ""
    owner_pac_id: str = ""
    owner_type: str = ""
    role_code: str = ""
    role_text: str = ""
    percentage_ownership: str = ""
    association_date: str = ""
    association_end_date: str = ""
    is_active: bool = True
    private_equity: str = ""
    reit: str = ""
    holding_company: str = ""
    source_name: str = ""
    source_url: str = ""
    landing_page: str = ""
    retrieved_at: str = ""
    source_modified: str = ""
    entity_scope: str = ""
    query: dict[str, Any] = Field(default_factory=dict)
    cache_key: str = ""
    confidence: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


class ChangeOfOwnershipRecord(BaseModel):
    """Normalized CHOW history row."""

    dataset_key: str = ""
    provider_category: str = ""
    enrollment_id: str = ""
    ccn: str = ""
    facility_name: str = ""
    state: str = ""
    owner_name: str = ""
    owner_associate_id: str = ""
    transaction_date: str = ""
    effective_date: str = ""
    change_type: str = ""
    source_name: str = ""
    source_url: str = ""
    landing_page: str = ""
    retrieved_at: str = ""
    source_modified: str = ""
    entity_scope: str = ""
    query: dict[str, Any] = Field(default_factory=dict)
    cache_key: str = ""
    confidence: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


class GraphNode(BaseModel):
    """Bounded ownership graph node."""

    id: str
    kind: str
    label: str = ""
    depth: int = 0
    attributes: dict[str, Any] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    """Bounded ownership graph edge."""

    source: str
    target: str
    relationship: str
    active: bool = True
    attributes: dict[str, Any] = Field(default_factory=dict)


class ProviderEnrollmentSearchResponse(BaseModel):
    """Response from search_provider_enrollment."""

    total_results: int = 0
    limit: int = 25
    enrollments: list[EnrollmentRecord] = Field(default_factory=list)
    metadata: list[SourceMetadata] = Field(default_factory=list)


class ProviderEnrollmentDetailResponse(BaseModel):
    """Response from get_provider_enrollment_detail."""

    query: dict[str, str] = Field(default_factory=dict)
    enrollments: list[EnrollmentRecord] = Field(default_factory=list)
    ownership: list[OwnershipRecord] = Field(default_factory=list)
    chow_history: list[ChangeOfOwnershipRecord] = Field(default_factory=list)
    metadata: list[SourceMetadata] = Field(default_factory=list)


class FacilityOwnershipResponse(BaseModel):
    """Response from get_facility_ownership."""

    query: dict[str, str] = Field(default_factory=dict)
    total_results: int = 0
    limit: int = 50
    owners: list[OwnershipRecord] = Field(default_factory=list)
    metadata: list[SourceMetadata] = Field(default_factory=list)


class OwnerNetworkResponse(BaseModel):
    """Response from trace_owner_network."""

    query: dict[str, str] = Field(default_factory=dict)
    depth: int = 1
    limit: int = 100
    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)
    shared_owners: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[SourceMetadata] = Field(default_factory=list)


class ChangeOfOwnershipSearchResponse(BaseModel):
    """Response from search_change_of_ownership."""

    query: dict[str, str] = Field(default_factory=dict)
    total_results: int = 0
    limit: int = 50
    events: list[ChangeOfOwnershipRecord] = Field(default_factory=list)
    metadata: list[SourceMetadata] = Field(default_factory=list)


class ProviderControlProfileResponse(BaseModel):
    """Response from profile_provider_control."""

    query: dict[str, str] = Field(default_factory=dict)
    enrollment: list[EnrollmentRecord] = Field(default_factory=list)
    ownership: list[OwnershipRecord] = Field(default_factory=list)
    chow_history: list[ChangeOfOwnershipRecord] = Field(default_factory=list)
    owner_network: OwnerNetworkResponse = Field(default_factory=OwnerNetworkResponse)
    join_keys: dict[str, list[str]] = Field(default_factory=dict)
    metadata: list[SourceMetadata] = Field(default_factory=list)
