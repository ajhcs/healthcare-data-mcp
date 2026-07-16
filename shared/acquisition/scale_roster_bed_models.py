"""Internal models for the governed Scale roster/bed acquisition workflow."""

from __future__ import annotations

from typing import Literal, Self
from urllib.parse import urlparse

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

WORKFLOW_ID = "scale-roster-bed-basis.v1"
CONNECTOR_VERSION = "scale-roster-bed-connector.v1"
PARSER_VERSION = "scale-roster-bed-parser.v1"
SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
ALLOWED_HOST_SUFFIXES = (
    "ahrq.gov",
    "christianacare.org",
    "cms.gov",
    "cooperhealth.org",
    "data.cms.gov",
    "delaware.gov",
    "healthapps.nj.gov",
    "jeffersonhealth.org",
    "mainlinehealth.org",
    "maryland.gov",
    "nj.gov",
    "pa.gov",
    "pennmedicine.org",
    "templehealth.org",
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


Rights = Literal["public_domain", "public_use_terms", "restricted_publication", "unknown_review_required"]
Missingness = Literal["not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict"]


def is_allowlisted_https_url(value: str) -> bool:
    parsed = urlparse(value)
    host = (parsed.hostname or "").casefold()
    return parsed.scheme == "https" and any(
        host == suffix or host.endswith(f".{suffix}") for suffix in ALLOWED_HOST_SUFFIXES
    )


class SourceSpec(StrictModel):
    source_id: str = Field(pattern=r"^[A-Za-z0-9._-]+$")
    source_name: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    registry_id: str = Field(min_length=1)
    registry_version: str = Field(min_length=1)
    url: str = Field(min_length=1)
    landing_page: str = Field(min_length=1)
    source_period: str = Field(min_length=1)
    expected_media_type: str = Field(min_length=1)
    rights_classification: Rights
    parser_kind: Literal["html", "pdf", "csv", "text"]

    @model_validator(mode="after")
    def require_allowlisted_https_source(self) -> Self:
        if not is_allowlisted_https_url(self.url) or not is_allowlisted_https_url(self.landing_page):
            raise ValueError("source and landing-page URLs must be HTTPS on the reviewed Scale source allowlist")
        return self


class EntitySpec(StrictModel):
    entity_id: str = Field(min_length=1)
    canonical_name: str = Field(min_length=1)
    entity_type: Literal["health_system", "hospital", "facility", "campus"]
    system_slug: str = Field(min_length=1)
    ccn: str = ""
    state_license_id: str = ""
    address: str = ""
    zip_code: str = ""
    aliases: list[str] = Field(default_factory=list)
    owner_entity_id: str = ""
    identity_conflicts: list[dict[str, str]] = Field(default_factory=list)
    unresolved_identifiers: list[dict[str, str]] = Field(default_factory=list)


class FactSpec(StrictModel):
    fact_id: str = Field(min_length=1)
    entity_id: str = Field(min_length=1)
    measure_id: str = Field(min_length=1)
    value_type: Literal["integer", "string", "boolean"]
    unit: str = Field(min_length=1)
    period_label: str = Field(min_length=1)
    period_start: str | None = None
    period_end: str | None = None
    denominator_scope: str = Field(min_length=1)
    source_id: str | None = None
    row_locator: str = ""
    match_basis: str = ""
    confidence: str = ""
    caveat: str = ""
    next_step: str = "Review this source-local fact before Toolkit identity or metric use."
    extraction_pattern: str | None = None
    table_match: dict[str, str] = Field(default_factory=dict)
    table_value_field: str = ""
    literal_value: JsonValue | None = None
    missingness: Missingness | None = None
    missingness_reason: str = ""
    dependency_cluster_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_fact_mode(self) -> Self:
        if self.missingness is not None:
            if (
                self.source_id
                or self.extraction_pattern
                or self.table_match
                or self.table_value_field
                or self.literal_value is not None
            ):
                raise ValueError("missingness facts cannot claim a source, extractor, or value")
            if not self.missingness_reason:
                raise ValueError("missingness facts require a reason")
            return self
        has_pattern = bool(self.extraction_pattern)
        has_table_extractor = bool(self.table_match and self.table_value_field)
        if not self.source_id or has_pattern == has_table_extractor or not self.row_locator:
            raise ValueError("populated facts require a source and exactly one complete extractor")
        if self.measure_id.startswith("bed_count.") and "basis=" not in self.denominator_scope:
            raise ValueError("bed observations require an explicit basis in denominator_scope")
        return self


class ConflictSpec(StrictModel):
    conflict_id: str = Field(min_length=1)
    conflict_type: str = Field(min_length=1)
    entity_ids: list[str] = Field(default_factory=list)
    fact_ids: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    status: Literal["open", "accepted_with_rationale", "resolved"] = "open"
    rationale: str = Field(min_length=1)


class AcquisitionSpec(StrictModel):
    bundle_id: str = Field(min_length=1)
    producer_version: str = Field(min_length=1)
    systems: list[str] = Field(min_length=6, max_length=6)
    market: dict[str, JsonValue]
    periods: list[str] = Field(min_length=1)
    sources: list[SourceSpec] = Field(min_length=1)
    entities: list[EntitySpec] = Field(min_length=1)
    facts: list[FactSpec] = Field(min_length=1)
    conflicts: list[ConflictSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_graph(self) -> Self:
        _require_unique("system", self.systems)
        source_ids = _require_unique("source_id", [item.source_id for item in self.sources])
        entity_ids = _require_unique("entity_id", [item.entity_id for item in self.entities])
        fact_ids = _require_unique("fact_id", [item.fact_id for item in self.facts])
        _require_unique("conflict_id", [item.conflict_id for item in self.conflicts])
        system_entities = [item for item in self.entities if item.entity_type == "health_system"]
        if len(system_entities) != 6 or {item.entity_id for item in system_entities} != set(self.systems):
            raise ValueError("systems must name exactly the six health-system entity IDs")
        _require_refs("fact entity", [item.entity_id for item in self.facts], entity_ids)
        _require_refs("fact source", [item.source_id for item in self.facts if item.source_id], source_ids)
        _require_refs("entity owner", [item.owner_entity_id for item in self.entities if item.owner_entity_id], entity_ids)
        for conflict in self.conflicts:
            _require_refs("conflict entity", conflict.entity_ids, entity_ids)
            _require_refs("conflict fact", conflict.fact_ids, fact_ids)
            _require_refs("conflict source", conflict.source_ids, source_ids)
        return self


class FrozenArtifact(StrictModel):
    source_id: str = Field(pattern=r"^[A-Za-z0-9._-]+$")
    artifact_id: str = Field(min_length=1)
    source_url: str = Field(min_length=1)
    final_url: str = Field(min_length=1)
    retrieved_at: AwareDatetime
    source_modified: AwareDatetime | None = None
    media_type: str = Field(min_length=1)
    checksum_sha256: str = Field(pattern=SHA256_PATTERN)
    content_length: int = Field(ge=1)
    cache_run_id: str = Field(pattern=r"^[A-Za-z0-9._-]+$")
    portable_uri: str = Field(pattern=r"^hc-cache://[A-Za-z0-9._/-]+$")
    connector_version: Literal[CONNECTOR_VERSION] = CONNECTOR_VERSION
    parser_version: Literal[PARSER_VERSION] = PARSER_VERSION
    schema_fingerprint: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_urls(self) -> Self:
        if not is_allowlisted_https_url(self.source_url) or not is_allowlisted_https_url(self.final_url):
            raise ValueError("source and final URLs must remain on the reviewed HTTPS allowlist")
        return self


class ExtractedFact(StrictModel):
    fact_id: str = Field(min_length=1)
    value: JsonValue
    normalized_content_checksum: str = Field(pattern=SHA256_PATTERN)


class FrozenAcquisition(StrictModel):
    workflow_id: Literal[WORKFLOW_ID] = WORKFLOW_ID
    acquired_at: AwareDatetime
    cache_run_id: str = Field(pattern=r"^[A-Za-z0-9._-]+$")
    artifacts: list[FrozenArtifact] = Field(min_length=1)
    extracted_facts: list[ExtractedFact]

    @model_validator(mode="after")
    def validate_graph(self) -> Self:
        _require_unique("frozen source_id", [item.source_id for item in self.artifacts])
        _require_unique("artifact_id", [item.artifact_id for item in self.artifacts])
        _require_unique("artifact checksum", [item.checksum_sha256 for item in self.artifacts])
        _require_unique("portable_uri", [item.portable_uri for item in self.artifacts])
        _require_unique("extracted fact_id", [item.fact_id for item in self.extracted_facts])
        if {item.cache_run_id for item in self.artifacts} != {self.cache_run_id}:
            raise ValueError("all frozen artifacts must belong to the acquisition cache run")
        return self


def _require_unique(label: str, values: list[str]) -> set[str]:
    if len(values) != len(set(values)):
        raise ValueError(f"duplicate {label}")
    return set(values)


def _require_refs(label: str, refs: list[str], allowed: set[str]) -> None:
    unknown = sorted(set(refs) - allowed)
    if unknown:
        raise ValueError(f"unknown {label} reference(s): {', '.join(unknown)}")


__all__ = [
    "CONNECTOR_VERSION",
    "PARSER_VERSION",
    "WORKFLOW_ID",
    "AcquisitionSpec",
    "EntitySpec",
    "ExtractedFact",
    "FactSpec",
    "FrozenAcquisition",
    "FrozenArtifact",
    "SourceSpec",
    "is_allowlisted_https_url",
]
