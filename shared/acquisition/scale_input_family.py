"""Governed, field-neutral Scale input-family acquisition contracts.

This module records source-local candidates without promoting them to Scale
inputs.  A candidate can coexist with blocked coverage when its period,
definition, or organizational boundary is not comparable across all systems.
"""

from __future__ import annotations

import hashlib
import io
import re
import subprocess
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, Mapping, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from pypdf import PdfReader

from shared.acquisition.scale_system_roster import SYSTEM_NAMES, SYSTEM_SLUGS
from shared.contracts.public_evidence import (
    PublicEvidenceBundleInput,
    canonical_sha256,
)

SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
SCALE_INPUT_FAMILIES = (
    "operating_revenue_usd",
    "annual_discharges",
    "physician_count",
    "service_line_count",
    "safety_net_patient_mix_pct",
    "emergency_department_count",
    "essential_service_designation_count",
)

Missingness = Literal[
    "not_yet_researched",
    "unavailable_public",
    "not_applicable",
    "blocked_source_conflict",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FrozenSourceArtifact(StrictModel):
    artifact_id: str = Field(min_length=1)
    system_slug: str = Field(min_length=1)
    source_name: str = Field(min_length=1)
    document_title: str = Field(min_length=1)
    audit_status: Literal["audited", "unaudited", "unavailable"]
    source_url: str = Field(pattern=r"^https://")
    landing_page: str = Field(pattern=r"^https://")
    source_period: str = Field(min_length=1)
    entity_boundary: str = Field(min_length=1)
    statement_locator: str = Field(min_length=1)
    retrieved_at: AwareDatetime
    http_status: int = Field(ge=100, le=599)
    media_type: str = Field(min_length=1)
    payload_sha256: str = Field(pattern=SHA256_PATTERN)
    content_length: int = Field(ge=1)
    rights_classification: Literal[
        "public_domain", "public_use_terms", "restricted_publication", "unknown_review_required"
    ]
    custody_state: Literal["frozen_verified", "blocked_http_response"]

    @model_validator(mode="after")
    def status_matches_custody(self) -> Self:
        if self.custody_state == "frozen_verified" and self.http_status != 200:
            raise ValueError("frozen_verified source requires HTTP 200")
        if self.custody_state == "blocked_http_response" and self.http_status < 400:
            raise ValueError("blocked HTTP custody requires an error response")
        if self.system_slug not in SYSTEM_SLUGS:
            raise ValueError("source system is outside the frozen six-system roster")
        return self


class CandidateExtraction(StrictModel):
    artifact_ref: str = Field(min_length=1)
    page_number: int = Field(ge=1)
    period_marker: str = Field(min_length=1)
    units_marker: str = Field(min_length=1)
    definition_marker: str = Field(min_length=1)
    basis_marker: str = Field(min_length=1)
    row_pattern: str = Field(min_length=1)
    raw_value: int | float = Field(ge=0)
    scale_multiplier: int | float = Field(gt=0)


class ScaleInputCandidate(StrictModel):
    system_slug: str
    input_family: str
    candidate_value: int | float | None = None
    unit: str = Field(min_length=1)
    source_period: str = Field(min_length=1)
    definition: str = Field(min_length=1)
    basis: str = Field(min_length=1)
    source_artifact_refs: list[str] = Field(min_length=1)
    extraction: CandidateExtraction | None = None
    missingness: Missingness
    blocker_codes: list[str] = Field(min_length=1)
    finding: str = Field(min_length=1)
    imputed: Literal[False] = False
    approved_for_scale: Literal[False] = False

    @model_validator(mode="after")
    def preserve_candidate_as_non_approved(self) -> Self:
        if self.system_slug not in SYSTEM_SLUGS:
            raise ValueError("candidate system is outside the frozen six-system roster")
        if self.input_family not in SCALE_INPUT_FAMILIES:
            raise ValueError("unknown Scale input family")
        if self.candidate_value is not None:
            if isinstance(self.candidate_value, bool) or self.candidate_value < 0:
                raise ValueError("candidate value must be a non-negative number")
            if self.missingness != "blocked_source_conflict":
                raise ValueError("a non-approved source-local candidate must remain blocked")
            if self.extraction is None:
                raise ValueError("source-local candidate requires a frozen extraction rule")
        elif self.extraction is not None:
            raise ValueError("missing candidate value cannot claim an extraction")
        return self


class ScaleInputFamilyAcquisition(StrictModel):
    schema_version: Literal["ushso.scale-input-family-acquisition.v1"] = (
        "ushso.scale-input-family-acquisition.v1"
    )
    acquisition_id: str = Field(min_length=1)
    workflow_id: Literal["scale-input-family-acquisition.v1"] = "scale-input-family-acquisition.v1"
    input_family: str
    systems: list[str]
    acquired_at: AwareDatetime
    producer_version: str = Field(min_length=1)
    source_artifacts: list[FrozenSourceArtifact] = Field(min_length=1)
    candidates: list[ScaleInputCandidate]
    prohibited_outputs: list[str] = Field(min_length=1)
    acquisition_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_graph_and_hash(self) -> Self:
        if tuple(self.systems) != SYSTEM_SLUGS:
            raise ValueError("acquisition must preserve exact frozen six-system order")
        if self.input_family not in SCALE_INPUT_FAMILIES:
            raise ValueError("unknown Scale input family")
        if [item.system_slug for item in self.candidates] != list(SYSTEM_SLUGS):
            raise ValueError("acquisition requires exactly one ordered candidate row per system")
        if any(item.input_family != self.input_family for item in self.candidates):
            raise ValueError("candidate family drift")
        artifact_ids = [item.artifact_id for item in self.source_artifacts]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("duplicate source artifact")
        allowed = set(artifact_ids)
        for candidate in self.candidates:
            unknown = set(candidate.source_artifact_refs) - allowed
            if unknown:
                raise ValueError(f"unknown source artifact reference: {sorted(unknown)}")
            if any(
                next(item for item in self.source_artifacts if item.artifact_id == ref).system_slug
                != candidate.system_slug
                for ref in candidate.source_artifact_refs
            ):
                raise ValueError("candidate cannot borrow another system's source")
            if candidate.extraction is not None:
                if candidate.extraction.artifact_ref not in candidate.source_artifact_refs:
                    raise ValueError("candidate extraction must use its own source artifact")
                artifact = next(
                    item
                    for item in self.source_artifacts
                    if item.artifact_id == candidate.extraction.artifact_ref
                )
                if artifact.audit_status != "audited":
                    raise ValueError("numeric candidate extraction requires an audited source")
                if candidate.source_period != artifact.source_period:
                    raise ValueError("candidate period must match its frozen source period")
                if candidate.definition != candidate.extraction.definition_marker:
                    raise ValueError("candidate definition must match its extracted row label")
                if candidate.basis != candidate.extraction.basis_marker:
                    raise ValueError("candidate basis must match its extracted statement boundary")
                expected_value = Decimal(str(candidate.extraction.raw_value)) * Decimal(
                    str(candidate.extraction.scale_multiplier)
                )
                if Decimal(str(candidate.candidate_value)) != expected_value:
                    raise ValueError("candidate value must match its frozen raw value and scale")
        required_prohibitions = {
            "scale_score",
            "component_score",
            "sensitivity_result",
            "projection",
            "adjudication",
            "recommendation",
            "promotion",
        }
        if set(self.prohibited_outputs) != required_prohibitions:
            raise ValueError("all no-execution prohibitions must remain explicit")
        expected = semantic_hash(self, "acquisition_sha256")
        if self.acquisition_sha256 != expected:
            raise ValueError("acquisition_sha256 does not match canonical content")
        return self


def semantic_hash(value: BaseModel, hash_field: str) -> str:
    return canonical_sha256(value.model_dump(mode="json", exclude={hash_field}))


def build_acquisition(payload: Mapping[str, object]) -> ScaleInputFamilyAcquisition:
    body = dict(payload)
    body.pop("acquisition_sha256", None)
    body["acquisition_sha256"] = canonical_sha256(body)
    return ScaleInputFamilyAcquisition.model_validate(body)


def build_public_evidence_input(
    acquisition: ScaleInputFamilyAcquisition,
    *,
    producer_commit: str = "0" * 40,
) -> PublicEvidenceBundleInput:
    """Map a frozen acquisition to Public Evidence Bundle v1 without promotion."""

    if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")

    lineage = {
        item.artifact_id: {
            "artifact_id": item.artifact_id,
            "checksum_sha256": item.payload_sha256,
            "media_type": item.media_type,
            "uri": f"hc-cache://scale-input-family-acquisition.v1/{acquisition.acquisition_id}/{item.artifact_id}",
            "cache_run_id": acquisition.acquisition_id,
            "connector": "governed-public-http",
            "connector_version": "scale-input-family-connector.v1",
            "parser_version": "scale-input-family-parser.v1",
            "schema_fingerprint": canonical_sha256(
                {"schema_version": acquisition.schema_version, "input_family": acquisition.input_family}
            ),
        }
        for item in acquisition.source_artifacts
    }
    sources = []
    for item in acquisition.source_artifacts:
        receipt_id = f"receipt:{item.artifact_id}"
        sources.append(
            {
                "source_id": f"source:{item.artifact_id}",
                "registry_id": f"scale-source:{item.system_slug}:{acquisition.input_family}",
                "registry_version": "1",
                "receipt": {
                    "receipt_id": receipt_id,
                    "source_name": item.source_name,
                    "source_url": item.source_url,
                    "dataset_id": item.artifact_id,
                    "source_period": item.source_period,
                    "landing_page": item.landing_page,
                    "retrieved_at": item.retrieved_at,
                    "source_modified": None,
                    "cache_status": item.custody_state,
                    "cache_freshness": f"Frozen for acquisition {acquisition.acquisition_id}",
                    "entity_scope": f"data-mcp:system:{item.system_slug}",
                    "query": {
                        "input_family": acquisition.input_family,
                        "statement_locator": item.statement_locator,
                        "http_status": item.http_status,
                    },
                    "cache_key": lineage[item.artifact_id]["uri"],
                    "match_basis": "source-local audited or filing-boundary candidate only",
                    "confidence": "blocked_pending_cross-system_comparability",
                    "caveat": item.entity_boundary,
                    "next_step": "Toolkit must preserve this as a non-approved candidate pending fitness review.",
                    "acquisition_method": "scale-input-family-connector.v1",
                    "rights_classification": item.rights_classification,
                    "row_locator": item.statement_locator,
                    "artifact": lineage[item.artifact_id],
                    "parent_receipt_ids": [],
                },
                "content_checksum": item.payload_sha256,
                "access_rights": item.rights_classification,
            }
        )
    observations = []
    for candidate in acquisition.candidates:
        if candidate.candidate_value is None:
            continue
        observation_id = f"observation:{candidate.system_slug}:{candidate.input_family}:candidate"
        observations.append(
            {
                "observation_id": observation_id,
                "measure_id": f"source_local_candidate.{candidate.input_family}",
                "value_type": "integer" if isinstance(candidate.candidate_value, int) else "number",
                "value": candidate.candidate_value,
                "unit": candidate.unit,
                "period": {"label": candidate.source_period},
                "denominator_scope": candidate.basis,
                "entity_ref": f"data-mcp:system:{candidate.system_slug}",
                "receipt_refs": [f"receipt:{ref}" for ref in candidate.source_artifact_refs],
                "derivation_class": "source_reported",
                "caveat": candidate.finding,
                "dependency_cluster_ids": [f"dependency:{ref}" for ref in candidate.source_artifact_refs],
            }
        )
    coverage = [
        {
            "coverage_id": f"coverage:{item.system_slug}:{item.input_family}",
            "entity_ref": f"data-mcp:system:{item.system_slug}",
            "measure_id": item.input_family,
            "status": item.missingness,
            "observation_refs": [],
            "reason": item.finding,
        }
        for item in acquisition.candidates
    ]
    conflicts = [
        {
            "conflict_id": f"conflict:{item.system_slug}:{item.input_family}:comparability",
            "conflict_type": "scale_input_comparability",
            "entity_refs": [f"data-mcp:system:{item.system_slug}"],
            "observation_refs": (
                [f"observation:{item.system_slug}:{item.input_family}:candidate"]
                if item.candidate_value is not None
                else []
            ),
            "receipt_refs": [f"receipt:{ref}" for ref in item.source_artifact_refs],
            "status": "open",
            "rationale": f"{','.join(item.blocker_codes)}: {item.finding}",
        }
        for item in acquisition.candidates
    ]
    return PublicEvidenceBundleInput.model_validate(
        {
            "bundle_id": f"{acquisition.acquisition_id}:public-evidence",
            "producer": {
                "repo": "healthcare-data-mcp",
                "version": acquisition.producer_version,
                "commit": producer_commit,
            },
            "created_at": acquisition.acquired_at,
            "request": {
                "workflow": acquisition.workflow_id,
                "parameters": {
                    "input_family": acquisition.input_family,
                    "no_scale_score": True,
                    "candidate_values_are_not_approved_inputs": True,
                },
            },
            "scope": {
                "systems": list(acquisition.systems),
                "market": {"name": "Philadelphia six-system Scale roster", "roster_frozen": True},
                "periods": sorted({item.source_period for item in acquisition.candidates}),
            },
            "entities": [
                {
                    "entity_id": f"data-mcp:system:{slug}",
                    "canonical_name": SYSTEM_NAMES[slug],
                    "entity_type": "health_system",
                    "aliases": [],
                    "match_decisions": [],
                    "conflicts": [],
                    "unresolved_identifiers": [],
                }
                for slug in acquisition.systems
            ],
            "observations": observations,
            "sources": sources,
            "coverage": coverage,
            "conflicts": conflicts,
            "input_artifacts": list(lineage.values()),
        }
    )


def verify_source_bytes(acquisition: ScaleInputFamilyAcquisition, cache_root: Path) -> None:
    """Require every recorded HTTP payload to exist in the disposable cache."""

    run_root = cache_root / acquisition.workflow_id / acquisition.acquisition_id
    for artifact in acquisition.source_artifacts:
        path = run_root / artifact.artifact_id
        raw = path.read_bytes()
        digest = f"sha256:{hashlib.sha256(raw).hexdigest()}"
        if len(raw) != artifact.content_length or digest != artifact.payload_sha256:
            raise ValueError(f"frozen source byte drift for {artifact.artifact_id}")
    for candidate in acquisition.candidates:
        extraction = candidate.extraction
        if extraction is None:
            continue
        artifact = next(item for item in acquisition.source_artifacts if item.artifact_id == extraction.artifact_ref)
        path = run_root / artifact.artifact_id
        pages = PdfReader(io.BytesIO(path.read_bytes())).pages
        if extraction.page_number > len(pages):
            raise ValueError(f"frozen extraction page drift for {candidate.system_slug}")
        page_text = pages[extraction.page_number - 1].extract_text() or ""
        required_markers = (
            extraction.period_marker,
            extraction.units_marker,
            extraction.definition_marker,
            extraction.basis_marker,
        )
        if any(marker not in page_text for marker in required_markers):
            raise ValueError(f"frozen extraction definition, basis, period, or units drift for {candidate.system_slug}")
        match = re.search(extraction.row_pattern, page_text, flags=re.MULTILINE)
        if match is None or "value" not in match.groupdict():
            raise ValueError(f"frozen extraction row drift for {candidate.system_slug}")
        raw_value = Decimal(match.group("value").replace(",", ""))
        if raw_value != Decimal(str(extraction.raw_value)):
            raise ValueError(f"frozen extraction raw value drift for {candidate.system_slug}")
        normalized = raw_value * Decimal(str(extraction.scale_multiplier))
        if normalized != Decimal(str(candidate.candidate_value)):
            raise ValueError(f"frozen extraction normalized value drift for {candidate.system_slug}")


def repository_top_level(source_path: Path) -> Path:
    """Resolve the Git checkout containing the executing source module."""

    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=source_path if source_path.is_dir() else source_path.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(result.stdout.strip()).resolve()


def require_clean_repository(repository_root: Path) -> None:
    """Reject rebuilds from a dirty or non-Git source tree."""

    result = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        raise ValueError("Scale acquisition rebuild requires a clean Git source tree")


def require_repository_commit(repository_root: Path, expected_commit: str) -> None:
    """Bind execution to the exact declared producer checkout."""

    if re.fullmatch(r"[0-9a-f]{40}", expected_commit) is None:
        raise ValueError("source commit must be a full lowercase Git SHA")
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout.strip() != expected_commit:
        raise ValueError("Scale acquisition source commit drift")


def require_outputs_outside_repository(repository_root: Path, outputs: list[Path]) -> None:
    """Keep clean-tree rebuild products outside the pinned source export."""

    root = repository_root.resolve()
    if any(path.resolve().is_relative_to(root) for path in outputs):
        raise ValueError("Scale acquisition rebuild outputs must be outside the source repository")


def iso_datetime(value: str) -> datetime:
    """Parse fixture timestamps while retaining timezone awareness."""

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed


__all__ = [
    "SCALE_INPUT_FAMILIES",
    "SYSTEM_SLUGS",
    "ScaleInputFamilyAcquisition",
    "build_acquisition",
    "build_public_evidence_input",
    "semantic_hash",
    "require_clean_repository",
    "require_outputs_outside_repository",
    "require_repository_commit",
    "repository_top_level",
    "verify_source_bytes",
]
