"""Tabular Scale input-family acquisition contract and v1 bundle adapter.

The original ``scale_input_family`` contract is intentionally specific to
audited/PDF financial evidence.  This additive v2 contract preserves exact
source-local CSV rows without calling them audited or promoting their values to
Scale inputs.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping, Self, Sequence

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from shared.acquisition.scale_system_roster import (
    SYSTEM_AHRQ_IDENTITIES,
    SYSTEM_NAMES,
    SYSTEM_SLUGS,
)
from shared.contracts.public_evidence import PublicEvidenceBundleInput, canonical_sha256

SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
PROHIBITED_OUTPUTS = {
    "scale_score",
    "component_score",
    "sensitivity_result",
    "projection",
    "adjudication",
    "recommendation",
    "promotion",
}
COMMON_BLOCKERS = {
    "source_vintage_2023",
    "highest_ownership_boundary_unreviewed",
    "current_roster_membership_unresolved",
    "utilization_denominator_undefined",
    "payer_setting_scope_undefined",
    "rehabilitation_inclusion_unreviewed",
    "shared_ccn_treatment_unreviewed",
    "technical_definition_not_receipted",
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True, slots=True)
class ExpectedLinkageRow:
    """Immutable annual declaration for one exact source linkage row."""

    system_slug: str
    artifact_ref: str
    row_number: int
    source_row_sha256: str
    compendium_hospital_id: str
    ccn: str
    hospital_name: str
    acutehosp_flag_raw: str
    health_sys_id: str
    health_sys_name: str
    hos_dsch_raw: str


LinkageRowIdentity = tuple[
    str,
    str,
    int,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
    str,
]


class ValidatedCacheReceipt(StrictModel):
    dataset_id: Literal["ahrq_health_system_compendium"]
    dataset_artifact_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    artifact_role: Literal["silver"]
    cache_status: Literal["ready"]
    retrieved_at: AwareDatetime
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    manifest_content_length: int = Field(ge=1)
    validation_status: Literal["pass"]
    loader_version: str = Field(min_length=1)
    validator_version: str = Field(min_length=1)
    etag: Literal[""] = ""
    last_modified: Literal[""] = ""
    source_period_metadata: Literal[
        "Source-declared period, retrieved_at, or modified timestamp."
    ]


class TabularSourceArtifact(StrictModel):
    artifact_id: str = Field(min_length=1)
    relative_path: str = Field(pattern=r"^[A-Za-z0-9._-]+$")
    source_name: str = Field(min_length=1)
    dataset_id: Literal["ahrq_health_system_compendium"]
    source_url: str = Field(pattern=r"^https://www\.ahrq\.gov/")
    landing_page: str = Field(pattern=r"^https://www\.ahrq\.gov/")
    source_period: Literal["2023"]
    cache_promoted_at: AwareDatetime
    media_type: Literal["text/csv"]
    encoding: Literal["cp1252"]
    payload_sha256: str = Field(pattern=SHA256_PATTERN)
    content_length: int = Field(ge=1)
    row_count: int = Field(ge=1)
    schema_fingerprint: str = Field(pattern=SHA256_PATTERN)
    header_sha256: str = Field(pattern=SHA256_PATTERN)
    validation_status: Literal["pass"]
    source_quality: Literal["validated_official_tabular_snapshot"]
    rights_classification: Literal["unknown_review_required"]


class SystemRowExtraction(StrictModel):
    system_slug: str
    artifact_ref: str = Field(min_length=1)
    row_number: int = Field(ge=2)
    source_row_sha256: str = Field(pattern=SHA256_PATTERN)
    row_key_column: Literal["health_sys_id"] = "health_sys_id"
    health_sys_id: str = Field(pattern=r"^HSI[0-9]{8}$")
    health_sys_name: str = Field(min_length=1)
    health_sys_city: str = Field(min_length=1)
    health_sys_state: str = Field(pattern=r"^[A-Z]{2}$")
    value_column: Literal["sys_dsch"] = "sys_dsch"
    raw_lexical_value: str = Field(pattern=r"^[0-9]+$")
    declared_type: Literal["integer"] = "integer"
    hosp_cnt_raw: str = Field(pattern=r"^[0-9]+$")
    acutehosp_cnt_raw: str = Field(pattern=r"^[0-9]+$")
    sys_multistate_raw: str = Field(pattern=r"^[0-9]+$")


class LinkageContextRow(StrictModel):
    system_slug: str
    artifact_ref: str = Field(min_length=1)
    row_number: int = Field(ge=2)
    source_row_sha256: str = Field(pattern=SHA256_PATTERN)
    compendium_hospital_id: str = Field(pattern=r"^CHSP[0-9]{8}$")
    ccn: str = Field(pattern=r"^[0-9]{6}$")
    hospital_name: str = Field(min_length=1)
    acutehosp_flag_raw: Literal["0", "1"]
    health_sys_id: str = Field(pattern=r"^HSI[0-9]{8}$")
    health_sys_name: str = Field(min_length=1)
    hos_dsch_raw: str = Field(pattern=r"^(?:[0-9]+)?$")


class TabularScaleInputCandidate(StrictModel):
    system_slug: str
    input_family: Literal["annual_discharges"]
    candidate_value: int = Field(ge=0)
    unit: Literal["discharges"] = "discharges"
    source_period: Literal["2023"] = "2023"
    definition: Literal[
        "AHRQ Compendium 2023 system-file column sys_dsch; technical definition not locally receipted"
    ]
    basis: Literal[
        "source-local AHRQ highest-ownership system row; denominator, setting, payer, rehabilitation, and shared-CCN treatment unapproved"
    ]
    source_artifact_refs: list[str] = Field(min_length=2, max_length=2)
    system_row_ref: str = Field(min_length=1)
    linkage_row_refs: list[str] = Field(min_length=1)
    missingness: Literal["blocked_source_conflict"] = "blocked_source_conflict"
    blocker_codes: list[str] = Field(min_length=8)
    finding: str = Field(min_length=1)
    imputed: Literal[False] = False
    aggregated: Literal[False] = False
    fabricated_zero: Literal[False] = False
    approved_for_scale: Literal[False] = False


class TabularScaleInputFamilyAcquisition(StrictModel):
    schema_version: Literal["ushso.scale-tabular-input-family-acquisition.v2"] = (
        "ushso.scale-tabular-input-family-acquisition.v2"
    )
    acquisition_id: str = Field(min_length=1)
    workflow_id: Literal["scale-tabular-input-family-acquisition.v2"] = (
        "scale-tabular-input-family-acquisition.v2"
    )
    input_family: Literal["annual_discharges"]
    systems: list[str]
    acquired_at: AwareDatetime
    producer_version: str = Field(min_length=1)
    cache_receipt: ValidatedCacheReceipt
    source_artifacts: list[TabularSourceArtifact] = Field(min_length=2, max_length=2)
    system_rows: list[SystemRowExtraction] = Field(min_length=6, max_length=6)
    linkage_rows: list[LinkageContextRow] = Field(min_length=1)
    candidates: list[TabularScaleInputCandidate] = Field(min_length=6, max_length=6)
    technical_definition_receipt: Literal[None] = None
    technical_definition_custody: Literal["not_locally_receipted"] = "not_locally_receipted"
    raw_http_receipt_custody: Literal["not_locally_receipted"] = "not_locally_receipted"
    redistribution_license_receipt: Literal[None] = None
    redistribution_rights_custody: Literal["unreviewed"] = "unreviewed"
    prohibited_outputs: list[str] = Field(min_length=1)
    acquisition_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_graph_and_hash(self) -> Self:
        if tuple(self.systems) != SYSTEM_SLUGS:
            raise ValueError("acquisition must preserve exact frozen six-system order")
        if [item.system_slug for item in self.system_rows] != list(SYSTEM_SLUGS):
            raise ValueError("system rows must preserve exact frozen six-system order")
        if [item.system_slug for item in self.candidates] != list(SYSTEM_SLUGS):
            raise ValueError("candidates must preserve exact frozen six-system order")

        artifacts = {item.artifact_id: item for item in self.source_artifacts}
        if len(artifacts) != 2:
            raise ValueError("tabular acquisition requires exact system and linkage artifacts")
        paths = {item.relative_path for item in self.source_artifacts}
        if paths != {"ahrq_system_2023.csv", "ahrq_hospital_linkage_2023.csv"}:
            raise ValueError("tabular acquisition source artifact set drift")

        row_refs = {_system_row_ref(item): item for item in self.system_rows}
        if len(row_refs) != 6:
            raise ValueError("duplicate system row reference")
        linkage_refs = {_linkage_row_ref(item): item for item in self.linkage_rows}
        if len(linkage_refs) != len(self.linkage_rows):
            raise ValueError("duplicate linkage row reference")
        if len({item.row_number for item in self.system_rows}) != 6:
            raise ValueError("duplicate system row number")
        if len({item.row_number for item in self.linkage_rows}) != len(self.linkage_rows):
            raise ValueError("duplicate linkage row number")

        for system_row in self.system_rows:
            if system_row.system_slug not in SYSTEM_SLUGS:
                raise ValueError("source row is outside the frozen six-system roster")
            if system_row.artifact_ref not in artifacts:
                raise ValueError("unknown source artifact reference")
            expected_identity = SYSTEM_AHRQ_IDENTITIES[system_row.system_slug]
            if (
                system_row.health_sys_id != expected_identity.health_sys_id
                or system_row.health_sys_name != expected_identity.source_name
            ):
                raise ValueError("system row product-to-AHRQ identity substitution")
        for linkage_row in self.linkage_rows:
            if linkage_row.system_slug not in SYSTEM_SLUGS:
                raise ValueError("source row is outside the frozen six-system roster")
            if linkage_row.artifact_ref not in artifacts:
                raise ValueError("unknown source artifact reference")
            expected_identity = SYSTEM_AHRQ_IDENTITIES[linkage_row.system_slug]
            if (
                linkage_row.health_sys_id != expected_identity.health_sys_id
                or linkage_row.health_sys_name != expected_identity.source_name
            ):
                raise ValueError("linkage row product-to-AHRQ identity substitution")
        system_artifact = next(item.artifact_id for item in self.source_artifacts if item.relative_path == "ahrq_system_2023.csv")
        linkage_artifact = next(
            item.artifact_id for item in self.source_artifacts if item.relative_path == "ahrq_hospital_linkage_2023.csv"
        )
        if any(item.artifact_ref != system_artifact for item in self.system_rows):
            raise ValueError("system row must reference the system CSV")
        if any(item.artifact_ref != linkage_artifact for item in self.linkage_rows):
            raise ValueError("linkage row must reference the linkage CSV")

        for candidate in self.candidates:
            candidate_system_row = row_refs.get(candidate.system_row_ref)
            if candidate_system_row is None or candidate_system_row.system_slug != candidate.system_slug:
                raise ValueError("candidate system row reference drift")
            linked = [linkage_refs.get(ref) for ref in candidate.linkage_row_refs]
            if any(item is None or item.system_slug != candidate.system_slug for item in linked):
                raise ValueError("candidate linkage row reference drift")
            expected_linkage_refs = {
                ref for ref, item in linkage_refs.items() if item.system_slug == candidate.system_slug
            }
            if len(candidate.linkage_row_refs) != len(set(candidate.linkage_row_refs)) or set(
                candidate.linkage_row_refs
            ) != expected_linkage_refs:
                raise ValueError("candidate must preserve every exact source-local linkage row")
            if set(candidate.source_artifact_refs) != {system_artifact, linkage_artifact}:
                raise ValueError("candidate must preserve both system and linkage receipts")
            if candidate.candidate_value != int(candidate_system_row.raw_lexical_value):
                raise ValueError("candidate value must equal exact sys_dsch lexical value")
            if not COMMON_BLOCKERS.issubset(candidate.blocker_codes):
                raise ValueError("candidate weakened mandatory comparability blockers")
            if len(candidate.blocker_codes) != len(set(candidate.blocker_codes)):
                raise ValueError("candidate blocker codes must be unique")

        if set(self.prohibited_outputs) != PROHIBITED_OUTPUTS:
            raise ValueError("all no-execution prohibitions must remain explicit")
        expected = semantic_hash(self, "acquisition_sha256")
        if self.acquisition_sha256 != expected:
            raise ValueError("acquisition_sha256 does not match canonical content")
        return self


def _system_row_ref(row: SystemRowExtraction) -> str:
    return f"row:system:{row.health_sys_id}:{row.row_number}"


def _linkage_row_ref(row: LinkageContextRow) -> str:
    return f"row:linkage:{row.compendium_hospital_id}:{row.row_number}"


def linkage_row_identity(row: LinkageContextRow | ExpectedLinkageRow) -> LinkageRowIdentity:
    """Return the exact immutable identity/content tuple for a linkage row."""

    return (
        row.system_slug,
        row.artifact_ref,
        row.row_number,
        row.source_row_sha256,
        row.compendium_hospital_id,
        row.ccn,
        row.hospital_name,
        row.acutehosp_flag_raw,
        row.health_sys_id,
        row.health_sys_name,
        row.hos_dsch_raw,
    )


def semantic_hash(value: BaseModel, hash_field: str) -> str:
    return canonical_sha256(value.model_dump(mode="json", exclude={hash_field}))


def build_tabular_acquisition(payload: Mapping[str, object]) -> TabularScaleInputFamilyAcquisition:
    body = dict(payload)
    body.pop("acquisition_sha256", None)
    body["acquisition_sha256"] = canonical_sha256(body)
    return TabularScaleInputFamilyAcquisition.model_validate(body)


def build_tabular_public_evidence_input(
    acquisition: TabularScaleInputFamilyAcquisition,
    *,
    producer_commit: str = "0" * 40,
) -> PublicEvidenceBundleInput:
    """Adapt exact tabular rows into unchanged Public Evidence Bundle v1."""

    if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    artifacts = {item.artifact_id: item for item in acquisition.source_artifacts}
    lineage = {
        item.artifact_id: {
            "artifact_id": item.artifact_id,
            "checksum_sha256": item.payload_sha256,
            "media_type": item.media_type,
            "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{item.relative_path}",
            "cache_run_id": acquisition.cache_receipt.run_id,
            "connector": "validated-cache-manifest",
            "connector_version": "scale-tabular-input-family-connector.v2",
            "parser_version": "scale-tabular-input-family-parser.v2",
            "schema_fingerprint": item.schema_fingerprint,
        }
        for item in acquisition.source_artifacts
    }
    system_rows = {item.system_slug: item for item in acquisition.system_rows}
    linkage_by_system = {
        slug: [item for item in acquisition.linkage_rows if item.system_slug == slug]
        for slug in acquisition.systems
    }
    sources: list[dict[str, object]] = []
    for slug in acquisition.systems:
        system_row = system_rows[slug]
        linked = linkage_by_system[slug]
        system_source_artifact = artifacts[system_row.artifact_ref]
        linkage_source_artifact = next(
            item
            for item in acquisition.source_artifacts
            if item.relative_path == "ahrq_hospital_linkage_2023.csv"
        )
        for kind, artifact, locator, query in (
            (
                "system",
                system_source_artifact,
                f"row={system_row.row_number}; health_sys_id={system_row.health_sys_id}; column=sys_dsch; raw={system_row.raw_lexical_value}",
                {
                    "row_number": system_row.row_number,
                    "row_sha256": system_row.source_row_sha256,
                    "row_key_column": system_row.row_key_column,
                    "row_key_value": system_row.health_sys_id,
                    "value_column": system_row.value_column,
                    "raw_lexical_value": system_row.raw_lexical_value,
                    "declared_type": system_row.declared_type,
                    "cache_promoted_at": system_source_artifact.cache_promoted_at.isoformat(),
                },
            ),
            (
                "linkage",
                linkage_source_artifact,
                f"health_sys_id={system_row.health_sys_id}; exact_rows={','.join(str(item.row_number) for item in linked)}",
                {
                    "health_sys_id": system_row.health_sys_id,
                    "row_numbers": [item.row_number for item in linked],
                    "row_sha256": [item.source_row_sha256 for item in linked],
                    "ccns": [item.ccn for item in linked],
                    "no_facility_aggregation": True,
                    "cache_promoted_at": linkage_source_artifact.cache_promoted_at.isoformat(),
                },
            ),
        ):
            receipt_id = f"receipt:{slug}:annual-discharges:{kind}"
            sources.append(
                {
                    "source_id": f"source:{slug}:annual-discharges:{kind}",
                    "registry_id": f"ahrq-compendium-2023:{kind}",
                    "registry_version": "2023",
                    "receipt": {
                        "receipt_id": receipt_id,
                        "source_name": artifact.source_name,
                        "source_url": artifact.source_url,
                        "dataset_id": artifact.dataset_id,
                        "source_period": artifact.source_period,
                        "landing_page": artifact.landing_page,
                        "retrieved_at": acquisition.cache_receipt.retrieved_at,
                        "source_modified": None,
                        "cache_status": "validated_frozen_snapshot",
                        "cache_freshness": "Frozen 2023 release; current-roster comparability blocked",
                        "entity_scope": f"data-mcp:system:{slug}",
                        "query": query,
                        "cache_key": lineage[artifact.artifact_id]["uri"],
                        "match_basis": "exact source-local tabular row; not an approved Scale input",
                        "confidence": "blocked_pending_definition_boundary_and_roster_review",
                        "caveat": "No locally receipted official technical definition for sys_dsch; no facility aggregation performed.",
                        "next_step": "Toolkit must retain the candidate and every open blocker pending utilization fitness review.",
                        "acquisition_method": "scale-tabular-input-family-connector.v2",
                        "rights_classification": artifact.rights_classification,
                        "row_locator": locator,
                        "artifact": lineage[artifact.artifact_id],
                        "parent_receipt_ids": [],
                    },
                    "content_checksum": artifact.payload_sha256,
                    "access_rights": artifact.rights_classification,
                }
            )
    candidates = {item.system_slug: item for item in acquisition.candidates}
    observations = [
        {
            "observation_id": f"observation:{slug}:annual-discharges:candidate",
            "measure_id": "source_local_candidate.annual_discharges",
            "value_type": "integer",
            "value": candidates[slug].candidate_value,
            "unit": candidates[slug].unit,
            "period": {"label": candidates[slug].source_period},
            "denominator_scope": candidates[slug].basis,
            "entity_ref": f"data-mcp:system:{slug}",
            "receipt_refs": [
                f"receipt:{slug}:annual-discharges:system",
                f"receipt:{slug}:annual-discharges:linkage",
            ],
            "derivation_class": "source_reported",
            "caveat": candidates[slug].finding,
            "dependency_cluster_ids": [
                f"dependency:{candidates[slug].system_row_ref}",
                *[f"dependency:{ref}" for ref in candidates[slug].linkage_row_refs],
            ],
        }
        for slug in acquisition.systems
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
                    "no_facility_aggregation": True,
                    "raw_http_receipt_available": False,
                    "redistribution_rights_reviewed": False,
                    "technical_definition_receipted": False,
                },
            },
            "scope": {
                "systems": list(acquisition.systems),
                "market": {"name": "Philadelphia six-system Scale roster", "roster_frozen": True},
                "periods": ["2023"],
            },
            "entities": [
                {
                    "entity_id": f"data-mcp:system:{slug}",
                    "canonical_name": SYSTEM_NAMES[slug],
                    "entity_type": "health_system",
                    "ahrq_system_id": system_rows[slug].health_sys_id,
                    "aliases": [
                        {
                            "source_name": "AHRQ Compendium 2023 system file",
                            "name": system_rows[slug].health_sys_name,
                            "identifier": system_rows[slug].health_sys_id,
                            "identifier_type": "ahrq_health_sys_id",
                        }
                    ],
                    "match_decisions": [],
                    "conflicts": [],
                    "unresolved_identifiers": [],
                }
                for slug in acquisition.systems
            ],
            "observations": observations,
            "sources": sources,
            "coverage": [
                {
                    "coverage_id": f"coverage:{slug}:annual_discharges",
                    "entity_ref": f"data-mcp:system:{slug}",
                    "measure_id": "annual_discharges",
                    "status": "blocked_source_conflict",
                    "observation_refs": [],
                    "reason": candidates[slug].finding,
                }
                for slug in acquisition.systems
            ],
            "conflicts": [
                {
                    "conflict_id": f"conflict:{slug}:annual-discharges:comparability",
                    "conflict_type": "scale_input_comparability",
                    "entity_refs": [f"data-mcp:system:{slug}"],
                    "observation_refs": [f"observation:{slug}:annual-discharges:candidate"],
                    "receipt_refs": [
                        f"receipt:{slug}:annual-discharges:system",
                        f"receipt:{slug}:annual-discharges:linkage",
                    ],
                    "status": "open",
                    "rationale": f"{','.join(candidates[slug].blocker_codes)}: {candidates[slug].finding}",
                }
                for slug in acquisition.systems
            ],
            "input_artifacts": list(lineage.values()),
        }
    )


def verify_tabular_source_bytes(
    acquisition: TabularScaleInputFamilyAcquisition,
    cache_root: Path,
    *,
    expected_linkage_rows: Sequence[ExpectedLinkageRow] | None = None,
) -> None:
    """Verify manifest, both full CSVs, and every frozen exact row."""

    manifest_path = cache_root / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    manifest_raw = manifest_path.read_bytes()
    if len(manifest_raw) != acquisition.cache_receipt.manifest_content_length or _sha256(manifest_raw) != acquisition.cache_receipt.manifest_sha256:
        raise ValueError("validated cache manifest byte drift")
    manifest = json.loads(manifest_raw)
    if not isinstance(manifest, dict):
        raise ValueError("validated cache manifest shape drift")
    expected_manifest = acquisition.cache_receipt
    for key, expected in (
        ("dataset_id", expected_manifest.dataset_id),
        ("artifact_id", expected_manifest.dataset_artifact_id),
        ("run_id", expected_manifest.run_id),
        ("artifact_role", expected_manifest.artifact_role),
        ("cache_status", expected_manifest.cache_status),
        ("validation_status", expected_manifest.validation_status),
        ("loader_version", expected_manifest.loader_version),
        ("validator_version", expected_manifest.validator_version),
        ("etag", expected_manifest.etag),
        ("last_modified", expected_manifest.last_modified),
        ("source_period", expected_manifest.source_period_metadata),
    ):
        if manifest.get(key) != expected:
            raise ValueError(f"validated cache manifest {key} drift")
    manifest_artifacts = {
        item["relative_path"]: item
        for item in manifest.get("artifacts", [])
        if isinstance(item, dict) and isinstance(item.get("relative_path"), str)
    }

    resolved: dict[str, Path] = {}
    for artifact in acquisition.source_artifacts:
        entry = manifest_artifacts.get(artifact.relative_path)
        if entry is None:
            raise ValueError("validated cache manifest artifact drift")
        artifact_expected: tuple[tuple[str, object], ...] = (
            ("checksum_sha256", artifact.payload_sha256.removeprefix("sha256:")),
            ("content_length", artifact.content_length),
            ("row_count", artifact.row_count),
            ("schema_fingerprint", artifact.schema_fingerprint.removeprefix("sha256:")),
            ("source_url", artifact.source_url),
            ("validation_status", artifact.validation_status),
            ("promoted_at", artifact.cache_promoted_at.isoformat()),
        )
        for key, artifact_expected_value in artifact_expected:
            if entry.get(key) != artifact_expected_value:
                raise ValueError(f"validated source artifact {key} drift")
        path_value = entry.get("path")
        if not isinstance(path_value, str):
            raise ValueError("validated source artifact path missing")
        path = Path(path_value).resolve()
        if not path.is_relative_to(cache_root.resolve()):
            raise ValueError("validated source artifact escaped cache root")
        raw = path.read_bytes()
        if len(raw) != artifact.content_length or _sha256(raw) != artifact.payload_sha256:
            raise ValueError(f"frozen tabular source byte drift for {artifact.artifact_id}")
        lines = raw.splitlines(keepends=True)
        if _sha256(lines[0]) != artifact.header_sha256:
            raise ValueError(f"frozen tabular source header drift for {artifact.artifact_id}")
        resolved[artifact.artifact_id] = path

    if expected_linkage_rows is not None:
        _verify_complete_linkage_source_slice(
            acquisition,
            resolved,
            expected_linkage_rows,
        )
    _verify_rows(acquisition.system_rows, resolved, _verify_system_row)
    _verify_rows(acquisition.linkage_rows, resolved, _verify_linkage_row)


def _verify_complete_linkage_source_slice(
    acquisition: TabularScaleInputFamilyAcquisition,
    paths: dict[str, Path],
    expected_rows: Sequence[ExpectedLinkageRow],
) -> None:
    expected = tuple(linkage_row_identity(item) for item in expected_rows)
    declared = tuple(linkage_row_identity(item) for item in acquisition.linkage_rows)
    if declared != expected:
        raise ValueError("annual acquisition must preserve the exact complete frozen linkage row set")
    if not expected_rows:
        raise ValueError("expected linkage row declaration cannot be empty")
    artifact_refs = {item.artifact_ref for item in expected_rows}
    if len(artifact_refs) != 1:
        raise ValueError("expected linkage rows must use one frozen linkage artifact")
    artifact_ref = next(iter(artifact_refs))
    path = paths.get(artifact_ref)
    if path is None:
        raise ValueError("expected linkage artifact is not present in verified source custody")

    inverse_identity = {
        identity.health_sys_id: (slug, identity.source_name)
        for slug, identity in SYSTEM_AHRQ_IDENTITIES.items()
    }
    raw_lines = path.read_bytes().splitlines(keepends=True)
    derived: list[ExpectedLinkageRow] = []
    with path.open(newline="", encoding="cp1252") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            health_sys_id = row.get("health_sys_id", "")
            identity = inverse_identity.get(health_sys_id)
            if identity is None:
                continue
            system_slug, expected_source_name = identity
            source_name = row.get("health_sys_name", "")
            if source_name != expected_source_name:
                raise ValueError("full linkage source slice contains an AHRQ source-name substitution")
            derived.append(
                ExpectedLinkageRow(
                    system_slug=system_slug,
                    artifact_ref=artifact_ref,
                    row_number=row_number,
                    source_row_sha256=_sha256(raw_lines[row_number - 1]),
                    compendium_hospital_id=row.get("compendium_hospital_id", ""),
                    ccn=row.get("ccn", ""),
                    hospital_name=row.get("hospital_name", ""),
                    acutehosp_flag_raw=row.get("acutehosp_flag", ""),
                    health_sys_id=health_sys_id,
                    health_sys_name=source_name,
                    hos_dsch_raw=row.get("hos_dsch", ""),
                )
            )
    if tuple(linkage_row_identity(item) for item in derived) != expected:
        raise ValueError("full verified CSV does not equal the exact complete frozen linkage row set")


def _verify_rows(
    rows: list[SystemRowExtraction] | list[LinkageContextRow],
    paths: dict[str, Path],
    verifier: Callable[[SystemRowExtraction | LinkageContextRow, dict[str, str]], None],
) -> None:
    by_artifact: dict[str, list[SystemRowExtraction | LinkageContextRow]] = {}
    for row in rows:
        by_artifact.setdefault(row.artifact_ref, []).append(row)
    for artifact_ref, expected_rows in by_artifact.items():
        path = paths[artifact_ref]
        raw_lines = path.read_bytes().splitlines(keepends=True)
        with path.open(newline="", encoding="cp1252") as handle:
            parsed_rows = {number: item for number, item in enumerate(csv.DictReader(handle), start=2)}
        for row in expected_rows:
            if row.row_number > len(raw_lines):
                raise ValueError("frozen tabular row number drift")
            if _sha256(raw_lines[row.row_number - 1]) != row.source_row_sha256:
                raise ValueError("frozen tabular source row byte drift")
            parsed = parsed_rows.get(row.row_number)
            if parsed is None:
                raise ValueError("frozen tabular parsed row drift")
            verifier(row, parsed)


def _verify_system_row(expected: SystemRowExtraction | LinkageContextRow, row: dict[str, str]) -> None:
    if not isinstance(expected, SystemRowExtraction):
        raise TypeError("system row verifier received linkage row")
    fields = {
        "health_sys_id": expected.health_sys_id,
        "health_sys_name": expected.health_sys_name,
        "health_sys_city": expected.health_sys_city,
        "health_sys_state": expected.health_sys_state,
        "sys_dsch": expected.raw_lexical_value,
        "hosp_cnt": expected.hosp_cnt_raw,
        "acutehosp_cnt": expected.acutehosp_cnt_raw,
        "sys_multistate": expected.sys_multistate_raw,
    }
    if any(row.get(key) != value for key, value in fields.items()):
        raise ValueError(f"frozen system row key, identity, column, or lexical value drift for {expected.system_slug}")


def _verify_linkage_row(expected: SystemRowExtraction | LinkageContextRow, row: dict[str, str]) -> None:
    if not isinstance(expected, LinkageContextRow):
        raise TypeError("linkage row verifier received system row")
    fields = {
        "compendium_hospital_id": expected.compendium_hospital_id,
        "ccn": expected.ccn,
        "hospital_name": expected.hospital_name,
        "acutehosp_flag": expected.acutehosp_flag_raw,
        "health_sys_id": expected.health_sys_id,
        "health_sys_name": expected.health_sys_name,
        "hos_dsch": expected.hos_dsch_raw,
    }
    if any(row.get(key) != value for key, value in fields.items()):
        raise ValueError(f"frozen linkage row identity or lexical value drift for {expected.compendium_hospital_id}")


def _sha256(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


__all__ = [
    "COMMON_BLOCKERS",
    "ExpectedLinkageRow",
    "LinkageRowIdentity",
    "LinkageContextRow",
    "SystemRowExtraction",
    "TabularScaleInputCandidate",
    "TabularScaleInputFamilyAcquisition",
    "TabularSourceArtifact",
    "ValidatedCacheReceipt",
    "build_tabular_acquisition",
    "build_tabular_public_evidence_input",
    "linkage_row_identity",
    "semantic_hash",
    "verify_tabular_source_bytes",
]
