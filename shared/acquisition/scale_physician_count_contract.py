"""Immutable v3 contract for source-local physician-count candidates."""

from __future__ import annotations

from typing import Any, Literal, Mapping, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode

from shared.acquisition.scale_physician_count_declaration import (
    EXTRA_BLOCKERS,
    FINDINGS,
    SYSTEM_ROW_DECLARATIONS,
)
from shared.acquisition.scale_system_roster import SYSTEM_AHRQ_IDENTITIES, SYSTEM_SLUGS
from shared.acquisition.scale_tabular_input_family import (
    TabularSourceArtifact,
    ValidatedCacheReceipt,
)
from shared.contracts.public_evidence import canonical_sha256

SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
PROHIBITED_OUTPUTS = (
    "adjudication",
    "component_score",
    "projection",
    "promotion",
    "recommendation",
    "scale_score",
    "sensitivity_result",
)
PHYSICIAN_BLOCKERS = {
    "source_vintage_2023",
    "highest_ownership_boundary_unreviewed",
    "current_roster_membership_unresolved",
    "physician_definition_not_receipted",
    "employed_affiliated_total_basis_unresolved",
    "active_status_unresolved",
    "duplicate_physician_treatment_unresolved",
    "source_system_variation_unresolved",
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PriorAnnualToolkitLineage(StrictModel):
    """Exact completed no-go that precedes this serial family."""

    repo: Literal["healthcare-toolkit"] = "healthcare-toolkit"
    input_family: Literal["annual_discharges"] = "annual_discharges"
    binding_merge: Literal["76e16247-cecc-e818-d777-b4a4ade56dc1-3dd7b2a8"]
    binding_tracker_merge: Literal["420d35d8-024d-e1c4-84c1-b16128836e0f-8b00375c"]
    admission_merge: Literal["9aed9059-962c-bf2a-03c7-c02e6056aee4-281ee340"]
    tracker_merge: Literal["2d33cab9-264e-636b-d392-b89757f8b05e-d2729ecb"]
    cumulative_packet_sha256: Literal[
        "sha256:bb569b3dde1fa4435c658488b11493ebcfe88898f8d7b0571231ce66ca7621a6"
    ]
    cumulative_review_sha256: Literal[
        "sha256:b83433afce89012b9584c8a5df4449e78f112916fd2db4894495f4e6b1bcf1d6"
    ]
    cumulative_assurance_sha256: Literal[
        "sha256:4caa86f1c57a8ce45cc3df304bd4f03e841f563418db1babfe39668343ff5cf1"
    ]
    terminal_status: Literal["blocked"] = "blocked"
    failure_code: Literal["human_review_required"] = "human_review_required"

    @model_validator(mode="after")
    def preserve_exact_predecessor_commits(self) -> Self:
        expected = {
            "binding_merge": "76e16247cecce818d777b4a4ade56dc13dd7b2a8",  # pragma: allowlist secret
            "binding_tracker_merge": "420d35d8024de1c484c1b16128836e0f8b00375c",  # pragma: allowlist secret
            "admission_merge": "9aed9059962cbf2a03c7c02e6056aee4281ee340",  # pragma: allowlist secret
            "tracker_merge": "2d33cab9264e636bd392b89757f8b05ed2729ecb",  # pragma: allowlist secret
        }
        if any(getattr(self, field).replace("-", "") != commit for field, commit in expected.items()):
            raise ValueError("prior annual Toolkit commit lineage drift")
        return self


class PhysicianSystemRow(StrictModel):
    system_slug: str
    artifact_ref: str = Field(min_length=1)
    row_number: int = Field(ge=2)
    source_row_sha256: str = Field(pattern=SHA256_PATTERN)
    row_key_column: Literal["health_sys_id"] = "health_sys_id"
    health_sys_id: str = Field(pattern=r"^HSI[0-9]{8}$")
    health_sys_name: str = Field(min_length=1)
    health_sys_city: str = Field(min_length=1)
    health_sys_state: str = Field(pattern=r"^[A-Z]{2}$")
    value_column: Literal["total_mds"] = "total_mds"
    raw_lexical_value: str = Field(pattern=r"^[1-9][0-9]*$")
    declared_type: Literal["integer"] = "integer"
    prim_care_mds_raw: str = Field(pattern=r"^[0-9]+$")
    hosp_cnt_raw: str = Field(pattern=r"^[0-9]+$")
    acutehosp_cnt_raw: str = Field(pattern=r"^[0-9]+$")
    sys_multistate_raw: str = Field(pattern=r"^[0-9]+$")


class PhysicianCountCandidate(StrictModel):
    system_slug: str
    input_family: Literal["physician_count"] = "physician_count"
    candidate_value: int = Field(ge=1)
    unit: Literal["physicians"] = "physicians"
    source_period: Literal["2023"] = "2023"
    definition: Literal[
        "AHRQ Compendium 2023 system-file column total_mds; official technical definition not locally receipted"
    ]
    basis: Literal[
        "source-local AHRQ highest-ownership system row; employed, affiliated, active-status, specialty, and deduplication bases unapproved"
    ]
    source_artifact_refs: list[str] = Field(min_length=1, max_length=1)
    system_row_ref: str = Field(min_length=1)
    missingness: Literal["blocked_source_conflict"] = "blocked_source_conflict"
    blocker_codes: list[str] = Field(min_length=8)
    finding: str = Field(min_length=1)
    imputed: Literal[False] = False
    aggregated: Literal[False] = False
    fabricated_zero: Literal[False] = False
    approved_for_scale: Literal[False] = False


class PhysicianCountAcquisition(StrictModel):
    schema_version: Literal["ushso.scale-physician-count-acquisition.v3"] = (
        "ushso.scale-physician-count-acquisition.v3"
    )
    acquisition_id: Literal["scale-physician-count-all-six-2026-07-18"]
    workflow_id: Literal["scale-physician-count-acquisition.v3"] = (
        "scale-physician-count-acquisition.v3"
    )
    input_family: Literal["physician_count"] = "physician_count"
    systems: list[str]
    acquired_at: AwareDatetime
    producer_version: Literal["HDM-auf"]
    prior_cycle: PriorAnnualToolkitLineage
    cache_receipt: ValidatedCacheReceipt
    source_artifacts: list[TabularSourceArtifact] = Field(min_length=1, max_length=1)
    system_rows: list[PhysicianSystemRow] = Field(min_length=6, max_length=6)
    candidates: list[PhysicianCountCandidate] = Field(min_length=6, max_length=6)
    physician_definition_receipt: Literal[None] = None
    physician_definition_custody: Literal["not_locally_receipted"] = "not_locally_receipted"
    raw_http_receipt_custody: Literal["not_locally_receipted"] = "not_locally_receipted"
    redistribution_license_receipt: Literal[None] = None
    redistribution_rights_custody: Literal["unreviewed"] = "unreviewed"
    prohibited_outputs: list[str] = Field(min_length=1)
    acquisition_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_graph_and_hash(self) -> Self:
        if self.acquired_at.isoformat() != "2026-07-18T12:00:00+00:00":
            raise ValueError("acquisition timestamp must equal the exact reviewed declaration")
        if tuple(self.systems) != SYSTEM_SLUGS:
            raise ValueError("acquisition must preserve exact frozen six-system order")
        if [row.system_slug for row in self.system_rows] != list(SYSTEM_SLUGS):
            raise ValueError("system rows must preserve exact frozen six-system order")
        if [row.system_slug for row in self.candidates] != list(SYSTEM_SLUGS):
            raise ValueError("candidates must preserve exact frozen six-system order")

        artifacts = {item.artifact_id: item for item in self.source_artifacts}
        if len(artifacts) != 1 or {item.relative_path for item in artifacts.values()} != {
            "ahrq_system_2023.csv"
        }:
            raise ValueError("physician acquisition source artifact set drift")
        from shared.acquisition.scale_annual_discharges_packet import acquisition as annual_acquisition

        annual = annual_acquisition()
        expected_artifact = next(
            item for item in annual.source_artifacts if item.relative_path == "ahrq_system_2023.csv"
        )
        if self.cache_receipt != annual.cache_receipt or self.source_artifacts != [expected_artifact]:
            raise ValueError("physician acquisition must preserve exact validated AHRQ custody")
        system_artifact = next(
            item.artifact_id for item in artifacts.values() if item.relative_path == "ahrq_system_2023.csv"
        )

        row_refs = {_system_row_ref(row): row for row in self.system_rows}
        if len(row_refs) != 6 or len({row.row_number for row in self.system_rows}) != 6:
            raise ValueError("duplicate physician system row")

        for row in self.system_rows:
            identity = SYSTEM_AHRQ_IDENTITIES.get(row.system_slug)
            declared = SYSTEM_ROW_DECLARATIONS.get(row.system_slug)
            if (
                identity is None
                or declared is None
                or row.health_sys_id != identity.health_sys_id
                or row.health_sys_name != identity.source_name
            ):
                raise ValueError("physician row product-to-AHRQ identity substitution")
            if row.artifact_ref != system_artifact:
                raise ValueError("physician row must reference the system CSV")
            actual_row = (
                row.row_number,
                row.source_row_sha256,
                row.health_sys_id,
                row.health_sys_name,
                row.health_sys_city,
                row.health_sys_state,
                row.raw_lexical_value,
                row.prim_care_mds_raw,
                row.hosp_cnt_raw,
                row.acutehosp_cnt_raw,
                row.sys_multistate_raw,
            )
            if actual_row != declared:
                raise ValueError("physician row must equal the exact reviewed source declaration")
        for candidate in self.candidates:
            candidate_row = row_refs.get(candidate.system_row_ref)
            if candidate_row is None or candidate_row.system_slug != candidate.system_slug:
                raise ValueError("candidate physician system row reference drift")
            if candidate.source_artifact_refs != [system_artifact]:
                raise ValueError("candidate must preserve the exact system source receipt")
            if candidate.candidate_value != int(candidate_row.raw_lexical_value):
                raise ValueError("candidate value must equal exact total_mds lexical value")
            if not PHYSICIAN_BLOCKERS.issubset(candidate.blocker_codes):
                raise ValueError("candidate weakened mandatory physician comparability blockers")
            if len(candidate.blocker_codes) != len(set(candidate.blocker_codes)):
                raise ValueError("candidate blocker codes must be unique")
            expected_blockers = sorted(
                PHYSICIAN_BLOCKERS | EXTRA_BLOCKERS[candidate.system_slug]
            )
            if candidate.blocker_codes != expected_blockers:
                raise ValueError("candidate must preserve exact evidence-specific physician blockers")
            if candidate.finding != FINDINGS[candidate.system_slug]:
                raise ValueError("candidate must preserve exact evidence-specific physician finding")

        if tuple(self.prohibited_outputs) != PROHIBITED_OUTPUTS:
            raise ValueError("all no-execution prohibitions must remain explicit")
        if self.acquisition_sha256 != semantic_hash(self):
            raise ValueError("acquisition_sha256 does not match canonical content")
        return self

    @classmethod
    def model_json_schema(
        cls,
        by_alias: bool = True,
        ref_template: str = "#/$defs/{model}",
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation",
        *,
        union_format: Literal["any_of", "primitive_type_array"] = "any_of",
    ) -> dict[str, Any]:
        """Export the structural schema plus the exact immutable v3 packet."""

        schema = super().model_json_schema(
            by_alias=by_alias,
            ref_template=ref_template,
            schema_generator=schema_generator,
            mode=mode,
            union_format=union_format,
        )
        from shared.acquisition.scale_physician_count_packet import acquisition

        schema["const"] = acquisition().model_dump(mode="json")
        return schema


def _system_row_ref(row: PhysicianSystemRow) -> str:
    return f"row:system:{row.health_sys_id}:{row.row_number}"


def semantic_hash(value: PhysicianCountAcquisition) -> str:
    return canonical_sha256(value.model_dump(mode="json", exclude={"acquisition_sha256"}))


def build_physician_count_acquisition(payload: Mapping[str, object]) -> PhysicianCountAcquisition:
    body = dict(payload)
    body.pop("acquisition_sha256", None)
    body["acquisition_sha256"] = canonical_sha256(body)
    return PhysicianCountAcquisition.model_validate(body)


__all__ = [
    "PHYSICIAN_BLOCKERS",
    "PhysicianCountAcquisition",
    "PhysicianCountCandidate",
    "PhysicianSystemRow",
    "PriorAnnualToolkitLineage",
    "build_physician_count_acquisition",
    "semantic_hash",
]
