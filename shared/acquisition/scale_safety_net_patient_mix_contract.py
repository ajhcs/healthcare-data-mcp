"""Immutable v5 contract for all-six safety-net patient-mix evaluation."""

from __future__ import annotations

from typing import Any, Literal, Mapping, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode

from shared.acquisition.scale_safety_net_patient_mix_declaration import (
    AHRQ_HEADER_COLUMNS,
    COMMON_BLOCKERS,
    EXTRA_BLOCKERS,
    FINDINGS,
    IDENTITY_ROWS,
    SAFETY_NET_INDICATOR_COLUMNS,
)
from shared.acquisition.scale_system_roster import SYSTEM_AHRQ_IDENTITIES, SYSTEM_SLUGS
from shared.acquisition.scale_tabular_input_family import TabularSourceArtifact, ValidatedCacheReceipt
from shared.contracts.public_evidence import canonical_sha256

SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
PROHIBITED_OUTPUTS = (
    "adjudication", "component_score", "projection", "promotion",
    "recommendation", "scale_score", "sensitivity_result",
)
PATIENT_MIX_DEFINITION = "Percent of patients or encounters meeting one preapproved safety-net definition, using a common numerator, denominator, setting, organizational boundary, and aligned period across all six product systems"
AHRQ_QUERY = "inspect the exact 2023 AHRQ system-file schema for a system-level safety_net_patient_mix_pct numerator, denominator, or percentage"
CMS_QUERY = "evaluate the CMS Medicare DSH disproportionate patient percentage as a common all-patient safety-net patient-mix percentage for the six product systems"
AHRQ_LOCATOR = "CSV header, columns sys_incl_highdpphosp, sys_highucburden, and sys_incl_highuchosp; exact header SHA-256 9cc022051910c61c2f66e60b81a450985996cca7fa981c85bd38fa8a9853a79f"
AHRQ_SCOPE = "All 639 AHRQ highest-ownership health-system rows, including exact rows for the six frozen product systems"
AHRQ_EXCLUSION = "The system file reports only binary high-DSH and uncompensated-care flags; it has no patient-mix numerator, denominator, or percentage and cannot be converted into one."
CMS_LOCATOR = "PDF physical pages 3, 5, and 7: DPP two-fraction definition, IPPS patient-day scope, and worked two-denominator example"
CMS_SCOPE = "Medicare DSH hospital-level IPPS disproportionate patient percentage and uncompensated-care payment context"
CMS_EXCLUSION = "CMS DPP is hospital-specific and sums a Medicare/SSI fraction and a Medicaid/non-Medicare fraction with different denominators; its IPPS inpatient scope is not a common full product-system patient denominator."


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PriorServiceLineToolkitLineage(StrictModel):
    repo: Literal["healthcare-toolkit"] = "healthcare-toolkit"
    input_family: Literal["service_line_count"] = "service_line_count"
    binding_merge: Literal["d57b1883-0444-75f9-dac8-7eae1ac6806f-da1d9728"]
    binding_tracker_merge: Literal["3d612de3-c513-7624-e845-771334807e55-0bbf8b83"]
    agents_review_merge: Literal["97b248f2-2e95-c335-db40-48b16c090792-e7d37801"]
    agents_tracker_merge: Literal["0ead3b38-3102-7ab2-e037-11efae5a30ca-67b620a9"]
    admission_merge: Literal["46ed66e6-9bcd-595a-a898-4d2c5b48d6b0-ab4f13de"]
    tracker_merge: Literal["df429e9a-b47d-6002-5258-942e88df036c-389c8731"]
    cumulative_packet_sha256: Literal["sha256:bb41a834d64c52ae65beef077292b6986ff9754bf81441464150b0ea976b30f6"]
    cumulative_review_sha256: Literal["sha256:004fcb8fbb6ae9a126bebb37dcba58496317fadfe7859df4415df43638808e55"]
    cumulative_review_transport_sha256: Literal["sha256:d78e39153a1fa7cc26232832b0f6e9d00229e51d7707e7896958acfcd7394920"]
    cumulative_assurance_sha256: Literal["sha256:0d5e9933a3538c6892f1050b42b5eeb3e56d040aa35af1478b683b207b77ad82"]
    cumulative_assurance_transport_sha256: Literal["sha256:2397b4f3c1d8bbb2dbf940d2b0113269fbbca8c2a163a2c8ec2cc1eac4563d65"]
    reusable_manifest_sha256: Literal["sha256:71713d716b2fc59379f6fe0e7ca1c80ca73bdb54eca93e723076502dd216978e"]
    reusable_manifest_transport_sha256: Literal["sha256:1d7132ac0814cbfc629f007b06870740c1591c4ee16a715a69b9ba6fb8dfa7f9"]
    terminal_status: Literal["blocked"] = "blocked"
    failure_code: Literal["human_review_required"] = "human_review_required"

    @model_validator(mode="after")
    def preserve_exact_commits(self) -> Self:
        expected = {
            "binding_merge": "d57b1883044475f9dac87eae1ac6806fda1d9728",  # pragma: allowlist secret
            "binding_tracker_merge": "3d612de3c5137624e845771334807e550bbf8b83",  # pragma: allowlist secret
            "agents_review_merge": "97b248f22e95c335db4048b16c090792e7d37801",  # pragma: allowlist secret
            "agents_tracker_merge": "0ead3b3831027ab2e03711efae5a30ca67b620a9",  # pragma: allowlist secret
            "admission_merge": "46ed66e69bcd595aa8984d2c5b48d6b0ab4f13de",  # pragma: allowlist secret
            "tracker_merge": "df429e9ab47d60025258942e88df036c389c8731",  # pragma: allowlist secret
        }
        if any(getattr(self, key).replace("-", "") != value for key, value in expected.items()):
            raise ValueError("prior service-line commit lineage drift")
        exact_merge_artifacts = {
            "cumulative_packet_sha256": "sha256:bb41a834d64c52ae65beef077292b6986ff9754bf81441464150b0ea976b30f6",
            "cumulative_review_sha256": "sha256:004fcb8fbb6ae9a126bebb37dcba58496317fadfe7859df4415df43638808e55",
            "cumulative_review_transport_sha256": "sha256:d78e39153a1fa7cc26232832b0f6e9d00229e51d7707e7896958acfcd7394920",
            "cumulative_assurance_sha256": "sha256:0d5e9933a3538c6892f1050b42b5eeb3e56d040aa35af1478b683b207b77ad82",
            "cumulative_assurance_transport_sha256": "sha256:2397b4f3c1d8bbb2dbf940d2b0113269fbbca8c2a163a2c8ec2cc1eac4563d65",
            "reusable_manifest_sha256": "sha256:71713d716b2fc59379f6fe0e7ca1c80ca73bdb54eca93e723076502dd216978e",
            "reusable_manifest_transport_sha256": "sha256:1d7132ac0814cbfc629f007b06870740c1591c4ee16a715a69b9ba6fb8dfa7f9",
        }
        if any(getattr(self, key) != value for key, value in exact_merge_artifacts.items()):
            raise ValueError("prior service-line merged artifact tuple drift")
        return self


class HttpReceipt(StrictModel):
    status: Literal[200] = 200
    final_url: Literal["https://www.cms.gov/outreach-and-education/medicare-learning-network-mln/mlnproducts/downloads/disproportionate_share_hospital.pdf"]
    content_type: Literal["application/pdf"] = "application/pdf"
    content_length: Literal[951147] = 951147
    last_modified: Literal["Wed, 18 Sep 2024 20:23:56 GMT"]
    retrieved_at: Literal["2026-07-19T01:15:08Z"]
    payload_sha256: Literal["sha256:a658fb1ec185cea715dbc175b8e225c39c806da2b353f8f86b617bcd8ebf390a"]
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_hash(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"receipt_sha256"}))
        if self.receipt_sha256 != expected:
            raise ValueError("HTTP receipt semantic hash drift")
        return self


class DshDefinitionArtifact(StrictModel):
    artifact_id: Literal["artifact:cms-mln:medicare-dsh:2024"]
    source_name: Literal["Centers for Medicare & Medicaid Services"]
    document_title: Literal["Medicare Disproportionate Share Hospital, MLN006741"]
    source_url: Literal["https://www.cms.gov/outreach-and-education/medicare-learning-network-mln/mlnproducts/downloads/disproportionate_share_hospital.pdf"]
    landing_page: Literal["https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/disproportionate-share-hospital-dsh"]
    source_period: Literal["September 2024 policy fact sheet; FY 2024 DPP rules"]
    media_type: Literal["application/pdf"] = "application/pdf"
    payload_sha256: Literal["sha256:a658fb1ec185cea715dbc175b8e225c39c806da2b353f8f86b617bcd8ebf390a"]
    content_length: Literal[951147] = 951147
    page_count: Literal[8] = 8
    rights_classification: Literal["unknown_review_required"] = "unknown_review_required"
    rights_basis: Literal["Public CMS fact sheet; linked MLN content disclaimer and registered-mark terms were not independently frozen or reviewed"]
    custody_state: Literal["frozen_verified_external"] = "frozen_verified_external"
    http_receipt: HttpReceipt


class SafetyNetIdentityRow(StrictModel):
    system_slug: str
    artifact_ref: Literal["artifact:ahrq-compendium:system:2023"]
    row_number: int = Field(ge=2)
    source_row_sha256: str = Field(pattern=SHA256_PATTERN)
    row_key_column: Literal["health_sys_id"] = "health_sys_id"
    health_sys_id: str = Field(pattern=r"^HSI[0-9]{8}$")
    health_sys_name: str = Field(min_length=1)
    health_sys_city: str = Field(min_length=1)
    health_sys_state: str = Field(pattern=r"^[A-Z]{2}$")


class SourceEvaluation(StrictModel):
    evaluation_id: str = Field(min_length=1)
    artifact_ref: str = Field(min_length=1)
    query: str = Field(min_length=1)
    query_sha256: str = Field(pattern=SHA256_PATTERN)
    exact_locator: str = Field(min_length=1)
    evaluated_scope: str = Field(min_length=1)
    system_level_identity_available: bool
    common_numerator_denominator_available: Literal[False] = False
    system_patient_mix_percentage_available: Literal[False] = False
    usable_for_scale_input: Literal[False] = False
    exclusion_reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_query_hash(self) -> Self:
        if self.query_sha256 != canonical_sha256(self.query):
            raise ValueError("source-evaluation query hash drift")
        return self


class SafetyNetMissingCell(StrictModel):
    system_slug: str
    input_family: Literal["safety_net_patient_mix_pct"] = "safety_net_patient_mix_pct"
    candidate_value: Literal[None] = None
    unit: Literal["percent"] = "percent"
    desired_definition: Literal[
        "Percent of patients or encounters meeting one preapproved safety-net definition, using a common numerator, denominator, setting, organizational boundary, and aligned period across all six product systems"
    ]
    source_period: Literal["not_available_on_comparable_basis"]
    source_artifact_refs: list[str] = Field(min_length=2, max_length=2)
    identity_row_ref: str = Field(min_length=1)
    missingness: Literal["unavailable_public"] = "unavailable_public"
    blocker_codes: list[str] = Field(min_length=12)
    finding: str = Field(min_length=1)
    imputed: Literal[False] = False
    aggregated: Literal[False] = False
    fabricated_zero: Literal[False] = False
    approved_for_scale: Literal[False] = False


class SafetyNetPatientMixAcquisition(StrictModel):
    schema_version: Literal["ushso.scale-safety-net-patient-mix-acquisition.v5"] = "ushso.scale-safety-net-patient-mix-acquisition.v5"
    acquisition_id: Literal["scale-safety-net-patient-mix-all-six-2026-07-19"]
    workflow_id: Literal["scale-safety-net-patient-mix-acquisition.v5"] = "scale-safety-net-patient-mix-acquisition.v5"
    input_family: Literal["safety_net_patient_mix_pct"] = "safety_net_patient_mix_pct"
    systems: list[str]
    acquired_at: AwareDatetime
    producer_version: Literal["HDM-jhh"]
    prior_cycle: PriorServiceLineToolkitLineage
    cache_receipt: ValidatedCacheReceipt
    ahrq_source_artifact: TabularSourceArtifact
    cms_dsh_artifact: DshDefinitionArtifact
    ahrq_header_columns: list[str] = Field(min_length=40, max_length=40)
    safety_net_indicator_columns: list[str] = Field(min_length=3, max_length=3)
    identity_rows: list[SafetyNetIdentityRow] = Field(min_length=6, max_length=6)
    source_evaluations: list[SourceEvaluation] = Field(min_length=2, max_length=2)
    cells: list[SafetyNetMissingCell] = Field(min_length=6, max_length=6)
    approved_numerator_receipt: Literal[None] = None
    approved_denominator_receipt: Literal[None] = None
    prohibited_outputs: list[str]
    acquisition_sha256: str = Field(pattern=SHA256_PATTERN)

    @field_validator("acquired_at", mode="before")
    @classmethod
    def preserve_timestamp_lexeme(cls, value: object) -> object:
        if value != "2026-07-19T01:15:08Z":
            raise ValueError("acquisition timestamp must use the exact reviewed JSON representation")
        return value

    @model_validator(mode="after")
    def validate_exact_packet(self) -> Self:
        if self.acquired_at.isoformat() != "2026-07-19T01:15:08+00:00":
            raise ValueError("acquisition timestamp drift")
        if tuple(self.systems) != SYSTEM_SLUGS:
            raise ValueError("acquisition must preserve exact frozen six-system order")
        if tuple(self.ahrq_header_columns) != AHRQ_HEADER_COLUMNS:
            raise ValueError("AHRQ header receipt drift")
        if tuple(self.safety_net_indicator_columns) != SAFETY_NET_INDICATOR_COLUMNS:
            raise ValueError("AHRQ safety-net indicator set drift")
        if any(name not in self.ahrq_header_columns for name in self.safety_net_indicator_columns):
            raise ValueError("AHRQ safety-net indicator is absent from exact header")
        from shared.acquisition.scale_service_line_count_packet import acquisition as service_line_acquisition

        service_line = service_line_acquisition()
        if self.cache_receipt != service_line.cache_receipt or self.ahrq_source_artifact != service_line.ahrq_source_artifact:
            raise ValueError("safety-net acquisition must preserve exact validated AHRQ custody")
        if [row.system_slug for row in self.identity_rows] != list(SYSTEM_SLUGS):
            raise ValueError("identity rows must preserve exact frozen six-system order")
        if [cell.system_slug for cell in self.cells] != list(SYSTEM_SLUGS):
            raise ValueError("cells must preserve exact frozen six-system order")
        row_refs: dict[str, SafetyNetIdentityRow] = {}
        for row in self.identity_rows:
            identity = SYSTEM_AHRQ_IDENTITIES.get(row.system_slug)
            if identity is None or (row.health_sys_id, row.health_sys_name) != identity:
                raise ValueError("safety-net product-to-AHRQ identity substitution")
            actual = (row.row_number, row.source_row_sha256, row.health_sys_id, row.health_sys_name, row.health_sys_city, row.health_sys_state)
            if actual != IDENTITY_ROWS[row.system_slug]:
                raise ValueError("safety-net identity row must equal exact reviewed declaration")
            row_refs[f"row:system:{row.health_sys_id}:{row.row_number}"] = row
        expected_evaluations = {
            "evaluation:ahrq-system-safety-net-schema": (
                self.ahrq_source_artifact.artifact_id, AHRQ_QUERY, True,
                AHRQ_LOCATOR, AHRQ_SCOPE, AHRQ_EXCLUSION,
            ),
            "evaluation:cms-medicare-dsh-definition": (
                self.cms_dsh_artifact.artifact_id, CMS_QUERY, False,
                CMS_LOCATOR, CMS_SCOPE, CMS_EXCLUSION,
            ),
        }
        if [item.evaluation_id for item in self.source_evaluations] != list(expected_evaluations):
            raise ValueError("source evaluation order or set drift")
        for item in self.source_evaluations:
            expected = expected_evaluations[item.evaluation_id]
            actual = (item.artifact_ref, item.query, item.system_level_identity_available, item.exact_locator, item.evaluated_scope, item.exclusion_reason)
            if actual != expected:
                raise ValueError("source evaluation meaning drift")
        expected_refs = [self.ahrq_source_artifact.artifact_id, self.cms_dsh_artifact.artifact_id]
        for cell in self.cells:
            row = row_refs.get(cell.identity_row_ref)
            if row is None or row.system_slug != cell.system_slug:
                raise ValueError("safety-net cell identity reference drift")
            if cell.source_artifact_refs != expected_refs:
                raise ValueError("safety-net cell source receipt set drift")
            if cell.blocker_codes != sorted(COMMON_BLOCKERS | EXTRA_BLOCKERS[cell.system_slug]):
                raise ValueError("safety-net cell blockers drift")
            if cell.finding != FINDINGS[cell.system_slug]:
                raise ValueError("safety-net cell finding drift")
        if tuple(self.prohibited_outputs) != PROHIBITED_OUTPUTS:
            raise ValueError("all no-execution prohibitions must remain explicit")
        if self.acquisition_sha256 != semantic_hash(self):
            raise ValueError("acquisition_sha256 does not match canonical content")
        return self

    @classmethod
    def model_json_schema(
        cls, by_alias: bool = True, ref_template: str = "#/$defs/{model}",
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation", *,
        union_format: Literal["any_of", "primitive_type_array"] = "any_of",
    ) -> dict[str, Any]:
        schema = super().model_json_schema(
            by_alias=by_alias, ref_template=ref_template,
            schema_generator=schema_generator, mode=mode, union_format=union_format,
        )
        from shared.acquisition.scale_safety_net_patient_mix_packet import acquisition

        schema["const"] = acquisition().model_dump(mode="json")
        return schema


def semantic_hash(value: SafetyNetPatientMixAcquisition) -> str:
    return canonical_sha256(value.model_dump(mode="json", exclude={"acquisition_sha256"}))


def build_safety_net_patient_mix_acquisition(
    payload: Mapping[str, object],
) -> SafetyNetPatientMixAcquisition:
    body = dict(payload)
    body.pop("acquisition_sha256", None)
    body["acquisition_sha256"] = canonical_sha256(body)
    return SafetyNetPatientMixAcquisition.model_validate(body)


__all__ = [
    "AHRQ_EXCLUSION", "AHRQ_LOCATOR", "AHRQ_QUERY", "AHRQ_SCOPE",
    "CMS_EXCLUSION", "CMS_LOCATOR", "CMS_QUERY", "CMS_SCOPE",
    "PATIENT_MIX_DEFINITION", "PROHIBITED_OUTPUTS", "DshDefinitionArtifact",
    "HttpReceipt", "PriorServiceLineToolkitLineage", "SafetyNetIdentityRow",
    "SafetyNetMissingCell", "SafetyNetPatientMixAcquisition", "SourceEvaluation",
    "build_safety_net_patient_mix_acquisition", "semantic_hash",
]
