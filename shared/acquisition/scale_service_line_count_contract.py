"""Immutable v4 contract for all-six service-line-count source evaluation."""

from __future__ import annotations

from typing import Any, Literal, Mapping, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode

from shared.acquisition.scale_service_line_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    COMMON_BLOCKERS,
    EXTRA_BLOCKERS,
    FINDINGS,
    IDENTITY_ROWS,
)
from shared.acquisition.scale_system_roster import SYSTEM_AHRQ_IDENTITIES, SYSTEM_SLUGS
from shared.acquisition.scale_tabular_input_family import TabularSourceArtifact, ValidatedCacheReceipt
from shared.contracts.public_evidence import canonical_sha256

SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
PROHIBITED_OUTPUTS = (
    "adjudication", "component_score", "projection", "promotion",
    "recommendation", "scale_score", "sensitivity_result",
)
SERVICE_LINE_DEFINITION = "Count of distinct clinical service lines offered by the product system under one preapproved common taxonomy, at the frozen roster boundary and aligned period"
AHRQ_QUERY = "inspect exact 2023 AHRQ system-file header for a system-level service_line_count field"
CMS_QUERY = "evaluate CMS RBCS 2025 as a common taxonomy for offered health-system service_line_count"
AHRQ_LOCATOR = "CSV header, 40 columns; exact header SHA-256 9cc022051910c61c2f66e60b81a450985996cca7fa981c85bd38fa8a9853a79f"
AHRQ_SCOPE = "All 639 AHRQ highest-ownership health-system rows, including exact rows for the six frozen product systems"
AHRQ_EXCLUSION = "The exact official system-file schema contains no service-line field and cannot establish offered-service presence or a common taxonomy."
CMS_LOCATOR = "PDF pages 8 and 22 (physical pages 8 and 22): HCPCS/Part B inclusion rules and Data Limitations"
CMS_SCOPE = "CMS taxonomy for clinically meaningful categories of Medicare Part B HCPCS billed services"
CMS_EXCLUSION = "RBCS classifies paid Medicare Part B HCPCS activity; it does not report which service lines a health system offers or bind categories to the six product systems. Conversion would require prohibited claims aggregation and boundary inference."


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PriorPhysicianToolkitLineage(StrictModel):
    repo: Literal["healthcare-toolkit"] = "healthcare-toolkit"
    input_family: Literal["physician_count"] = "physician_count"
    binding_merge: Literal["581265a2-f2c8-0f71-832b-87de787b8b93-e3ac8b1c"]
    binding_tracker_merge: Literal["4f62f957-c438-9a80-101d-15902d2b72cc-4e089e07"]
    admission_merge: Literal["cc3ccb3d-26e4-4d41-0546-003b7dec073a-2b74ab17"]
    tracker_merge: Literal["208b2ab9-7594-316f-0a3b-d64649423091-c11e6bbf"]
    cumulative_packet_sha256: Literal["sha256:282a369b9121a27afebbb20fec4810464d1b7efa3d67a07ea119537cbbed9aa5"]
    cumulative_packet_transport_sha256: Literal["sha256:3b8a7fc610adaf77107658005d56d78268cdafc54254cb7aaa1a02e1d4566128"]
    cumulative_review_sha256: Literal["sha256:181691932c17f47e42865422f30be923f9ed739cbacb8be23266dea5342f4d30"]
    cumulative_review_transport_sha256: Literal["sha256:d5e4e4eb39a2c247a31ab4589134392f1f641df51ed97acbff9158c5cf2847d2"]
    cumulative_assurance_sha256: Literal["sha256:8f82c8573ecea197d5ea79784e5f0c806a5ce4fb6a98e70d1ea1ec71a3ca28b8"]
    cumulative_assurance_transport_sha256: Literal["sha256:4a4302a57757e9aba56a686b83272cd6043195b6b4c6b647517e3069cff90854"]
    reusable_manifest_sha256: Literal["sha256:069a75443da0b8f39b778bb4abb4bd76807f48cb60be53c76dc96070f7ba794e"]
    reusable_manifest_transport_sha256: Literal["sha256:958f86022d660c1b44094f6a0d47bbaf2175b7d418ee94043ada17239b2f54b4"]
    terminal_status: Literal["blocked"] = "blocked"
    failure_code: Literal["human_review_required"] = "human_review_required"

    @model_validator(mode="after")
    def preserve_exact_commits(self) -> Self:
        expected = {
            "binding_merge": "581265a2f2c80f71832b87de787b8b93e3ac8b1c",  # pragma: allowlist secret
            "binding_tracker_merge": "4f62f957c4389a80101d15902d2b72cc4e089e07",  # pragma: allowlist secret
            "admission_merge": "cc3ccb3d26e44d410546003b7dec073a2b74ab17",  # pragma: allowlist secret
            "tracker_merge": "208b2ab97594316f0a3bd64649423091c11e6bbf",  # pragma: allowlist secret
        }
        if any(getattr(self, key).replace("-", "") != value for key, value in expected.items()):
            raise ValueError("prior physician Toolkit commit lineage drift")
        return self


class HttpReceipt(StrictModel):
    status: Literal[200] = 200
    final_url: Literal["https://data.cms.gov/sites/default/files/2025-12/a167eaff-5167-4c2c-a133-9ec94f0ee112/RBCS%20Final%20Report_RY2025.pdf"]
    content_type: Literal["application/pdf"] = "application/pdf"
    content_length: Literal[839953] = 839953
    last_modified: Literal["Mon, 08 Dec 2025 17:51:06 GMT"]
    retrieved_at: Literal["2026-07-18T18:15:29Z"]
    payload_sha256: Literal["sha256:68ac55dcc2812c6d692134dec827ffc5056f60b5ddcf605575fb6f2025b193e4"]
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_hash(self) -> Self:
        if self.receipt_sha256 != canonical_sha256(
            self.model_dump(mode="json", exclude={"receipt_sha256"})
        ):
            raise ValueError("HTTP receipt semantic hash drift")
        return self


class EvaluatedDocumentArtifact(StrictModel):
    artifact_id: Literal["artifact:cms-rbcs:final-report:2025"]
    source_name: Literal["Centers for Medicare & Medicaid Services"]
    document_title: Literal["Restructured BETOS Classification System Final Report, Release Year 2025"]
    source_url: Literal["https://data.cms.gov/sites/default/files/2025-12/a167eaff-5167-4c2c-a133-9ec94f0ee112/RBCS%20Final%20Report_RY2025.pdf"]
    landing_page: Literal["https://data.cms.gov/provider-summary-by-type-of-service/provider-service-classifications/restructured-betos-classification-system"]
    source_period: Literal["2025 release; Medicare Part B analysis window described by source"]
    media_type: Literal["application/pdf"] = "application/pdf"
    payload_sha256: Literal["sha256:68ac55dcc2812c6d692134dec827ffc5056f60b5ddcf605575fb6f2025b193e4"]
    content_length: Literal[839953] = 839953
    page_count: Literal[48] = 48
    rights_classification: Literal["public_domain"] = "public_domain"
    custody_state: Literal["frozen_verified_external"] = "frozen_verified_external"
    http_receipt: HttpReceipt


class ServiceLineIdentityRow(StrictModel):
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
    common_service_taxonomy_available: Literal[False] = False
    system_service_line_count_available: Literal[False] = False
    usable_for_scale_input: Literal[False] = False
    exclusion_reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_query_hash(self) -> Self:
        if self.query_sha256 != canonical_sha256(self.query):
            raise ValueError("source-evaluation query hash drift")
        return self


class ServiceLineMissingCell(StrictModel):
    system_slug: str
    input_family: Literal["service_line_count"] = "service_line_count"
    candidate_value: Literal[None] = None
    unit: Literal["service_lines"] = "service_lines"
    desired_definition: Literal[
        "Count of distinct clinical service lines offered by the product system under one preapproved common taxonomy, at the frozen roster boundary and aligned period"
    ]
    source_period: Literal["not_available_on_comparable_basis"]
    source_artifact_refs: list[str] = Field(min_length=2, max_length=2)
    identity_row_ref: str = Field(min_length=1)
    missingness: Literal["unavailable_public"] = "unavailable_public"
    blocker_codes: list[str] = Field(min_length=9)
    finding: str = Field(min_length=1)
    imputed: Literal[False] = False
    aggregated: Literal[False] = False
    fabricated_zero: Literal[False] = False
    approved_for_scale: Literal[False] = False


class ServiceLineCountAcquisition(StrictModel):
    schema_version: Literal["ushso.scale-service-line-count-acquisition.v4"] = "ushso.scale-service-line-count-acquisition.v4"
    acquisition_id: Literal["scale-service-line-count-all-six-2026-07-18"]
    workflow_id: Literal["scale-service-line-count-acquisition.v4"] = "scale-service-line-count-acquisition.v4"
    input_family: Literal["service_line_count"] = "service_line_count"
    systems: list[str]
    acquired_at: AwareDatetime
    producer_version: Literal["HDM-kh4"]
    prior_cycle: PriorPhysicianToolkitLineage
    cache_receipt: ValidatedCacheReceipt
    ahrq_source_artifact: TabularSourceArtifact
    cms_taxonomy_artifact: EvaluatedDocumentArtifact
    ahrq_header_columns: list[str] = Field(min_length=40, max_length=40)
    identity_rows: list[ServiceLineIdentityRow] = Field(min_length=6, max_length=6)
    source_evaluations: list[SourceEvaluation] = Field(min_length=2, max_length=2)
    cells: list[ServiceLineMissingCell] = Field(min_length=6, max_length=6)
    common_taxonomy_receipt: Literal[None] = None
    hand_counted_marketing_pages: Literal[False] = False
    prohibited_outputs: list[str]
    acquisition_sha256: str = Field(pattern=SHA256_PATTERN)

    @field_validator("acquired_at", mode="before")
    @classmethod
    def preserve_timestamp_lexeme(cls, value: object) -> object:
        if value != "2026-07-18T18:15:29Z":
            raise ValueError("acquisition timestamp must use the exact reviewed JSON representation")
        return value

    @model_validator(mode="after")
    def validate_exact_packet(self) -> Self:
        if self.acquired_at.isoformat() != "2026-07-18T18:15:29+00:00":
            raise ValueError("acquisition timestamp drift")
        if tuple(self.systems) != SYSTEM_SLUGS:
            raise ValueError("acquisition must preserve exact frozen six-system order")
        if tuple(self.ahrq_header_columns) != AHRQ_HEADER_COLUMNS or any(
            "service_line" in column for column in self.ahrq_header_columns
        ):
            raise ValueError("AHRQ header receipt drift or fabricated service-line field")
        from shared.acquisition.scale_physician_count_packet import acquisition as physician_acquisition

        physician = physician_acquisition()
        expected_artifact = physician.source_artifacts[0]
        if self.cache_receipt != physician.cache_receipt or self.ahrq_source_artifact != expected_artifact:
            raise ValueError("service-line acquisition must preserve exact validated AHRQ custody")
        if [row.system_slug for row in self.identity_rows] != list(SYSTEM_SLUGS):
            raise ValueError("identity rows must preserve exact frozen six-system order")
        if [cell.system_slug for cell in self.cells] != list(SYSTEM_SLUGS):
            raise ValueError("cells must preserve exact frozen six-system order")
        row_refs: dict[str, ServiceLineIdentityRow] = {}
        for row in self.identity_rows:
            identity = SYSTEM_AHRQ_IDENTITIES.get(row.system_slug)
            if identity is None or (row.health_sys_id, row.health_sys_name) != identity:
                raise ValueError("service-line product-to-AHRQ identity substitution")
            actual = (row.row_number, row.source_row_sha256, row.health_sys_id, row.health_sys_name, row.health_sys_city, row.health_sys_state)
            if actual != IDENTITY_ROWS[row.system_slug]:
                raise ValueError("service-line identity row must equal exact reviewed declaration")
            row_refs[f"row:system:{row.health_sys_id}:{row.row_number}"] = row
        expected_evaluations = {
            "evaluation:ahrq-system-header": (
                self.ahrq_source_artifact.artifact_id, AHRQ_QUERY, True,
                AHRQ_LOCATOR, AHRQ_SCOPE, AHRQ_EXCLUSION,
            ),
            "evaluation:cms-rbcs-taxonomy": (
                self.cms_taxonomy_artifact.artifact_id, CMS_QUERY, False,
                CMS_LOCATOR, CMS_SCOPE, CMS_EXCLUSION,
            ),
        }
        if {item.evaluation_id for item in self.source_evaluations} != set(expected_evaluations):
            raise ValueError("source evaluation set drift")
        for item in self.source_evaluations:
            artifact_ref, query, identity_available, locator, scope, exclusion = expected_evaluations[item.evaluation_id]
            if (
                item.artifact_ref,
                item.query,
                item.system_level_identity_available,
                item.exact_locator,
                item.evaluated_scope,
                item.exclusion_reason,
            ) != (artifact_ref, query, identity_available, locator, scope, exclusion):
                raise ValueError("source evaluation meaning drift")
        expected_refs = [self.ahrq_source_artifact.artifact_id, self.cms_taxonomy_artifact.artifact_id]
        for cell in self.cells:
            matched_row = row_refs.get(cell.identity_row_ref)
            if matched_row is None or matched_row.system_slug != cell.system_slug:
                raise ValueError("service-line cell identity reference drift")
            if cell.source_artifact_refs != expected_refs:
                raise ValueError("service-line cell source receipt set drift")
            if cell.blocker_codes != sorted(COMMON_BLOCKERS | EXTRA_BLOCKERS[cell.system_slug]):
                raise ValueError("service-line cell blockers drift")
            if cell.finding != FINDINGS[cell.system_slug]:
                raise ValueError("service-line cell finding drift")
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
        from shared.acquisition.scale_service_line_count_packet import acquisition

        schema["const"] = acquisition().model_dump(mode="json")
        return schema


def semantic_hash(value: ServiceLineCountAcquisition) -> str:
    return canonical_sha256(value.model_dump(mode="json", exclude={"acquisition_sha256"}))


def build_service_line_count_acquisition(payload: Mapping[str, object]) -> ServiceLineCountAcquisition:
    body = dict(payload)
    body.pop("acquisition_sha256", None)
    body["acquisition_sha256"] = canonical_sha256(body)
    return ServiceLineCountAcquisition.model_validate(body)


__all__ = [
    "AHRQ_EXCLUSION", "AHRQ_LOCATOR", "AHRQ_QUERY", "AHRQ_SCOPE",
    "CMS_EXCLUSION", "CMS_LOCATOR", "CMS_QUERY", "CMS_SCOPE",
    "PROHIBITED_OUTPUTS", "SERVICE_LINE_DEFINITION",
    "EvaluatedDocumentArtifact", "HttpReceipt", "PriorPhysicianToolkitLineage",
    "ServiceLineCountAcquisition", "ServiceLineIdentityRow", "ServiceLineMissingCell",
    "SourceEvaluation", "build_service_line_count_acquisition", "semantic_hash",
]
