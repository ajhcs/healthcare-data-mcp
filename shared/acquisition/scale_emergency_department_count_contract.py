"""Immutable v6 contract for all-six emergency-department count evaluation."""

from __future__ import annotations

from typing import Any, Literal, Mapping, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode

from shared.acquisition.scale_emergency_department_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    COMMON_BLOCKERS,
    EXTRA_BLOCKERS,
    FINDINGS,
    HGI_COLUMNS,
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
ED_DEFINITION = "Count of distinct dedicated emergency departments under 42 CFR 489.24(b), including qualifying on-campus and off-campus departments or facilities, within one approved current product-system roster and common period"

EXPECTED_ARTIFACTS = {
    "artifact:ahrq-compendium:hospital-linkage:2023": (
        "AHRQ Compendium of U.S. Health Systems Hospital Linkage", "2023 dated hospital-to-system linkage",
        "text/csv", "sha256:a86146f10c8de626fea1da3a24b756e6a68165e449ae3687f1e90d6bdf129727", 1528734,
        "2026-07-16T21:39:39.964148Z", "2025-06-02T20:40:39Z",
        "CSV header and exact rows keyed by health_sys_id and CCN; schema fingerprint sha256:633359a9d9203acdcd4d2acec3d89761434e0cb0eeda9e8246c06a5f292e7150",
    ),
    "artifact:cms-provider-data:hgi:2026-04-28": (
        "CMS Hospital General Information", "current snapshot modified 2026-04-28",
        "text/csv", "sha256:83c98b2e8687580e0482b13e1e9acd5813534be243e5ccd9f55556a869595d40", 1453884,
        "2026-07-16T21:39:39.998325Z", "2026-04-28T22:10:52Z",
        "CSV columns Facility ID and Emergency Services; exact 38-column header; schema fingerprint sha256:bd2e6e437118a83f9dfef8893049c36ab9d3c3aefbc7a16fde3a86b1240fc2df",
    ),
    "artifact:cms-provider-data:hgi-metadata:2026-04-28": (
        "CMS Provider Data metastore metadata for xubh-q36u", "metadata modified 2026-04-28",
        "application/json", "sha256:a421368204acb1b91b4074ef797145aac3a11be132ae285730577b151e370cc4", 1215,
        "2026-07-19T05:05:00Z", "2026-04-28",
        "JSON title, description, modified, distribution downloadURL, and describedBy dictionary URL",
    ),
    "artifact:cms-provider-data:hospital-dictionary:2026-04": (
        "CMS Hospital Data Dictionary", "April 2026 downloadable database dictionary",
        "application/pdf", "sha256:cd5016abee26e914b273a8fea8ab698710ff60f1c53a1b66e43bbd7168f6cb81", 1291356,
        "2026-07-19T05:05:10Z", "2026-04",
        "PDF physical page 20: Hospital General Information table lists Char(6) Facility ID and Char(3) Emergency Services",
    ),
    "artifact:ecfr:42-cfr-489.24:2026-07-16": (
        "Electronic Code of Federal Regulations, 42 CFR 489.24", "regulation as of 2026-07-16",
        "application/xml", "sha256:aa51da81ea3ffbee2da8dff522bcd7c64e9ba8c667acb608bdb8c08b61407546", 32721,
        "2026-07-19T05:06:00Z", "2026-07-16",
        "Section 489.24(b), paragraphs Dedicated emergency department (1)-(3): on/off-campus, state license, held-out-to-public, and one-third outpatient-visit tests",
    ),
}

EXPECTED_ARTIFACT_URLS = {
    "artifact:ahrq-compendium:hospital-linkage:2023": (
        "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
        "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
    ),
    "artifact:cms-provider-data:hgi:2026-04-28": (
        "https://data.cms.gov/provider-data/sites/default/files/resources/893c372430d9d71a1c52737d01239d47_1777413958/Hospital_General_Information.csv",
        "https://data.cms.gov/provider-data/dataset/xubh-q36u",
    ),
    "artifact:cms-provider-data:hgi-metadata:2026-04-28": (
        "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/xubh-q36u",
        "https://data.cms.gov/provider-data/dataset/xubh-q36u",
    ),
    "artifact:cms-provider-data:hospital-dictionary:2026-04": (
        "https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/hospital/HOSPITAL_Data_Dictionary.pdf",
        "https://data.cms.gov/provider-data/dataset/xubh-q36u",
    ),
    "artifact:ecfr:42-cfr-489.24:2026-07-16": (
        "https://www.ecfr.gov/api/versioner/v1/full/2026-07-16/title-42.xml?chapter=IV&subchapter=G&part=489&section=489.24",
        "https://www.ecfr.gov/current/title-42/section-489.24",
    ),
}

EVALUATION_IDS = (
    "evaluation:ahrq-system-schema",
    "evaluation:ahrq-ccn-membership",
    "evaluation:cms-hgi-facility-boolean",
    "evaluation:cms-hgi-metadata",
    "evaluation:cms-hospital-dictionary",
    "evaluation:ecfr-dedicated-ed-definition",
)

EXPECTED_EVALUATIONS = {
    "evaluation:ahrq-system-schema": (
        "artifact:ahrq-compendium:system:2023",
        "Exact 40-column system CSV header; no emergency-department field",
        "system_row",
        "The AHRQ system row binds identity but reports no emergency-department count or inventory.",
    ),
    "evaluation:ahrq-ccn-membership": (
        "artifact:ahrq-compendium:hospital-linkage:2023",
        EXPECTED_ARTIFACTS["artifact:ahrq-compendium:hospital-linkage:2023"][7],
        "ccn_hospital",
        "A 2023 CCN hospital membership row is not a dedicated emergency-department unit and cannot resolve current roster membership.",
    ),
    "evaluation:cms-hgi-facility-boolean": (
        "artifact:cms-provider-data:hgi:2026-04-28",
        EXPECTED_ARTIFACTS["artifact:cms-provider-data:hgi:2026-04-28"][7],
        "facility_boolean",
        "Emergency Services is one Yes/No flag per Facility ID, not an enumerated count of on-campus and off-campus dedicated ED departments.",
    ),
    "evaluation:cms-hgi-metadata": (
        "artifact:cms-provider-data:hgi-metadata:2026-04-28",
        EXPECTED_ARTIFACTS["artifact:cms-provider-data:hgi-metadata:2026-04-28"][7],
        "dataset_metadata",
        "CMS describes a list of Medicare-registered hospitals, not a system or dedicated-department inventory.",
    ),
    "evaluation:cms-hospital-dictionary": (
        "artifact:cms-provider-data:hospital-dictionary:2026-04",
        EXPECTED_ARTIFACTS["artifact:cms-provider-data:hospital-dictionary:2026-04"][7],
        "data_dictionary",
        "The dictionary declares a Char(3) Emergency Services field but supplies no department-count or campus enumeration field.",
    ),
    "evaluation:ecfr-dedicated-ed-definition": (
        "artifact:ecfr:42-cfr-489.24:2026-07-16",
        EXPECTED_ARTIFACTS["artifact:ecfr:42-cfr-489.24:2026-07-16"][7],
        "dedicated_emergency_department",
        "The regulation defines the required unit but does not enumerate qualifying departments by product system and period.",
    ),
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FrozenValidatedCacheReceipt(ValidatedCacheReceipt):
    """Immutable v6 projection of the inherited validated cache receipt."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class FrozenTabularSourceArtifact(TabularSourceArtifact):
    """Immutable v6 projection of the inherited AHRQ system artifact."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class PriorSafetyNetToolkitLineage(StrictModel):
    input_family: Literal["safety_net_patient_mix_pct"] = "safety_net_patient_mix_pct"
    data_feature: Literal["5a248a6d-3eb4-52c4-82c0-a60d8b9168d7-9ae9be26"]
    data_merge: Literal["50eba1ef-da52-2e87-5ebf-b0b3feadfd80-f4073a78"]
    data_tracker_merge: Literal["83ca5ddf-9a2f-df7e-b8af-ebf68955600b-9270a52e"]
    binding_merge: Literal["9376d387-58d2-098b-8c1d-a09aac615ea5-d4affb50"]
    binding_tracker_merge: Literal["b3113f48-67fb-0eda-8428-0b2245607e24-a3959226"]
    agents_review_merge: Literal["bd7b0954-5de1-c3b7-f17c-306b6543440c-493bc669"]
    agents_tracker_merge: Literal["cbf9d93a-7132-6e40-0143-d491a2d3adbb-513e96dc"]
    admission_merge: Literal["61a67481-a9f8-bb40-e81a-2f8f59061664-ca5694ba"]
    tracker_merge: Literal["01aba0aa-5644-8f17-504e-91f7f9754d96-eb77ee7c"]
    cumulative_packet_sha256: Literal["sha256:af7ac7ce87a991b227673cfa8b6d92374bd01625217e7e21835f39abb289f365"]
    cumulative_review_sha256: Literal["sha256:68658952d09b2f9b24ec9b062dace730b8ae1ef52b68a79d49de7c3059632fb4"]
    cumulative_review_transport_sha256: Literal["sha256:e648b443d1b9d96552eaa2b1153bdc10eee608e97d11f9df87547cbaec83c8a3"]
    cumulative_assurance_sha256: Literal["sha256:b82c08cd4ff3d7acf7be5b64c2463b22cd37b370e4f40eeb8fd5c7c04fe7f419"]
    cumulative_assurance_transport_sha256: Literal["sha256:e7e2ce1dadc4d2c5549459ae6f5bd9362b6e1e06d32a4227952cbf07e3d2a2d6"]
    reusable_manifest_sha256: Literal["sha256:86f148e3627f4e2b655bb3bab1c0e225ae9a5ab25399e80e2411e3b1a04991c1"]
    reusable_manifest_transport_sha256: Literal["sha256:b00d79b155abe12bb24535f4b3b380c17483c415d974af257432f816ed2e268e"]
    terminal_status: Literal["blocked"] = "blocked"
    failure_code: Literal["human_review_required"] = "human_review_required"


class EmergencyDepartmentArtifact(StrictModel):
    artifact_id: str
    source_name: str
    source_url: str
    landing_page: str
    source_period: str
    media_type: Literal["text/csv", "application/json", "application/pdf", "application/xml"]
    payload_sha256: str = Field(pattern=SHA256_PATTERN)
    content_length: int = Field(gt=0)
    retrieved_at: str
    source_modified: str
    rights_classification: Literal["public_domain"] = "public_domain"
    rights_basis: Literal["United States government primary-source work"] = "United States government primary-source work"
    custody_state: Literal["frozen_verified_external"] = "frozen_verified_external"
    exact_locator: str

    @model_validator(mode="after")
    def preserve_exact_artifact(self) -> Self:
        expected = EXPECTED_ARTIFACTS.get(self.artifact_id)
        actual = (
            self.source_name, self.source_period, self.media_type, self.payload_sha256,
            self.content_length, self.retrieved_at, self.source_modified, self.exact_locator,
        )
        if expected is None or actual != expected:
            raise ValueError("emergency-department source artifact drift")
        if (self.source_url, self.landing_page) != EXPECTED_ARTIFACT_URLS[self.artifact_id]:
            raise ValueError("emergency-department source locator drift")
        return self


class EmergencyDepartmentIdentityRow(StrictModel):
    system_slug: str
    artifact_ref: Literal["artifact:ahrq-compendium:system:2023"]
    row_number: int = Field(ge=2)
    source_row_sha256: str = Field(pattern=SHA256_PATTERN)
    health_sys_id: str = Field(pattern=r"^HSI[0-9]{8}$")
    health_sys_name: str
    health_sys_city: str
    health_sys_state: str = Field(pattern=r"^[A-Z]{2}$")


class EmergencyDepartmentSourceEvaluation(StrictModel):
    evaluation_id: str
    artifact_ref: str
    exact_locator: str
    evaluated_unit: Literal["system_row", "ccn_hospital", "facility_boolean", "dataset_metadata", "data_dictionary", "dedicated_emergency_department"]
    reports_system_count: Literal[False] = False
    enumerates_dedicated_departments: Literal[False] = False
    usable_for_scale_input: Literal[False] = False
    exclusion_reason: str = Field(min_length=1)


class EmergencyDepartmentMissingCell(StrictModel):
    system_slug: str
    input_family: Literal["emergency_department_count"] = "emergency_department_count"
    candidate_value: Literal[None] = None
    unit: Literal["dedicated_emergency_departments"] = "dedicated_emergency_departments"
    desired_definition: Literal[
        "Count of distinct dedicated emergency departments under 42 CFR 489.24(b), including qualifying on-campus and off-campus departments or facilities, within one approved current product-system roster and common period"
    ]
    source_period: Literal["not_available_on_comparable_basis"]
    source_artifact_refs: tuple[str, ...] = Field(min_length=6, max_length=6)
    identity_row_ref: str
    missingness: Literal["unavailable_public"] = "unavailable_public"
    blocker_codes: tuple[str, ...] = Field(min_length=13)
    finding: str
    aggregated: Literal[False] = False
    flag_sum_used: Literal[False] = False
    campus_inference_used: Literal[False] = False
    missing_as_no: Literal[False] = False
    imputed: Literal[False] = False
    fabricated_zero: Literal[False] = False
    approved_for_scale: Literal[False] = False


class EmergencyDepartmentCountAcquisition(StrictModel):
    schema_version: Literal["ushso.scale-emergency-department-count-acquisition.v6"]
    acquisition_id: Literal["scale-emergency-department-count-all-six-2026-07-19"]
    workflow_id: Literal["scale-emergency-department-count-acquisition.v6"]
    input_family: Literal["emergency_department_count"]
    systems: tuple[str, ...]
    acquired_at: AwareDatetime
    producer_version: Literal["HDM-3d9"]
    prior_cycle: PriorSafetyNetToolkitLineage
    cache_receipt: FrozenValidatedCacheReceipt
    ahrq_system_artifact: FrozenTabularSourceArtifact
    source_artifacts: tuple[EmergencyDepartmentArtifact, ...] = Field(min_length=5, max_length=5)
    ahrq_header_columns: tuple[str, ...] = Field(min_length=40, max_length=40)
    hgi_header_columns: tuple[str, ...] = Field(min_length=38, max_length=38)
    identity_rows: tuple[EmergencyDepartmentIdentityRow, ...] = Field(min_length=6, max_length=6)
    source_evaluations: tuple[EmergencyDepartmentSourceEvaluation, ...] = Field(min_length=6, max_length=6)
    cells: tuple[EmergencyDepartmentMissingCell, ...] = Field(min_length=6, max_length=6)
    approved_department_inventory_receipt: Literal[None] = None
    approved_facility_system_crosswalk_receipt: Literal[None] = None
    prohibited_outputs: tuple[str, ...]
    acquisition_sha256: str = Field(pattern=SHA256_PATTERN)

    @field_validator("acquired_at", mode="before")
    @classmethod
    def preserve_timestamp_lexeme(cls, value: object) -> object:
        if value != "2026-07-19T05:06:00Z":
            raise ValueError("acquisition timestamp drift")
        return value

    @model_validator(mode="after")
    def validate_packet(self) -> Self:
        if tuple(self.systems) != SYSTEM_SLUGS:
            raise ValueError("exact six-system order required")
        if tuple(self.ahrq_header_columns) != AHRQ_HEADER_COLUMNS or tuple(self.hgi_header_columns) != HGI_COLUMNS:
            raise ValueError("source header drift")
        from shared.acquisition.scale_safety_net_patient_mix_packet import acquisition as prior_acquisition

        prior = prior_acquisition()
        if (
            self.cache_receipt.model_dump(mode="json") != prior.cache_receipt.model_dump(mode="json")
            or self.ahrq_system_artifact.model_dump(mode="json")
            != prior.ahrq_source_artifact.model_dump(mode="json")
        ):
            raise ValueError("exact inherited AHRQ system custody required")
        if [item.artifact_id for item in self.source_artifacts] != list(EXPECTED_ARTIFACTS):
            raise ValueError("source artifact order or set drift")
        if [row.system_slug for row in self.identity_rows] != list(SYSTEM_SLUGS):
            raise ValueError("identity order drift")
        refs: dict[str, EmergencyDepartmentIdentityRow] = {}
        for row in self.identity_rows:
            if (row.health_sys_id, row.health_sys_name) != SYSTEM_AHRQ_IDENTITIES[row.system_slug]:
                raise ValueError("product-to-AHRQ identity substitution")
            actual = (row.row_number, row.source_row_sha256, row.health_sys_id, row.health_sys_name, row.health_sys_city, row.health_sys_state)
            if actual != IDENTITY_ROWS[row.system_slug]:
                raise ValueError("identity row drift")
            refs[f"row:system:{row.health_sys_id}:{row.row_number}"] = row
        if [item.evaluation_id for item in self.source_evaluations] != list(EVALUATION_IDS):
            raise ValueError("source evaluation order or set drift")
        for item in self.source_evaluations:
            actual_evaluation = (
                item.artifact_ref,
                item.exact_locator,
                item.evaluated_unit,
                item.exclusion_reason,
            )
            if actual_evaluation != EXPECTED_EVALUATIONS[item.evaluation_id]:
                raise ValueError("source evaluation semantic drift")
        all_artifacts = (self.ahrq_system_artifact.artifact_id, *EXPECTED_ARTIFACTS)
        if [cell.system_slug for cell in self.cells] != list(SYSTEM_SLUGS):
            raise ValueError("cell order drift")
        for cell in self.cells:
            row = refs.get(cell.identity_row_ref)
            if row is None or row.system_slug != cell.system_slug:
                raise ValueError("cell identity reference drift")
            if cell.source_artifact_refs != all_artifacts:
                raise ValueError("cell source graph drift")
            if cell.blocker_codes != tuple(sorted(COMMON_BLOCKERS | EXTRA_BLOCKERS[cell.system_slug])):
                raise ValueError("cell blocker drift")
            if cell.finding != FINDINGS[cell.system_slug]:
                raise ValueError("cell finding drift")
        if self.prohibited_outputs != PROHIBITED_OUTPUTS:
            raise ValueError("no-execution prohibitions drift")
        if self.acquisition_sha256 != semantic_hash(self):
            raise ValueError("acquisition self-hash drift")
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
        from shared.acquisition.scale_emergency_department_count_packet import acquisition

        schema["const"] = acquisition().model_dump(mode="json")
        return schema


def semantic_hash(value: EmergencyDepartmentCountAcquisition) -> str:
    return canonical_sha256(value.model_dump(mode="json", exclude={"acquisition_sha256"}))


def build_emergency_department_count_acquisition(
    payload: Mapping[str, object],
) -> EmergencyDepartmentCountAcquisition:
    body = dict(payload)
    body.pop("acquisition_sha256", None)
    body["acquisition_sha256"] = canonical_sha256(body)
    return EmergencyDepartmentCountAcquisition.model_validate(body)


__all__ = [
    "ED_DEFINITION", "EVALUATION_IDS", "EXPECTED_ARTIFACTS", "EXPECTED_ARTIFACT_URLS",
    "EXPECTED_EVALUATIONS", "PROHIBITED_OUTPUTS",
    "EmergencyDepartmentArtifact", "EmergencyDepartmentCountAcquisition",
    "EmergencyDepartmentIdentityRow", "EmergencyDepartmentMissingCell",
    "EmergencyDepartmentSourceEvaluation", "PriorSafetyNetToolkitLineage",
    "FrozenTabularSourceArtifact", "FrozenValidatedCacheReceipt",
    "build_emergency_department_count_acquisition", "semantic_hash",
]
