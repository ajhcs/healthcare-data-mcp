"""Immutable v7 contract for all-six essential-service designation evaluation."""

from __future__ import annotations

from typing import Any, Literal, Mapping, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode

from shared.acquisition.scale_essential_service_designation_count_declaration import (
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
DESIGNATION_DEFINITION = "Count of current eligible essential-service designations under one approved issuer taxonomy and duplicate rule, within one approved product-system boundary and common effective period"

EXPECTED_ARTIFACTS = {
    "artifact:ahrq-compendium:hospital-linkage:2023": (
        "AHRQ Compendium of U.S. Health Systems Hospital Linkage", "2023 dated hospital-to-system linkage",
        "text/csv", "sha256:a86146f10c8de626fea1da3a24b756e6a68165e449ae3687f1e90d6bdf129727", 1528734,
        "2026-07-16T21:39:39.964148Z", "2025-06-02T20:40:39Z",
        "CSV header and exact rows keyed by health_sys_id and CCN; schema fingerprint sha256:633359a9d9203acdcd4d2acec3d89761434e0cb0eeda9e8246c06a5f292e7150",
    ),
    "artifact:cms:psf-parquet-april-2026": (
        "CMS Provider Specific File Parquet", "April 2026 release",
        "application/zip", "sha256:979aa5997d0e7cf309d2ce19b52aa500e62fd0df8df19f0d530ff3fa3924a3ba", 34168146,
        "2026-07-19T09:04:42Z", "2026-04-15T10:54:40Z",
        "ZIP member 2026Q3/PSF Parquet (April 2026)/IPSF_INP_LRO_2026-04-01.parquet; member sha256:d35f2489bdd61279a3817a93282d72c1a014f301ac434f83b860415f5df68925; 681137 bytes; columns include providerCcn, effectiveDate, terminationDate, providerType, and stateCode",
    ),
    "artifact:cms:claims-processing-manual-ch3:rev-13757": (
        "CMS Medicare Claims Processing Manual Chapter 3", "Revision 13757 issued 2026-04-30",
        "application/pdf", "sha256:b02bd622dd8494a5120c2409d3d4cd48512df998d5a4b9efe354c6cc851a714c", 2565672,
        "2026-07-19T09:04:42Z", "2026-07-17T19:04:01Z",
        "Physical pages 371-374, Addendum A: CCN/providerType definitions and codes 07, 14-18, 21-23, and 37",
    ),
    "artifact:cms:psf-release-page:april-2026": (
        "CMS Provider Specific Data release page", "April 2026 release page",
        "text/html", "sha256:fd7edb400dd79908c99d1f07ad943e3a97f0f0b08a7017b4dccd7f2663ceea76", 213453,
        "2026-07-19T09:04:43Z", "2026-07-19T04:20:34Z",
        "Page identifies the April 2026 Parquet release and describes PSF as provider facts",
    ),
}

EXPECTED_ARTIFACT_URLS = {
    "artifact:ahrq-compendium:hospital-linkage:2023": (
        "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
        "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
    ),
    "artifact:cms:psf-parquet-april-2026": (
        "https://www.cms.gov/files/zip/psf-parquet-april-2026.zip",
        "https://www.cms.gov/medicare/payment/prospective-payment-systems/provider-specific-data-public-use-sas-format",
    ),
    "artifact:cms:claims-processing-manual-ch3:rev-13757": (
        "https://www.cms.gov/regulations-and-guidance/guidance/manuals/downloads/clm104c03.pdf",
        "https://www.cms.gov/medicare/regulations-guidance/manuals/internet-only-manuals-ioms-items/cms018912",
    ),
    "artifact:cms:psf-release-page:april-2026": (
        "https://www.cms.gov/medicare/payment/prospective-payment-systems/provider-specific-data-public-use-sas-format",
        "https://www.cms.gov/medicare/payment/prospective-payment-systems/provider-specific-data-public-use-sas-format",
    ),
}

EVALUATION_IDS = (
    "evaluation:ahrq-system-schema",
    "evaluation:ahrq-ccn-membership",
    "evaluation:cms-psf-provider-record",
    "evaluation:cms-provider-type-manual",
    "evaluation:cms-psf-release-page",
)

EXPECTED_EVALUATIONS = {
    "evaluation:ahrq-system-schema": (
        "artifact:ahrq-compendium:system:2023",
        "Exact 40-column system CSV header; no essential-service designation field",
        "system_row",
        "The AHRQ system row binds identity but reports no essential-service designation count or approved taxonomy.",
    ),
    "evaluation:ahrq-ccn-membership": (
        "artifact:ahrq-compendium:hospital-linkage:2023",
        EXPECTED_ARTIFACTS["artifact:ahrq-compendium:hospital-linkage:2023"][7],
        "ccn_hospital",
        "A 2023 CCN membership row is stale identity context, not a current designation or product-system crosswalk.",
    ),
    "evaluation:cms-psf-provider-record": (
        "artifact:cms:psf-parquet-april-2026",
        EXPECTED_ARTIFACTS["artifact:cms:psf-parquet-april-2026"][7],
        "ccn_effective_provider_record",
        "providerType and effective-record context are not countable without approved eligibility, expiry, combination-code, and crosswalk rules.",
    ),
    "evaluation:cms-provider-type-manual": (
        "artifact:cms:claims-processing-manual-ch3:rev-13757",
        EXPECTED_ARTIFACTS["artifact:cms:claims-processing-manual-ch3:rev-13757"][7],
        "provider_type_code_definition",
        "The manual defines facility payment classifications but does not approve an essential-service taxonomy or system rollup.",
    ),
    "evaluation:cms-psf-release-page": (
        "artifact:cms:psf-release-page:april-2026",
        EXPECTED_ARTIFACTS["artifact:cms:psf-release-page:april-2026"][7],
        "release_metadata",
        "The release page establishes custody and vintage only; it does not define a countable designation taxonomy.",
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


class PriorEmergencyDepartmentToolkitLineage(StrictModel):
    input_family: Literal["emergency_department_count"] = "emergency_department_count"
    data_feature: Literal["95e7f51d-fe9e-c8c3-f7b4-9e5145685fdc-54df049c"]
    data_merge: Literal["ec350c6a-0b4e-d62a-efc9-c6e5e1be0a0c-0e6b5f62"]
    data_tracker_merge: Literal["d4936645-e7be-04c2-2191-6d33d6d805d9-d509bb44"]
    binding_feature: Literal["9e96aa1c-e379-3c19-b43d-1e19f5791fac-c543d113"]
    binding_merge: Literal["1154c2bf-c85f-193b-0bfc-18773e12aa21-ab4d2fba"]
    binding_tracker_merge: Literal["ebffaffd-fb4e-20f5-65f1-3cbadd9b5f89-27a2ca4b"]
    agents_review_feature: Literal["7167d204-900e-e3fa-d10a-3a4d0f141ba3-45ac2c6a"]
    agents_review_merge: Literal["335f3f44-c655-54a6-a0be-67507db85a67-784e4be5"]
    agents_tracker_merge: Literal["4bc1d367-4a46-5ba5-e6cb-815d57519a22-294c32c8"]
    admission_feature: Literal["3c6dc390-5e75-e1dc-21e9-8cca3def0901-8e95a805"]
    admission_merge: Literal["c4adbb04-44ff-ac14-1247-a170dd03a538-a80855d3"]
    tracker_merge: Literal["9bb773be-66ad-e3bd-1236-30b1fa6b4485-a4f54038"]
    cumulative_packet_sha256: Literal["sha256:7679f9a26936cf5508e1909ceec974f1025eaf7212c5ac844bc2a21ff5d8551e"]
    cumulative_packet_transport_sha256: Literal["sha256:75fefeed38d2885351e89b4a828f1b666e0092dffe31ab64f86df5c79fc1dde3"]
    agents_manifest_sha256: Literal["sha256:b84587ce02e4d69089b68d19bd160a731bf2ea28f961ee6fb6e841271ece5e24"]
    cumulative_review_sha256: Literal["sha256:80c7e8578a3ec22c6fdd8a22f50296485ba5952fd75ab4256754296e165749f2"]
    cumulative_review_transport_sha256: Literal["sha256:630ab57ae1d16f2f926668dcca3b1beb2d2a115ce1fe28bd33411106254e8334"]
    cumulative_assurance_sha256: Literal["sha256:628c7b0b3d0318fe801b59f01793adf1ba0d0a6999aae4fb67f4e210a8ce1856"]
    cumulative_assurance_transport_sha256: Literal["sha256:045cdd33eecae8977e0389d6c9fb89e4c04bda54642cbaad969936fac1190240"]
    reusable_manifest_sha256: Literal["sha256:8d397c152d63b0805d5a398b0f6c0e9e54e17a3ea358ec570b8b5034ec5cbf0d"]
    reusable_manifest_transport_sha256: Literal["sha256:a5635b5c5f9571678685f9dd2f2876b39eab8a5f35d45973f6f6b7077e811b08"]
    terminal_status: Literal["blocked"] = "blocked"
    failure_code: Literal["human_review_required"] = "human_review_required"


class EssentialServiceDesignationArtifact(StrictModel):
    artifact_id: str
    source_name: str
    source_url: str
    landing_page: str
    source_period: str
    media_type: Literal["text/csv", "application/zip", "application/pdf", "text/html"]
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
            raise ValueError("essential-service-designation source artifact drift")
        if (self.source_url, self.landing_page) != EXPECTED_ARTIFACT_URLS[self.artifact_id]:
            raise ValueError("essential-service-designation source locator drift")
        return self


class EssentialServiceDesignationIdentityRow(StrictModel):
    system_slug: str
    artifact_ref: Literal["artifact:ahrq-compendium:system:2023"]
    row_number: int = Field(ge=2)
    source_row_sha256: str = Field(pattern=SHA256_PATTERN)
    health_sys_id: str = Field(pattern=r"^HSI[0-9]{8}$")
    health_sys_name: str
    health_sys_city: str
    health_sys_state: str = Field(pattern=r"^[A-Z]{2}$")


class EssentialServiceDesignationSourceEvaluation(StrictModel):
    evaluation_id: str
    artifact_ref: str
    exact_locator: str
    evaluated_unit: Literal["system_row", "ccn_hospital", "ccn_effective_provider_record", "provider_type_code_definition", "release_metadata"]
    reports_system_count: Literal[False] = False
    approved_taxonomy: Literal[False] = False
    approved_current_crosswalk: Literal[False] = False
    provider_type_aggregation_performed: Literal[False] = False
    usable_for_scale_input: Literal[False] = False
    exclusion_reason: str = Field(min_length=1)


class EssentialServiceDesignationMissingCell(StrictModel):
    system_slug: str
    input_family: Literal["essential_service_designation_count"] = "essential_service_designation_count"
    candidate_value: Literal[None] = None
    unit: Literal["eligible_current_designations"] = "eligible_current_designations"
    desired_definition: Literal[
        "Count of current eligible essential-service designations under one approved issuer taxonomy and duplicate rule, within one approved product-system boundary and common effective period"
    ]
    source_period: Literal["not_available_on_comparable_basis"]
    source_artifact_refs: tuple[str, ...] = Field(min_length=5, max_length=5)
    identity_row_ref: str
    missingness: Literal["unavailable_public"] = "unavailable_public"
    blocker_codes: tuple[str, ...] = Field(min_length=13)
    finding: str
    provider_type_aggregated: Literal[False] = False
    combination_codes_expanded: Literal[False] = False
    combination_codes_deduplicated: Literal[False] = False
    stale_ahrq_rollup_used: Literal[False] = False
    expired_or_terminated_included: Literal[False] = False
    state_federal_mixed: Literal[False] = False
    narrative_substitution_used: Literal[False] = False
    missing_as_zero: Literal[False] = False
    imputed: Literal[False] = False
    fabricated_zero: Literal[False] = False
    approved_for_scale: Literal[False] = False


class EssentialServiceDesignationCountAcquisition(StrictModel):
    schema_version: Literal["ushso.scale-essential-service-designation-count-acquisition.v7"]
    acquisition_id: Literal["scale-essential-service-designation-count-all-six-2026-07-19"]
    workflow_id: Literal["scale-essential-service-designation-count-acquisition.v7"]
    input_family: Literal["essential_service_designation_count"]
    systems: tuple[str, ...]
    acquired_at: AwareDatetime
    producer_version: Literal["HDM-tmj"]
    prior_cycle: PriorEmergencyDepartmentToolkitLineage
    cache_receipt: FrozenValidatedCacheReceipt
    ahrq_system_artifact: FrozenTabularSourceArtifact
    source_artifacts: tuple[EssentialServiceDesignationArtifact, ...] = Field(min_length=4, max_length=4)
    ahrq_header_columns: tuple[str, ...] = Field(min_length=40, max_length=40)
    identity_rows: tuple[EssentialServiceDesignationIdentityRow, ...] = Field(min_length=6, max_length=6)
    source_evaluations: tuple[EssentialServiceDesignationSourceEvaluation, ...] = Field(min_length=5, max_length=5)
    cells: tuple[EssentialServiceDesignationMissingCell, ...] = Field(min_length=6, max_length=6)
    approved_designation_taxonomy_receipt: Literal[None] = None
    approved_facility_system_crosswalk_receipt: Literal[None] = None
    prohibited_outputs: tuple[str, ...]
    acquisition_sha256: str = Field(pattern=SHA256_PATTERN)

    @field_validator("acquired_at", mode="before")
    @classmethod
    def preserve_timestamp_lexeme(cls, value: object) -> object:
        if value != "2026-07-19T09:04:43Z":
            raise ValueError("acquisition timestamp drift")
        return value

    @model_validator(mode="after")
    def validate_packet(self) -> Self:
        if tuple(self.systems) != SYSTEM_SLUGS:
            raise ValueError("exact six-system order required")
        if tuple(self.ahrq_header_columns) != AHRQ_HEADER_COLUMNS:
            raise ValueError("source header drift")
        from shared.acquisition.scale_emergency_department_count_packet import acquisition as prior_acquisition

        prior = prior_acquisition()
        if (
            self.cache_receipt.model_dump(mode="json") != prior.cache_receipt.model_dump(mode="json")
            or self.ahrq_system_artifact.model_dump(mode="json")
            != prior.ahrq_system_artifact.model_dump(mode="json")
        ):
            raise ValueError("exact inherited AHRQ system custody required")
        if [item.artifact_id for item in self.source_artifacts] != list(EXPECTED_ARTIFACTS):
            raise ValueError("source artifact order or set drift")
        if [row.system_slug for row in self.identity_rows] != list(SYSTEM_SLUGS):
            raise ValueError("identity order drift")
        refs: dict[str, EssentialServiceDesignationIdentityRow] = {}
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
        from shared.acquisition.scale_essential_service_designation_count_packet import acquisition

        schema["const"] = acquisition().model_dump(mode="json")
        return schema


def semantic_hash(value: EssentialServiceDesignationCountAcquisition) -> str:
    return canonical_sha256(value.model_dump(mode="json", exclude={"acquisition_sha256"}))


def build_essential_service_designation_count_acquisition(
    payload: Mapping[str, object],
) -> EssentialServiceDesignationCountAcquisition:
    body = dict(payload)
    body.pop("acquisition_sha256", None)
    body["acquisition_sha256"] = canonical_sha256(body)
    return EssentialServiceDesignationCountAcquisition.model_validate(body)


__all__ = [
    "DESIGNATION_DEFINITION", "EVALUATION_IDS", "EXPECTED_ARTIFACTS", "EXPECTED_ARTIFACT_URLS",
    "EXPECTED_EVALUATIONS", "PROHIBITED_OUTPUTS",
    "EssentialServiceDesignationArtifact", "EssentialServiceDesignationCountAcquisition",
    "EssentialServiceDesignationIdentityRow", "EssentialServiceDesignationMissingCell",
    "EssentialServiceDesignationSourceEvaluation", "PriorEmergencyDepartmentToolkitLineage",
    "FrozenTabularSourceArtifact", "FrozenValidatedCacheReceipt",
    "build_essential_service_designation_count_acquisition", "semantic_hash",
]
