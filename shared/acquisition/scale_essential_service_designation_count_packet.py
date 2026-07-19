"""Reviewed all-six essential-service designation no-go packet."""

from __future__ import annotations

import hashlib
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import polars as pl
from pypdf import PdfReader

from shared.acquisition.scale_emergency_department_count_packet import acquisition as prior_acquisition
from shared.acquisition.scale_essential_service_designation_count_contract import (
    DESIGNATION_DEFINITION,
    EXPECTED_ARTIFACTS,
    EXPECTED_ARTIFACT_URLS,
    PROHIBITED_OUTPUTS,
    EssentialServiceDesignationCountAcquisition,
    build_essential_service_designation_count_acquisition,
)
from shared.acquisition.scale_essential_service_designation_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    COMMON_BLOCKERS,
    EXTRA_BLOCKERS,
    FINDINGS,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS

ACQUIRED_AT = "2026-07-19T09:04:43Z"
LRO_MEMBER = "2026Q3/PSF Parquet (April 2026)/IPSF_INP_LRO_2026-04-01.parquet"
LRO_SHA256 = "sha256:d35f2489bdd61279a3817a93282d72c1a014f301ac434f83b860415f5df68925"
LRO_LENGTH = 681137
LRO_REQUIRED_COLUMNS = (
    "providerCcn", "effectiveDate", "terminationDate", "providerType", "stateCode"
)


def acquisition() -> EssentialServiceDesignationCountAcquisition:
    """Return the immutable seventh-cycle no-count evaluation."""

    prior = prior_acquisition()
    rows = {row.system_slug: row for row in prior.identity_rows}
    artifacts = [
        {
            "artifact_id": artifact_id,
            "source_name": item[0],
            "source_url": EXPECTED_ARTIFACT_URLS[artifact_id][0],
            "landing_page": EXPECTED_ARTIFACT_URLS[artifact_id][1],
            "source_period": item[1],
            "media_type": item[2],
            "payload_sha256": item[3],
            "content_length": item[4],
            "retrieved_at": item[5],
            "source_modified": item[6],
            "rights_classification": "public_domain",
            "rights_basis": "United States government primary-source work",
            "custody_state": "frozen_verified_external",
            "exact_locator": item[7],
        }
        for artifact_id, item in EXPECTED_ARTIFACTS.items()
    ]
    evaluations = [
        {
            "evaluation_id": evaluation_id,
            "artifact_ref": artifact_ref,
            "exact_locator": locator,
            "evaluated_unit": unit,
            "reports_system_count": False,
            "approved_taxonomy": False,
            "approved_current_crosswalk": False,
            "provider_type_aggregation_performed": False,
            "usable_for_scale_input": False,
            "exclusion_reason": reason,
        }
        for evaluation_id, artifact_ref, locator, unit, reason in (
            ("evaluation:ahrq-system-schema", prior.ahrq_system_artifact.artifact_id, "Exact 40-column system CSV header; no essential-service designation field", "system_row", "The AHRQ system row binds identity but reports no essential-service designation count or approved taxonomy."),
            ("evaluation:ahrq-ccn-membership", "artifact:ahrq-compendium:hospital-linkage:2023", EXPECTED_ARTIFACTS["artifact:ahrq-compendium:hospital-linkage:2023"][7], "ccn_hospital", "A 2023 CCN membership row is stale identity context, not a current designation or product-system crosswalk."),
            ("evaluation:cms-psf-provider-record", "artifact:cms:psf-parquet-april-2026", EXPECTED_ARTIFACTS["artifact:cms:psf-parquet-april-2026"][7], "ccn_effective_provider_record", "providerType and effective-record context are not countable without approved eligibility, expiry, combination-code, and crosswalk rules."),
            ("evaluation:cms-provider-type-manual", "artifact:cms:claims-processing-manual-ch3:rev-13757", EXPECTED_ARTIFACTS["artifact:cms:claims-processing-manual-ch3:rev-13757"][7], "provider_type_code_definition", "The manual defines facility payment classifications but does not approve an essential-service taxonomy or system rollup."),
            ("evaluation:cms-psf-release-page", "artifact:cms:psf-release-page:april-2026", EXPECTED_ARTIFACTS["artifact:cms:psf-release-page:april-2026"][7], "release_metadata", "The release page establishes custody and vintage only; it does not define a countable designation taxonomy."),
        )
    ]
    all_refs = (prior.ahrq_system_artifact.artifact_id, *EXPECTED_ARTIFACTS)
    payload = {
        "schema_version": "ushso.scale-essential-service-designation-count-acquisition.v7",
        "acquisition_id": "scale-essential-service-designation-count-all-six-2026-07-19",
        "workflow_id": "scale-essential-service-designation-count-acquisition.v7",
        "input_family": "essential_service_designation_count",
        "systems": list(SYSTEM_SLUGS), "acquired_at": ACQUIRED_AT, "producer_version": "HDM-tmj",
        "prior_cycle": {
            "input_family": "emergency_department_count",
            "data_feature": _group("95e7f51dfe9ec8c3f7b49e5145685fdc54df049c"),
            "data_merge": _group("ec350c6a0b4ed62aefc9c6e5e1be0a0c0e6b5f62"),
            "data_tracker_merge": _group("d4936645e7be04c221916d33d6d805d9d509bb44"),
            "binding_feature": _group("9e96aa1ce3793c19b43d1e19f5791facc543d113"),
            "binding_merge": _group("1154c2bfc85f193b0bfc18773e12aa21ab4d2fba"),
            "binding_tracker_merge": _group("ebffaffdfb4e20f565f13cbadd9b5f8927a2ca4b"),
            "agents_review_feature": _group("7167d204900ee3fad10a3a4d0f141ba345ac2c6a"),
            "agents_review_merge": _group("335f3f44c65554a6a0be67507db85a67784e4be5"),
            "agents_tracker_merge": _group("4bc1d3674a465ba5e6cb815d57519a22294c32c8"),
            "admission_feature": _group("3c6dc3905e75e1dc21e98cca3def09018e95a805"),
            "admission_merge": _group("c4adbb0444ffac141247a170dd03a538a80855d3"),
            "tracker_merge": _group("9bb773be66ade3bd123630b1fa6b4485a4f54038"),
            "cumulative_packet_sha256": "sha256:7679f9a26936cf5508e1909ceec974f1025eaf7212c5ac844bc2a21ff5d8551e",
            "cumulative_packet_transport_sha256": "sha256:75fefeed38d2885351e89b4a828f1b666e0092dffe31ab64f86df5c79fc1dde3",
            "agents_manifest_sha256": "sha256:b84587ce02e4d69089b68d19bd160a731bf2ea28f961ee6fb6e841271ece5e24",
            "cumulative_review_sha256": "sha256:80c7e8578a3ec22c6fdd8a22f50296485ba5952fd75ab4256754296e165749f2",
            "cumulative_review_transport_sha256": "sha256:630ab57ae1d16f2f926668dcca3b1beb2d2a115ce1fe28bd33411106254e8334",
            "cumulative_assurance_sha256": "sha256:628c7b0b3d0318fe801b59f01793adf1ba0d0a6999aae4fb67f4e210a8ce1856",
            "cumulative_assurance_transport_sha256": "sha256:045cdd33eecae8977e0389d6c9fb89e4c04bda54642cbaad969936fac1190240",
            "reusable_manifest_sha256": "sha256:8d397c152d63b0805d5a398b0f6c0e9e54e17a3ea358ec570b8b5034ec5cbf0d",
            "reusable_manifest_transport_sha256": "sha256:a5635b5c5f9571678685f9dd2f2876b39eab8a5f35d45973f6f6b7077e811b08",
            "terminal_status": "blocked", "failure_code": "human_review_required",
        },
        "cache_receipt": prior.cache_receipt.model_dump(mode="json"),
        "ahrq_system_artifact": prior.ahrq_system_artifact.model_dump(mode="json"),
        "source_artifacts": artifacts, "ahrq_header_columns": list(AHRQ_HEADER_COLUMNS),
        "identity_rows": [row.model_dump(mode="json") for row in prior.identity_rows],
        "source_evaluations": evaluations,
        "cells": [{
            "system_slug": slug, "input_family": "essential_service_designation_count",
            "candidate_value": None, "unit": "eligible_current_designations",
            "desired_definition": DESIGNATION_DEFINITION,
            "source_period": "not_available_on_comparable_basis",
            "source_artifact_refs": list(all_refs),
            "identity_row_ref": f"row:system:{rows[slug].health_sys_id}:{rows[slug].row_number}",
            "missingness": "unavailable_public",
            "blocker_codes": sorted(COMMON_BLOCKERS | EXTRA_BLOCKERS[slug]),
            "finding": FINDINGS[slug],
            "provider_type_aggregated": False, "combination_codes_expanded": False,
            "combination_codes_deduplicated": False,
            "stale_ahrq_rollup_used": False, "expired_or_terminated_included": False,
            "state_federal_mixed": False, "narrative_substitution_used": False,
            "missing_as_zero": False, "imputed": False, "fabricated_zero": False,
            "approved_for_scale": False,
        } for slug in SYSTEM_SLUGS],
        "approved_designation_taxonomy_receipt": None,
        "approved_facility_system_crosswalk_receipt": None,
        "prohibited_outputs": list(PROHIBITED_OUTPUTS),
    }
    return build_essential_service_designation_count_acquisition(payload)


def verify_essential_service_designation_count_source_bytes(
    value: EssentialServiceDesignationCountAcquisition,
    cache_root: Path, ahrq_linkage: Path, cms_psf_zip: Path,
    cms_manual: Path, cms_release_page: Path,
) -> None:
    """Verify frozen custody and the unaggregated provider-record schema."""

    from shared.acquisition.scale_physician_count_packet import (
        acquisition as physician_acquisition,
        verify_physician_count_source_bytes,
    )
    verify_physician_count_source_bytes(physician_acquisition(), cache_root)
    for artifact, path in zip(value.source_artifacts, (ahrq_linkage, cms_psf_zip, cms_manual, cms_release_page), strict=True):
        if not path.is_file():
            raise ValueError(f"source file missing: {artifact.artifact_id}")
        raw = path.read_bytes()
        if len(raw) != artifact.content_length or _sha(raw) != artifact.payload_sha256:
            raise ValueError(f"source byte drift: {artifact.artifact_id}")
    with ZipFile(cms_psf_zip) as archive:
        raw_lro = archive.read(LRO_MEMBER)
    if len(raw_lro) != LRO_LENGTH or _sha(raw_lro) != LRO_SHA256:
        raise ValueError("CMS PSF inner parquet drift")
    schema = pl.read_parquet_schema(BytesIO(raw_lro))
    if not all(column in schema for column in LRO_REQUIRED_COLUMNS):
        raise ValueError("CMS PSF provider-record schema drift")
    page_text = " ".join((PdfReader(cms_manual).pages[index].extract_text() or "") for index in range(370, 374))
    if not all(marker in page_text for marker in ("Provider Type", "Sole Community Hospital", "Critical Access Hospital")):
        raise ValueError("CMS providerType manual locator drift")
    html = cms_release_page.read_text(encoding="utf-8", errors="strict")
    if "PSF Parquet (April 2026)" not in html:
        raise ValueError("CMS PSF release-page locator drift")


def _sha(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _group(value: str) -> str:
    return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:32]}-{value[32:]}"


__all__ = ["ACQUIRED_AT", "acquisition", "verify_essential_service_designation_count_source_bytes"]
