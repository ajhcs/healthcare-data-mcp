"""Reviewed all-six safety-net patient-mix source-evaluation packet."""

from __future__ import annotations

import hashlib
from pathlib import Path

from pypdf import PdfReader

from shared.acquisition.scale_safety_net_patient_mix_contract import (
    AHRQ_EXCLUSION,
    AHRQ_LOCATOR,
    AHRQ_QUERY,
    AHRQ_SCOPE,
    CMS_EXCLUSION,
    CMS_LOCATOR,
    CMS_QUERY,
    CMS_SCOPE,
    PATIENT_MIX_DEFINITION,
    PROHIBITED_OUTPUTS,
    SafetyNetPatientMixAcquisition,
    build_safety_net_patient_mix_acquisition,
)
from shared.acquisition.scale_safety_net_patient_mix_declaration import (
    AHRQ_HEADER_COLUMNS,
    COMMON_BLOCKERS,
    EXTRA_BLOCKERS,
    FINDINGS,
    SAFETY_NET_INDICATOR_COLUMNS,
)
from shared.acquisition.scale_service_line_count_packet import (
    acquisition as service_line_acquisition,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS
from shared.contracts.public_evidence import canonical_sha256

ACQUIRED_AT = "2026-07-19T01:15:08Z"
CMS_URL = "https://www.cms.gov/outreach-and-education/medicare-learning-network-mln/mlnproducts/downloads/disproportionate_share_hospital.pdf"


def acquisition() -> SafetyNetPatientMixAcquisition:
    """Return the immutable fifth-cycle safety-net patient-mix evaluation."""

    service_line = service_line_acquisition()
    rows = {row.system_slug: row for row in service_line.identity_rows}
    ahrq = service_line.ahrq_source_artifact
    http_body = {
        "status": 200,
        "final_url": CMS_URL,
        "content_type": "application/pdf",
        "content_length": 951147,
        "last_modified": "Wed, 18 Sep 2024 20:23:56 GMT",
        "retrieved_at": ACQUIRED_AT,
        "payload_sha256": "sha256:a658fb1ec185cea715dbc175b8e225c39c806da2b353f8f86b617bcd8ebf390a",
    }
    return build_safety_net_patient_mix_acquisition(
        {
            "schema_version": "ushso.scale-safety-net-patient-mix-acquisition.v5",
            "acquisition_id": "scale-safety-net-patient-mix-all-six-2026-07-19",
            "workflow_id": "scale-safety-net-patient-mix-acquisition.v5",
            "input_family": "safety_net_patient_mix_pct",
            "systems": list(SYSTEM_SLUGS),
            "acquired_at": ACQUIRED_AT,
            "producer_version": "HDM-jhh",
            "prior_cycle": {
                "repo": "healthcare-toolkit",
                "input_family": "service_line_count",
                "binding_merge": _group_git_sha("d57b1883044475f9dac87eae1ac6806fda1d9728"),
                "binding_tracker_merge": _group_git_sha("3d612de3c5137624e845771334807e550bbf8b83"),
                "agents_review_merge": _group_git_sha("97b248f22e95c335db4048b16c090792e7d37801"),
                "agents_tracker_merge": _group_git_sha("0ead3b3831027ab2e03711efae5a30ca67b620a9"),
                "admission_merge": _group_git_sha("46ed66e69bcd595aa8984d2c5b48d6b0ab4f13de"),
                "tracker_merge": _group_git_sha("df429e9ab47d60025258942e88df036c389c8731"),
                "cumulative_packet_sha256": "sha256:bb41a834d64c52ae65beef077292b6986ff9754bf81441464150b0ea976b30f6",
                "cumulative_review_sha256": "sha256:004fcb8fbb6ae9a126bebb37dcba58496317fadfe7859df4415df43638808e55",
                "cumulative_review_transport_sha256": "sha256:d78e39153a1fa7cc26232832b0f6e9d00229e51d7707e7896958acfcd7394920",
                "cumulative_assurance_sha256": "sha256:0d5e9933a3538c6892f1050b42b5eeb3e56d040aa35af1478b683b207b77ad82",
                "cumulative_assurance_transport_sha256": "sha256:2397b4f3c1d8bbb2dbf940d2b0113269fbbca8c2a163a2c8ec2cc1eac4563d65",
                "reusable_manifest_sha256": "sha256:71713d716b2fc59379f6fe0e7ca1c80ca73bdb54eca93e723076502dd216978e",
                "reusable_manifest_transport_sha256": "sha256:1d7132ac0814cbfc629f007b06870740c1591c4ee16a715a69b9ba6fb8dfa7f9",
                "terminal_status": "blocked",
                "failure_code": "human_review_required",
            },
            "cache_receipt": service_line.cache_receipt.model_dump(mode="json"),
            "ahrq_source_artifact": ahrq.model_dump(mode="json"),
            "cms_dsh_artifact": {
                "artifact_id": "artifact:cms-mln:medicare-dsh:2024",
                "source_name": "Centers for Medicare & Medicaid Services",
                "document_title": "Medicare Disproportionate Share Hospital, MLN006741",
                "source_url": CMS_URL,
                "landing_page": "https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/disproportionate-share-hospital-dsh",
                "source_period": "September 2024 policy fact sheet; FY 2024 DPP rules",
                "media_type": "application/pdf",
                "payload_sha256": http_body["payload_sha256"],
                "content_length": 951147,
                "page_count": 8,
                "rights_classification": "unknown_review_required",
                "rights_basis": "Public CMS fact sheet; linked MLN content disclaimer and registered-mark terms were not independently frozen or reviewed",
                "custody_state": "frozen_verified_external",
                "http_receipt": {**http_body, "receipt_sha256": canonical_sha256(http_body)},
            },
            "ahrq_header_columns": list(AHRQ_HEADER_COLUMNS),
            "safety_net_indicator_columns": list(SAFETY_NET_INDICATOR_COLUMNS),
            "identity_rows": [
                {
                    "system_slug": slug,
                    "artifact_ref": ahrq.artifact_id,
                    "row_number": rows[slug].row_number,
                    "source_row_sha256": rows[slug].source_row_sha256,
                    "row_key_column": "health_sys_id",
                    "health_sys_id": rows[slug].health_sys_id,
                    "health_sys_name": rows[slug].health_sys_name,
                    "health_sys_city": rows[slug].health_sys_city,
                    "health_sys_state": rows[slug].health_sys_state,
                }
                for slug in SYSTEM_SLUGS
            ],
            "source_evaluations": [
                {
                    "evaluation_id": "evaluation:ahrq-system-safety-net-schema",
                    "artifact_ref": ahrq.artifact_id,
                    "query": AHRQ_QUERY,
                    "query_sha256": canonical_sha256(AHRQ_QUERY),
                    "exact_locator": AHRQ_LOCATOR,
                    "evaluated_scope": AHRQ_SCOPE,
                    "system_level_identity_available": True,
                    "common_numerator_denominator_available": False,
                    "system_patient_mix_percentage_available": False,
                    "usable_for_scale_input": False,
                    "exclusion_reason": AHRQ_EXCLUSION,
                },
                {
                    "evaluation_id": "evaluation:cms-medicare-dsh-definition",
                    "artifact_ref": "artifact:cms-mln:medicare-dsh:2024",
                    "query": CMS_QUERY,
                    "query_sha256": canonical_sha256(CMS_QUERY),
                    "exact_locator": CMS_LOCATOR,
                    "evaluated_scope": CMS_SCOPE,
                    "system_level_identity_available": False,
                    "common_numerator_denominator_available": False,
                    "system_patient_mix_percentage_available": False,
                    "usable_for_scale_input": False,
                    "exclusion_reason": CMS_EXCLUSION,
                },
            ],
            "cells": [
                {
                    "system_slug": slug,
                    "input_family": "safety_net_patient_mix_pct",
                    "candidate_value": None,
                    "unit": "percent",
                    "desired_definition": PATIENT_MIX_DEFINITION,
                    "source_period": "not_available_on_comparable_basis",
                    "source_artifact_refs": [ahrq.artifact_id, "artifact:cms-mln:medicare-dsh:2024"],
                    "identity_row_ref": f"row:system:{rows[slug].health_sys_id}:{rows[slug].row_number}",
                    "missingness": "unavailable_public",
                    "blocker_codes": sorted(COMMON_BLOCKERS | EXTRA_BLOCKERS[slug]),
                    "finding": FINDINGS[slug],
                    "imputed": False,
                    "aggregated": False,
                    "fabricated_zero": False,
                    "approved_for_scale": False,
                }
                for slug in SYSTEM_SLUGS
            ],
            "approved_numerator_receipt": None,
            "approved_denominator_receipt": None,
            "prohibited_outputs": list(PROHIBITED_OUTPUTS),
        }
    )


def verify_safety_net_patient_mix_source_bytes(
    value: SafetyNetPatientMixAcquisition,
    cache_root: Path,
    cms_dsh_report: Path,
) -> None:
    """Verify exact inherited AHRQ custody and CMS DSH definition bytes."""

    # Reuse the exact physician acquisition because it owns the AHRQ byte and
    # row-receipt verifier inherited unchanged by later cycles.
    from shared.acquisition.scale_physician_count_packet import (
        acquisition as physician_acquisition,
        verify_physician_count_source_bytes,
    )

    verify_physician_count_source_bytes(physician_acquisition(), cache_root)
    if not cms_dsh_report.is_file():
        raise ValueError("CMS DSH source file missing")
    raw = cms_dsh_report.read_bytes()
    cms = value.cms_dsh_artifact
    if len(raw) != cms.content_length or _sha256(raw) != cms.payload_sha256:
        raise ValueError("CMS DSH source byte drift")
    reader = PdfReader(cms_dsh_report)
    if len(reader.pages) != cms.page_count:
        raise ValueError("CMS DSH page count drift")
    page_3 = reader.pages[2].extract_text() or ""
    page_5 = reader.pages[4].extract_text() or ""
    page_7 = reader.pages[6].extract_text() or ""
    if not all(marker in page_3 for marker in ("disproportionate patient percentage", "total Medicare patient days", "total patient days")):
        raise ValueError("CMS DSH numerator/denominator marker drift")
    if not all(marker in page_5 for marker in ("Acute Care Hospital IPPS", "Worksheet S-10", "uncompensated care costs")):
        raise ValueError("CMS DSH scope marker drift")
    if not all(marker in page_7 for marker in ("Medicaid/non-Medicare days", "Total Medicare Days", "Total Patient Days")):
        raise ValueError("CMS DSH worked-example marker drift")


def _sha256(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _group_git_sha(value: str) -> str:
    if len(value) != 40:
        raise ValueError("Git SHA-1 must contain exactly 40 characters")
    return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:32]}-{value[32:]}"


__all__ = ["ACQUIRED_AT", "acquisition", "verify_safety_net_patient_mix_source_bytes"]
