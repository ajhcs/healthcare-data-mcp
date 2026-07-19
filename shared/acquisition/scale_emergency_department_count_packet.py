"""Reviewed all-six emergency-department count no-go packet."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from xml.etree import ElementTree

from pypdf import PdfReader

from shared.acquisition.scale_emergency_department_count_contract import (
    ED_DEFINITION,
    EXPECTED_ARTIFACTS,
    EXPECTED_ARTIFACT_URLS,
    PROHIBITED_OUTPUTS,
    EmergencyDepartmentCountAcquisition,
    build_emergency_department_count_acquisition,
)
from shared.acquisition.scale_emergency_department_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    AHRQ_LINKAGE_COLUMNS,
    COMMON_BLOCKERS,
    EXTRA_BLOCKERS,
    FINDINGS,
    HGI_COLUMNS,
)
from shared.acquisition.scale_safety_net_patient_mix_packet import acquisition as prior_acquisition
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS

ACQUIRED_AT = "2026-07-19T05:06:00Z"

def acquisition() -> EmergencyDepartmentCountAcquisition:
    """Return the immutable sixth-cycle emergency-department evaluation."""

    prior = prior_acquisition()
    rows = {row.system_slug: row for row in prior.identity_rows}
    source_artifacts = [
        {
            "artifact_id": artifact_id,
            "source_name": expected[0],
            "source_url": EXPECTED_ARTIFACT_URLS[artifact_id][0],
            "landing_page": EXPECTED_ARTIFACT_URLS[artifact_id][1],
            "source_period": expected[1],
            "media_type": expected[2],
            "payload_sha256": expected[3],
            "content_length": expected[4],
            "retrieved_at": expected[5],
            "source_modified": expected[6],
            "rights_classification": "public_domain",
            "rights_basis": "United States government primary-source work",
            "custody_state": "frozen_verified_external",
            "exact_locator": expected[7],
        }
        for artifact_id, expected in EXPECTED_ARTIFACTS.items()
    ]
    all_refs = [prior.ahrq_source_artifact.artifact_id, *EXPECTED_ARTIFACTS]
    evaluations = [
        {
            "evaluation_id": "evaluation:ahrq-system-schema",
            "artifact_ref": prior.ahrq_source_artifact.artifact_id,
            "exact_locator": "Exact 40-column system CSV header; no emergency-department field",
            "evaluated_unit": "system_row",
            "reports_system_count": False,
            "enumerates_dedicated_departments": False,
            "usable_for_scale_input": False,
            "exclusion_reason": "The AHRQ system row binds identity but reports no emergency-department count or inventory.",
        },
        {
            "evaluation_id": "evaluation:ahrq-ccn-membership",
            "artifact_ref": "artifact:ahrq-compendium:hospital-linkage:2023",
            "exact_locator": EXPECTED_ARTIFACTS["artifact:ahrq-compendium:hospital-linkage:2023"][7],
            "evaluated_unit": "ccn_hospital",
            "reports_system_count": False,
            "enumerates_dedicated_departments": False,
            "usable_for_scale_input": False,
            "exclusion_reason": "A 2023 CCN hospital membership row is not a dedicated emergency-department unit and cannot resolve current roster membership.",
        },
        {
            "evaluation_id": "evaluation:cms-hgi-facility-boolean",
            "artifact_ref": "artifact:cms-provider-data:hgi:2026-04-28",
            "exact_locator": EXPECTED_ARTIFACTS["artifact:cms-provider-data:hgi:2026-04-28"][7],
            "evaluated_unit": "facility_boolean",
            "reports_system_count": False,
            "enumerates_dedicated_departments": False,
            "usable_for_scale_input": False,
            "exclusion_reason": "Emergency Services is one Yes/No flag per Facility ID, not an enumerated count of on-campus and off-campus dedicated ED departments.",
        },
        {
            "evaluation_id": "evaluation:cms-hgi-metadata",
            "artifact_ref": "artifact:cms-provider-data:hgi-metadata:2026-04-28",
            "exact_locator": EXPECTED_ARTIFACTS["artifact:cms-provider-data:hgi-metadata:2026-04-28"][7],
            "evaluated_unit": "dataset_metadata",
            "reports_system_count": False,
            "enumerates_dedicated_departments": False,
            "usable_for_scale_input": False,
            "exclusion_reason": "CMS describes a list of Medicare-registered hospitals, not a system or dedicated-department inventory.",
        },
        {
            "evaluation_id": "evaluation:cms-hospital-dictionary",
            "artifact_ref": "artifact:cms-provider-data:hospital-dictionary:2026-04",
            "exact_locator": EXPECTED_ARTIFACTS["artifact:cms-provider-data:hospital-dictionary:2026-04"][7],
            "evaluated_unit": "data_dictionary",
            "reports_system_count": False,
            "enumerates_dedicated_departments": False,
            "usable_for_scale_input": False,
            "exclusion_reason": "The dictionary declares a Char(3) Emergency Services field but supplies no department-count or campus enumeration field.",
        },
        {
            "evaluation_id": "evaluation:ecfr-dedicated-ed-definition",
            "artifact_ref": "artifact:ecfr:42-cfr-489.24:2026-07-16",
            "exact_locator": EXPECTED_ARTIFACTS["artifact:ecfr:42-cfr-489.24:2026-07-16"][7],
            "evaluated_unit": "dedicated_emergency_department",
            "reports_system_count": False,
            "enumerates_dedicated_departments": False,
            "usable_for_scale_input": False,
            "exclusion_reason": "The regulation defines the required unit but does not enumerate qualifying departments by product system and period.",
        },
    ]
    return build_emergency_department_count_acquisition(
        {
            "schema_version": "ushso.scale-emergency-department-count-acquisition.v6",
            "acquisition_id": "scale-emergency-department-count-all-six-2026-07-19",
            "workflow_id": "scale-emergency-department-count-acquisition.v6",
            "input_family": "emergency_department_count",
            "systems": list(SYSTEM_SLUGS),
            "acquired_at": ACQUIRED_AT,
            "producer_version": "HDM-3d9",
            "prior_cycle": {
                "input_family": "safety_net_patient_mix_pct",
                "data_feature": _group_git_sha("5a248a6d3eb452c482c0a60d8b9168d79ae9be26"),
                "data_merge": _group_git_sha("50eba1efda522e875ebfb0b3feadfd80f4073a78"),
                "data_tracker_merge": _group_git_sha("83ca5ddf9a2fdf7eb8afebf68955600b9270a52e"),
                "binding_merge": _group_git_sha("9376d38758d2098b8c1da09aac615ea5d4affb50"),
                "binding_tracker_merge": _group_git_sha("b3113f4867fb0eda84280b2245607e24a3959226"),
                "agents_review_merge": _group_git_sha("bd7b09545de1c3b7f17c306b6543440c493bc669"),
                "agents_tracker_merge": _group_git_sha("cbf9d93a71326e400143d491a2d3adbb513e96dc"),
                "admission_merge": _group_git_sha("61a67481a9f8bb40e81a2f8f59061664ca5694ba"),
                "tracker_merge": _group_git_sha("01aba0aa56448f17504e91f7f9754d96eb77ee7c"),
                "cumulative_packet_sha256": "sha256:af7ac7ce87a991b227673cfa8b6d92374bd01625217e7e21835f39abb289f365",
                "cumulative_review_sha256": "sha256:68658952d09b2f9b24ec9b062dace730b8ae1ef52b68a79d49de7c3059632fb4",
                "cumulative_review_transport_sha256": "sha256:e648b443d1b9d96552eaa2b1153bdc10eee608e97d11f9df87547cbaec83c8a3",
                "cumulative_assurance_sha256": "sha256:b82c08cd4ff3d7acf7be5b64c2463b22cd37b370e4f40eeb8fd5c7c04fe7f419",
                "cumulative_assurance_transport_sha256": "sha256:e7e2ce1dadc4d2c5549459ae6f5bd9362b6e1e06d32a4227952cbf07e3d2a2d6",
                "reusable_manifest_sha256": "sha256:86f148e3627f4e2b655bb3bab1c0e225ae9a5ab25399e80e2411e3b1a04991c1",
                "reusable_manifest_transport_sha256": "sha256:b00d79b155abe12bb24535f4b3b380c17483c415d974af257432f816ed2e268e",
                "terminal_status": "blocked",
                "failure_code": "human_review_required",
            },
            "cache_receipt": prior.cache_receipt.model_dump(mode="json"),
            "ahrq_system_artifact": prior.ahrq_source_artifact.model_dump(mode="json"),
            "source_artifacts": source_artifacts,
            "ahrq_header_columns": list(AHRQ_HEADER_COLUMNS),
            "hgi_header_columns": list(HGI_COLUMNS),
            "identity_rows": [
                {
                    "system_slug": slug,
                    "artifact_ref": prior.ahrq_source_artifact.artifact_id,
                    "row_number": rows[slug].row_number,
                    "source_row_sha256": rows[slug].source_row_sha256,
                    "health_sys_id": rows[slug].health_sys_id,
                    "health_sys_name": rows[slug].health_sys_name,
                    "health_sys_city": rows[slug].health_sys_city,
                    "health_sys_state": rows[slug].health_sys_state,
                }
                for slug in SYSTEM_SLUGS
            ],
            "source_evaluations": evaluations,
            "cells": [
                {
                    "system_slug": slug,
                    "input_family": "emergency_department_count",
                    "candidate_value": None,
                    "unit": "dedicated_emergency_departments",
                    "desired_definition": ED_DEFINITION,
                    "source_period": "not_available_on_comparable_basis",
                    "source_artifact_refs": all_refs,
                    "identity_row_ref": f"row:system:{rows[slug].health_sys_id}:{rows[slug].row_number}",
                    "missingness": "unavailable_public",
                    "blocker_codes": sorted(COMMON_BLOCKERS | EXTRA_BLOCKERS[slug]),
                    "finding": FINDINGS[slug],
                    "aggregated": False,
                    "flag_sum_used": False,
                    "campus_inference_used": False,
                    "missing_as_no": False,
                    "imputed": False,
                    "fabricated_zero": False,
                    "approved_for_scale": False,
                }
                for slug in SYSTEM_SLUGS
            ],
            "approved_department_inventory_receipt": None,
            "approved_facility_system_crosswalk_receipt": None,
            "prohibited_outputs": list(PROHIBITED_OUTPUTS),
        }
    )


def verify_emergency_department_count_source_bytes(
    value: EmergencyDepartmentCountAcquisition,
    cache_root: Path,
    ahrq_linkage: Path,
    cms_hgi: Path,
    cms_metadata: Path,
    cms_dictionary: Path,
    ecfr_definition: Path,
) -> None:
    """Verify all exact custody and reproduce the non-promoted row evaluation."""

    from shared.acquisition.scale_physician_count_packet import (
        acquisition as physician_acquisition,
        verify_physician_count_source_bytes,
    )

    verify_physician_count_source_bytes(physician_acquisition(), cache_root)
    paths = [ahrq_linkage, cms_hgi, cms_metadata, cms_dictionary, ecfr_definition]
    for artifact, path in zip(value.source_artifacts, paths, strict=True):
        if not path.is_file():
            raise ValueError(f"source file missing: {artifact.artifact_id}")
        raw = path.read_bytes()
        if len(raw) != artifact.content_length or _sha256(raw) != artifact.payload_sha256:
            raise ValueError(f"source byte drift: {artifact.artifact_id}")
    with ahrq_linkage.open(newline="", encoding="cp1252") as stream:
        linkage_reader = csv.DictReader(stream)
        if tuple(linkage_reader.fieldnames or ()) != AHRQ_LINKAGE_COLUMNS:
            raise ValueError("AHRQ hospital linkage header drift")
    with cms_hgi.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != HGI_COLUMNS:
            raise ValueError("CMS HGI header drift")
    metadata = json.loads(cms_metadata.read_text(encoding="utf-8"))
    distribution = metadata.get("distribution")
    if (
        metadata.get("title") != "Hospital General Information"
        or not isinstance(distribution, list)
        or not distribution
        or not isinstance(distribution[0], dict)
    ):
        raise ValueError("CMS HGI metadata meaning drift")
    first = distribution[0]
    if first.get("downloadURL") != EXPECTED_ARTIFACT_URLS["artifact:cms-provider-data:hgi:2026-04-28"][0] or first.get("describedBy") != EXPECTED_ARTIFACT_URLS["artifact:cms-provider-data:hospital-dictionary:2026-04"][0]:
        raise ValueError("CMS HGI metadata lineage drift")
    pdf = PdfReader(cms_dictionary)
    if len(pdf.pages) != 105:
        raise ValueError("CMS dictionary page count drift")
    page_20 = pdf.pages[19].extract_text() or ""
    if not all(marker in page_20 for marker in ("Hospital General Information", "Facility ID", "Emergency Services")):
        raise ValueError("CMS dictionary locator drift")
    root = ElementTree.parse(ecfr_definition).getroot()
    text = " ".join(part.strip() for part in root.itertext() if part.strip())
    if not all(marker in text for marker in ("Dedicated emergency department", "on or off the main hospital campus", "licensed by the State", "held out to the public", "one-third of all of its outpatient visits")):
        raise ValueError("eCFR dedicated-ED definition drift")


def _sha256(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _group_git_sha(value: str) -> str:
    if len(value) != 40:
        raise ValueError("Git SHA-1 must contain exactly 40 characters")
    return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:32]}-{value[32:]}"


__all__ = ["ACQUIRED_AT", "acquisition", "verify_emergency_department_count_source_bytes"]
