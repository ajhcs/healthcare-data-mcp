"""Reviewed all-six service-line-count source-evaluation packet."""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from pypdf import PdfReader

from shared.acquisition.scale_physician_count_packet import (
    acquisition as physician_acquisition,
    verify_physician_count_source_bytes,
)
from shared.acquisition.scale_service_line_count_contract import (
    AHRQ_EXCLUSION,
    AHRQ_LOCATOR,
    AHRQ_QUERY,
    AHRQ_SCOPE,
    CMS_EXCLUSION,
    CMS_LOCATOR,
    CMS_QUERY,
    CMS_SCOPE,
    PROHIBITED_OUTPUTS,
    SERVICE_LINE_DEFINITION,
    ServiceLineCountAcquisition,
    build_service_line_count_acquisition,
)
from shared.acquisition.scale_service_line_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    COMMON_BLOCKERS,
    EXTRA_BLOCKERS,
    FINDINGS,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS
from shared.contracts.public_evidence import canonical_sha256

ACQUIRED_AT = "2026-07-18T18:15:29Z"


def acquisition() -> ServiceLineCountAcquisition:
    """Return the immutable fourth-cycle service-line-count evaluation."""

    physician = physician_acquisition()
    physician_rows = {row.system_slug: row for row in physician.system_rows}
    ahrq = physician.source_artifacts[0]
    cms_url = "https://data.cms.gov/sites/default/files/2025-12/a167eaff-5167-4c2c-a133-9ec94f0ee112/RBCS%20Final%20Report_RY2025.pdf"
    http_receipt_body = {
        "status": 200,
        "final_url": cms_url,
        "content_type": "application/pdf",
        "content_length": 839953,
        "last_modified": "Mon, 08 Dec 2025 17:51:06 GMT",
        "retrieved_at": ACQUIRED_AT,
        "payload_sha256": "sha256:68ac55dcc2812c6d692134dec827ffc5056f60b5ddcf605575fb6f2025b193e4",
    }
    identity_rows = [
        {
            "system_slug": slug,
            "artifact_ref": ahrq.artifact_id,
            "row_number": physician_rows[slug].row_number,
            "source_row_sha256": physician_rows[slug].source_row_sha256,
            "row_key_column": "health_sys_id",
            "health_sys_id": physician_rows[slug].health_sys_id,
            "health_sys_name": physician_rows[slug].health_sys_name,
            "health_sys_city": physician_rows[slug].health_sys_city,
            "health_sys_state": physician_rows[slug].health_sys_state,
        }
        for slug in SYSTEM_SLUGS
    ]
    return build_service_line_count_acquisition(
        {
            "schema_version": "ushso.scale-service-line-count-acquisition.v4",
            "acquisition_id": "scale-service-line-count-all-six-2026-07-18",
            "workflow_id": "scale-service-line-count-acquisition.v4",
            "input_family": "service_line_count",
            "systems": list(SYSTEM_SLUGS),
            "acquired_at": ACQUIRED_AT,
            "producer_version": "HDM-kh4",
            "prior_cycle": {
                "repo": "healthcare-toolkit",
                "input_family": "physician_count",
                "binding_merge": _group_git_sha("581265a2f2c80f71832b87de787b8b93e3ac8b1c"),
                "binding_tracker_merge": _group_git_sha("4f62f957c4389a80101d15902d2b72cc4e089e07"),
                "admission_merge": _group_git_sha("cc3ccb3d26e44d410546003b7dec073a2b74ab17"),
                "tracker_merge": _group_git_sha("208b2ab97594316f0a3bd64649423091c11e6bbf"),
                "cumulative_packet_sha256": "sha256:282a369b9121a27afebbb20fec4810464d1b7efa3d67a07ea119537cbbed9aa5",
                "cumulative_packet_transport_sha256": "sha256:3b8a7fc610adaf77107658005d56d78268cdafc54254cb7aaa1a02e1d4566128",
                "cumulative_review_sha256": "sha256:181691932c17f47e42865422f30be923f9ed739cbacb8be23266dea5342f4d30",
                "cumulative_review_transport_sha256": "sha256:d5e4e4eb39a2c247a31ab4589134392f1f641df51ed97acbff9158c5cf2847d2",
                "cumulative_assurance_sha256": "sha256:8f82c8573ecea197d5ea79784e5f0c806a5ce4fb6a98e70d1ea1ec71a3ca28b8",
                "cumulative_assurance_transport_sha256": "sha256:4a4302a57757e9aba56a686b83272cd6043195b6b4c6b647517e3069cff90854",
                "reusable_manifest_sha256": "sha256:069a75443da0b8f39b778bb4abb4bd76807f48cb60be53c76dc96070f7ba794e",
                "reusable_manifest_transport_sha256": "sha256:958f86022d660c1b44094f6a0d47bbaf2175b7d418ee94043ada17239b2f54b4",
                "terminal_status": "blocked",
                "failure_code": "human_review_required",
            },
            "cache_receipt": physician.cache_receipt.model_dump(mode="json"),
            "ahrq_source_artifact": ahrq.model_dump(mode="json"),
            "cms_taxonomy_artifact": {
                "artifact_id": "artifact:cms-rbcs:final-report:2025",
                "source_name": "Centers for Medicare & Medicaid Services",
                "document_title": "Restructured BETOS Classification System Final Report, Release Year 2025",
                "source_url": cms_url,
                "landing_page": "https://data.cms.gov/provider-summary-by-type-of-service/provider-service-classifications/restructured-betos-classification-system",
                "source_period": "2025 release; Medicare Part B analysis window described by source",
                "media_type": "application/pdf",
                "payload_sha256": http_receipt_body["payload_sha256"],
                "content_length": 839953,
                "page_count": 48,
                "rights_classification": "public_domain",
                "custody_state": "frozen_verified_external",
                "http_receipt": {
                    **http_receipt_body,
                    "receipt_sha256": canonical_sha256(http_receipt_body),
                },
            },
            "ahrq_header_columns": list(AHRQ_HEADER_COLUMNS),
            "identity_rows": identity_rows,
            "source_evaluations": [
                {
                    "evaluation_id": "evaluation:ahrq-system-header",
                    "artifact_ref": ahrq.artifact_id,
                    "query": AHRQ_QUERY,
                    "query_sha256": canonical_sha256(AHRQ_QUERY),
                    "exact_locator": AHRQ_LOCATOR,
                    "evaluated_scope": AHRQ_SCOPE,
                    "system_level_identity_available": True,
                    "common_service_taxonomy_available": False,
                    "system_service_line_count_available": False,
                    "usable_for_scale_input": False,
                    "exclusion_reason": AHRQ_EXCLUSION,
                },
                {
                    "evaluation_id": "evaluation:cms-rbcs-taxonomy",
                    "artifact_ref": "artifact:cms-rbcs:final-report:2025",
                    "query": CMS_QUERY,
                    "query_sha256": canonical_sha256(CMS_QUERY),
                    "exact_locator": CMS_LOCATOR,
                    "evaluated_scope": CMS_SCOPE,
                    "system_level_identity_available": False,
                    "common_service_taxonomy_available": False,
                    "system_service_line_count_available": False,
                    "usable_for_scale_input": False,
                    "exclusion_reason": CMS_EXCLUSION,
                },
            ],
            "cells": [
                {
                    "system_slug": slug,
                    "input_family": "service_line_count",
                    "candidate_value": None,
                    "unit": "service_lines",
                    "desired_definition": SERVICE_LINE_DEFINITION,
                    "source_period": "not_available_on_comparable_basis",
                    "source_artifact_refs": [ahrq.artifact_id, "artifact:cms-rbcs:final-report:2025"],
                    "identity_row_ref": f"row:system:{physician_rows[slug].health_sys_id}:{physician_rows[slug].row_number}",
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
            "common_taxonomy_receipt": None,
            "hand_counted_marketing_pages": False,
            "prohibited_outputs": list(PROHIBITED_OUTPUTS),
        }
    )


def verify_service_line_count_source_bytes(
    value: ServiceLineCountAcquisition,
    cache_root: Path,
    cms_report: Path,
) -> None:
    """Verify exact AHRQ identity/schema custody and CMS taxonomy bytes."""

    verify_physician_count_source_bytes(physician_acquisition(), cache_root)
    manifest_path = cache_root / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    import json

    manifest = json.loads(manifest_path.read_bytes())
    entry = next(item for item in manifest["artifacts"] if item["relative_path"] == "ahrq_system_2023.csv")
    ahrq_path = Path(entry["path"]).resolve()
    with ahrq_path.open(newline="", encoding="cp1252") as handle:
        header = tuple(next(csv.reader(handle)))
    if header != tuple(value.ahrq_header_columns) or any("service_line" in item for item in header):
        raise ValueError("AHRQ service-line source evaluation header drift")

    raw = cms_report.read_bytes()
    cms = value.cms_taxonomy_artifact
    if len(raw) != cms.content_length or _sha256(raw) != cms.payload_sha256:
        raise ValueError("CMS RBCS source byte drift")
    reader = PdfReader(cms_report)
    if len(reader.pages) != cms.page_count:
        raise ValueError("CMS RBCS page count drift")
    page_8 = reader.pages[7].extract_text() or ""
    page_22 = reader.pages[21].extract_text() or ""
    if not all(marker in page_8 for marker in ("HCPCS Code Dictionary", "Medicare Part B")):
        raise ValueError("CMS RBCS taxonomy-scope marker drift")
    if not all(marker in page_22 for marker in ("Data Limitations", "Medicare Part B fee-for-service claims")):
        raise ValueError("CMS RBCS limitation marker drift")


def _sha256(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _group_git_sha(value: str) -> str:
    if len(value) != 40:
        raise ValueError("Git SHA-1 must contain exactly 40 characters")
    return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:32]}-{value[32:]}"


__all__ = ["ACQUIRED_AT", "acquisition", "verify_service_line_count_source_bytes"]
