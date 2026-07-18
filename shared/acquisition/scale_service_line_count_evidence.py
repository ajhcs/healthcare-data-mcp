"""Public Evidence Bundle v1 adapter for service-line-count acquisition v4."""

from __future__ import annotations

import re

from shared.acquisition.scale_service_line_count_contract import ServiceLineCountAcquisition
from shared.acquisition.scale_system_roster import SYSTEM_NAMES
from shared.contracts.public_evidence import PublicEvidenceBundleInput


def build_service_line_count_public_evidence_input(
    acquisition: ServiceLineCountAcquisition,
    *,
    producer_commit: str = "0" * 40,
) -> PublicEvidenceBundleInput:
    """Adapt explicit unavailable cells without creating numeric observations."""

    if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    ahrq = acquisition.ahrq_source_artifact
    cms = acquisition.cms_taxonomy_artifact
    ahrq_lineage = {
        "artifact_id": ahrq.artifact_id,
        "checksum_sha256": ahrq.payload_sha256,
        "media_type": ahrq.media_type,
        "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{ahrq.relative_path}",
        "cache_run_id": acquisition.cache_receipt.run_id,
        "connector": "validated-cache-manifest",
        "connector_version": "scale-service-line-count-connector.v4",
        "parser_version": "scale-service-line-count-parser.v4",
        "schema_fingerprint": ahrq.schema_fingerprint,
    }
    cms_lineage = {
        "artifact_id": cms.artifact_id,
        "checksum_sha256": cms.payload_sha256,
        "media_type": cms.media_type,
        "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/cms-rbcs-final-report-2025.pdf",
        "cache_run_id": acquisition.acquisition_id,
        "connector": "governed-public-http",
        "connector_version": "scale-service-line-count-connector.v4",
        "parser_version": "pypdf-text-markers.v1",
        "schema_fingerprint": cms.payload_sha256,
    }
    rows = {row.system_slug: row for row in acquisition.identity_rows}
    cells = {cell.system_slug: cell for cell in acquisition.cells}
    ahrq_evaluation = next(
        item for item in acquisition.source_evaluations if item.evaluation_id == "evaluation:ahrq-system-header"
    )
    cms_evaluation = next(
        item for item in acquisition.source_evaluations if item.evaluation_id == "evaluation:cms-rbcs-taxonomy"
    )
    sources: list[dict[str, object]] = []
    for slug in acquisition.systems:
        row = rows[slug]
        sources.append(
            {
                "source_id": f"source:{slug}:service-line-count:ahrq-identity",
                "registry_id": "ahrq-compendium-2023:system",
                "registry_version": "2023",
                "receipt": {
                    "receipt_id": f"receipt:{slug}:service-line-count:ahrq-identity",
                    "source_name": ahrq.source_name,
                    "source_url": ahrq.source_url,
                    "dataset_id": ahrq.dataset_id,
                    "source_period": ahrq.source_period,
                    "landing_page": ahrq.landing_page,
                    "retrieved_at": acquisition.cache_receipt.retrieved_at,
                    "source_modified": None,
                    "cache_status": "validated_frozen_snapshot",
                    "cache_freshness": "Frozen 2023 release; current-roster comparability blocked",
                    "entity_scope": f"data-mcp:system:{slug}",
                    "query": {
                        "evaluation_query": ahrq_evaluation.query,
                        "evaluation_query_sha256": ahrq_evaluation.query_sha256,
                        "row_number": row.row_number,
                        "row_sha256": row.source_row_sha256,
                        "row_key_column": row.row_key_column,
                        "row_key_value": row.health_sys_id,
                        "header_sha256": ahrq.header_sha256,
                    },
                    "cache_key": ahrq_lineage["uri"],
                    "match_basis": "exact source-local AHRQ identity row; no service-line value reported",
                    "confidence": "exact_identity_and_schema_absence; unavailable_for_service_line_count",
                    "caveat": ahrq_evaluation.exclusion_reason,
                    "next_step": "Toolkit must preserve unavailable_public and every open taxonomy/boundary blocker.",
                    "acquisition_method": "scale-service-line-count-connector.v4",
                    "rights_classification": ahrq.rights_classification,
                    "row_locator": f"row={row.row_number}; health_sys_id={row.health_sys_id}; CSV header has no service_line field",
                    "artifact": ahrq_lineage,
                    "parent_receipt_ids": [],
                },
                "content_checksum": ahrq.payload_sha256,
                "access_rights": ahrq.rights_classification,
            }
        )
    sources.append(
        {
            "source_id": "source:all-six:service-line-count:cms-rbcs-taxonomy",
            "registry_id": "cms-rbcs-final-report",
            "registry_version": "2025",
            "receipt": {
                "receipt_id": "receipt:all-six:service-line-count:cms-rbcs-taxonomy",
                "source_name": cms.source_name,
                "source_url": cms.source_url,
                "dataset_id": "cms-rbcs-2025-final-report",
                "source_period": cms.source_period,
                "landing_page": cms.landing_page,
                "retrieved_at": cms.http_receipt.retrieved_at,
                "source_modified": "2025-12-08T17:51:06Z",
                "cache_status": "frozen_verified_external",
                "cache_freshness": "Exact 2025 release; excluded as a system service-offering source",
                "entity_scope": "Philadelphia six-system Scale roster",
                "query": {
                    "evaluation_query": cms_evaluation.query,
                    "evaluation_query_sha256": cms_evaluation.query_sha256,
                    "http_receipt_sha256": cms.http_receipt.receipt_sha256,
                    "page_count": cms.page_count,
                },
                "cache_key": cms_lineage["uri"],
                "match_basis": "taxonomy applicability evaluation only; no product-system identity join",
                "confidence": "exact_taxonomy_scope; not_a_service_line_offering_count",
                "caveat": cms_evaluation.exclusion_reason,
                "next_step": "A later Data acquisition must receipt a common offered-service taxonomy and six system-bound counts; claims aggregation is prohibited.",
                "acquisition_method": "scale-service-line-count-connector.v4",
                "rights_classification": cms.rights_classification,
                "row_locator": cms_evaluation.exact_locator,
                "artifact": cms_lineage,
                "parent_receipt_ids": [],
            },
            "content_checksum": cms.payload_sha256,
            "access_rights": cms.rights_classification,
        }
    )

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
                    "desired_definition": cells[acquisition.systems[0]].desired_definition,
                    "prior_physician_admission_merge_grouped_sha1": acquisition.prior_cycle.admission_merge,
                    "prior_physician_tracker_merge_grouped_sha1": acquisition.prior_cycle.tracker_merge,
                    "no_scale_score": True,
                    "no_service_line_hand_count": True,
                    "no_claims_aggregation": True,
                    "no_imputation": True,
                    "common_taxonomy_receipted": False,
                },
            },
            "scope": {
                "systems": list(acquisition.systems),
                "market": {"name": "Philadelphia six-system Scale roster", "roster_frozen": True},
                "periods": ["2023", "2025 taxonomy evaluation", "not_available_on_comparable_basis"],
            },
            "entities": [
                {
                    "entity_id": f"data-mcp:system:{slug}",
                    "canonical_name": SYSTEM_NAMES[slug],
                    "entity_type": "health_system",
                    "ahrq_system_id": rows[slug].health_sys_id,
                    "aliases": [
                        {
                            "source_name": ahrq.source_name,
                            "name": rows[slug].health_sys_name,
                            "identifier": rows[slug].health_sys_id,
                            "identifier_type": "ahrq_health_sys_id",
                        }
                    ],
                    "match_decisions": [], "conflicts": [], "unresolved_identifiers": [],
                }
                for slug in acquisition.systems
            ],
            "observations": [],
            "sources": sources,
            "coverage": [
                {
                    "coverage_id": f"coverage:{slug}:service_line_count",
                    "entity_ref": f"data-mcp:system:{slug}",
                    "measure_id": "service_line_count",
                    "status": "unavailable_public",
                    "observation_refs": [],
                    "reason": cells[slug].finding,
                }
                for slug in acquisition.systems
            ],
            "conflicts": [
                {
                    "conflict_id": f"conflict:{slug}:service-line-count:taxonomy-boundary",
                    "conflict_type": "scale_input_comparability",
                    "entity_refs": [f"data-mcp:system:{slug}"],
                    "observation_refs": [],
                    "receipt_refs": [
                        f"receipt:{slug}:service-line-count:ahrq-identity",
                        "receipt:all-six:service-line-count:cms-rbcs-taxonomy",
                    ],
                    "status": "open",
                    "rationale": f"{','.join(cells[slug].blocker_codes)}: {cells[slug].finding}",
                }
                for slug in acquisition.systems
            ],
            "input_artifacts": [ahrq_lineage, cms_lineage],
        }
    )


__all__ = ["build_service_line_count_public_evidence_input"]
