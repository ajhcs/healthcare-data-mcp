"""Public Evidence Bundle v1 adapter for emergency-department count v6."""

from __future__ import annotations

import re

from shared.acquisition.scale_emergency_department_count_contract import (
    EmergencyDepartmentCountAcquisition,
)
from shared.acquisition.scale_system_roster import SYSTEM_NAMES
from shared.contracts.public_evidence import PublicEvidenceBundleInput


def build_emergency_department_count_public_evidence_input(
    acquisition: EmergencyDepartmentCountAcquisition,
    *,
    producer_commit: str = "0" * 40,
) -> PublicEvidenceBundleInput:
    """Adapt exact missingness without summing flags or inventing a count."""

    if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    ahrq = acquisition.ahrq_system_artifact
    ahrq_lineage = {
        "artifact_id": ahrq.artifact_id,
        "checksum_sha256": ahrq.payload_sha256,
        "media_type": ahrq.media_type,
        "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{ahrq.relative_path}",
        "cache_run_id": acquisition.cache_receipt.run_id,
        "connector": "validated-cache-manifest",
        "connector_version": "scale-emergency-department-count-connector.v6",
        "parser_version": "scale-emergency-department-count-parser.v6",
        "schema_fingerprint": ahrq.schema_fingerprint,
    }
    external_lineages = [
        {
            "artifact_id": artifact.artifact_id,
            "checksum_sha256": artifact.payload_sha256,
            "media_type": artifact.media_type,
            "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{artifact.artifact_id.replace(':', '-')}",
            "cache_run_id": acquisition.acquisition_id,
            "connector": "governed-public-primary-source",
            "connector_version": "scale-emergency-department-count-connector.v6",
            "parser_version": "scale-emergency-department-count-parser.v6",
            "schema_fingerprint": artifact.payload_sha256,
        }
        for artifact in acquisition.source_artifacts
    ]
    lineage_by_id = {item["artifact_id"]: item for item in [ahrq_lineage, *external_lineages]}
    rows = {row.system_slug: row for row in acquisition.identity_rows}
    cells = {cell.system_slug: cell for cell in acquisition.cells}
    evaluation_by_artifact = {
        item.artifact_ref: item for item in acquisition.source_evaluations
        if item.artifact_ref != ahrq.artifact_id
    }
    sources: list[dict[str, object]] = []
    identity_receipts: list[str] = []
    for slug in acquisition.systems:
        row = rows[slug]
        receipt_id = f"receipt:{slug}:emergency-department-count:ahrq-identity"
        identity_receipts.append(receipt_id)
        sources.append(
            {
                "source_id": f"source:{slug}:emergency-department-count:ahrq-identity",
                "registry_id": "ahrq-compendium-2023:system",
                "registry_version": "2023",
                "receipt": {
                    "receipt_id": receipt_id,
                    "source_name": ahrq.source_name,
                    "source_url": ahrq.source_url,
                    "dataset_id": ahrq.dataset_id,
                    "source_period": ahrq.source_period,
                    "landing_page": ahrq.landing_page,
                    "retrieved_at": acquisition.cache_receipt.retrieved_at,
                    "source_modified": None,
                    "cache_status": "validated_frozen_snapshot",
                    "cache_freshness": "Frozen 2023 system release; no ED field",
                    "entity_scope": f"data-mcp:system:{slug}",
                    "query": {
                        "row_number": row.row_number,
                        "row_sha256": row.source_row_sha256,
                        "row_key": row.health_sys_id,
                        "header_sha256": ahrq.header_sha256,
                    },
                    "cache_key": ahrq_lineage["uri"],
                    "match_basis": "exact source-local AHRQ system identity row; schema has no ED count",
                    "confidence": "exact_identity; unavailable_for_emergency_department_count",
                    "caveat": "Identity only; no department inventory or count is reported.",
                    "next_step": "Preserve unavailable_public until a common dedicated-ED inventory and product-system crosswalk are receipted.",
                    "acquisition_method": "scale-emergency-department-count-connector.v6",
                    "rights_classification": ahrq.rights_classification,
                    "row_locator": f"row={row.row_number}; health_sys_id={row.health_sys_id}",
                    "artifact": ahrq_lineage,
                    "parent_receipt_ids": [],
                },
                "content_checksum": ahrq.payload_sha256,
                "access_rights": ahrq.rights_classification,
            }
        )
    evaluation_receipts: list[str] = []
    for artifact in acquisition.source_artifacts:
        evaluation = evaluation_by_artifact[artifact.artifact_id]
        short = artifact.artifact_id.split(":", 1)[1].replace(":", "-")
        receipt_id = f"receipt:all-six:emergency-department-count:{short}"
        evaluation_receipts.append(receipt_id)
        sources.append(
            {
                "source_id": f"source:all-six:emergency-department-count:{short}",
                "registry_id": artifact.artifact_id,
                "registry_version": artifact.source_period,
                "receipt": {
                    "receipt_id": receipt_id,
                    "source_name": artifact.source_name,
                    "source_url": artifact.source_url,
                    "dataset_id": artifact.artifact_id,
                    "source_period": artifact.source_period,
                    "landing_page": artifact.landing_page,
                    "retrieved_at": artifact.retrieved_at,
                    "source_modified": (
                        artifact.source_modified
                        if "T" in artifact.source_modified
                        else None
                    ),
                    "cache_status": "frozen_verified_external",
                    "cache_freshness": "Exact reviewed source bytes; excluded as a comparable system-count source",
                    "entity_scope": "Philadelphia six-system Scale roster",
                    "query": {
                        "evaluated_unit": evaluation.evaluated_unit,
                        "reports_system_count": False,
                        "enumerates_dedicated_departments": False,
                    },
                    "cache_key": lineage_by_id[artifact.artifact_id]["uri"],
                    "match_basis": "primary-source definition or schema evidence only; no approved aggregation",
                    "confidence": "exact_source_semantics; unavailable_for_system_ed_count",
                    "caveat": evaluation.exclusion_reason,
                    "next_step": "Acquire an exact common-period dedicated-ED inventory and approved facility/campus-to-product-system crosswalk.",
                    "acquisition_method": "scale-emergency-department-count-connector.v6",
                    "rights_classification": artifact.rights_classification,
                    "row_locator": artifact.exact_locator,
                    "artifact": lineage_by_id[artifact.artifact_id],
                    "parent_receipt_ids": [],
                },
                "content_checksum": artifact.payload_sha256,
                "access_rights": artifact.rights_classification,
            }
        )
    all_receipts = [*identity_receipts, *evaluation_receipts]
    return PublicEvidenceBundleInput.model_validate(
        {
            "bundle_id": f"{acquisition.acquisition_id}:public-evidence",
            "producer": {"repo": "healthcare-data-mcp", "version": acquisition.producer_version, "commit": producer_commit},
            "created_at": acquisition.acquired_at,
            "request": {
                "workflow": acquisition.workflow_id,
                "parameters": {
                    "input_family": acquisition.input_family,
                    "desired_definition": cells[acquisition.systems[0]].desired_definition,
                    "prior_safety_net_data_feature": acquisition.prior_cycle.data_feature,
                    "prior_safety_net_data_merge": acquisition.prior_cycle.data_merge,
                    "prior_safety_net_data_tracker_merge": acquisition.prior_cycle.data_tracker_merge,
                    "prior_safety_net_binding_merge": acquisition.prior_cycle.binding_merge,
                    "prior_safety_net_binding_tracker_merge": acquisition.prior_cycle.binding_tracker_merge,
                    "prior_safety_net_agents_review_merge": acquisition.prior_cycle.agents_review_merge,
                    "prior_safety_net_agents_tracker_merge": acquisition.prior_cycle.agents_tracker_merge,
                    "prior_safety_net_admission_merge": acquisition.prior_cycle.admission_merge,
                    "prior_safety_net_tracker_merge": acquisition.prior_cycle.tracker_merge,
                    "prior_safety_net_packet_sha256": acquisition.prior_cycle.cumulative_packet_sha256,
                    "prior_safety_net_review_sha256": acquisition.prior_cycle.cumulative_review_sha256,
                    "prior_safety_net_review_transport_sha256": acquisition.prior_cycle.cumulative_review_transport_sha256,
                    "prior_safety_net_assurance_sha256": acquisition.prior_cycle.cumulative_assurance_sha256,
                    "prior_safety_net_assurance_transport_sha256": acquisition.prior_cycle.cumulative_assurance_transport_sha256,
                    "prior_safety_net_manifest_sha256": acquisition.prior_cycle.reusable_manifest_sha256,
                    "prior_safety_net_manifest_transport_sha256": acquisition.prior_cycle.reusable_manifest_transport_sha256,
                    "prior_safety_net_terminal_status": acquisition.prior_cycle.terminal_status,
                    "prior_safety_net_failure_code": acquisition.prior_cycle.failure_code,
                    "no_scale_score": True,
                    "no_flag_sum": True,
                    "no_facility_aggregation": True,
                    "no_campus_inference": True,
                    "no_missing_as_no_or_zero": True,
                    "approved_department_inventory_receipted": False,
                    "approved_crosswalk_receipted": False,
                },
            },
            "scope": {
                "systems": list(acquisition.systems),
                "market": {"name": "Philadelphia six-system Scale roster", "roster_frozen": True},
                "periods": ["2023", "2026-04-28", "42 CFR 489.24 as of 2026-07-16", "not_available_on_comparable_basis"],
            },
            "entities": [
                {
                    "entity_id": f"data-mcp:system:{slug}",
                    "canonical_name": SYSTEM_NAMES[slug],
                    "entity_type": "health_system",
                    "ahrq_system_id": rows[slug].health_sys_id,
                    "aliases": [{"source_name": ahrq.source_name, "name": rows[slug].health_sys_name, "identifier": rows[slug].health_sys_id, "identifier_type": "ahrq_health_sys_id"}],
                    "match_decisions": [], "conflicts": [], "unresolved_identifiers": [],
                }
                for slug in acquisition.systems
            ],
            "observations": [],
            "sources": sources,
            "coverage": [
                {
                    "coverage_id": f"coverage:{slug}:emergency_department_count",
                    "entity_ref": f"data-mcp:system:{slug}",
                    "measure_id": "emergency_department_count",
                    "status": "unavailable_public",
                    "observation_refs": [],
                    "reason": cells[slug].finding,
                }
                for slug in acquisition.systems
            ],
            "conflicts": [
                {
                    "conflict_id": f"conflict:{slug}:emergency-department-count:definition-period-boundary",
                    "conflict_type": "scale_input_comparability",
                    "entity_refs": [f"data-mcp:system:{slug}"],
                    "observation_refs": [],
                    "receipt_refs": all_receipts,
                    "status": "open",
                    "rationale": f"{','.join(cells[slug].blocker_codes)}: {cells[slug].finding}",
                }
                for slug in acquisition.systems
            ],
            "input_artifacts": [ahrq_lineage, *external_lineages],
        }
    )


__all__ = ["build_emergency_department_count_public_evidence_input"]
