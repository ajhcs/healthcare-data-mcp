"""Public Evidence Bundle v1 adapter for safety-net patient-mix v5."""

from __future__ import annotations

import re

from shared.acquisition.scale_safety_net_patient_mix_contract import SafetyNetPatientMixAcquisition
from shared.acquisition.scale_system_roster import SYSTEM_NAMES
from shared.contracts.public_evidence import PublicEvidenceBundleInput


def build_safety_net_patient_mix_public_evidence_input(
    acquisition: SafetyNetPatientMixAcquisition,
    *,
    producer_commit: str = "0" * 40,
) -> PublicEvidenceBundleInput:
    """Adapt explicit missingness without inventing a percentage observation."""

    if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    ahrq = acquisition.ahrq_source_artifact
    cms = acquisition.cms_dsh_artifact
    ahrq_lineage = {
        "artifact_id": ahrq.artifact_id,
        "checksum_sha256": ahrq.payload_sha256,
        "media_type": ahrq.media_type,
        "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{ahrq.relative_path}",
        "cache_run_id": acquisition.cache_receipt.run_id,
        "connector": "validated-cache-manifest",
        "connector_version": "scale-safety-net-patient-mix-connector.v5",
        "parser_version": "scale-safety-net-patient-mix-parser.v5",
        "schema_fingerprint": ahrq.schema_fingerprint,
    }
    cms_lineage = {
        "artifact_id": cms.artifact_id,
        "checksum_sha256": cms.payload_sha256,
        "media_type": cms.media_type,
        "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/cms-mln-medicare-dsh-2024.pdf",
        "cache_run_id": acquisition.acquisition_id,
        "connector": "governed-public-http",
        "connector_version": "scale-safety-net-patient-mix-connector.v5",
        "parser_version": "pypdf-text-markers.v1",
        "schema_fingerprint": cms.payload_sha256,
    }
    rows = {row.system_slug: row for row in acquisition.identity_rows}
    cells = {cell.system_slug: cell for cell in acquisition.cells}
    ahrq_evaluation = next(
        item for item in acquisition.source_evaluations
        if item.evaluation_id == "evaluation:ahrq-system-safety-net-schema"
    )
    cms_evaluation = next(
        item for item in acquisition.source_evaluations
        if item.evaluation_id == "evaluation:cms-medicare-dsh-definition"
    )
    sources: list[dict[str, object]] = []
    for slug in acquisition.systems:
        row = rows[slug]
        sources.append(
            {
                "source_id": f"source:{slug}:safety-net-patient-mix:ahrq-identity",
                "registry_id": "ahrq-compendium-2023:system",
                "registry_version": "2023",
                "receipt": {
                    "receipt_id": f"receipt:{slug}:safety-net-patient-mix:ahrq-identity",
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
                        "binary_indicator_columns": list(acquisition.safety_net_indicator_columns),
                    },
                    "cache_key": ahrq_lineage["uri"],
                    "match_basis": "exact source-local AHRQ identity row; only binary burden flags, no patient-mix percentage",
                    "confidence": "exact_identity_and_schema_absence; unavailable_for_safety_net_patient_mix_pct",
                    "caveat": ahrq_evaluation.exclusion_reason,
                    "next_step": "Toolkit must preserve unavailable_public and every numerator, denominator, period, and boundary blocker.",
                    "acquisition_method": "scale-safety-net-patient-mix-connector.v5",
                    "rights_classification": ahrq.rights_classification,
                    "row_locator": f"row={row.row_number}; health_sys_id={row.health_sys_id}; binary burden flags are not a percentage",
                    "artifact": ahrq_lineage,
                    "parent_receipt_ids": [],
                },
                "content_checksum": ahrq.payload_sha256,
                "access_rights": ahrq.rights_classification,
            }
        )
    sources.append(
        {
            "source_id": "source:all-six:safety-net-patient-mix:cms-dsh-definition",
            "registry_id": "cms-mln-medicare-dsh",
            "registry_version": "MLN006741-2024-09",
            "receipt": {
                "receipt_id": "receipt:all-six:safety-net-patient-mix:cms-dsh-definition",
                "source_name": cms.source_name,
                "source_url": cms.source_url,
                "dataset_id": "cms-mln006741-2024",
                "source_period": cms.source_period,
                "landing_page": cms.landing_page,
                "retrieved_at": cms.http_receipt.retrieved_at,
                "source_modified": "2024-09-18T20:23:56Z",
                "cache_status": "frozen_verified_external",
                "cache_freshness": "Exact September 2024 fact sheet; excluded as a product-system patient-mix source",
                "entity_scope": "Philadelphia six-system Scale roster",
                "query": {
                    "evaluation_query": cms_evaluation.query,
                    "evaluation_query_sha256": cms_evaluation.query_sha256,
                    "http_receipt_sha256": cms.http_receipt.receipt_sha256,
                    "page_count": cms.page_count,
                },
                "cache_key": cms_lineage["uri"],
                "match_basis": "definition applicability evaluation only; hospital IPPS DPP has no product-system identity join",
                "confidence": "exact_dpp_definition; not_a_common_product_system_patient_mix_percentage",
                "caveat": cms_evaluation.exclusion_reason,
                "next_step": "A later acquisition must receipt one approved numerator and denominator for all six product systems; facility aggregation and denominator substitution remain prohibited.",
                "acquisition_method": "scale-safety-net-patient-mix-connector.v5",
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
                    "prior_service_line_admission_merge_grouped_sha1": acquisition.prior_cycle.admission_merge,
                    "prior_service_line_tracker_merge_grouped_sha1": acquisition.prior_cycle.tracker_merge,
                    "no_scale_score": True,
                    "no_facility_aggregation": True,
                    "no_denominator_substitution": True,
                    "no_imputation": True,
                    "approved_numerator_receipted": False,
                    "approved_denominator_receipted": False,
                },
            },
            "scope": {
                "systems": list(acquisition.systems),
                "market": {"name": "Philadelphia six-system Scale roster", "roster_frozen": True},
                "periods": ["2023", "September 2024 policy fact sheet", "not_available_on_comparable_basis"],
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
                    "coverage_id": f"coverage:{slug}:safety_net_patient_mix_pct",
                    "entity_ref": f"data-mcp:system:{slug}",
                    "measure_id": "safety_net_patient_mix_pct",
                    "status": "unavailable_public",
                    "observation_refs": [],
                    "reason": cells[slug].finding,
                }
                for slug in acquisition.systems
            ],
            "conflicts": [
                {
                    "conflict_id": f"conflict:{slug}:safety-net-patient-mix:numerator-denominator-boundary",
                    "conflict_type": "scale_input_comparability",
                    "entity_refs": [f"data-mcp:system:{slug}"],
                    "observation_refs": [],
                    "receipt_refs": [
                        f"receipt:{slug}:safety-net-patient-mix:ahrq-identity",
                        "receipt:all-six:safety-net-patient-mix:cms-dsh-definition",
                    ],
                    "status": "open",
                    "rationale": f"{','.join(cells[slug].blocker_codes)}: {cells[slug].finding}",
                }
                for slug in acquisition.systems
            ],
            "input_artifacts": [ahrq_lineage, cms_lineage],
        }
    )


__all__ = ["build_safety_net_patient_mix_public_evidence_input"]
