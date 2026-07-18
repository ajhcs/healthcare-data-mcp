"""Public Evidence Bundle v1 adapter for physician-count acquisition v3."""

from __future__ import annotations

import re

from shared.acquisition.scale_physician_count_contract import PhysicianCountAcquisition
from shared.acquisition.scale_system_roster import SYSTEM_NAMES
from shared.acquisition.scale_tabular_input_family import TabularSourceArtifact
from shared.contracts.public_evidence import PublicEvidenceBundleInput


def build_physician_count_public_evidence_input(
    acquisition: PhysicianCountAcquisition,
    *,
    producer_commit: str = "0" * 40,
) -> PublicEvidenceBundleInput:
    """Adapt blocked source-local candidates without creating approved inputs."""

    if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    lineage: dict[str, dict[str, object]] = {
        item.artifact_id: {
            "artifact_id": item.artifact_id,
            "checksum_sha256": item.payload_sha256,
            "media_type": item.media_type,
            "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{item.relative_path}",
            "cache_run_id": acquisition.cache_receipt.run_id,
            "connector": "validated-cache-manifest",
            "connector_version": "scale-physician-count-connector.v3",
            "parser_version": "scale-physician-count-parser.v3",
            "schema_fingerprint": item.schema_fingerprint,
        }
        for item in acquisition.source_artifacts
    }
    system_rows = {item.system_slug: item for item in acquisition.system_rows}
    candidates = {item.system_slug: item for item in acquisition.candidates}
    system_artifact = next(
        item for item in acquisition.source_artifacts if item.relative_path == "ahrq_system_2023.csv"
    )
    sources: list[dict[str, object]] = []
    for slug in acquisition.systems:
        row = system_rows[slug]
        sources.append(
            _source(
                acquisition,
                slug=slug,
                artifact=system_artifact,
                lineage=lineage[system_artifact.artifact_id],
                locator=(
                    f"row={row.row_number}; health_sys_id={row.health_sys_id}; "
                    f"column=total_mds; raw={row.raw_lexical_value}"
                ),
                query={
                    "row_number": row.row_number,
                    "row_sha256": row.source_row_sha256,
                    "row_key_column": row.row_key_column,
                    "row_key_value": row.health_sys_id,
                    "value_column": row.value_column,
                    "raw_lexical_value": row.raw_lexical_value,
                    "prim_care_mds_raw": row.prim_care_mds_raw,
                    "cache_promoted_at": system_artifact.cache_promoted_at.isoformat(),
                },
            )
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
                    "prior_annual_admission_merge_grouped_sha1": acquisition.prior_cycle.admission_merge,
                    "prior_annual_tracker_merge_grouped_sha1": acquisition.prior_cycle.tracker_merge,
                    "no_scale_score": True,
                    "candidate_values_are_not_approved_inputs": True,
                    "no_physician_aggregation": True,
                    "raw_http_receipt_available": False,
                    "redistribution_rights_reviewed": False,
                    "physician_definition_receipted": False,
                },
            },
            "scope": {
                "systems": list(acquisition.systems),
                "market": {
                    "name": "Philadelphia six-system Scale roster",
                    "roster_frozen": True,
                },
                "periods": ["2023"],
            },
            "entities": [
                {
                    "entity_id": f"data-mcp:system:{slug}",
                    "canonical_name": SYSTEM_NAMES[slug],
                    "entity_type": "health_system",
                    "ahrq_system_id": system_rows[slug].health_sys_id,
                    "aliases": [
                        {
                            "source_name": system_artifact.source_name,
                            "name": system_rows[slug].health_sys_name,
                            "identifier": system_rows[slug].health_sys_id,
                            "identifier_type": "ahrq_health_sys_id",
                        }
                    ],
                    "match_decisions": [],
                    "conflicts": [],
                    "unresolved_identifiers": [],
                }
                for slug in acquisition.systems
            ],
            "observations": [
                {
                    "observation_id": f"observation:{slug}:physician-count:candidate",
                    "measure_id": "source_local_candidate.physician_count",
                    "value_type": "integer",
                    "value": candidates[slug].candidate_value,
                    "unit": candidates[slug].unit,
                    "period": {"label": candidates[slug].source_period},
                    "denominator_scope": candidates[slug].basis,
                    "entity_ref": f"data-mcp:system:{slug}",
                    "receipt_refs": [f"receipt:{slug}:physician-count:system"],
                    "derivation_class": "source_reported",
                    "caveat": candidates[slug].finding,
                    "dependency_cluster_ids": [
                        f"dependency:{candidates[slug].system_row_ref}"
                    ],
                }
                for slug in acquisition.systems
            ],
            "sources": sources,
            "coverage": [
                {
                    "coverage_id": f"coverage:{slug}:physician_count",
                    "entity_ref": f"data-mcp:system:{slug}",
                    "measure_id": "physician_count",
                    "status": "blocked_source_conflict",
                    "observation_refs": [],
                    "reason": candidates[slug].finding,
                }
                for slug in acquisition.systems
            ],
            "conflicts": [
                {
                    "conflict_id": f"conflict:{slug}:physician-count:comparability",
                    "conflict_type": "scale_input_comparability",
                    "entity_refs": [f"data-mcp:system:{slug}"],
                    "observation_refs": [f"observation:{slug}:physician-count:candidate"],
                    "receipt_refs": [f"receipt:{slug}:physician-count:system"],
                    "status": "open",
                    "rationale": (
                        f"{','.join(candidates[slug].blocker_codes)}: "
                        f"{candidates[slug].finding}"
                    ),
                }
                for slug in acquisition.systems
            ],
            "input_artifacts": list(lineage.values()),
        }
    )


def _source(
    acquisition: PhysicianCountAcquisition,
    *,
    slug: str,
    artifact: TabularSourceArtifact,
    lineage: dict[str, object],
    locator: str,
    query: dict[str, object],
) -> dict[str, object]:
    source_artifact = artifact
    kind = "system"
    receipt_id = f"receipt:{slug}:physician-count:{kind}"
    return {
        "source_id": f"source:{slug}:physician-count:{kind}",
        "registry_id": f"ahrq-compendium-2023:{kind}",
        "registry_version": "2023",
        "receipt": {
            "receipt_id": receipt_id,
            "source_name": source_artifact.source_name,
            "source_url": source_artifact.source_url,
            "dataset_id": source_artifact.dataset_id,
            "source_period": source_artifact.source_period,
            "landing_page": source_artifact.landing_page,
            "retrieved_at": acquisition.cache_receipt.retrieved_at,
            "source_modified": None,
            "cache_status": "validated_frozen_snapshot",
            "cache_freshness": "Frozen 2023 release; current-roster comparability blocked",
            "entity_scope": f"data-mcp:system:{slug}",
            "query": query,
            "cache_key": lineage["uri"],
            "match_basis": "exact source-local tabular row; not an approved Scale input",
            "confidence": "blocked_pending_physician_definition_boundary_and_roster_review",
            "caveat": "No locally receipted official technical definition for total_mds; no physician or facility aggregation performed.",
            "next_step": "Toolkit must retain the candidate and every open blocker pending physician-workforce fitness review.",
            "acquisition_method": "scale-physician-count-connector.v3",
            "rights_classification": source_artifact.rights_classification,
            "row_locator": locator,
            "artifact": lineage,
            "parent_receipt_ids": [],
        },
        "content_checksum": source_artifact.payload_sha256,
        "access_rights": source_artifact.rights_classification,
    }


__all__ = ["build_physician_count_public_evidence_input"]
