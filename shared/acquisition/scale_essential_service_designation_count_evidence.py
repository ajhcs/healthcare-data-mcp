"""Public Evidence Bundle adapter for essential-service designation count v7."""

from __future__ import annotations

import re

from shared.acquisition.scale_essential_service_designation_count_contract import EssentialServiceDesignationCountAcquisition
from shared.acquisition.scale_system_roster import SYSTEM_NAMES
from shared.contracts.public_evidence import PublicEvidenceBundleInput


def build_essential_service_designation_count_public_evidence_input(
    acquisition: EssentialServiceDesignationCountAcquisition, *, producer_commit: str = "0" * 40,
) -> PublicEvidenceBundleInput:
    """Adapt only identity and missingness; never aggregate providerType codes."""

    if re.fullmatch(r"[0-9a-f]{40}", producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    ahrq = acquisition.ahrq_system_artifact
    ahrq_lineage = {
        "artifact_id": ahrq.artifact_id, "checksum_sha256": ahrq.payload_sha256,
        "media_type": ahrq.media_type,
        "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{ahrq.relative_path}",
        "cache_run_id": acquisition.cache_receipt.run_id, "connector": "validated-cache-manifest",
        "connector_version": "scale-essential-service-designation-count-connector.v7",
        "parser_version": "scale-essential-service-designation-count-parser.v7",
        "schema_fingerprint": ahrq.schema_fingerprint,
    }
    external = [{
        "artifact_id": item.artifact_id, "checksum_sha256": item.payload_sha256,
        "media_type": item.media_type,
        "uri": f"hc-cache://{acquisition.workflow_id}/{acquisition.acquisition_id}/{item.artifact_id.replace(':', '-')}",
        "cache_run_id": acquisition.acquisition_id, "connector": "governed-public-primary-source",
        "connector_version": "scale-essential-service-designation-count-connector.v7",
        "parser_version": "scale-essential-service-designation-count-parser.v7",
        "schema_fingerprint": item.payload_sha256,
    } for item in acquisition.source_artifacts]
    lineage = {item["artifact_id"]: item for item in [ahrq_lineage, *external]}
    rows = {row.system_slug: row for row in acquisition.identity_rows}
    cells = {cell.system_slug: cell for cell in acquisition.cells}
    evaluations = {item.artifact_ref: item for item in acquisition.source_evaluations}
    sources: list[dict[str, object]] = []
    receipts: list[str] = []
    for slug in acquisition.systems:
        row = rows[slug]
        receipt_id = f"receipt:{slug}:essential-service-designation-count:ahrq-identity"
        receipts.append(receipt_id)
        sources.append({
            "source_id": f"source:{slug}:essential-service-designation-count:ahrq-identity",
            "registry_id": "ahrq-compendium-2023:system", "registry_version": "2023",
            "receipt": {
                "receipt_id": receipt_id, "source_name": ahrq.source_name, "source_url": ahrq.source_url,
                "dataset_id": ahrq.dataset_id, "source_period": ahrq.source_period,
                "landing_page": ahrq.landing_page, "retrieved_at": acquisition.cache_receipt.retrieved_at,
                "source_modified": None, "cache_status": "validated_frozen_snapshot",
                "cache_freshness": "Frozen identity only; no designation field", "entity_scope": f"data-mcp:system:{slug}",
                "query": {"row_number": row.row_number, "row_sha256": row.source_row_sha256, "row_key": row.health_sys_id, "reports_system_count": False, "approved_taxonomy": False, "approved_current_crosswalk": False},
                "cache_key": ahrq_lineage["uri"], "match_basis": "exact source-local identity only",
                "confidence": "exact_identity; unavailable_for_designation_count",
                "caveat": "Identity only; no countable designation taxonomy is present.",
                "next_step": "Freeze an approved taxonomy, effective rule, duplicate rule, and current facility crosswalk.",
                "acquisition_method": "scale-essential-service-designation-count-connector.v7",
                "rights_classification": ahrq.rights_classification,
                "row_locator": f"row={row.row_number}; health_sys_id={row.health_sys_id}",
                "artifact": ahrq_lineage, "parent_receipt_ids": [],
            }, "content_checksum": ahrq.payload_sha256, "access_rights": ahrq.rights_classification,
        })
    receipt_suffixes = (
        "ahrq-hospital-linkage-2023", "cms-psf-april-2026",
        "cms-psf-manual-rev-13757", "cms-psf-release-page-april-2026",
    )
    for artifact, suffix in zip(acquisition.source_artifacts, receipt_suffixes, strict=True):
        evaluation = evaluations[artifact.artifact_id]
        receipt_id = f"receipt:all-six:essential-service-designation-count:{suffix}"
        receipts.append(receipt_id)
        sources.append({
            "source_id": f"source:all-six:essential-service-designation-count:{suffix}",
            "registry_id": artifact.artifact_id, "registry_version": artifact.source_period,
            "receipt": {
                "receipt_id": receipt_id, "source_name": artifact.source_name, "source_url": artifact.source_url,
                "dataset_id": artifact.artifact_id, "source_period": artifact.source_period,
                "landing_page": artifact.landing_page, "retrieved_at": artifact.retrieved_at,
                "source_modified": artifact.source_modified if "T" in artifact.source_modified else None,
                "cache_status": "frozen_verified_external", "cache_freshness": "Exact frozen bytes; non-countable absent approved governance",
                "entity_scope": "Philadelphia six-system Scale roster",
                "query": {"evaluated_unit": evaluation.evaluated_unit, "reports_system_count": False, "approved_taxonomy": False, "approved_current_crosswalk": False, "provider_type_aggregation_performed": False},
                "cache_key": lineage[artifact.artifact_id]["uri"],
                "match_basis": "custody or semantic context only; no code aggregation or system rollup",
                "confidence": "exact_source_semantics; unavailable_for_system_designation_count",
                "caveat": evaluation.exclusion_reason,
                "next_step": "Freeze an approved taxonomy, effective rule, duplicate rule, and current facility crosswalk.",
                "acquisition_method": "scale-essential-service-designation-count-connector.v7",
                "rights_classification": artifact.rights_classification, "row_locator": artifact.exact_locator,
                "artifact": lineage[artifact.artifact_id], "parent_receipt_ids": [],
            }, "content_checksum": artifact.payload_sha256, "access_rights": artifact.rights_classification,
        })
    prior = acquisition.prior_cycle.model_dump(mode="json")
    return PublicEvidenceBundleInput.model_validate({
        "bundle_id": f"{acquisition.acquisition_id}:public-evidence",
        "producer": {"repo": "healthcare-data-mcp", "version": acquisition.producer_version, "commit": producer_commit},
        "created_at": acquisition.acquired_at,
        "request": {"workflow": acquisition.workflow_id, "parameters": {
            "input_family": acquisition.input_family, "desired_definition": cells[acquisition.systems[0]].desired_definition,
            **{f"prior_emergency_department_{key}": value for key, value in prior.items() if key != "input_family"},
            "no_scale_score": True, "no_provider_type_aggregation": True,
            "no_combination_code_expansion": True, "no_combination_code_deduplication": True,
            "no_stale_ahrq_rollup": True,
            "no_expired_or_terminated_records": True, "no_state_federal_mixing": True,
            "no_narrative_service_or_safety_net_substitution": True,
            "no_missing_as_zero_or_imputation": True,
            "approved_taxonomy_receipted": False, "approved_current_crosswalk_receipted": False,
        }},
        "scope": {"systems": list(acquisition.systems), "market": {"name": "Philadelphia six-system Scale roster", "roster_frozen": True}, "periods": ["2023", "April 2026", "not_available_on_comparable_basis"]},
        "entities": [{"entity_id": f"data-mcp:system:{slug}", "canonical_name": SYSTEM_NAMES[slug], "entity_type": "health_system", "ahrq_system_id": rows[slug].health_sys_id, "aliases": [{"source_name": ahrq.source_name, "name": rows[slug].health_sys_name, "identifier": rows[slug].health_sys_id, "identifier_type": "ahrq_health_sys_id"}], "match_decisions": [], "conflicts": [], "unresolved_identifiers": []} for slug in acquisition.systems],
        "observations": [], "sources": sources,
        "coverage": [{"coverage_id": f"coverage:{slug}:essential_service_designation_count", "entity_ref": f"data-mcp:system:{slug}", "measure_id": "essential_service_designation_count", "status": "unavailable_public", "observation_refs": [], "reason": cells[slug].finding} for slug in acquisition.systems],
        "conflicts": [{"conflict_id": f"conflict:{slug}:essential-service-designation-count:taxonomy-period-boundary", "conflict_type": "scale_input_comparability", "entity_refs": [f"data-mcp:system:{slug}"], "observation_refs": [], "receipt_refs": receipts, "status": "open", "rationale": f"{','.join(cells[slug].blocker_codes)}: {cells[slug].finding}"} for slug in acquisition.systems],
        "input_artifacts": [ahrq_lineage, *external],
    })


__all__ = ["build_essential_service_designation_count_public_evidence_input"]
