"""Build a Public Evidence Bundle input from verified roster/bed acquisition state."""

from __future__ import annotations

from pathlib import Path

from pydantic import JsonValue

from shared.acquisition.scale_roster_bed_models import (
    CONNECTOR_VERSION,
    WORKFLOW_ID,
    AcquisitionSpec,
    EntitySpec,
    FactSpec,
    FrozenAcquisition,
    FrozenArtifact,
    SourceSpec,
)
from shared.acquisition.scale_roster_bed_validation import (
    normalized_fact_payload,
    validate_frozen_against_spec,
)
from shared.contracts.public_evidence import PublicEvidenceBundleInput, canonical_sha256
from shared.utils.cache import write_atomic_json


def build_bundle_input(spec: AcquisitionSpec, frozen: FrozenAcquisition) -> PublicEvidenceBundleInput:
    """Build a deterministic Public Evidence Bundle input from frozen acquisition state."""

    source_specs = {item.source_id: item for item in spec.sources}
    artifacts = {item.source_id: item for item in frozen.artifacts}
    extracted = {item.fact_id: item for item in frozen.extracted_facts}
    validate_frozen_against_spec(spec, frozen)
    observations: list[dict[str, JsonValue]] = []
    coverage: list[dict[str, JsonValue]] = []
    sources: list[dict[str, JsonValue]] = []
    observation_by_fact: dict[str, str] = {}
    receipt_by_fact: dict[str, str] = {}
    for fact in sorted(spec.facts, key=lambda item: item.fact_id):
        coverage_id = f"coverage:{fact.fact_id}"
        if fact.missingness is not None:
            coverage.append(
                {
                    "coverage_id": coverage_id,
                    "entity_ref": fact.entity_id,
                    "measure_id": fact.measure_id,
                    "status": fact.missingness,
                    "observation_refs": [],
                    "reason": fact.missingness_reason,
                }
            )
            continue
        item = extracted[fact.fact_id]
        expected_checksum = canonical_sha256(normalized_fact_payload(fact, item.value))
        if item.normalized_content_checksum != expected_checksum:
            raise ValueError(f"normalized row checksum drift for fact {fact.fact_id}")
        source_spec = source_specs[fact.source_id or ""]
        artifact = artifacts[source_spec.source_id]
        observation_id = f"observation:{fact.fact_id}"
        receipt_id = f"receipt:{fact.fact_id}"
        observation_by_fact[fact.fact_id] = observation_id
        receipt_by_fact[fact.fact_id] = receipt_id
        period: dict[str, JsonValue] = {"label": fact.period_label}
        if fact.period_start:
            period["start"] = fact.period_start
        if fact.period_end:
            period["end"] = fact.period_end
        observations.append(
            {
                "observation_id": observation_id,
                "measure_id": fact.measure_id,
                "value_type": fact.value_type,
                "value": item.value,
                "unit": fact.unit,
                "period": period,
                "denominator_scope": fact.denominator_scope,
                "entity_ref": fact.entity_id,
                "receipt_refs": [receipt_id],
                "derivation_class": "source_reported",
                "caveat": fact.caveat,
                "dependency_cluster_ids": fact.dependency_cluster_ids or [f"dependency:{source_spec.source_id}"],
            }
        )
        coverage.append(
            {
                "coverage_id": coverage_id,
                "entity_ref": fact.entity_id,
                "measure_id": fact.measure_id,
                "status": "populated",
                "observation_refs": [observation_id],
                "reason": "Frozen source row was verified and has a matching receipt.",
            }
        )
        artifact_payload = _artifact_payload(artifact)
        sources.append(
            {
                "source_id": f"source:{fact.fact_id}",
                "registry_id": source_spec.registry_id,
                "registry_version": source_spec.registry_version,
                "receipt": {
                    "receipt_id": receipt_id,
                    "source_name": source_spec.source_name,
                    "source_url": artifact.final_url,
                    "dataset_id": source_spec.dataset_id,
                    "source_period": source_spec.source_period,
                    "landing_page": source_spec.landing_page,
                    "retrieved_at": artifact.retrieved_at,
                    "source_modified": artifact.source_modified,
                    "cache_status": "frozen_verified",
                    "cache_freshness": f"Frozen in cache run {frozen.cache_run_id}",
                    "entity_scope": fact.entity_id,
                    "query": {
                        "workflow": WORKFLOW_ID,
                        "fact_id": fact.fact_id,
                        "source_field": fact.table_value_field or "named regex group: value",
                        "table_match": fact.table_match,
                    },
                    "cache_key": artifact.portable_uri,
                    "match_basis": fact.match_basis,
                    "confidence": fact.confidence,
                    "caveat": fact.caveat,
                    "next_step": fact.next_step,
                    "acquisition_method": CONNECTOR_VERSION,
                    "rights_classification": source_spec.rights_classification,
                    "row_locator": fact.row_locator,
                    "artifact": artifact_payload,
                    "parent_receipt_ids": [],
                },
                "content_checksum": item.normalized_content_checksum,
                "access_rights": source_spec.rights_classification,
            }
        )
    conflicts = []
    for conflict in sorted(spec.conflicts, key=lambda item: item.conflict_id):
        conflicts.append(
            {
                "conflict_id": conflict.conflict_id,
                "conflict_type": conflict.conflict_type,
                "entity_refs": conflict.entity_ids,
                "observation_refs": [
                    observation_by_fact[item] for item in conflict.fact_ids if item in observation_by_fact
                ],
                "receipt_refs": [receipt_by_fact[item] for item in conflict.fact_ids if item in receipt_by_fact],
                "status": conflict.status,
                "rationale": conflict.rationale,
            }
        )
    return PublicEvidenceBundleInput.model_validate(
        {
            "bundle_id": spec.bundle_id,
            "producer": {
                "repo": "healthcare-data-mcp",
                "version": spec.producer_version,
                "commit": "0" * 40,
            },
            "created_at": frozen.acquired_at,
            "request": {
                "workflow": WORKFLOW_ID,
                "parameters": {"acquisition_cutoff": frozen.acquired_at.isoformat(), "no_scale_score": True},
            },
            "scope": {"systems": spec.systems, "market": spec.market, "periods": spec.periods},
            "entities": [
                _entity_payload(item, source_specs, spec.facts, artifacts)
                for item in sorted(spec.entities, key=lambda value: value.entity_id)
            ],
            "observations": observations,
            "sources": sources,
            "coverage": coverage,
            "conflicts": conflicts,
            "input_artifacts": [
                _artifact_payload(item) for item in sorted(frozen.artifacts, key=lambda value: value.artifact_id)
            ],
        }
    )


def write_bundle_input(path: Path, bundle_input: PublicEvidenceBundleInput) -> None:
    write_atomic_json(path, bundle_input.model_dump(mode="json"))


def _entity_payload(
    entity: EntitySpec,
    source_specs: dict[str, SourceSpec],
    facts: list[FactSpec],
    artifacts: dict[str, FrozenArtifact],
) -> dict[str, JsonValue]:
    aliases = []
    identity_fact = next(
        (
            fact
            for fact in facts
            if fact.entity_id == entity.entity_id and fact.measure_id == "system_identity" and fact.source_id
        ),
        None,
    )
    for alias in entity.aliases:
        row: dict[str, JsonValue] = {"source_name": "Reviewed public identity source", "name": alias}
        if identity_fact is not None:
            source = source_specs[identity_fact.source_id or ""]
            artifact = artifacts[source.source_id]
            row.update(
                {
                    "source_name": source.source_name,
                    "source_url": artifact.final_url,
                    "retrieved_at": artifact.retrieved_at.isoformat(),
                }
            )
        aliases.append(row)
    unresolved = list(entity.unresolved_identifiers)
    if entity.state_license_id:
        unresolved.append({"identifier_type": "state_license_id", "identifier": entity.state_license_id})
    return {
        "entity_id": entity.entity_id,
        "canonical_name": entity.canonical_name,
        "entity_type": entity.entity_type,
        "ccn": entity.ccn,
        "owner_id": entity.owner_entity_id,
        "address": entity.address,
        "zip_code": entity.zip_code,
        "aliases": aliases,
        "match_decisions": [],
        "conflicts": entity.identity_conflicts,
        "unresolved_identifiers": unresolved,
    }


def _artifact_payload(artifact: FrozenArtifact) -> dict[str, JsonValue]:
    return {
        "artifact_id": artifact.artifact_id,
        "checksum_sha256": artifact.checksum_sha256,
        "media_type": artifact.media_type,
        "uri": artifact.portable_uri,
        "cache_run_id": artifact.cache_run_id,
        "connector": CONNECTOR_VERSION,
        "connector_version": artifact.connector_version,
        "parser_version": artifact.parser_version,
        "schema_fingerprint": artifact.schema_fingerprint,
    }


__all__ = ["build_bundle_input", "write_bundle_input"]
