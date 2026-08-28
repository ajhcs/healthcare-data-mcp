"""Conformance fixtures for the pinned Healthcare Data Platform v1 bundle."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from shared.contracts.healthcare_data_platform import (
    HealthcareDataPlatformContractError,
    contract_bindings,
    validate_observation_envelope,
    validate_pinned_bundle,
)


def _envelope() -> dict[str, Any]:
    artifact_id = "artifact:fixture:v1"
    release_id = "release:fixture:v1"
    receipt_id = "receipt:fixture:v1"
    activity_id = "activity:fixture:v1"
    observation_id = "observation:fixture:v1:1"
    return {
        "schema_version": "hdp.observation-envelope.v1",
        "record_type": "observation_envelope",
        "record_id": "hdp:observation-envelope:fixture",
        "packet_id": "p0-09-observation-provenance-delta-v1",
        "tracking_bead": "healthcare-toolkit-rrna.9",
        "frozen_dispatch_base": "11d16f8303226619161f9bef03cb312f693b2d49",
        "source_release": {
            "source_id": "source:fixture",
            "release_id": release_id,
            "release_label": "Synthetic fixture v1",
            "source_kind": "synthetic_fixture",
            "release_sha256": "sha256:" + "a" * 64,
            "evidence_locator": "object://fixture/source",
            "coverage_state": "present",
        },
        "artifact": {
            "artifact_id": artifact_id,
            "release_ref": release_id,
            "artifact_kind": "raw_source",
            "media_type": "application/json",
            "content_sha256": "sha256:" + "b" * 64,
            "byte_length": 128,
            "custody": {
                "locator": "object://fixture/artifact",
                "storage_plane": "object_storage",
                "immutable": True,
                "retention": "indefinite",
            },
        },
        "receipt": {
            "receipt_id": receipt_id,
            "producer": "healthcare-data-mcp-fixture",
            "receipt_schema": "hdp.receipt.v1",
            "source_release_ref": release_id,
            "artifact_ref": artifact_id,
            "source_release_sha256": "sha256:" + "a" * 64,
            "artifact_sha256": "sha256:" + "b" * 64,
            "receipt_sha256": "sha256:" + "c" * 64,
            "state": "succeeded",
            "recorded_at": "2026-08-28T00:00:00Z",
            "evidence_locator": "object://fixture/receipt",
        },
        "activity": {
            "activity_id": activity_id,
            "run_id": "run:fixture:v1",
            "activity_type": "observation",
            "actor": {"actor_type": "system_ingest", "actor_id": "fixture"},
            "started_at": "2026-08-28T00:00:00Z",
            "ended_at": "2026-08-28T00:00:01Z",
            "status": "succeeded",
            "input_artifact_refs": [artifact_id],
            "output_artifact_refs": [artifact_id],
        },
        "observations": [
            {
                "observation_id": observation_id,
                "identity_key": "identity:fixture",
                "subject_ref": "entity:fixture",
                "attribute_term_ref": "term:fixture",
                "value": "observed value",
                "value_state": "observed",
                "source_scope": {
                    "scope_id": "scope:fixture",
                    "source_id": "source:fixture",
                    "release_ref": release_id,
                    "artifact_ref": artifact_id,
                    "custody_locator": "object://fixture/artifact",
                    "selector": "record:row-1",
                    "authority_state": "source_scoped",
                },
                "activity_ref": activity_id,
                "receipt_ref": receipt_id,
                "valid_time": {
                    "precision": "day",
                    "as_of": "2026-08-27",
                    "valid_from": "2026-08-27",
                    "valid_to": "2026-08-27",
                },
                "transaction_time": {"recorded_from": "2026-08-28T00:00:01Z", "recorded_to": None},
                "conflict": {
                    "state": "none",
                    "reason": "Synthetic fixture has one source value.",
                    "resolution": "not_required",
                    "competing_observation_refs": [],
                },
                "promotion_state": "unpromoted_observation",
            }
        ],
        "lineage": {
            "lineage_id": "lineage:fixture:v1",
            "source_release_ref": release_id,
            "artifact_ref": artifact_id,
            "receipt_ref": receipt_id,
            "activity_ref": activity_id,
            "observation_ids": [observation_id],
            "deterministic_order": [observation_id],
            "replay": {
                "idempotency_key": "idempotency:fixture:v1",
                "state": "first_seen",
                "replay_of": None,
                "deterministic": True,
            },
        },
        "authority_limits": {
            "acquisition_allowed": False,
            "mutation_allowed": False,
            "deletion_allowed": False,
            "publication_allowed": False,
            "release_allowed": False,
            "production_allowed": False,
            "runtime_allowed": False,
            "current_projection_allowed": False,
            "identity_promotion_allowed": False,
        },
    }


def test_pinned_bundle_resolves_all_five_source_bindings_and_hashes() -> None:
    bundle = validate_pinned_bundle()

    assert bundle.bundle_id == "bundle:healthcare-data-platform:v1"
    assert bundle.bundle_semver == "1.0.0"
    assert bundle.compatibility_floor == "1.0"
    assert bundle.bundle_sha256 == "sha256:dda28dc777b264892e9f0817bbf40fb73f12ff34d35115d4d3d4a146e73b371c"
    assert {binding.family for binding in bundle.bindings} == {
        "ontology",
        "observation",
        "fact_coverage",
        "storage_workload",
        "storage_benchmark",
    }
    assert contract_bindings() == bundle.bindings


def test_source_scoped_observation_envelope_passes_schema_and_lineage_checks() -> None:
    payload = _envelope()

    assert validate_observation_envelope(payload) == payload


def test_unknown_field_is_rejected_fail_closed() -> None:
    payload = _envelope()
    payload["unexpected"] = "must not cross the contract"

    with pytest.raises(HealthcareDataPlatformContractError, match="failed schema validation"):
        validate_observation_envelope(payload)


def test_stale_schema_version_is_rejected() -> None:
    payload = _envelope()
    payload["schema_version"] = "hdp.observation-envelope.v2"

    with pytest.raises(HealthcareDataPlatformContractError, match="failed schema validation"):
        validate_observation_envelope(payload)


def test_missingness_is_explicit_and_never_treated_as_zero() -> None:
    payload = _envelope()
    observation = payload["observations"][0]
    observation["value"] = None
    observation["value_state"] = "not_yet_researched"
    observation["conflict"] = {
        "state": "missingness",
        "reason": "Fixture intentionally has no public value.",
        "resolution": "abstained",
        "competing_observation_refs": [],
    }

    assert validate_observation_envelope(payload) == payload


def test_replay_requires_prior_lineage_and_preserves_deterministic_order() -> None:
    payload = _envelope()
    replay = payload["lineage"]["replay"]
    replay["state"] = "replayed"

    with pytest.raises(HealthcareDataPlatformContractError, match="failed schema validation"):
        validate_observation_envelope(payload)

    replay["replay_of"] = "lineage:fixture:prior"
    assert validate_observation_envelope(payload) == payload

    reordered = deepcopy(payload)
    reordered["lineage"]["deterministic_order"] = ["observation:fixture:v1:unknown"]
    with pytest.raises(HealthcareDataPlatformContractError, match="lineage observation ids"):
        validate_observation_envelope(reordered)


def test_source_scope_mismatch_is_rejected_after_schema_validation() -> None:
    payload = _envelope()
    payload["observations"][0]["source_scope"]["release_ref"] = "release:other"

    with pytest.raises(HealthcareDataPlatformContractError, match="release_ref does not match"):
        validate_observation_envelope(payload)
