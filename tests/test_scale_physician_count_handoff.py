"""Acceptance and adversarial tests for physician-count acquisition v3."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from shared.acquisition.scale_annual_discharges_packet import acquisition as annual_acquisition
from shared.acquisition.scale_physician_count_contract import (
    PHYSICIAN_BLOCKERS,
    PhysicianCountAcquisition,
    build_physician_count_acquisition,
)
from shared.acquisition.scale_physician_count_evidence import (
    build_physician_count_public_evidence_input,
)
from shared.acquisition.scale_physician_count_packet import (
    acquisition,
    verify_physician_count_source_bytes,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS
from shared.contracts.public_evidence import build_public_evidence_bundle, canonical_sha256
from shared.utils.cache import write_atomic_json

ROOT = Path(__file__).resolve().parents[1]
ACQUISITION = ROOT / "contracts" / "v3" / "fixtures" / "scale-physician-count-acquisition.json"
EVIDENCE = ROOT / "contracts" / "v3" / "fixtures" / "scale-physician-count-input.json"
V3_SCHEMA = ROOT / "contracts" / "v3" / "scale-physician-count-acquisition.schema.json"
V1_SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"
ANNUAL_ACQUISITION = ROOT / "contracts" / "v2" / "fixtures" / "scale-annual-discharges-acquisition.json"
ANNUAL_EVIDENCE = ROOT / "contracts" / "v2" / "fixtures" / "scale-annual-discharges-input.json"
REVENUE_ACQUISITION = ROOT / "contracts" / "v1" / "fixtures" / "scale-operating-revenue-acquisition.json"
REVENUE_EVIDENCE = ROOT / "contracts" / "v1" / "fixtures" / "scale-operating-revenue-input.json"
VALIDATED_CACHE = Path.home() / ".healthcare-data-mcp" / "cache"


def _checked_in() -> PhysicianCountAcquisition:
    return PhysicianCountAcquisition.model_validate_json(ACQUISITION.read_text(encoding="utf-8"))


def test_checked_in_v3_schema_and_evidence_are_deterministic() -> None:
    frozen = acquisition()
    assert frozen == acquisition() == _checked_in()
    assert json.loads(ACQUISITION.read_text(encoding="utf-8")) == frozen.model_dump(mode="json")
    evidence = build_physician_count_public_evidence_input(frozen)
    assert json.loads(EVIDENCE.read_text(encoding="utf-8")) == evidence.model_dump(mode="json")
    assert evidence.producer.commit == "0" * 40

    schema = json.loads(V3_SCHEMA.read_text(encoding="utf-8"))
    assert schema == PhysicianCountAcquisition.model_json_schema()
    assert list(Draft202012Validator(schema).iter_errors(frozen.model_dump(mode="json"))) == []
    public_schema = json.loads(V1_SCHEMA.read_text(encoding="utf-8"))
    bundle = build_public_evidence_bundle(evidence)
    assert list(Draft202012Validator(public_schema).iter_errors(bundle.model_dump(mode="json"))) == []


def test_exact_all_six_source_rows_remain_nonapproved_and_blocked() -> None:
    frozen = _checked_in()
    assert tuple(frozen.systems) == SYSTEM_SLUGS
    assert [row.system_slug for row in frozen.system_rows] == list(SYSTEM_SLUGS)
    assert [row.row_number for row in frozen.system_rows] == [110, 18, 466, 361, 475, 268]
    assert [row.health_sys_id for row in frozen.system_rows] == [
        "HSI00000218",
        "HSI00000048",
        "HSI00001065",
        "HSI00000820",
        "HSI00001079",
        "HSI00000608",
    ]
    assert [row.raw_lexical_value for row in frozen.system_rows] == [
        "1054",
        "3811",
        "1281",
        "4336",
        "1012",
        "1084",
    ]
    assert [item.candidate_value for item in frozen.candidates] == [1054, 3811, 1281, 4336, 1012, 1084]
    assert len(frozen.source_artifacts) == 1
    assert frozen.source_artifacts[0].relative_path == "ahrq_system_2023.csv"
    assert all(PHYSICIAN_BLOCKERS.issubset(item.blocker_codes) for item in frozen.candidates)
    assert all(
        item.missingness == "blocked_source_conflict"
        and not item.imputed
        and not item.aggregated
        and not item.fabricated_zero
        and not item.approved_for_scale
        for item in frozen.candidates
    )
    assert frozen.physician_definition_receipt is None
    assert frozen.raw_http_receipt_custody == "not_locally_receipted"
    assert frozen.redistribution_license_receipt is None
    assert frozen.redistribution_rights_custody == "unreviewed"


def test_prior_annual_no_go_lineage_is_exact_and_cannot_authorize_execution() -> None:
    prior = _checked_in().prior_cycle
    assert prior.binding_merge.replace("-", "") == "76e16247cecce818d777b4a4ade56dc13dd7b2a8"  # pragma: allowlist secret
    assert prior.binding_tracker_merge.replace("-", "") == "420d35d8024de1c484c1b16128836e0f8b00375c"  # pragma: allowlist secret
    assert prior.admission_merge.replace("-", "") == "9aed9059962cbf2a03c7c02e6056aee4281ee340"  # pragma: allowlist secret
    assert prior.tracker_merge.replace("-", "") == "2d33cab9264e636bd392b89757f8b05ed2729ecb"  # pragma: allowlist secret
    assert prior.terminal_status == "blocked"
    assert prior.failure_code == "human_review_required"

    payload = _checked_in().model_dump(mode="json")
    for field, value in (
        ("admission_merge", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa-aaaaaaaa"),
        ("tracker_merge", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb-bbbbbbbb"),
        ("terminal_status", "complete"),
        ("failure_code", "approved"),
    ):
        prior_payload = {**payload["prior_cycle"], field: value}
        with pytest.raises(ValidationError):
            build_physician_count_acquisition({**payload, "prior_cycle": prior_payload})

    schema = json.loads(V3_SCHEMA.read_text(encoding="utf-8"))
    drifted_prior = {
        **payload["prior_cycle"],
        "admission_merge": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa-aaaaaaaa",
    }
    errors = list(
        Draft202012Validator(schema).iter_errors({**payload, "prior_cycle": drifted_prior})
    )
    assert any("9aed9059-962c-bf2a-03c7-c02e6056aee4-281ee340" in error.message for error in errors)


def test_public_bundle_preserves_candidates_as_conflicted_context_only() -> None:
    evidence = build_physician_count_public_evidence_input(_checked_in(), producer_commit="a" * 40)
    bundle = build_public_evidence_bundle(evidence)
    assert len(bundle.entities) == 6
    assert len(bundle.observations) == 6
    assert len(bundle.sources) == 6
    assert len(bundle.input_artifacts) == 1
    assert len(bundle.coverage) == 6
    assert len(bundle.conflicts) == 6
    assert {item.measure_id for item in bundle.observations} == {
        "source_local_candidate.physician_count"
    }
    assert {item.measure_id for item in bundle.coverage} == {"physician_count"}
    assert all(item.status == "blocked_source_conflict" and not item.observation_refs for item in bundle.coverage)
    assert {item.status for item in bundle.conflicts} == {"open"}
    assert bundle.request.parameters["no_scale_score"] is True
    assert bundle.request.parameters["no_physician_aggregation"] is True
    assert bundle.request.parameters["physician_definition_receipted"] is False
    with pytest.raises(ValueError, match="producer commit"):
        build_physician_count_public_evidence_input(_checked_in(), producer_commit="not-a-commit")


def test_contract_rejects_roster_identity_value_and_row_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    with pytest.raises(ValidationError, match="six-system order"):
        build_physician_count_acquisition({**payload, "systems": payload["systems"][:-1]})

    rows = [dict(item) for item in payload["system_rows"]]
    rows[0]["raw_lexical_value"] = "999"
    with pytest.raises(ValidationError, match="exact reviewed source declaration"):
        build_physician_count_acquisition({**payload, "system_rows": rows})

    rows = [dict(item) for item in payload["system_rows"]]
    candidates = [dict(item) for item in payload["candidates"]]
    rows[0]["raw_lexical_value"] = "9999"
    rows[0]["source_row_sha256"] = "sha256:" + "a" * 64
    candidates[0]["candidate_value"] = 9999
    with pytest.raises(ValidationError, match="exact reviewed source declaration"):
        build_physician_count_acquisition(
            {**payload, "system_rows": rows, "candidates": candidates}
        )

    rows = [dict(item) for item in payload["system_rows"]]
    rows[0]["health_sys_id"], rows[1]["health_sys_id"] = rows[1]["health_sys_id"], rows[0]["health_sys_id"]
    rows[0]["health_sys_name"], rows[1]["health_sys_name"] = (
        rows[1]["health_sys_name"],
        rows[0]["health_sys_name"],
    )
    with pytest.raises(ValidationError, match="identity substitution"):
        build_physician_count_acquisition({**payload, "system_rows": rows})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["system_row_ref"] = candidates[1]["system_row_ref"]
    with pytest.raises(ValidationError, match="row reference drift"):
        build_physician_count_acquisition({**payload, "candidates": candidates})


def test_contract_rejects_imputation_zero_approval_definition_and_weakened_no_go() -> None:
    payload = _checked_in().model_dump(mode="json")
    for field, value in (
        ("approved_for_scale", True),
        ("imputed", True),
        ("aggregated", True),
        ("fabricated_zero", True),
        ("candidate_value", 0),
        ("source_period", "2024"),
        ("definition", "marketing page physician count"),
        ("basis", "employed and affiliated assumed comparable"),
    ):
        candidates = [dict(item) for item in payload["candidates"]]
        candidates[0][field] = value
        with pytest.raises(ValidationError):
            build_physician_count_acquisition({**payload, "candidates": candidates})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["blocker_codes"] = [
        code for code in candidates[0]["blocker_codes"] if code != "duplicate_physician_treatment_unresolved"
    ]
    with pytest.raises(ValidationError, match="mandatory physician comparability blockers"):
        build_physician_count_acquisition({**payload, "candidates": candidates})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["finding"] = "blocked"
    with pytest.raises(ValidationError, match="exact evidence-specific physician finding"):
        build_physician_count_acquisition({**payload, "candidates": candidates})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["blocker_codes"] = [*candidates[0]["blocker_codes"], "invented_gate"]
    with pytest.raises(ValidationError, match="exact evidence-specific physician blockers"):
        build_physician_count_acquisition({**payload, "candidates": candidates})

    with pytest.raises(ValidationError, match="prohibitions"):
        build_physician_count_acquisition(
            {**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]}
        )
    with pytest.raises(ValidationError):
        build_physician_count_acquisition({**payload, "scale_score": 1})


def test_contract_rejects_source_receipt_and_semantic_hash_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    artifacts = [dict(item) for item in payload["source_artifacts"]]
    artifacts[0]["payload_sha256"] = "sha256:" + "a" * 64
    with pytest.raises(ValidationError, match="exact validated AHRQ custody"):
        build_physician_count_acquisition({**payload, "source_artifacts": artifacts})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["source_artifact_refs"] = []
    with pytest.raises(ValidationError):
        build_physician_count_acquisition({**payload, "candidates": candidates})

    with pytest.raises(ValidationError, match="acquisition_sha256"):
        PhysicianCountAcquisition.model_validate(
            {**payload, "acquisition_sha256": "sha256:" + "f" * 64}
        )


def test_validated_cache_accepts_only_exact_frozen_receipt(tmp_path: Path) -> None:
    manifest_source = VALIDATED_CACHE / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    if not manifest_source.exists():
        pytest.skip("validated local AHRQ cache is not installed")
    verify_physician_count_source_bytes(_checked_in(), VALIDATED_CACHE)

    source_manifest = json.loads(manifest_source.read_text(encoding="utf-8"))
    cache_root = tmp_path / "cache"
    manifest_path = cache_root / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    fabricated_manifest = {
        **source_manifest,
        "artifact_id": "fabricated-artifact-id",
        "run_id": "fabricated-run",
        "retrieved_at": "2026-07-18T00:00:00+00:00",
        "loader_version": "fabricated-loader",
        "validator_version": "fabricated-validator",
    }
    write_atomic_json(manifest_path, fabricated_manifest)
    manifest_raw = manifest_path.read_bytes()
    payload = _checked_in().model_dump(mode="json")
    receipt = {
        **payload["cache_receipt"],
        "dataset_artifact_id": fabricated_manifest["artifact_id"],
        "run_id": fabricated_manifest["run_id"],
        "retrieved_at": fabricated_manifest["retrieved_at"],
        "loader_version": fabricated_manifest["loader_version"],
        "validator_version": fabricated_manifest["validator_version"],
        "manifest_sha256": f"sha256:{hashlib.sha256(manifest_raw).hexdigest()}",
        "manifest_content_length": len(manifest_raw),
    }
    with pytest.raises(ValidationError, match="exact validated AHRQ custody"):
        build_physician_count_acquisition({**payload, "cache_receipt": receipt})


def test_json_schema_rejects_every_runtime_immutable_mutation() -> None:
    payload = _checked_in().model_dump(mode="json")
    schema = json.loads(V3_SCHEMA.read_text(encoding="utf-8"))

    rows = [dict(item) for item in payload["system_rows"]]
    candidates = [dict(item) for item in payload["candidates"]]
    rows[0]["raw_lexical_value"] = "9999"
    rows[0]["source_row_sha256"] = "sha256:" + "a" * 64
    candidates[0]["candidate_value"] = 9999

    invented_blockers = [dict(item) for item in payload["candidates"]]
    invented_blockers[0]["blocker_codes"] = [
        *invented_blockers[0]["blocker_codes"],
        "invented_gate",
    ]
    shortened_finding = [dict(item) for item in payload["candidates"]]
    shortened_finding[0]["finding"] = "blocked"
    drifted_receipt = {**payload["cache_receipt"], "run_id": "fabricated-run"}

    runtime_mutations = [
        {**payload, "acquired_at": "2026-07-18T12:01:00Z"},
        {**payload, "systems": list(reversed(payload["systems"]))},
        {**payload, "system_rows": rows, "candidates": candidates},
        {**payload, "candidates": invented_blockers},
        {**payload, "candidates": shortened_finding},
        {**payload, "cache_receipt": drifted_receipt},
        {**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]},
        {**payload, "prohibited_outputs": list(reversed(payload["prohibited_outputs"]))},
        {**payload, "prohibited_outputs": [*payload["prohibited_outputs"], "scale_score"]},
    ]
    for mutated in runtime_mutations:
        with pytest.raises(ValidationError):
            build_physician_count_acquisition(mutated)
        assert list(Draft202012Validator(schema).iter_errors(mutated))

    fake_hash = {**payload, "acquisition_sha256": "sha256:" + "f" * 64}
    with pytest.raises(ValidationError, match="acquisition_sha256"):
        PhysicianCountAcquisition.model_validate(fake_hash)
    assert list(Draft202012Validator(schema).iter_errors(fake_hash))


def test_prior_v1_and_v2_models_and_bytes_remain_unchanged() -> None:
    assert annual_acquisition().model_dump(mode="json") == json.loads(
        ANNUAL_ACQUISITION.read_text(encoding="utf-8")
    )
    expected = {
        REVENUE_ACQUISITION: "ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0",  # pragma: allowlist secret
        REVENUE_EVIDENCE: "04fadae952898bc6dac87d0aaf4a3b04711cc9acc387ec751612f4b937b5b89f",  # pragma: allowlist secret
        ANNUAL_ACQUISITION: "aa0027e2af3dc5e29fc2e5245b6e3d36370b83560ed8bbf64f9de12c6908495a",  # pragma: allowlist secret
        ANNUAL_EVIDENCE: "29229692c230073770d5ecbd766d385bd2b9f44eb5c6be2d8640d5480b0fc1d3",  # pragma: allowlist secret
    }
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in expected} == expected


def test_two_in_memory_builds_are_byte_identical() -> None:
    first = acquisition()
    second = acquisition()
    first_acquisition = json.dumps(first.model_dump(mode="json"), indent=2, sort_keys=True).encode()
    second_acquisition = json.dumps(second.model_dump(mode="json"), indent=2, sort_keys=True).encode()
    first_evidence = build_physician_count_public_evidence_input(first).model_dump(mode="json")
    second_evidence = build_physician_count_public_evidence_input(second).model_dump(mode="json")
    assert first_acquisition == second_acquisition
    assert canonical_sha256(first_evidence) == canonical_sha256(second_evidence)
