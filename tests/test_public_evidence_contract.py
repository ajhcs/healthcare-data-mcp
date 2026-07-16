"""Consumer-facing tests for Public Evidence Bundle v1."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from shared.contracts.public_evidence import (
    PUBLIC_EVIDENCE_BUNDLE_SCHEMA_VERSION,
    PublicEvidenceBundle,
    PublicEvidenceBundleInput,
    build_public_evidence_bundle,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "contracts" / "v1" / "fixtures" / "public-evidence-input.json"
SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"


def _fixture_payload() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_bundle_is_hash_stable_and_schema_versioned() -> None:
    source = PublicEvidenceBundleInput.model_validate(_fixture_payload())

    first = build_public_evidence_bundle(source)
    second = build_public_evidence_bundle(source)

    assert first == second
    assert first.schema_version == PUBLIC_EVIDENCE_BUNDLE_SCHEMA_VERSION
    assert first.bundle_sha256.startswith("sha256:")
    assert PublicEvidenceBundle.model_validate_json(first.model_dump_json()) == first


def test_bundle_rejects_unknown_receipt_reference() -> None:
    payload = _fixture_payload()
    observations = payload["observations"]
    assert isinstance(observations, list)
    assert isinstance(observations[0], dict)
    observations[0]["receipt_refs"] = ["receipt:missing"]

    with pytest.raises(ValidationError, match="unknown observation receipt"):
        build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(payload))


@pytest.mark.parametrize(
    "uri",
    [
        "/home/operator/cache/source.json",
        "C:/cache/file",
        "C:cache/file",
        r"\\server\share",
        "FILE:///home/operator/cache.json",
        "file:C:/cache.json",
    ],
)
def test_bundle_rejects_absolute_cache_locator(uri: str) -> None:
    payload = _fixture_payload()
    artifacts = payload["input_artifacts"]
    assert isinstance(artifacts, list)
    assert isinstance(artifacts[0], dict)
    artifacts[0]["uri"] = uri

    with pytest.raises(ValidationError, match="portable"):
        PublicEvidenceBundleInput.model_validate(payload)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_bundle_rejects_non_finite_numeric_observation(value: float) -> None:
    payload = _fixture_payload()
    observations = payload["observations"]
    assert isinstance(observations, list) and isinstance(observations[0], dict)
    observations[0]["value_type"] = "number"
    observations[0]["value"] = value

    with pytest.raises(ValidationError, match="must be finite"):
        PublicEvidenceBundleInput.model_validate(payload)


def test_bundle_rejects_duplicate_cache_artifact_identity() -> None:
    payload = _fixture_payload()
    artifacts = payload["input_artifacts"]
    assert isinstance(artifacts, list) and isinstance(artifacts[0], dict)
    artifacts.append(dict(artifacts[0]))

    with pytest.raises(ValidationError, match="duplicate cache artifact_id"):
        build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(payload))


@pytest.mark.parametrize(("collection", "identity"), [("coverage", "coverage_id"), ("conflicts", "conflict_id")])
def test_bundle_rejects_duplicate_coverage_or_conflict_identity(collection: str, identity: str) -> None:
    payload = _fixture_payload()
    items = payload[collection]
    assert isinstance(items, list)
    if not items:
        items.append(
            {
                "conflict_id": "conflict:fixture",
                "conflict_type": "fixture",
                "entity_refs": [],
                "observation_refs": [],
                "receipt_refs": [],
                "status": "open",
                "rationale": "Synthetic duplicate identity fixture.",
            }
        )
    assert isinstance(items[0], dict) and identity in items[0]
    items.append(dict(items[0]))

    with pytest.raises(ValidationError, match=f"duplicate {identity}"):
        build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(payload))


def test_bundle_accepts_arbitrarily_large_integer_without_overflow() -> None:
    payload = _fixture_payload()
    observations = payload["observations"]
    assert isinstance(observations, list) and isinstance(observations[0], dict)
    observations[0]["value_type"] = "number"
    observations[0]["value"] = 10**1000

    bundle = build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(payload))
    assert bundle.observations[0].value == 10**1000


def test_integer_observation_accepts_json_schema_integral_number_and_normalizes() -> None:
    payload = _fixture_payload()
    observations = payload["observations"]
    assert isinstance(observations, list) and isinstance(observations[0], dict)
    observations[0]["value_type"] = "integer"
    observations[0]["value"] = 1.0

    source = PublicEvidenceBundleInput.model_validate(payload)
    assert source.observations[0].value == 1
    assert isinstance(source.observations[0].value, int)


def test_bundle_requires_full_producer_commit() -> None:
    payload = _fixture_payload()
    producer = payload["producer"]
    assert isinstance(producer, dict)
    producer["commit"] = "abcdef0"

    with pytest.raises(ValidationError, match="commit"):
        PublicEvidenceBundleInput.model_validate(payload)


def test_bundle_rejects_receipt_artifact_checksum_conflict() -> None:
    payload = _fixture_payload()
    sources = payload["sources"]
    assert isinstance(sources, list) and isinstance(sources[0], dict)
    receipt = sources[0]["receipt"]
    assert isinstance(receipt, dict) and isinstance(receipt["artifact"], dict)
    receipt["artifact"]["checksum_sha256"] = "sha256:" + "b" * 64

    with pytest.raises(ValidationError, match="must match input artifact lineage"):
        build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(payload))


def test_bundle_rejects_coverage_for_different_entity_or_measure() -> None:
    payload = _fixture_payload()
    coverage = payload["coverage"]
    assert isinstance(coverage, list) and isinstance(coverage[0], dict)
    coverage[0]["entity_ref"] = "data-mcp:ahrq:system:main-line"

    with pytest.raises(ValidationError, match="coverage observation must match"):
        build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(payload))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("created_at", "2026-07-16T00:00:00"),
        ("retrieved_at", "2026-07-16T00:00:00"),
        ("source_modified", "not-a-date"),
    ],
)
def test_bundle_rejects_naive_or_malformed_provenance_time(field: str, value: str) -> None:
    payload = _fixture_payload()
    if field == "created_at":
        payload[field] = value
    else:
        sources = payload["sources"]
        assert isinstance(sources, list) and isinstance(sources[0], dict)
        receipt = sources[0]["receipt"]
        assert isinstance(receipt, dict)
        receipt[field] = value

    with pytest.raises(ValidationError):
        PublicEvidenceBundleInput.model_validate(payload)


def test_bundle_rejects_reversed_evidence_period() -> None:
    payload = _fixture_payload()
    observations = payload["observations"]
    assert isinstance(observations, list) and isinstance(observations[0], dict)
    period = observations[0]["period"]
    assert isinstance(period, dict)
    period.update({"start": "2025-01-01", "end": "2024-01-01"})

    with pytest.raises(ValidationError, match="end cannot precede start"):
        PublicEvidenceBundleInput.model_validate(payload)


def test_bundle_rejects_unknown_schema_version_and_tampered_hash() -> None:
    bundle = build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(_fixture_payload()))
    payload = bundle.model_dump(mode="json")
    payload["schema_version"] = "ushso.public-evidence-bundle.v2"
    with pytest.raises(ValidationError, match="schema_version"):
        PublicEvidenceBundle.model_validate(payload)

    payload = bundle.model_dump(mode="json")
    payload["bundle_id"] = "bundle:tampered"
    with pytest.raises(ValidationError, match="bundle_sha256"):
        PublicEvidenceBundle.model_validate(payload)


def test_cli_builds_bundle_without_intermediate_copy(tmp_path: Path) -> None:
    output = tmp_path / "bundle.json"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "shared.contracts.cli",
            "build-public-evidence",
            "--input",
            str(FIXTURE),
            "--output",
            str(output),
            "--producer-commit",
            "d" * 40,
        ],
        cwd=ROOT,
        check=True,
    )

    bundle = PublicEvidenceBundle.model_validate_json(output.read_text(encoding="utf-8"))
    assert bundle.observations[0].receipt_refs == ["receipt:pa-doh:temple:fy2024"]
    assert bundle.producer.commit == "d" * 40


def test_checked_in_schema_matches_model() -> None:
    expected = json.dumps(PublicEvidenceBundle.model_json_schema(), indent=2, sort_keys=True) + "\n"
    assert SCHEMA.read_text(encoding="utf-8") == expected

    schema = json.loads(expected)
    validator = Draft202012Validator(schema)
    bundle = build_public_evidence_bundle(PublicEvidenceBundleInput.model_validate(_fixture_payload()))
    assert list(validator.iter_errors(bundle.model_dump(mode="json"))) == []

    malformed = bundle.model_dump(mode="json")
    malformed["observations"][0]["value_type"] = "number"
    malformed["observations"][0]["value"] = "not-a-number"
    assert list(validator.iter_errors(malformed))
