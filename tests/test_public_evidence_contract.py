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


def test_bundle_rejects_absolute_cache_locator() -> None:
    payload = _fixture_payload()
    artifacts = payload["input_artifacts"]
    assert isinstance(artifacts, list)
    assert isinstance(artifacts[0], dict)
    artifacts[0]["uri"] = "/home/operator/cache/source.json"

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
