"""Consumer-facing tests for Public Evidence Bundle v1."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest
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
        ],
        cwd=ROOT,
        check=True,
    )

    bundle = PublicEvidenceBundle.model_validate_json(output.read_text(encoding="utf-8"))
    assert bundle.observations[0].receipt_refs == ["receipt:pa-doh:temple:fy2024"]


def test_checked_in_schema_matches_model() -> None:
    expected = json.dumps(PublicEvidenceBundle.model_json_schema(), indent=2, sort_keys=True) + "\n"
    assert SCHEMA.read_text(encoding="utf-8") == expected
