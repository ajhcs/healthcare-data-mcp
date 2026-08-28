"""Contract and transition tests for the AHRQ official-release detector."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import pytest

from shared.acquisition.ahrq_detector import (
    AhrqChangeDetector,
    AhrqDetectorError,
    ReleaseMetadata,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/ahrq/v1/fixtures"


def test_release_fixtures_are_schema_valid_and_invalid_fixture_is_rejected() -> None:
    detector = AhrqChangeDetector()

    for name in ("official-release-changed.json", "official-release-unchanged.json", "failed-release-probe.json"):
        assert detector.load_fixture(FIXTURE_ROOT / name)["schema_version"] == "hdp.ahrq-release-probe.v1"

    with pytest.raises(AhrqDetectorError, match="schema validation"):
        detector.load_fixture(FIXTURE_ROOT / "invalid-release-probe.json")


def test_resolve_release_metadata_and_compute_stable_semantic_hash() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    raw = fixture["release_metadata"]
    assert isinstance(raw, dict)

    metadata = ReleaseMetadata.from_mapping(raw)

    assert metadata.source_id == "source:ahrq:lighthouse"
    assert metadata.release_id == "release:ahrq:2026-08-22"
    assert metadata.release_fingerprint.startswith("sha256:")
    assert metadata.release_fingerprint == ReleaseMetadata.from_mapping(raw).release_fingerprint


def test_changed_probe_emits_release_and_response_receipt() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")

    receipt = detector.detect(fixture)

    assert receipt.state == "changed"
    assert receipt.release_id == "release:ahrq:2026-08-22"
    assert receipt.release_fingerprint is not None
    assert receipt.response_fingerprint is not None
    assert receipt.receipt_id.startswith("receipt:ahrq:")
    assert receipt.failure_reason is None


def test_same_release_is_semantic_noop_even_when_response_bytes_change() -> None:
    detector = AhrqChangeDetector()
    changed_fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    unchanged_fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-unchanged.json")
    prior = detector.detect(changed_fixture)

    receipt = detector.detect(unchanged_fixture, prior=prior)

    assert receipt.state == "unchanged"
    assert receipt.release_fingerprint == prior.release_fingerprint
    assert receipt.response_fingerprint != prior.response_fingerprint
    assert receipt.prior_release_fingerprint == prior.release_fingerprint
    assert receipt.prior_response_fingerprint == prior.response_fingerprint


def test_failed_probe_preserves_prior_release_and_response_receipt() -> None:
    detector = AhrqChangeDetector()
    prior = detector.detect(detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json"))
    failed = detector.load_fixture(FIXTURE_ROOT / "failed-release-probe.json")

    receipt = detector.detect(failed, prior=prior)

    assert receipt.state == "failed_probe"
    assert receipt.release_id == prior.release_id
    assert receipt.release_fingerprint == prior.release_fingerprint
    assert receipt.response_fingerprint == prior.response_fingerprint
    assert receipt.failure_reason


def test_failed_probe_without_prior_is_explicit_but_has_no_release_claim() -> None:
    detector = AhrqChangeDetector()
    receipt = detector.detect(detector.load_fixture(FIXTURE_ROOT / "failed-release-probe.json"))

    assert receipt.state == "failed_probe"
    assert receipt.release_id is None
    assert receipt.release_fingerprint is None
    assert receipt.response_fingerprint is None


def test_response_metadata_mismatch_fails_closed() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    body = fixture["response_body"]
    assert isinstance(body, str)
    fixture["response_body"] = body.replace("2026-08-22", "2026-08-23")

    with pytest.raises(AhrqDetectorError, match="does not match"):
        detector.detect(fixture)


def test_source_url_and_response_lineage_mismatch_fails_closed() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    metadata = fixture["release_metadata"]
    assert isinstance(metadata, dict)
    metadata["source_url"] = "https://evil.example/release.json"

    with pytest.raises(AhrqDetectorError, match="source_url"):
        detector.detect(fixture)

    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    body = fixture["response_body"]
    assert isinstance(body, str)
    fixture["response_body"] = body[:-1] + ',"source_url":"https://evil.example/release.json"}'
    with pytest.raises(AhrqDetectorError, match="response_body source_url"):
        detector.detect(fixture)


def test_generic_source_fixture_is_rejected_by_ahrq_binding() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    fixture["source_id"] = "source:other:release"
    metadata = fixture["release_metadata"]
    assert isinstance(metadata, dict)
    metadata["source_id"] = "source:other:release"

    with pytest.raises(AhrqDetectorError, match="schema validation|unsupported AHRQ source"):
        detector.detect(fixture)


def test_malformed_prior_receipt_is_rejected_before_classification() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    prior = detector.detect(fixture)

    with pytest.raises(AhrqDetectorError, match="prior .*fingerprint"):
        detector.detect(fixture, prior=replace(prior, release_fingerprint="bogus"))
    with pytest.raises(AhrqDetectorError, match="both release and response"):
        detector.detect(fixture, prior=replace(prior, response_fingerprint=None))


def test_non_ascii_response_is_bounded_by_utf8_bytes() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    body = fixture["response_body"]
    assert isinstance(body, str)
    fixture["response_body"] = body[:-1] + ',"padding":"' + ("😀" * 40_000) + '"}'

    with pytest.raises(AhrqDetectorError, match="131072-byte"):
        detector.detect(fixture)


def test_invalid_utf8_fixture_is_wrapped_as_detector_error(tmp_path: Path) -> None:
    path = tmp_path / "invalid-encoding.json"
    path.write_bytes(b'{"schema_version":"hdp.ahrq-release-probe.v1",\xff}')

    with pytest.raises(AhrqDetectorError, match="unable to read AHRQ release fixture"):
        AhrqChangeDetector().load_fixture(path)


def test_prior_receipt_from_another_source_is_rejected() -> None:
    detector = AhrqChangeDetector()
    fixture = detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json")
    prior = detector.detect(fixture)
    foreign = replace(prior, source_id="source:cms:pdc")
    with pytest.raises(AhrqDetectorError, match="source_id"):
        detector.detect(fixture, prior=foreign)


def test_duplicate_fixture_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text(
        '{"schema_version":"hdp.ahrq-release-probe.v1","schema_version":"bad"}',
        encoding="utf-8",
    )

    with pytest.raises(AhrqDetectorError, match="duplicate JSON key"):
        AhrqChangeDetector().load_fixture(path)


def test_receipt_is_json_serializable() -> None:
    detector = AhrqChangeDetector()
    receipt = detector.detect(detector.load_fixture(FIXTURE_ROOT / "official-release-changed.json"))

    encoded = json.dumps(receipt.as_dict(), sort_keys=True)

    assert "receipt:ahrq:" in encoded
