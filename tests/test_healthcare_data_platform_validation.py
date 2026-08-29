"""Focused P1-10 schema, drift, redaction, and projection tests."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Mapping

import pytest

from shared.validation import (
    DistributionRule,
    DriftBaseline,
    DriftValidationError,
    QuarantineError,
    QuarantineStore,
    build_quarantine_record,
    validate_and_project,
    validate_drift,
)
from shared.validation.drift import _candidate_key_digest
from tests.test_healthcare_data_platform_contract_adoption import _envelope


def _batch() -> dict[str, object]:
    return {
        "schema_version": "adapter.batch.v1",
        "source_id": "source:fixture",
        "release_id": "release:fixture:v1",
        "artifact_id": "artifact:fixture:v1",
        "artifact_sha256": "sha256:" + "b" * 64,
        "custody_locator": "object://fixture/artifact",
        "rows": [
            {"row_id": "row:1", "scope": "system", "secret": "patient-name"},
            {"row_id": "row:2", "scope": "facility", "secret": "ssn-123"},
        ],
    }


def _batch_baseline() -> DriftBaseline:
    return DriftBaseline(
        source_id="source:fixture",
        schema_version="adapter.batch.v1",
        expected_row_count=2,
        expected_observation_ids=("row:1", "row:2"),
        row_id_field="row_id",
        max_row_count_delta_ratio=0,
        distribution_rules=(
            DistributionRule(
                "scope",
                {"system": 1, "facility": 1},
                min_ratio=1,
                max_ratio=1,
                denominator=True,
                expected_total=2,
            ),
        ),
    )


def test_valid_adapter_batch_is_accepted_with_explicit_distribution_baseline() -> None:
    report = validate_drift(_batch(), _batch_baseline())

    assert report.accepted
    assert report.issue_codes == ()
    assert report.row_count == 2
    assert report.current_projection_preserved is True
    assert "patient-name" not in json.dumps(report.as_dict(), sort_keys=True)


def test_generic_adapter_batch_requires_an_explicit_adapter_schema_pin() -> None:
    baseline = DriftBaseline(expected_row_count=2, row_id_field="row_id")

    report = validate_drift(_batch(), baseline)

    assert report.rejected
    assert "schema.unregistered_adapter_version" in report.issue_codes


def test_unsupported_observation_envelope_version_is_classified_and_rejected() -> None:
    candidate = {"schema_version": "hdp.observation-envelope.v2", "rows": _batch()["rows"]}

    report = validate_drift(candidate)

    assert report.rejected
    assert "schema.version_drift" in report.issue_codes


def test_schema_drift_is_fail_closed_and_report_is_redacted() -> None:
    candidate = _envelope()
    candidate["unexpected"] = "secret source payload"

    report = validate_drift(candidate)
    assert report.rejected
    assert "schema.unknown_field" in report.issue_codes
    assert all(issue.kind == "schema" for issue in report.issues)
    assert "secret source payload" not in json.dumps(report.as_dict(), sort_keys=True)


def test_row_key_and_order_drift_are_reported_without_retaining_row_values() -> None:
    candidate = _batch()
    candidate["rows"] = [
        {"row_id": "row:2", "scope": "system", "secret": "different"},
        {"row_id": "row:2", "scope": "system", "secret": "different"},
    ]
    report = validate_drift(candidate, _batch_baseline())

    assert report.rejected
    assert {"row.duplicate_key", "row.missing_key"} <= set(report.issue_codes)
    assert "different" not in json.dumps(report.as_dict(), sort_keys=True)


def test_key_drift_detects_source_change_and_lineage_joins() -> None:
    candidate = _batch()
    candidate["source_id"] = "source:other"
    report = validate_drift(candidate, _batch_baseline())

    assert report.rejected
    assert "key.source_drift" in report.issue_codes

    envelope = _envelope()
    envelope["artifact"]["release_ref"] = "release:wrong"
    lineage_report = validate_drift(envelope)
    assert lineage_report.rejected
    assert "key.lineage_drift" in lineage_report.issue_codes


def test_distribution_denominator_drift_is_explicit() -> None:
    candidate = _batch()
    candidate["rows"] = [{"row_id": "row:1", "scope": "system"}]
    report = validate_drift(candidate, _batch_baseline())

    assert report.rejected
    assert "row.denominator_drift" in report.issue_codes
    assert "distribution.denominator_drift" in report.issue_codes


def test_missing_or_empty_distribution_values_fail_closed_even_at_zero_ratio() -> None:
    candidate = _batch()
    candidate["rows"] = [
        {"row_id": "row:1"},
        {"row_id": "row:2", "scope": ""},
    ]
    baseline = DriftBaseline(
        schema_version="adapter.batch.v1",
        expected_row_count=2,
        row_id_field="row_id",
        distribution_rules=(DistributionRule("scope", {"system": 1}, min_ratio=0, max_ratio=2),),
    )

    report = validate_drift(candidate, baseline)

    assert report.rejected
    assert "distribution.missing_value" in report.issue_codes


def test_distribution_policy_rejects_non_finite_or_reversed_bounds() -> None:
    with pytest.raises(DriftValidationError, match="finite"):
        DistributionRule("scope", {"system": 1}, min_ratio=float("nan"))
    with pytest.raises(DriftValidationError, match="at least"):
        DistributionRule("scope", {"system": 1}, min_ratio=2, max_ratio=1)
    with pytest.raises(DriftValidationError, match="expected_total or cardinality"):
        DistributionRule("scope", {"system": 1}, denominator=True)
    assert DistributionRule("scope", {"system": 1}, denominator=True, cardinality=1).expected_total == 1


def test_malformed_numeric_rows_are_quarantined_with_unique_reasons_and_no_raw_values() -> None:
    raw_value = 9876543210123456789
    candidate = _batch()
    candidate["rows"] = [raw_value, raw_value]

    report = validate_drift(candidate, _batch_baseline())
    record = build_quarantine_record(candidate, report, recorded_at="2026-08-29T00:00:00Z")
    encoded = json.dumps({"report": report.as_dict(), "record": record.as_dict()}, sort_keys=True)

    assert report.rejected
    assert report.issue_codes.count("row.malformed") == 2
    assert len(record.reason_codes) == len(set(record.reason_codes))
    assert str(raw_value) not in encoded


def test_untrusted_quarantine_metadata_is_redacted_or_prefixed() -> None:
    candidate = _batch()
    candidate["source_id"] = "patient name"
    candidate["release_id"] = 123456789
    candidate["artifact_id"] = {"untrusted": "raw-artifact-value"}
    candidate["rows"] = []

    report = validate_drift(candidate, _batch_baseline())
    record = build_quarantine_record(candidate, report, recorded_at="2026-08-29T00:00:00Z")
    encoded = json.dumps({"report": report.as_dict(), "record": record.as_dict()}, sort_keys=True)

    assert record.source_id is not None and record.source_id.startswith("redacted:")
    assert record.release_id is not None and record.release_id.startswith("redacted:")
    assert record.artifact_id is not None and record.artifact_id.startswith("redacted:")
    assert "patient name" not in encoded
    assert "raw-artifact-value" not in encoded
    assert str(123456789) not in encoded


def test_checkpoint_and_cursor_identity_participate_in_key_drift() -> None:
    candidate = _batch()
    candidate["checkpoint"] = {"checkpoint_id": "checkpoint:fixture:v1", "cursor": "cursor:1"}
    expected_digest = _candidate_key_digest(candidate)
    assert expected_digest is not None
    baseline = DriftBaseline(
        source_id="source:fixture",
        schema_version="adapter.batch.v1",
        expected_row_count=2,
        expected_observation_ids=("row:1", "row:2"),
        row_id_field="row_id",
        max_row_count_delta_ratio=0,
        expected_key_digest=expected_digest,
        expected_checkpoint_id="checkpoint:fixture:v1",
        expected_cursor="cursor:1",
    )

    assert validate_drift(candidate, baseline).accepted
    drifted = {**candidate, "checkpoint": {"checkpoint_id": "checkpoint:fixture:v1", "cursor": "cursor:2"}}
    report = validate_drift(drifted, baseline)

    assert report.rejected
    assert "key.cursor_drift" in report.issue_codes
    assert "key.identity_drift" in report.issue_codes


def test_idempotency_reuse_with_changed_observation_content_is_rejected() -> None:
    candidate = _envelope()
    baseline = DriftBaseline.from_envelope(candidate)
    changed = deepcopy(candidate)
    changed["observations"][0]["identity_key"] = "identity:changed"

    report = validate_drift(changed, baseline)

    assert report.rejected
    assert "key.identity_drift" in report.issue_codes


def test_rejected_candidate_is_quarantined_and_never_projected(tmp_path: Path) -> None:
    candidate = _batch()
    candidate["rows"] = [{"row_id": "row:1", "scope": "system", "secret": "do-not-store"}]
    projected: list[Mapping[str, object]] = []

    # Keep the callback annotation narrow and avoid accepting a mutable alias.
    def project(value: Mapping[str, object]) -> None:
        projected.append(value)

    outcome = validate_and_project(
        candidate,
        _batch_baseline(),
        quarantine_store=QuarantineStore(tmp_path / "quarantine"),
        current_projection=project,
        recorded_at="2026-08-29T00:00:00Z",
    )

    assert outcome.rejected
    assert outcome.current_projection_preserved is True
    assert projected == []
    assert outcome.quarantine is not None
    assert outcome.quarantine_receipt is not None
    encoded = Path(outcome.quarantine_receipt.path).read_text(encoding="utf-8")
    assert "do-not-store" not in encoded
    assert "secret" not in encoded
    assert QuarantineStore(tmp_path / "quarantine").read(outcome.quarantine.quarantine_id) == outcome.quarantine


def test_quarantine_retry_is_idempotent_across_recording_times(tmp_path: Path) -> None:
    candidate = _batch()
    candidate["rows"] = []
    report = validate_drift(candidate, _batch_baseline())
    first = build_quarantine_record(candidate, report, recorded_at="2026-08-29T00:00:00Z")
    second = build_quarantine_record(candidate, report, recorded_at="2026-08-29T00:00:01Z")
    store = QuarantineStore(tmp_path)

    first_receipt = store.put(first)
    duplicate_receipt = store.put(second)

    assert first_receipt.state == "stored"
    assert duplicate_receipt.state == "duplicate"
    assert duplicate_receipt.record_sha256 == first.record_sha256


def test_quarantine_record_integrity_is_verified_on_read(tmp_path: Path) -> None:
    candidate = _batch()
    candidate["rows"] = []
    report = validate_drift(candidate, _batch_baseline())
    record = build_quarantine_record(candidate, report, recorded_at="2026-08-29T00:00:00Z")
    store = QuarantineStore(tmp_path)
    receipt = store.put(record)
    path = Path(receipt.path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["record_sha256"] = "sha256:" + "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(QuarantineError, match="integrity"):
        store.read(record.quarantine_id)


def test_redacted_metadata_quarantine_record_round_trips(tmp_path: Path) -> None:
    candidate = _batch()
    candidate["source_id"] = "patient name"
    candidate["rows"] = []
    report = validate_drift(candidate, _batch_baseline())
    record = build_quarantine_record(candidate, report, recorded_at="2026-08-29T00:00:00Z")
    store = QuarantineStore(tmp_path)

    store.put(record)
    restored = store.read(record.quarantine_id)

    assert restored == record
    assert restored.source_id == record.source_id
    assert restored.source_id is not None and restored.source_id.startswith("redacted:")


def test_quarantine_rejects_record_path_symlink(tmp_path: Path) -> None:
    candidate = _batch()
    candidate["rows"] = []
    report = validate_drift(candidate, _batch_baseline())
    record = build_quarantine_record(candidate, report, recorded_at="2026-08-29T00:00:00Z")
    store = QuarantineStore(tmp_path / "quarantine")
    path = Path(store.root) / f"quarantine-{record.quarantine_id.removeprefix('quarantine:')}.json"
    path.symlink_to(tmp_path / "outside.json")

    with pytest.raises(QuarantineError, match="symlink"):
        store.put(record)


def test_quarantine_rejects_unsafe_custody_locator() -> None:
    candidate = _batch()
    candidate["rows"] = []
    report = validate_drift(candidate, _batch_baseline())

    with pytest.raises(QuarantineError, match="safe custody locator"):
        build_quarantine_record(candidate, report, custody_locator="../../source.csv")


def test_accepted_candidate_can_be_projected_after_validation() -> None:
    projected: list[Mapping[str, object]] = []

    def project(value: Mapping[str, object]) -> None:
        projected.append(value)

    outcome = validate_and_project(_batch(), _batch_baseline(), current_projection=project)
    assert outcome.accepted
    assert outcome.report.projection_applied is True
    assert outcome.current_projection_preserved is False
    assert projected == [_batch()]
