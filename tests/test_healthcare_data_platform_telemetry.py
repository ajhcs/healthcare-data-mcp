"""Focused contract, privacy, and durability tests for P1-13 telemetry."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import TypeAlias, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from shared.telemetry import (
    RunTelemetry,
    TelemetryCapacityError,
    TelemetryCollisionError,
    TelemetryNotFoundError,
    TelemetryRecorder,
    TelemetryStatus,
    TelemetryValidationError,
    build_run_telemetry,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "contracts/healthcare-data-platform/telemetry/v1/telemetry.schema.json"
FIXTURE_PATH = ROOT / "contracts/healthcare-data-platform/telemetry/v1/fixtures/valid-run-telemetry.json"
T0 = datetime(2026, 8, 29, tzinfo=timezone.utc)
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


def _event(
    suffix: str,
    *,
    status: str = "succeeded",
    bytes_in: int = 10,
    bytes_out: int = 8,
    rows_in: int = 3,
    rows_out: int = 2,
    retry_count: int = 0,
    lag_seconds: float = 4.0,
    dimensions: dict[str, str] | None = None,
    observed_at: datetime | None = None,
) -> RunTelemetry:
    failure = status in {"failed", "dead_lettered"}
    return build_run_telemetry(
        f"telemetry:run:{suffix}",
        "run:fixture:20260829",
        "source:fixture:public",
        "artifact:fixture:20260829",
        "hdp:observation-envelope:fixture:20260829",
        observed_at or T0,
        cast(TelemetryStatus, status),
        freshness_state="fresh",
        freshness_seconds=12.5,
        source_observed_at=T0 - timedelta(seconds=12.5),
        failure_code="source_timeout" if failure else None,
        failure_message="Authorization: Bearer super-secret-token" if failure else None,
        failure_retryable=True if failure else None,
        lag_seconds=lag_seconds,
        bytes_in=bytes_in,
        bytes_out=bytes_out,
        rows_in=rows_in,
        rows_out=rows_out,
        retry_count=retry_count,
        dlq_state="queued" if status == "dead_lettered" else "none",
        dlq_count=1 if status == "dead_lettered" else 0,
        dlq_reason="dead-lettered source item" if status == "dead_lettered" else None,
        dimensions={} if dimensions is None else dimensions,
    )


def test_event_correlates_all_required_ids_and_derives_stable_hash() -> None:
    event = _event("correlated")

    assert event.run_id == "run:fixture:20260829"
    assert event.source_id == "source:fixture:public"
    assert event.artifact_id == "artifact:fixture:20260829"
    assert event.envelope_id == "hdp:observation-envelope:fixture:20260829"
    assert event.input_bytes == event.bytes_in == 10
    assert event.output_rows == event.rows_out == 2
    assert event.telemetry_sha256 is not None and event.telemetry_sha256.startswith("sha256:")
    assert event == RunTelemetry.from_mapping(event.as_dict())


def test_recorder_is_idempotent_and_rejects_hash_collision(tmp_path: Path) -> None:
    database = tmp_path / "telemetry.sqlite"
    event = _event("idempotent")
    changed = _event("idempotent", bytes_in=11)
    with TelemetryRecorder(database) as recorder:
        first = recorder.record(event)
        duplicate = recorder.record(event)

        assert first.action == "recorded"
        assert duplicate.action == "duplicate"
        with pytest.raises(TelemetryCollisionError):
            recorder.record(changed)
        assert recorder.count() == 1

    with TelemetryRecorder(database) as restarted:
        assert restarted.read(event.telemetry_id) == event


def test_summary_aggregates_freshness_failure_lag_volume_retries_and_dlq() -> None:
    with TelemetryRecorder(":memory:") as recorder:
        recorder.record(_event("summary-start", status="started", observed_at=T0))
        recorder.record(_event("summary-success", retry_count=2, observed_at=T0 + timedelta(seconds=1)))
        recorder.record(
            _event("summary-failure", status="failed", retry_count=3, observed_at=T0 + timedelta(seconds=2))
        )
        recorder.record(
            _event("summary-dlq", status="dead_lettered", retry_count=4, observed_at=T0 + timedelta(seconds=3))
        )

        summary = recorder.summarize("run:fixture:20260829")

    assert summary.event_count == 4
    assert dict(summary.status_counts) == {"dead_lettered": 1, "failed": 1, "started": 1, "succeeded": 1}
    assert summary.failure_count == 2
    assert summary.freshness_max_seconds == 12.5
    assert summary.lag_max_seconds == 4.0
    assert summary.bytes_in == 40
    assert summary.bytes_out == 32
    assert summary.rows_in == 12
    assert summary.rows_out == 8
    assert summary.retry_count == 9
    assert summary.dlq_count == 1
    assert summary.as_dict()["record_type"] == "run_telemetry_summary"


def test_summary_volume_bounds_match_schema_field_cap() -> None:
    with TelemetryRecorder(":memory:") as recorder:
        recorder.record(_event("summary-bounds"))
        summary = recorder.summarize("run:fixture:20260829")

    for field in ("bytes_in", "bytes_out", "rows_in", "rows_out"):
        with pytest.raises(TelemetryValidationError, match="between"):
            replace(summary, **{field: 1_000_000_000_001})


def test_failure_and_dlq_text_is_redacted_before_persistence(tmp_path: Path) -> None:
    database = tmp_path / "telemetry.sqlite"
    event = _event("redaction", status="failed")
    assert event.failure_message is not None
    assert "super-secret-token" not in event.failure_message
    assert "Bearer" not in event.failure_message

    with TelemetryRecorder(database) as recorder:
        recorder.record(event)
        stored = recorder.read(event.telemetry_id)
        raw = (
            sqlite3.connect(database)
            .execute("SELECT failure_message FROM telemetry_events WHERE telemetry_id = ?", (event.telemetry_id,))
            .fetchone()[0]
        )

    assert stored.failure_message == event.failure_message
    assert "super-secret-token" not in str(raw)
    assert "Authorization" not in str(raw)


@pytest.mark.parametrize(
    "message",
    (
        "key=secret",
        "credential=secret",
        "token secret",
        "GET https://example.test/request?token=secret&query=phi",
    ),
)
def test_failure_redaction_covers_secret_forms_and_request_urls(message: str) -> None:
    event = replace(
        _event("redaction-regression", status="failed"),
        failure_message=message,
        telemetry_sha256=None,
    )

    assert event.failure_message is not None
    assert "secret" not in event.failure_message.lower()
    assert "https://" not in event.failure_message.lower()
    assert "token=secret" not in event.failure_message.lower()


def test_dimension_allowlist_and_cardinality_are_bounded() -> None:
    with TelemetryRecorder(":memory:", max_dimension_values=1, dimension_allowlist={"dataset"}) as recorder:
        recorder.record(_event("dimension-one", dimensions={"dataset": "cms"}))
        recorder.record(_event("dimension-two", dimensions={"dataset": "cms"}))
        with pytest.raises(TelemetryCapacityError, match="cardinality"):
            recorder.record(_event("dimension-three", dimensions={"dataset": "ahrq"}))
        with pytest.raises(TelemetryValidationError, match="allowlist"):
            recorder.record(_event("dimension-four", dimensions={"operation": "load"}))


def test_sensitive_dimension_and_unbounded_dimensions_fail_closed() -> None:
    with pytest.raises(TelemetryValidationError, match="sensitive"):
        _event("sensitive-key", dimensions={"token": "redacted"})
    with pytest.raises(TelemetryValidationError, match="more than"):
        _event(
            "too-many",
            dimensions={
                "stage": "x",
                "dataset": "x",
                "operation": "x",
                "release": "x",
                "source_kind": "x",
                "environment": "x",
                "worker_class": "x",
                "extra": "x",
                "region": "x",
            },
        )


def test_metric_bounds_and_overflow_are_validation_errors() -> None:
    with pytest.raises(TelemetryValidationError, match="finite"):
        _event("overflow", lag_seconds=10**1000)
    with pytest.raises(TelemetryValidationError, match="finite"):
        build_run_telemetry(
            "telemetry:run:overflow",
            "run:fixture:20260829",
            "source:fixture:public",
            "artifact:fixture:20260829",
            "hdp:observation-envelope:fixture:20260829",
            T0,
            "succeeded",
            freshness_state="fresh",
            freshness_seconds=10**1000,
        )
    with pytest.raises(TelemetryValidationError, match="between"):
        _event("negative", bytes_in=-1)


def test_dead_lettered_status_requires_dlq_and_failure_evidence() -> None:
    dead = _event("dead-letter", status="dead_lettered")
    assert dead.dlq_state == "queued"
    assert dead.dlq_count == 1
    with pytest.raises(TelemetryValidationError, match="queued DLQ"):
        build_run_telemetry(
            "telemetry:run:bad-dead-letter",
            "run:fixture:20260829",
            "source:fixture:public",
            "artifact:fixture:20260829",
            "hdp:observation-envelope:fixture:20260829",
            T0,
            "dead_lettered",
            failure_code="source_timeout",
            freshness_state="unknown",
        )


def test_schema_and_fixture_validate() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    event = _event("schema")
    with TelemetryRecorder(":memory:") as recorder:
        receipt = recorder.record(event)
        summary = recorder.summarize(event.run_id)

    assert list(validator.iter_errors(cast(JsonValue, fixture))) == []
    assert list(validator.iter_errors(cast(JsonValue, event.as_dict()))) == []
    assert list(validator.iter_errors(cast(JsonValue, receipt.as_dict()))) == []
    assert list(validator.iter_errors(cast(JsonValue, summary.as_dict()))) == []


def test_mapping_rejects_unknown_fields_and_hash_drift() -> None:
    payload = _event("mapping").as_dict()
    payload["unexpected"] = True
    with pytest.raises(TelemetryValidationError, match="schema validation"):
        RunTelemetry.from_mapping(payload)

    payload = _event("hash-drift").as_dict()
    payload["telemetry_sha256"] = "sha256:" + "b" * 64
    with pytest.raises(TelemetryValidationError, match="telemetry_sha256"):
        RunTelemetry.from_mapping(payload)


def test_bare_sha256_is_normalized_for_events_and_mappings() -> None:
    event = _event("bare-hash")
    assert event.telemetry_sha256 is not None
    bare_hash = event.telemetry_sha256.removeprefix("sha256:")

    direct = replace(event, telemetry_sha256=bare_hash)
    assert direct.telemetry_sha256 == event.telemetry_sha256
    assert direct.as_dict()["telemetry_sha256"] == event.telemetry_sha256

    payload = event.as_dict()
    payload["telemetry_sha256"] = bare_hash
    parsed = RunTelemetry.from_mapping(payload)
    assert parsed.telemetry_sha256 == event.telemetry_sha256
    assert parsed.as_dict()["telemetry_sha256"] == event.telemetry_sha256


def test_database_does_not_store_source_payload_columns() -> None:
    connection = sqlite3.connect(":memory:")
    with TelemetryRecorder(connection) as recorder:
        recorder.record(_event("columns"))
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(telemetry_events)")}

    assert "payload" not in columns
    assert "request_body" not in columns
    assert "failure_message" in columns


def test_missing_run_is_explicit() -> None:
    with TelemetryRecorder(":memory:") as recorder:
        with pytest.raises(TelemetryNotFoundError):
            recorder.summarize("run:missing")
