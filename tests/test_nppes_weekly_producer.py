"""Focused conformance tests for the NPPES weekly change producer."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Iterable, Literal, cast

import pytest

from shared.acquisition.nppes import (
    NPPES_SOURCE_ID,
    NPPES_SOURCE_URL,
    NppesCatalogEntry,
    NppesFileDescriptor,
    NppesFileKind,
)
from shared.acquisition.nppes.weekly import (
    NppesWeeklyObservation,
    NppesWeeklyProducer,
    NppesWeeklyProducerError,
    NppesWeeklyReceipt,
    NppesWeeklyRelease,
    NppesWeeklyReplayConflictError,
    NppesWeeklySourceState,
    produce_nppes_weekly,
    validate_nppes_weekly_receipt,
)


RECORDED_AT = datetime(2026, 8, 29, tzinfo=timezone.utc)
VALID_NPI = "0123456789"
LiteralAvailability = Literal["present", "unavailable_public", "not_applicable"]
WeeklyProbe = Literal["changed", "not_modified", "failed_probe"]


def _catalog(**overrides: object) -> NppesCatalogEntry:
    values: dict[str, object] = {
        "max_bytes": 1_000_000,
        "max_chunks": 1_000,
        "max_seconds": 120,
        "max_chunk_bytes": 128,
    }
    values.update(overrides)
    return NppesCatalogEntry(**values)  # type: ignore[arg-type]


def _descriptor(
    kind: NppesFileKind, *, availability: str = "present", source_url: str = NPPES_SOURCE_URL
) -> NppesFileDescriptor:
    return NppesFileDescriptor(
        file_kind=kind,
        source_file_name=f"nppes_weekly_{kind}.csv",
        source_url=source_url,
        evidence_locator=source_url,
        availability=cast("LiteralAvailability", availability),
    )


def _release(
    *,
    release_id: str = "release:nppes:weekly:2026-08-07",
    release_sequence: int = 10,
    probe_state: str = "changed",
    kinds: tuple[NppesFileKind, ...] = ("provider", "location", "endpoint", "reference", "deactivation"),
    source_url: str = NPPES_SOURCE_URL,
    final_url: str | None = NPPES_SOURCE_URL,
    evidence_locator: str = NPPES_SOURCE_URL,
) -> NppesWeeklyRelease:
    typed_probe = cast("WeeklyProbe", probe_state)
    return NppesWeeklyRelease(
        release_id=release_id,
        release_label="NPPES weekly changes 2026-08-07",
        source_id=NPPES_SOURCE_ID,
        week_start="2026-08-01",
        week_end="2026-08-07",
        release_sequence=release_sequence,
        source_url=source_url,
        probe_state=typed_probe,
        final_url=final_url,
        http_status=200 if probe_state == "changed" else (304 if probe_state == "not_modified" else 503),
        published_at="2026-08-08T00:00:00Z" if probe_state == "changed" else None,
        evidence_locator=evidence_locator,
        files=tuple(_descriptor(kind) for kind in kinds) if probe_state == "changed" else (),
        failure_reason="source_unavailable" if probe_state == "failed_probe" else None,
    )


def _streams(*, provider: Iterable[bytes] | None = None) -> dict[NppesFileKind, Iterable[bytes]]:
    return {
        "provider": provider
        if provider is not None
        else [
            b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE,OPERATION\n",
            b"0123456789,provider-1,2026-08-07,upsert\n",
        ],
        "location": [b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE\n0123456789,location-1,2026-08-07\n"],
        "endpoint": [b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE\n0123456789,endpoint-1,2026-08-07\n"],
        "reference": [b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE\n0123456789,reference-1,2026-08-07\n"],
        "deactivation": [b"NPI,SOURCE_ROW_ID,DEACTIVATION_DATE\n0123456789,deactivation-1,2026-08-06\n"],
    }


def _provider_release() -> NppesWeeklyRelease:
    return _release(kinds=("provider",))


def _state(*, source_row_id: str = "provider-old", effective_date: str = "2026-08-06") -> NppesWeeklySourceState:
    return NppesWeeklyObservation(
        source_key=f"provider:{VALID_NPI}",
        file_kind="provider",
        source_file_name="nppes_weekly_provider.csv",
        npi=VALID_NPI,
        source_row_id=source_row_id,
        row_number=2,
        operation="upsert",
        effective_date=effective_date,
        release_sequence=9,
        release_id="release:nppes:weekly:2026-08-06",
        row_sha256="sha256:" + "0" * 64,
        evidence_locator=NPPES_SOURCE_URL,
    )


def test_changed_weekly_preserves_source_metadata_and_deactivation_review() -> None:
    captured: list[NppesWeeklyObservation] = []
    receipt = produce_nppes_weekly(
        _catalog(),
        _release(),
        _streams(),
        recorded_at=RECORDED_AT,
        row_sink=captured.append,
    )

    assert receipt.state == "changed"
    assert receipt.current_projection_preserved is True
    assert receipt.source_id == NPPES_SOURCE_ID
    assert receipt.source_url == NPPES_SOURCE_URL
    assert receipt.probe_state == "changed"
    assert len(captured) == 5
    assert [row.source_row_id for row in captured] == [
        "provider-1",
        "location-1",
        "endpoint-1",
        "reference-1",
        "deactivation-1",
    ]
    assert all(row.source_key == f"{row.file_kind}:{row.npi}" for row in captured)
    assert receipt.deactivation_count == 1
    opportunity = receipt.deactivation_opportunities[0]
    assert opportunity.npi == VALID_NPI
    assert opportunity.deactivation_date == "2026-08-06"
    assert opportunity.review_state == "review_required"
    assert all(row.operation == "deactivate" for row in captured if row.file_kind == "deactivation")
    assert all("NPPES weekly changes" not in str(file_receipt.as_dict()) for file_receipt in receipt.files)


def test_shuffled_rows_use_deterministic_ties_and_preserve_late_rows() -> None:
    provider = [
        b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE,OPERATION\n",
        b"0123456789,provider-z,2026-08-07,upsert\n",
        b"0123456789,provider-a,2026-08-07,upsert\n",
        b"0123456789,provider-late,2026-08-05,upsert\n",
    ]
    captured: list[NppesWeeklyObservation] = []
    receipt = NppesWeeklyProducer(_catalog()).produce(
        _provider_release(),
        {"provider": provider},
        previous_state={f"provider:{VALID_NPI}": _state()},
        recorded_at=RECORDED_AT,
        row_sink=captured.append,
    )

    assert receipt.state == "changed"
    assert receipt.applied_count == 1
    assert receipt.applied_samples[0].source_row_id == "provider-z"
    assert receipt.late_count == 1
    assert receipt.late_samples[0].source_row_id == "provider-late"
    assert receipt.out_of_order_count == 2
    assert [row.source_row_id for row in captured] == ["provider-z", "provider-a", "provider-late"]


def test_omitted_optional_files_are_unavailable_and_missing_rows_are_not_deletions() -> None:
    captured: list[NppesWeeklyObservation] = []
    receipt = produce_nppes_weekly(
        _catalog(),
        _provider_release(),
        {"provider": _streams()["provider"]},
        previous_state={f"provider:{VALID_NPI}": _state()},
        recorded_at=RECORDED_AT,
        row_sink=captured.append,
    )

    assert receipt.state == "changed"
    assert receipt.missing_file_count == 4
    assert {item.file_kind for item in receipt.files if item.state == "unavailable_public"} == {
        "location",
        "endpoint",
        "reference",
        "deactivation",
    }
    assert all(
        item.error_code == "file_descriptor_omitted" for item in receipt.files if item.state == "unavailable_public"
    )
    assert receipt.deactivation_count == 0
    assert [row.source_row_id for row in captured] == ["provider-1"]
    assert all(row.operation != "deactivate" for row in captured)


def test_no_op_and_failed_probe_do_not_consume_streams() -> None:
    consumed = False

    def forbidden() -> Iterable[bytes]:
        nonlocal consumed
        consumed = True
        raise AssertionError("probe without changed content must not read source files")
        yield b"never"

    no_op = produce_nppes_weekly(
        _catalog(),
        _release(probe_state="not_modified", final_url=None),
        {"provider": forbidden()},
        recorded_at=RECORDED_AT,
    )
    failed = produce_nppes_weekly(
        _catalog(),
        _release(probe_state="failed_probe", final_url=None),
        {"provider": forbidden()},
        recorded_at=RECORDED_AT,
    )

    assert no_op.state == "no_op"
    assert no_op.files == ()
    assert failed.state == "failed_probe"
    assert failed.failure_code == "failed_probe"
    assert failed.files == ()
    assert consumed is False


def test_only_successful_receipts_suppress_replay_and_failed_receipts_retry() -> None:
    release = _provider_release()
    first = produce_nppes_weekly(_catalog(), release, {"provider": _streams()["provider"]}, recorded_at=RECORDED_AT)
    replay_rows: list[NppesWeeklyObservation] = []
    replay = produce_nppes_weekly(
        _catalog(),
        release,
        {"provider": _streams()["provider"]},
        previous=first,
        recorded_at=RECORDED_AT,
        row_sink=replay_rows.append,
    )
    altered = _streams(provider=[b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE\n0123456789,provider-altered,2026-08-07\n"])

    assert first.state == "changed"
    assert replay.state == "replayed"
    assert replay_rows == []
    with pytest.raises(NppesWeeklyReplayConflictError):
        produce_nppes_weekly(_catalog(), release, {"provider": altered["provider"]}, previous=first)

    bad = produce_nppes_weekly(
        _catalog(),
        release,
        {"provider": [b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE,OPERATION\n0123,bad,2026-08-07,upsert\n"]},
        recorded_at=RECORDED_AT,
    )
    retry_rows: list[NppesWeeklyObservation] = []
    retry = produce_nppes_weekly(
        _catalog(),
        release,
        {"provider": _streams()["provider"]},
        previous=bad,
        recorded_at=RECORDED_AT,
        row_sink=retry_rows.append,
    )

    assert bad.state == "schema_drift"
    assert retry.state == "changed"
    assert [row.source_row_id for row in retry_rows] == ["provider-1"]


def test_blocked_stream_is_retryable_with_same_release() -> None:
    clock_values = iter((0.0, 2.0))
    blocked = produce_nppes_weekly(
        _catalog(max_seconds=1),
        _provider_release(),
        {"provider": _streams()["provider"]},
        recorded_at=RECORDED_AT,
        clock=lambda: next(clock_values),
    )
    retried = produce_nppes_weekly(
        _catalog(max_seconds=1),
        _provider_release(),
        {"provider": _streams()["provider"]},
        previous=blocked,
        recorded_at=RECORDED_AT,
        clock=lambda: 0.0,
    )

    assert blocked.state == "blocked"
    assert blocked.failure_code == "stream_interrupted"
    assert retried.state == "changed"


def test_catalog_binding_rejects_release_and_file_hosts() -> None:
    bad_release = _release(
        source_url="https://evil.example/nppes/weekly.csv",
        final_url="https://evil.example/nppes/weekly.csv",
        evidence_locator="https://evil.example/nppes/weekly.csv",
    )
    with pytest.raises(NppesWeeklyProducerError, match="approved by the NPPES catalog"):
        produce_nppes_weekly(_catalog(), bad_release, _streams())

    bad_descriptor = replace(
        _descriptor("provider"),
        source_url="https://evil.example/nppes/provider.csv",
        evidence_locator="https://evil.example/nppes/provider.csv",
    )
    release = replace(_provider_release(), files=(bad_descriptor,), release_sha256=None)
    with pytest.raises(NppesWeeklyProducerError, match="approved by the NPPES catalog"):
        produce_nppes_weekly(_catalog(), release, {"provider": _streams()["provider"]})


def test_malformed_rows_fail_closed_and_receipts_round_trip() -> None:
    malformed = produce_nppes_weekly(
        _catalog(),
        _provider_release(),
        {"provider": [b"NPI,SOURCE_ROW_ID,EFFECTIVE_DATE,OPERATION\n0123456789,row-1,2026-08-07,unexpected\n"]},
        recorded_at=RECORDED_AT,
    )
    assert malformed.state == "schema_drift"
    assert malformed.failure_code == "operation_schema_drift"
    assert malformed.current_projection_preserved is True

    changed = produce_nppes_weekly(
        _catalog(), _provider_release(), {"provider": _streams()["provider"]}, recorded_at=RECORDED_AT
    )
    restored = validate_nppes_weekly_receipt(changed.as_dict())
    assert isinstance(restored, NppesWeeklyReceipt)
    assert restored.receipt_sha256 == changed.receipt_sha256
    with pytest.raises(NppesWeeklyProducerError):
        validate_nppes_weekly_receipt({**changed.as_dict(), "unexpected": True})
    with pytest.raises(NppesWeeklyProducerError):
        validate_nppes_weekly_receipt({**changed.as_dict(), "applied_samples": [None]})
