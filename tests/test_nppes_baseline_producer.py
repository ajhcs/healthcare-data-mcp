"""Focused conformance tests for the NPPES V2 full-baseline producer."""

from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace
import json
from pathlib import Path
from typing import Iterable, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from shared.acquisition.nppes import (
    NPPES_SOURCE_ID,
    NPPES_SOURCE_URL,
    NppesBaselineProducer,
    NppesCatalogEntry,
    NppesContractError,
    NppesFileDescriptor,
    NppesFileKind,
    NppesProducerError,
    NppesReleaseDescriptor,
    NppesReplayConflictError,
    NppesStreamBudget,
    produce_nppes_baseline,
    validate_nppes_catalog,
    validate_nppes_receipt,
    validate_nppes_release,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/nppes/v2/fixtures"
RECORDED_AT = datetime(2026, 8, 29, tzinfo=timezone.utc)


def _catalog(**overrides: object) -> NppesCatalogEntry:
    values: dict[str, object] = {
        "max_bytes": 1_000_000,
        "max_chunks": 1_000,
        "max_seconds": 120,
        "max_chunk_bytes": 128,
    }
    values.update(overrides)
    return NppesCatalogEntry(**values)  # type: ignore[arg-type]


def _descriptors(*, all_present: bool = True) -> tuple[NppesFileDescriptor, ...]:
    descriptors: list[NppesFileDescriptor] = []
    for kind in ("provider", "location", "endpoint", "reference", "deactivation"):
        typed_kind = cast(NppesFileKind, kind)
        availability = "present" if all_present or kind == "provider" else "unavailable_public"
        descriptors.append(
            NppesFileDescriptor(
                file_kind=typed_kind,
                source_file_name=f"nppes_{kind}.csv",
                source_url=NPPES_SOURCE_URL,
                evidence_locator=NPPES_SOURCE_URL,
                availability=availability,
            )
        )
    return tuple(descriptors)


def _release(
    *,
    probe_state: str = "changed",
    all_present: bool = True,
    release_id: str = "release:nppes:2026-08",
) -> NppesReleaseDescriptor:
    return NppesReleaseDescriptor(
        release_id=release_id,
        release_label="NPPES August 2026",
        source_id=NPPES_SOURCE_ID,
        source_url=NPPES_SOURCE_URL,
        probe_state=probe_state,  # type: ignore[arg-type]
        final_url=NPPES_SOURCE_URL if probe_state != "failed_probe" else None,
        http_status=200 if probe_state == "changed" else (304 if probe_state == "not_modified" else 503),
        published_at="2026-08-01T00:00:00Z" if probe_state == "changed" else None,
        evidence_locator=NPPES_SOURCE_URL,
        files=_descriptors(all_present=all_present) if probe_state == "changed" else (),
        failure_reason="service_unavailable" if probe_state == "failed_probe" else None,
    )


def _streams() -> dict[NppesFileKind, Iterable[bytes]]:
    return {
        "provider": [b"NPI,SOURCE_ROW_ID\n0123456789,provider-1\n"],
        "location": [b"NPI,SOURCE_ROW_ID,LOCATION_ID\n0123456789,location-1,loc-7\n"],
        "endpoint": [b"NPI,SOURCE_ROW_ID,ENDPOINT_ID\n0123456789,endpoint-1,end-7\n"],
        "reference": [b"NPI,SOURCE_ROW_ID,OTHER_NAME\n0123456789,reference-1,Example\n"],
        "deactivation": [b"NPI,SOURCE_ROW_ID,DEACTIVATION_DATE\n0123456789,deactivation-1,2026-07-31\n"],
    }


def test_changed_baseline_preserves_source_probe_identifiers_and_deactivation_review() -> None:
    captured = []
    receipt = produce_nppes_baseline(
        _catalog(),
        _release(),
        _streams(),
        recorded_at=RECORDED_AT,
        row_sink=captured.append,
    )

    assert receipt.state == "changed"
    assert receipt.current_projection_preserved is True
    assert receipt.source_url == NPPES_SOURCE_URL
    assert receipt.probe_state == "changed"
    assert [row.npi for row in captured] == ["0123456789"] * 5
    assert [row.source_row_id for row in captured] == [
        "provider-1",
        "location-1",
        "endpoint-1",
        "reference-1",
        "deactivation-1",
    ]
    assert receipt.deactivation_count == 1
    opportunity = receipt.deactivation_opportunities[0]
    assert opportunity.npi == "0123456789"
    assert opportunity.deactivation_date == "2026-07-31"
    assert opportunity.review_state == "review_required"
    assert all("Example" not in json.dumps(file_receipt.as_dict()) for file_receipt in receipt.files)
    assert validate_nppes_receipt(receipt.as_dict()).receipt_sha256 == receipt.receipt_sha256


def test_optional_files_require_explicit_unavailable_state() -> None:
    release = _release(all_present=False)
    files: dict[NppesFileKind, Iterable[bytes]] = {cast(NppesFileKind, "provider"): _streams()["provider"]}
    receipt = NppesBaselineProducer(_catalog()).produce(release, files, recorded_at=RECORDED_AT)

    assert receipt.state == "changed"
    states = {item.file_kind: item.state for item in receipt.files}
    assert states == {
        "provider": "accepted",
        "location": "unavailable_public",
        "endpoint": "unavailable_public",
        "reference": "unavailable_public",
        "deactivation": "unavailable_public",
    }


def test_stream_without_a_declared_file_descriptor_is_rejected() -> None:
    release = replace(_release(), files=(_descriptors()[0],), release_sha256=None)
    with pytest.raises(NppesProducerError, match="lacks a release descriptor"):
        produce_nppes_baseline(
            _catalog(),
            release,
            {cast(NppesFileKind, "provider"): _streams()["provider"], cast(NppesFileKind, "endpoint"): []},
            recorded_at=RECORDED_AT,
        )


def test_large_stream_is_bounded_and_never_acknowledged() -> None:
    catalog = _catalog(max_bytes=50, max_chunk_bytes=32)
    release = _release(all_present=False)
    provider = [b"NPI,SOURCE_ROW_ID\n", b"0123456789,provider-1\n", b"0123456789,provider-2\n"]
    receipt = produce_nppes_baseline(
        catalog,
        release,
        {cast(NppesFileKind, "provider"): provider},
        recorded_at=RECORDED_AT,
    )

    assert receipt.state == "blocked"
    provider_receipt = next(item for item in receipt.files if item.file_kind == "provider")
    assert provider_receipt.state == "interrupted"
    assert provider_receipt.content_sha256 is not None
    assert receipt.current_projection_preserved is True

    with pytest.raises(NppesProducerError):
        NppesStreamBudget(max_bytes=32, max_chunk_bytes=64)


def test_no_op_does_not_consume_file_streams() -> None:
    consumed = False

    def forbidden() -> Iterable[bytes]:
        nonlocal consumed
        consumed = True
        raise AssertionError("304 no-op must not read source files")
        yield b"never"

    receipt = produce_nppes_baseline(
        _catalog(),
        _release(probe_state="not_modified"),
        {cast(NppesFileKind, "provider"): forbidden()},
        recorded_at=RECORDED_AT,
    )

    assert receipt.state == "no_op"
    assert receipt.probe_state == "not_modified"
    assert consumed is False
    assert receipt.files == ()


def test_exact_replay_is_deterministic_and_does_not_reemit_rows() -> None:
    first_rows = []
    first = produce_nppes_baseline(
        _catalog(), _release(), _streams(), recorded_at=RECORDED_AT, row_sink=first_rows.append
    )
    replay_rows = []
    replay = produce_nppes_baseline(
        _catalog(),
        _release(),
        _streams(),
        previous=first,
        recorded_at=RECORDED_AT,
        row_sink=replay_rows.append,
    )

    assert first.state == "changed"
    assert replay.state == "replayed"
    assert replay_rows == []
    assert replay.release_sha256 == first.release_sha256
    assert [item.content_sha256 for item in replay.files] == [item.content_sha256 for item in first.files]

    altered = _streams()
    altered["provider"] = [b"NPI,SOURCE_ROW_ID\n0123456789,provider-altered\n"]
    with pytest.raises(NppesReplayConflictError):
        produce_nppes_baseline(_catalog(), _release(), altered, previous=first, recorded_at=RECORDED_AT)


def test_schema_drift_preserves_projection_and_rejects_malformed_npi() -> None:
    streams = _streams()
    streams["provider"] = [b"NPI,SOURCE_ROW_ID\n123,provider-1\n"]
    receipt = produce_nppes_baseline(_catalog(), _release(), streams, recorded_at=RECORDED_AT)

    assert receipt.state == "schema_drift"
    assert receipt.failure_code == "invalid_npi"
    assert receipt.current_projection_preserved is True
    provider = next(item for item in receipt.files if item.file_kind == "provider")
    assert provider.state == "schema_drift"
    assert provider.error_code == "invalid_npi"


def test_catalog_release_and_receipt_fixtures_validate_with_closed_shapes() -> None:
    catalog_value = json.loads((FIXTURE_ROOT / "valid-catalog.json").read_text(encoding="utf-8"))
    release_value = json.loads((FIXTURE_ROOT / "valid-release.json").read_text(encoding="utf-8"))
    receipt_value = json.loads((FIXTURE_ROOT / "valid-changed.json").read_text(encoding="utf-8"))
    assert validate_nppes_catalog(catalog_value).source.source_id == NPPES_SOURCE_ID
    assert validate_nppes_release(release_value).release_id == "release:nppes:2026-08"
    assert validate_nppes_receipt(receipt_value).state == "changed"

    with pytest.raises(NppesContractError):
        validate_nppes_receipt({**receipt_value, "unexpected": True})


def test_all_checked_in_fixtures_validate_against_their_draft_2020_schemas() -> None:
    catalog_schema = json.loads((FIXTURE_ROOT.parent / "nppes-catalog.schema.json").read_text(encoding="utf-8"))
    release_schema = json.loads((FIXTURE_ROOT.parent / "nppes-release.schema.json").read_text(encoding="utf-8"))
    receipt_schema = json.loads((FIXTURE_ROOT.parent / "nppes-baseline.schema.json").read_text(encoding="utf-8"))
    for schema in (catalog_schema, release_schema, receipt_schema):
        Draft202012Validator.check_schema(schema)

    pairs = (
        (catalog_schema, ("valid-catalog.json",)),
        (release_schema, ("valid-release.json",)),
        (
            receipt_schema,
            ("valid-changed.json", "valid-no-op.json", "valid-replayed.json", "large-stream-blocked.json"),
        ),
    )
    for schema, fixture_names in pairs:
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        for fixture_name in fixture_names:
            fixture = json.loads((FIXTURE_ROOT / fixture_name).read_text(encoding="utf-8"))
            assert list(validator.iter_errors(fixture)) == [], fixture_name
