"""Focused conformance and delivery tests for the P1-07 AHRQ producer."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Literal, Mapping, cast

import pytest

from shared.acquisition.ahrq_detector import AhrqChangeDetector, DetectionReceipt
from shared.acquisition.ahrq_observation_envelope import (
    AHRQ_SOURCE_ID,
    AhrqAcknowledgement,
    AhrqAcknowledgementError,
    AhrqArtifactLocator,
    AhrqCheckpointConflictError,
    AhrqCheckpointPrecondition,
    AhrqObservationProducer,
    AhrqParsedRows,
    AhrqProducerError,
    AhrqReplayConflictError,
    AhrqRowParseError,
    FACILITY_REQUIRED_COLUMNS,
    FileAhrqAcknowledgementStore,
    FileAhrqCheckpointStore,
    InMemoryAhrqAcknowledgementStore,
    InMemoryAhrqCheckpointStore,
    SYSTEM_REQUIRED_COLUMNS,
    build_ahrq_observation_envelope,
    parse_ahrq_system_rows,
    parse_ahrq_source_rows,
    write_ahrq_observation_envelope,
)
from shared.contracts.healthcare_data_platform import validate_observation_envelope
from shared.storage.raw_custody import RawArtifactStore


ROOT = Path(__file__).resolve().parents[1]
RELEASE_FIXTURE = ROOT / "contracts/healthcare-data-platform/ahrq/v1/fixtures/official-release-changed.json"

SYSTEM_HEADERS = (
    "health_sys_id",
    "health_sys_name",
    "health_sys_city",
    "health_sys_state",
    "hosp_cnt",
    "acutehosp_cnt",
)
FACILITY_HEADERS = (
    "compendium_hospital_id",
    "ccn",
    "hospital_name",
    "hospital_street",
    "hospital_city",
    "hospital_state",
    "hospital_zip",
    "acutehosp_flag",
    "health_sys_id",
    "health_sys_name",
    "health_sys_city",
    "health_sys_state",
    "corp_parent_id",
    "corp_parent_name",
    "corp_parent_type",
    "hos_beds",
    "hos_ownership",
)


def _release() -> DetectionReceipt:
    detector = AhrqChangeDetector()
    return detector.detect(detector.load_fixture(RELEASE_FIXTURE))


def _artifact(role: Literal["system", "facility"], content: bytes, release: DetectionReceipt) -> AhrqArtifactLocator:
    content_sha256 = "sha256:" + hashlib.sha256(content).hexdigest()
    identity = hashlib.sha256(f"{AHRQ_SOURCE_ID}|{release.release_id}|{content_sha256}".encode("utf-8")).hexdigest()[
        :32
    ]
    return AhrqArtifactLocator(
        role=role,
        artifact_id=f"artifact:raw:{identity}",
        custody_locator=f"object://raw/{role}/{identity}",
        content_sha256=content_sha256,
        byte_length=len(content),
        release_id=release.release_id,
    )


def _csv_bytes(headers: tuple[str, ...], values: tuple[str, ...]) -> bytes:
    return (",".join(headers) + "\n" + ",".join(values) + "\n").encode("cp1252")


def _source_files(tmp_path: Path, release: DetectionReceipt) -> tuple[Path, Path, AhrqParsedRows]:
    system_content = _csv_bytes(
        SYSTEM_HEADERS,
        ("SYS-1", "Café Health", "Harrisburg", "PA", "2", "2"),
    )
    facility_content = _csv_bytes(
        FACILITY_HEADERS,
        (
            "H-1",
            "123456",
            "Café Hospital",
            "1 Main St",
            "Harrisburg",
            "PA",
            "17101",
            "Y",
            "SYS-1",
            "Café Health",
            "Harrisburg",
            "PA",
            "P-1",
            "Parent Health",
            "system",
            "100",
            "nonprofit",
        ),
    )
    system_path = tmp_path / "systems.csv"
    facility_path = tmp_path / "facilities.csv"
    system_path.write_bytes(system_content)
    facility_path.write_bytes(facility_content)
    parsed = parse_ahrq_source_rows(
        system_path,
        facility_path,
        system_artifact=_artifact("system", system_content, release),
        facility_artifact=_artifact("facility", facility_content, release),
    )
    return system_path, facility_path, parsed


def _envelope(tmp_path: Path, release: DetectionReceipt | None = None) -> dict[str, object]:
    current_release = release or _release()
    _, _, parsed = _source_files(tmp_path, current_release)
    return build_ahrq_observation_envelope(
        parsed,
        current_release,
        source_url="https://example.gov/ahrq/release.json",
        recorded_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
    )


def test_parser_preserves_source_native_strings_and_custody_claims(tmp_path: Path) -> None:
    release = _release()
    system_path, facility_path, parsed = _source_files(tmp_path, release)

    assert system_path.is_file() and facility_path.is_file()
    assert parsed.system_rows[0].fields["health_sys_name"] == "Café Health"
    assert parsed.facility_rows[0].fields["hos_beds"] == "100"
    assert parsed.system_rows[0].artifact.verified is True
    assert parsed.facility_rows[0].artifact.content_sha256.startswith("sha256:")
    assert set(parsed.system_rows[0].fields) == SYSTEM_REQUIRED_COLUMNS
    assert set(parsed.facility_rows[0].fields) == FACILITY_REQUIRED_COLUMNS


def test_parser_accepts_mixed_case_headers_and_rejects_missing_or_duplicate_headers(tmp_path: Path) -> None:
    release = _release()
    system_content = _csv_bytes(
        ("Health_Sys_ID", "Health_Sys_Name", "Health_Sys_City", "Health_Sys_State", "Hosp_Cnt", "AcuteHosp_Cnt"),
        ("SYS-2", "North Health", "Erie", "PA", "1", "1"),
    )
    system_path = tmp_path / "system.csv"
    system_path.write_bytes(system_content)
    rows = parse_ahrq_system_rows(system_path, artifact=_artifact("system", system_content, release))
    assert rows[0].fields["health_sys_id"] == "SYS-2"

    missing_path = tmp_path / "missing.csv"
    missing_path.write_text(",".join(SYSTEM_HEADERS[:-1]) + "\nSYS-3,Name,City,PA,1\n", encoding="cp1252")
    with pytest.raises(AhrqRowParseError, match="missing required columns"):
        parse_ahrq_system_rows(missing_path)

    duplicate_path = tmp_path / "duplicate.csv"
    duplicate_path.write_text(
        "health_sys_id,HEALTH_SYS_ID," + ",".join(SYSTEM_HEADERS[1:]) + "\n" + "SYS-4,SYS-4,Name,City,PA,1,1\n",
        encoding="cp1252",
    )
    with pytest.raises(AhrqRowParseError, match="duplicate header"):
        parse_ahrq_system_rows(duplicate_path)


def test_parser_rejects_orphan_link_and_raw_hash_mismatch(tmp_path: Path) -> None:
    release = _release()
    system_content = _csv_bytes(SYSTEM_HEADERS, ("SYS-1", "Health", "City", "PA", "1", "1"))
    facility_content = _csv_bytes(
        FACILITY_HEADERS,
        (
            "H-1",
            "123456",
            "Hospital",
            "1 Main",
            "City",
            "PA",
            "17101",
            "Y",
            "SYS-NOT-PRESENT",
            "Unknown",
            "City",
            "PA",
            "P-1",
            "Parent",
            "system",
            "10",
            "nonprofit",
        ),
    )
    system_path = tmp_path / "system.csv"
    facility_path = tmp_path / "facility.csv"
    system_path.write_bytes(system_content)
    facility_path.write_bytes(facility_content)

    with pytest.raises(AhrqRowParseError, match="unknown health_sys_id"):
        parse_ahrq_source_rows(
            system_path,
            facility_path,
            system_artifact=_artifact("system", system_content, release),
            facility_artifact=_artifact("facility", facility_content, release),
        )

    valid_system = _csv_bytes(SYSTEM_HEADERS, ("SYS-1", "Health", "City", "PA", "1", "1"))
    valid_facility = _csv_bytes(
        FACILITY_HEADERS,
        (
            "H-1",
            "123456",
            "Hospital",
            "1 Main",
            "City",
            "PA",
            "17101",
            "Y",
            "SYS-1",
            "Health",
            "City",
            "PA",
            "P-1",
            "Parent",
            "system",
            "10",
            "nonprofit",
        ),
    )
    system_path.write_bytes(valid_system)
    facility_path.write_bytes(valid_facility)
    bad_claim = _artifact("system", valid_system + b"tampered", release)
    with pytest.raises(AhrqProducerError, match="raw artifact hash or length"):
        parse_ahrq_source_rows(
            system_path,
            facility_path,
            system_artifact=bad_claim,
            facility_artifact=_artifact("facility", valid_facility, release),
        )


def test_envelope_is_pinned_source_scoped_and_deterministic(tmp_path: Path) -> None:
    release = _release()
    first = _envelope(tmp_path, release)
    second = _envelope(tmp_path, release)

    assert json.dumps(first, sort_keys=True, separators=(",", ":")) == json.dumps(
        second, sort_keys=True, separators=(",", ":")
    )
    assert first["schema_version"] == "hdp.observation-envelope.v1"
    source_release = cast(Mapping[str, object], first["source_release"])
    assert source_release["source_id"] == AHRQ_SOURCE_ID
    observations = cast(list[object], first["observations"])
    value = cast(Mapping[str, object], cast(Mapping[str, object], observations[0])["value"])
    assert cast(str, value["source_custody_locator"]).startswith("object://")
    assert cast(str, value["source_content_sha256"]).startswith("sha256:")
    assert str(tmp_path) not in json.dumps(first, sort_keys=True)
    assert validate_observation_envelope(first) == first


def test_normalized_rows_can_be_committed_to_p1_04_custody(tmp_path: Path) -> None:
    release = _release()
    _, _, parsed = _source_files(tmp_path, release)
    store = RawArtifactStore(tmp_path / "raw")
    envelope = build_ahrq_observation_envelope(
        parsed,
        release,
        normalized_store=store,
        source_url="https://example.gov/ahrq/release.json",
    )
    artifact = cast(Mapping[str, object], envelope["artifact"])
    artifact_id = cast(str, artifact["artifact_id"])
    content = store.read_bytes(artifact_id)
    assert content
    assert artifact["content_sha256"] == "sha256:" + hashlib.sha256(content).hexdigest()
    custody = cast(Mapping[str, object], artifact["custody"])
    assert cast(str, custody["locator"]).startswith("object://objects/")


def test_acknowledgement_replay_is_idempotent_and_conflicting_bytes_fail(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    store = InMemoryAhrqAcknowledgementStore()
    first = store.acknowledge(envelope)
    duplicate = store.acknowledge(envelope)

    assert first.duplicate is False
    assert duplicate.duplicate is True
    assert duplicate.acknowledgement_id == first.acknowledgement_id
    changed = deepcopy(envelope)
    changed_receipt = cast(dict[str, object], changed["receipt"])
    changed_receipt["producer"] = "healthcare-data-mcp:ahrq-producer-replay"
    with pytest.raises(AhrqReplayConflictError, match="different envelope bytes"):
        store.acknowledge(changed)


def test_checkpoint_advances_only_after_durable_ack_and_stale_cas_is_replayable(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    acknowledgements = InMemoryAhrqAcknowledgementStore()
    checkpoints = InMemoryAhrqCheckpointStore()
    producer = AhrqObservationProducer(acknowledger=acknowledgements, checkpoint_store=checkpoints)
    precondition = AhrqCheckpointPrecondition(
        source_id=AHRQ_SOURCE_ID,
        expected_generation=0,
        expected_cursor=None,
        next_cursor="release:ahrq:2026-08-22",
    )
    accepted = producer.acknowledge_and_checkpoint(envelope, checkpoint=precondition)
    duplicate = producer.acknowledge_and_checkpoint(envelope, checkpoint=precondition)

    assert accepted.state == "accepted"
    assert accepted.checkpoint_generation == 1
    assert duplicate.state == "duplicate"
    assert duplicate.checkpoint_generation == 1
    current = checkpoints.current()
    assert current is not None
    assert current.generation == 1

    with pytest.raises(AhrqCheckpointConflictError, match="stale checkpoint"):
        producer.acknowledge_and_checkpoint(
            envelope,
            checkpoint=AhrqCheckpointPrecondition(
                source_id=AHRQ_SOURCE_ID,
                expected_generation=0,
                expected_cursor=None,
                next_cursor="release:ahrq:2026-08-23",
            ),
        )
    current = checkpoints.current()
    assert current is not None
    assert current.generation == 1


def test_rejected_acknowledgement_never_writes_checkpoint(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    checkpoints = InMemoryAhrqCheckpointStore()

    def reject(_: Mapping[str, object]) -> AhrqAcknowledgement:
        raise RuntimeError("admission unavailable")

    producer = AhrqObservationProducer(acknowledger=reject, checkpoint_store=checkpoints)
    with pytest.raises(AhrqAcknowledgementError, match="durable acknowledgement failed"):
        producer.acknowledge_and_checkpoint(
            envelope,
            checkpoint=AhrqCheckpointPrecondition(AHRQ_SOURCE_ID, 0, None, "release:ahrq:2026-08-22"),
        )
    assert checkpoints.current() is None


def test_checkpoint_source_mismatch_is_rejected_after_envelope_validation(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    acknowledgements = InMemoryAhrqAcknowledgementStore()
    checkpoints = InMemoryAhrqCheckpointStore()
    producer = AhrqObservationProducer(acknowledger=acknowledgements, checkpoint_store=checkpoints)
    with pytest.raises(AhrqCheckpointConflictError, match="source_id does not match"):
        producer.acknowledge_and_checkpoint(
            envelope,
            checkpoint=AhrqCheckpointPrecondition("source:other", 0, None, "release:ahrq:2026-08-22"),
        )
    assert checkpoints.current() is None


def test_file_acknowledgement_and_checkpoint_stores_replay_without_duplicate_generation(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    acknowledgement_store = FileAhrqAcknowledgementStore(tmp_path / "ack.json")
    checkpoint_store = FileAhrqCheckpointStore(tmp_path / "checkpoint.json")
    producer = AhrqObservationProducer(acknowledger=acknowledgement_store, checkpoint_store=checkpoint_store)
    precondition = AhrqCheckpointPrecondition(AHRQ_SOURCE_ID, 0, None, "release:ahrq:2026-08-22")

    first = producer.acknowledge_and_checkpoint(envelope, checkpoint=precondition)
    second = producer.acknowledge_and_checkpoint(envelope, checkpoint=precondition)
    checkpoint = checkpoint_store.current()

    assert first.state == "accepted"
    assert second.state == "duplicate"
    assert checkpoint is not None
    assert checkpoint.generation == 1
    persisted = json.loads((tmp_path / "ack.json").read_text(encoding="utf-8"))
    assert len(persisted) == 1


def test_write_envelope_revalidates_unknown_fields(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    destination = write_ahrq_observation_envelope(tmp_path / "envelope.json", envelope)
    assert json.loads(destination.read_text(encoding="utf-8")) == envelope
    invalid = deepcopy(envelope)
    invalid["unexpected"] = True
    with pytest.raises(AhrqProducerError, match="failed contract validation"):
        write_ahrq_observation_envelope(tmp_path / "invalid.json", invalid)
