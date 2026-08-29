"""AHRQ source-row producer for the Healthcare Data Platform envelope.

The producer is intentionally transport-neutral.  It parses caller-provided
system and hospital-linkage bytes, verifies their P1-04 custody claims, builds
one deterministic ``hdp.observation-envelope.v1`` payload, and hands that
payload to a caller-owned durable acknowledgement seam.  Checkpoint publication
is a separate compare-and-swap operation performed only after that
acknowledgement has been verified.

No network acquisition, database write, queue publication, projection, or
production authority is performed here.
"""

from __future__ import annotations

import csv
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
from hashlib import sha256
import io
import json
from pathlib import Path
import re
import threading
from typing import Callable, Iterator, Literal, Mapping, Protocol, Sequence, cast

from shared.acquisition.ahrq_detector import DetectionReceipt
from shared.storage.raw_custody import MAX_ARTIFACT_BYTES, RawArtifactMetadata, RawArtifactStore
from shared.utils.cache import write_atomic_json


ArtifactRole = Literal["system", "facility"]
ProducerState = Literal["accepted", "duplicate"]

AHRQ_SOURCE_ID = "source:ahrq:lighthouse"
AHRQ_DEFAULT_SOURCE_URL = "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html"
AHRQ_SOURCE_PERIOD = "2023"
AHRQ_PACKET_ID = "p0-09-observation-provenance-delta-v1"
AHRQ_TRACKING_BEAD = "healthcare-toolkit-rrna.9"
AHRQ_FROZEN_DISPATCH_BASE = "11d16f8303226619161f9bef03cb312f693b2d49"
AHRQ_ENVELOPE_SCHEMA_VERSION = "hdp.observation-envelope.v1"
AHRQ_ENVELOPE_RECORD_TYPE = "observation_envelope"
AHRQ_RECEIPT_SCHEMA = "hdp.ahrq-producer-receipt.v1"
AHRQ_PRODUCER_NAME = "healthcare-data-mcp:ahrq-observation-producer"
AHRQ_OFFLINE_ARTIFACT_PREFIX = "artifact:ahrq:offline:"

_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:[a-z0-9][a-z0-9._:-]*$")
_ARTIFACT_ID = re.compile(r"^artifact:[a-z0-9][a-z0-9._:-]*$")
_RECEIPT_ID = re.compile(r"^receipt:[a-z0-9][a-z0-9._:-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_LOCATOR = re.compile(r"^(?:https://|object://|parquet://|docs/|contracts/)[A-Za-z0-9._:/-]+$")
_SOURCE_ROW_ID = re.compile(r"^[a-z0-9][a-z0-9._:-]*$")

SYSTEM_REQUIRED_COLUMNS = frozenset(
    {
        "health_sys_id",
        "health_sys_name",
        "health_sys_city",
        "health_sys_state",
        "hosp_cnt",
        "acutehosp_cnt",
    }
)
FACILITY_REQUIRED_COLUMNS = frozenset(
    {
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
    }
)


class AhrqProducerError(ValueError):
    """Raised when source rows, custody, envelope, or delivery is unsafe."""


class AhrqRowParseError(AhrqProducerError):
    """Raised when source-native AHRQ CSV rows fail closed validation."""


class AhrqReplayConflictError(AhrqProducerError):
    """Raised when an idempotency key is reused for different envelope bytes."""


class AhrqAcknowledgementError(AhrqProducerError):
    """Raised when a durable admission acknowledgement is absent or invalid."""


class AhrqCheckpointConflictError(AhrqProducerError):
    """Raised when a producer checkpoint compare-and-swap is stale."""


@dataclass(frozen=True, slots=True)
class AhrqArtifactLocator:
    """Public-safe identity and custody locator for one raw source artifact."""

    role: ArtifactRole
    artifact_id: str
    custody_locator: str
    content_sha256: str
    byte_length: int
    source_id: str = AHRQ_SOURCE_ID
    release_id: str | None = None
    verified: bool = True

    def __post_init__(self) -> None:
        if self.role not in {"system", "facility"}:
            raise AhrqProducerError("artifact role must be system or facility")
        _require_id(self.artifact_id, "artifact_id", _ARTIFACT_ID)
        _require_hash(self.content_sha256, "content_sha256")
        _require_locator(self.custody_locator, "custody_locator")
        _require_id(self.source_id, "source_id", _SOURCE_ID)
        if self.release_id is not None:
            _require_id(self.release_id, "release_id", _RELEASE_ID)
        if isinstance(self.byte_length, bool) or not isinstance(self.byte_length, int):
            raise AhrqProducerError("artifact byte_length must be an integer")
        if self.byte_length < 1 or self.byte_length > MAX_ARTIFACT_BYTES:
            raise AhrqProducerError(f"artifact byte_length must be between 1 and {MAX_ARTIFACT_BYTES}")
        if not isinstance(self.verified, bool):
            raise AhrqProducerError("artifact verified flag must be boolean")

    @classmethod
    def from_raw_metadata(
        cls,
        role: ArtifactRole,
        metadata: RawArtifactMetadata | Mapping[str, object],
        *,
        custody_locator: str | None = None,
    ) -> "AhrqArtifactLocator":
        """Build a locator from P1-04 metadata or a strict metadata mapping."""

        item = metadata if isinstance(metadata, RawArtifactMetadata) else RawArtifactMetadata.from_mapping(metadata)
        digest = item.content_sha256.removeprefix("sha256:")
        locator = custody_locator or f"object://objects/sha256/{digest[:2]}/{digest}"
        return cls(
            role=role,
            artifact_id=item.artifact_id,
            custody_locator=locator,
            content_sha256=item.content_sha256,
            byte_length=item.byte_length,
            source_id=item.source_id,
            release_id=item.release_id,
            verified=True,
        )

    @classmethod
    def from_mapping(cls, role: ArtifactRole, value: Mapping[str, object]) -> "AhrqArtifactLocator":
        """Parse a small public-safe locator mapping without accepting secrets."""

        declared_role = value.get("role")
        if declared_role is not None and declared_role != role:
            raise AhrqProducerError("artifact role does not match source file")
        artifact_id = value.get("artifact_id")
        custody_locator = value.get("custody_locator", value.get("locator"))
        content_sha256 = value.get(
            "content_sha256",
            value.get("checksum_sha256", value.get("payload_sha256")),
        )
        byte_length = value.get("byte_length", value.get("content_length"))
        source_id = value.get("source_id", AHRQ_SOURCE_ID)
        release_id = value.get("release_id")
        verified = value.get("verified", False)
        if not isinstance(artifact_id, str):
            raise AhrqProducerError("artifact_id is missing from custody locator")
        if not isinstance(custody_locator, str):
            object_key = value.get("object_key")
            if isinstance(object_key, str):
                custody_locator = f"object://{object_key}"
        if not isinstance(custody_locator, str):
            raise AhrqProducerError("custody locator is missing")
        if not isinstance(content_sha256, str):
            raise AhrqProducerError("content_sha256 is missing from custody locator")
        if isinstance(byte_length, bool) or not isinstance(byte_length, int):
            raise AhrqProducerError("byte_length is missing from custody locator")
        if not isinstance(source_id, str):
            raise AhrqProducerError("source_id is invalid in custody locator")
        if release_id is not None and not isinstance(release_id, str):
            raise AhrqProducerError("release_id is invalid in custody locator")
        if not isinstance(verified, bool):
            raise AhrqProducerError("verified must be boolean in custody locator")
        return cls(
            role=role,
            artifact_id=artifact_id,
            custody_locator=custody_locator,
            content_sha256=content_sha256,
            byte_length=byte_length,
            source_id=source_id,
            release_id=release_id,
            verified=verified,
        )

    def as_dict(self) -> dict[str, object]:
        """Return JSON-safe custody metadata."""

        return {
            "role": self.role,
            "artifact_id": self.artifact_id,
            "custody_locator": self.custody_locator,
            "content_sha256": self.content_sha256,
            "byte_length": self.byte_length,
            "source_id": self.source_id,
            "release_id": self.release_id,
            "verified": self.verified,
        }


@dataclass(frozen=True, slots=True)
class AhrqSourceRow:
    """One source-native row with deterministic row and artifact lineage."""

    role: ArtifactRole
    row_number: int
    source_row_id: str
    fields: Mapping[str, str]
    row_sha256: str
    artifact: AhrqArtifactLocator

    def __post_init__(self) -> None:
        if isinstance(self.row_number, bool) or not isinstance(self.row_number, int) or self.row_number < 2:
            raise AhrqRowParseError("source row numbers must start at 2")
        if not self.source_row_id or _SOURCE_ROW_ID.fullmatch(self.source_row_id) is None:
            raise AhrqRowParseError("source row id is malformed")
        _require_hash(self.row_sha256, "row_sha256")
        if self.artifact.role != self.role:
            raise AhrqRowParseError("source row artifact role does not match row")

    def as_dict(self) -> dict[str, object]:
        """Return the source row and custody fields carried by an observation."""

        return {
            "source_row_id": self.source_row_id,
            "row_number": self.row_number,
            "source_role": self.role,
            "source_artifact_id": self.artifact.artifact_id,
            "source_custody_locator": self.artifact.custody_locator,
            "source_content_sha256": self.artifact.content_sha256,
            "row_sha256": self.row_sha256,
            "fields": dict(self.fields),
        }


@dataclass(frozen=True, slots=True)
class AhrqParsedRows:
    """Parsed source-native system and facility rows in source order."""

    system_rows: tuple[AhrqSourceRow, ...]
    facility_rows: tuple[AhrqSourceRow, ...]

    def __post_init__(self) -> None:
        if not self.system_rows:
            raise AhrqRowParseError("AHRQ system file has no data rows")
        if not self.facility_rows:
            raise AhrqRowParseError("AHRQ facility file has no data rows")

    @property
    def rows(self) -> tuple[AhrqSourceRow, ...]:
        """Return deterministic system-then-facility source row order."""

        return self.system_rows + self.facility_rows


@dataclass(frozen=True, slots=True)
class AhrqAcknowledgement:
    """Durable admission acknowledgement returned by the Toolkit seam."""

    acknowledgement_id: str
    envelope_id: str
    idempotency_key: str
    envelope_sha256: str
    durable: bool = True
    duplicate: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.acknowledgement_id, str) or not self.acknowledgement_id.strip():
            raise AhrqAcknowledgementError("acknowledgement_id must be non-empty")
        _require_id(self.envelope_id, "envelope_id", re.compile(r"^hdp:observation-envelope:[a-z0-9][a-z0-9._:-]*$"))
        _require_id(self.idempotency_key, "idempotency_key", re.compile(r"^idempotency:[a-z0-9][a-z0-9._:-]*$"))
        _require_hash(self.envelope_sha256, "envelope_sha256")
        if not isinstance(self.durable, bool) or not self.durable:
            raise AhrqAcknowledgementError("acknowledgement is not durable")
        if not isinstance(self.duplicate, bool):
            raise AhrqAcknowledgementError("acknowledgement duplicate flag must be boolean")

    def as_dict(self) -> dict[str, object]:
        """Return JSON-safe acknowledgement evidence."""

        return {
            "acknowledgement_id": self.acknowledgement_id,
            "envelope_id": self.envelope_id,
            "idempotency_key": self.idempotency_key,
            "envelope_sha256": self.envelope_sha256,
            "durable": self.durable,
            "duplicate": self.duplicate,
        }


@dataclass(frozen=True, slots=True)
class AhrqProducerReceipt:
    """Public-safe result of acknowledgement and optional checkpoint CAS."""

    state: ProducerState
    envelope_id: str
    idempotency_key: str
    envelope_sha256: str
    acknowledgement_id: str
    acknowledged: bool
    checkpoint_generation: int | None
    artifact_id: str
    artifact_sha256: str
    row_count: int

    def as_dict(self) -> dict[str, object]:
        """Return a receipt suitable for a secret-free handoff ledger."""

        return {
            "schema_version": AHRQ_RECEIPT_SCHEMA,
            "state": self.state,
            "envelope_id": self.envelope_id,
            "idempotency_key": self.idempotency_key,
            "envelope_sha256": self.envelope_sha256,
            "acknowledgement_id": self.acknowledgement_id,
            "acknowledged": self.acknowledged,
            "checkpoint_generation": self.checkpoint_generation,
            "artifact_id": self.artifact_id,
            "artifact_sha256": self.artifact_sha256,
            "row_count": self.row_count,
        }


@dataclass(frozen=True, slots=True)
class AhrqCheckpoint:
    """Durable source checkpoint state advanced by a successful CAS."""

    source_id: str
    generation: int
    cursor: str
    envelope_id: str
    acknowledgement_id: str
    envelope_sha256: str
    artifact_sha256: str
    updated_at: str

    def __post_init__(self) -> None:
        _require_id(self.source_id, "source_id", _SOURCE_ID)
        if isinstance(self.generation, bool) or not isinstance(self.generation, int) or self.generation < 1:
            raise AhrqCheckpointConflictError("checkpoint generation must be a positive integer")
        if not isinstance(self.cursor, str) or not self.cursor.strip():
            raise AhrqCheckpointConflictError("checkpoint cursor must be non-empty")
        _require_id(
            self.envelope_id,
            "envelope_id",
            re.compile(r"^hdp:observation-envelope:[a-z0-9][a-z0-9._:-]*$"),
        )
        if not isinstance(self.acknowledgement_id, str) or not self.acknowledgement_id.strip():
            raise AhrqCheckpointConflictError("checkpoint acknowledgement_id must be non-empty")
        _require_hash(self.envelope_sha256, "envelope_sha256")
        _require_hash(self.artifact_sha256, "artifact_sha256")
        if not isinstance(self.updated_at, str) or not self.updated_at.strip():
            raise AhrqCheckpointConflictError("checkpoint updated_at must be non-empty")

    def as_dict(self) -> dict[str, object]:
        """Return strict JSON-safe checkpoint state."""

        return {
            "schema_version": "hdp.ahrq-producer-checkpoint.v1",
            "record_type": "ahrq_producer_checkpoint",
            "source_id": self.source_id,
            "generation": self.generation,
            "cursor": self.cursor,
            "envelope_id": self.envelope_id,
            "acknowledgement_id": self.acknowledgement_id,
            "envelope_sha256": self.envelope_sha256,
            "artifact_sha256": self.artifact_sha256,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True, slots=True)
class AhrqCheckpointPrecondition:
    """Expected pointer state and the cursor to publish after acknowledgement."""

    source_id: str
    expected_generation: int
    expected_cursor: str | None
    next_cursor: str

    def __post_init__(self) -> None:
        _require_id(self.source_id, "source_id", _SOURCE_ID)
        if (
            isinstance(self.expected_generation, bool)
            or not isinstance(self.expected_generation, int)
            or self.expected_generation < 0
        ):
            raise AhrqCheckpointConflictError("expected_generation must be non-negative")
        if self.expected_cursor is not None:
            if not isinstance(self.expected_cursor, str) or not self.expected_cursor.strip():
                raise AhrqCheckpointConflictError("expected_cursor must be non-empty when supplied")
        if not isinstance(self.next_cursor, str) or not self.next_cursor.strip():
            raise AhrqCheckpointConflictError("next_cursor must be non-empty")


class AhrqAcknowledgementSink(Protocol):
    """Protocol implemented by a durable Toolkit admission client."""

    def acknowledge(self, envelope: Mapping[str, object]) -> AhrqAcknowledgement:
        """Persist or replay an envelope acknowledgement."""

        ...


class AhrqCheckpointSink(Protocol):
    """Protocol implemented by a producer checkpoint store."""

    def compare_and_swap(
        self,
        precondition: AhrqCheckpointPrecondition,
        *,
        envelope: Mapping[str, object],
        acknowledgement: AhrqAcknowledgement,
    ) -> AhrqCheckpoint:
        """Advance a checkpoint only when its expected state still matches."""

        ...


def parse_ahrq_system_rows(
    path: str | Path,
    *,
    artifact: AhrqArtifactLocator | Mapping[str, object] | None = None,
    encoding: str = "cp1252",
) -> tuple[AhrqSourceRow, ...]:
    """Parse and validate source-native AHRQ system rows without coercion."""

    source_path = Path(path)
    locator = _coerce_artifact("system", artifact)
    raw = _verify_file_claim(source_path, locator, "system")
    return _parse_rows(
        source_path,
        role="system",
        required_columns=SYSTEM_REQUIRED_COLUMNS,
        artifact=locator,
        encoding=encoding,
        raw=raw,
    )


def parse_ahrq_facility_rows(
    path: str | Path,
    *,
    artifact: AhrqArtifactLocator | Mapping[str, object] | None = None,
    encoding: str = "cp1252",
) -> tuple[AhrqSourceRow, ...]:
    """Parse and validate source-native AHRQ hospital-linkage rows."""

    source_path = Path(path)
    locator = _coerce_artifact("facility", artifact)
    raw = _verify_file_claim(source_path, locator, "facility")
    return _parse_rows(
        source_path,
        role="facility",
        required_columns=FACILITY_REQUIRED_COLUMNS,
        artifact=locator,
        encoding=encoding,
        raw=raw,
    )


def parse_ahrq_source_rows(
    system_path: str | Path,
    facility_path: str | Path,
    *,
    system_artifact: AhrqArtifactLocator | Mapping[str, object] | None = None,
    facility_artifact: AhrqArtifactLocator | Mapping[str, object] | None = None,
    encoding: str = "cp1252",
) -> AhrqParsedRows:
    """Parse both AHRQ source files and enforce cross-file linkage integrity."""

    system_locator = _coerce_artifact("system", system_artifact)
    facility_locator = _coerce_artifact("facility", facility_artifact)
    system_path_value = Path(system_path)
    facility_path_value = Path(facility_path)
    system_raw = _verify_file_claim(system_path_value, system_locator, "system")
    facility_raw = _verify_file_claim(facility_path_value, facility_locator, "facility")
    systems = _parse_rows(
        system_path_value,
        role="system",
        required_columns=SYSTEM_REQUIRED_COLUMNS,
        artifact=system_locator,
        encoding=encoding,
        raw=system_raw,
    )
    facilities = _parse_rows(
        facility_path_value,
        role="facility",
        required_columns=FACILITY_REQUIRED_COLUMNS,
        artifact=facility_locator,
        encoding=encoding,
        raw=facility_raw,
    )
    system_ids = {row.fields["health_sys_id"] for row in systems}
    linked_ids = {row.fields["health_sys_id"] for row in facilities}
    orphaned = sorted(linked_ids - system_ids)
    if orphaned:
        raise AhrqRowParseError("facility rows reference unknown health_sys_id: " + ", ".join(orphaned))
    return AhrqParsedRows(system_rows=systems, facility_rows=facilities)


def build_ahrq_observation_envelope(
    parsed: AhrqParsedRows,
    release: DetectionReceipt,
    *,
    normalized_artifact: AhrqArtifactLocator | Mapping[str, object] | None = None,
    normalized_store: RawArtifactStore | None = None,
    source_url: str = AHRQ_DEFAULT_SOURCE_URL,
    recorded_at: datetime | None = None,
    source_period: str = AHRQ_SOURCE_PERIOD,
    prior_lineage_id: str | None = None,
) -> dict[str, object]:
    """Build and validate one deterministic source-scoped AHRQ envelope.

    ``release`` must be a successful detector receipt.  A normalized artifact
    locator may be supplied when the caller already persisted the normalized
    row bytes; otherwise an optional :class:`RawArtifactStore` receives those
    bytes before the envelope is emitted.  Supplying neither is useful for
    offline contract tests only and marks the derived locator as unverified.
    """

    _validate_release(release)
    _require_locator(source_url, "source_url")
    if not source_period or re.fullmatch(r"[0-9]{4}", source_period) is None:
        raise AhrqProducerError("source_period must be a four-digit year")
    observed_at = (
        _canonical_timestamp(recorded_at, "recorded_at")
        if recorded_at is not None
        else _canonical_timestamp_text(cast(str, release.published_at), "release.published_at")
    )
    raw_rows = parsed.rows
    normalized_bytes = _normalized_rows_bytes(parsed, release, source_period)
    normalized_hash = _sha256(normalized_bytes)
    normalized = _coerce_normalized_artifact(
        normalized_artifact,
        normalized_hash,
        len(normalized_bytes),
        source_id=release.source_id,
        release_id=cast(str, release.release_id),
    )
    if normalized is None:
        if normalized_store is not None:
            normalized = _store_normalized_artifact(
                normalized_store,
                release=release,
                source_url=source_url,
                content=normalized_bytes,
                captured_at=observed_at,
            )
        else:
            digest = normalized_hash.removeprefix("sha256:")
            normalized = AhrqArtifactLocator(
                role="system",
                artifact_id=f"{AHRQ_OFFLINE_ARTIFACT_PREFIX}{digest[:32]}",
                custody_locator=f"object://ahrq/normalized/{digest}",
                content_sha256=normalized_hash,
                byte_length=len(normalized_bytes),
                source_id=release.source_id,
                release_id=release.release_id,
                verified=False,
            )
    elif normalized.content_sha256 != normalized_hash or normalized.byte_length != len(normalized_bytes):
        raise AhrqProducerError("normalized artifact hash or length does not match parsed rows")
    if not normalized.verified and normalized_store is not None:
        raise AhrqProducerError("normalized artifact is not verified after local custody")
    _validate_raw_artifact_lineage(raw_rows, release)

    digest_material = f"{release.source_id}|{release.release_id}|{normalized_hash}".encode("utf-8")
    digest = sha256(digest_material).hexdigest()
    record_id = f"hdp:observation-envelope:ahrq:{digest[:32]}"
    activity_id = f"activity:ahrq:parse:{digest[:32]}"
    run_id = f"run:ahrq:{digest[:32]}"
    lineage_id = f"lineage:ahrq:{digest[:32]}"
    receipt_id = f"receipt:ahrq:{digest[:24]}"
    idempotency_key = f"idempotency:ahrq:{digest[:32]}"
    observation_ids = [f"observation:ahrq:{row.role}:{_slug(row.source_row_id)}" for row in raw_rows]
    replay_state: Literal["first_seen", "replayed"] = "first_seen"
    if prior_lineage_id is not None:
        _require_id(prior_lineage_id, "prior_lineage_id", re.compile(r"^lineage:[a-z0-9][a-z0-9._:-]*$"))
        replay_state = "replayed"

    source_release = {
        "source_id": release.source_id,
        "release_id": cast(str, release.release_id),
        "release_label": cast(str, release.release_label),
        "source_kind": "official_dataset",
        "release_sha256": cast(str, release.release_fingerprint),
        "evidence_locator": source_url,
        "coverage_state": "present",
    }
    artifact_payload: dict[str, object] = {
        "artifact_id": normalized.artifact_id,
        "release_ref": release.release_id,
        "artifact_kind": "normalized_rows",
        "media_type": "application/json",
        "content_sha256": normalized.content_sha256,
        "byte_length": normalized.byte_length,
        "custody": {
            "locator": normalized.custody_locator,
            "storage_plane": "object_storage",
            "immutable": True,
            "retention": "append_only",
        },
    }
    receipt_without_hash: dict[str, object] = {
        "receipt_id": receipt_id,
        "producer": AHRQ_PRODUCER_NAME,
        "receipt_schema": AHRQ_RECEIPT_SCHEMA,
        "source_release_ref": release.release_id,
        "artifact_ref": normalized.artifact_id,
        "source_release_sha256": release.release_fingerprint,
        "artifact_sha256": normalized.content_sha256,
        "state": "succeeded",
        "recorded_at": observed_at,
        "evidence_locator": source_url,
    }
    receipt_payload = dict(receipt_without_hash)
    receipt_payload["receipt_sha256"] = _sha256(_canonical_json_bytes(receipt_without_hash))
    activity = {
        "activity_id": activity_id,
        "run_id": run_id,
        "activity_type": "parsing",
        "actor": {"actor_type": "deterministic_transform", "actor_id": AHRQ_PRODUCER_NAME},
        "started_at": observed_at,
        "ended_at": observed_at,
        "status": "succeeded",
        "input_artifact_refs": [
            raw_rows[0].artifact.artifact_id,
            raw_rows[len(parsed.system_rows)].artifact.artifact_id,
            normalized.artifact_id,
        ],
        "output_artifact_refs": [normalized.artifact_id],
    }
    observations = [
        _observation_payload(
            row,
            release=release,
            normalized=normalized,
            activity_id=activity_id,
            receipt_id=receipt_id,
            observation_id=observation_id,
            source_period=source_period,
            recorded_at=observed_at,
        )
        for row, observation_id in zip(raw_rows, observation_ids)
    ]
    envelope: dict[str, object] = {
        "schema_version": AHRQ_ENVELOPE_SCHEMA_VERSION,
        "record_type": AHRQ_ENVELOPE_RECORD_TYPE,
        "record_id": record_id,
        "packet_id": AHRQ_PACKET_ID,
        "tracking_bead": AHRQ_TRACKING_BEAD,
        "frozen_dispatch_base": AHRQ_FROZEN_DISPATCH_BASE,
        "source_release": source_release,
        "artifact": artifact_payload,
        "receipt": receipt_payload,
        "activity": activity,
        "observations": observations,
        "lineage": {
            "lineage_id": lineage_id,
            "source_release_ref": release.release_id,
            "artifact_ref": normalized.artifact_id,
            "receipt_ref": receipt_id,
            "activity_ref": activity_id,
            "observation_ids": observation_ids,
            "deterministic_order": observation_ids,
            "replay": {
                "idempotency_key": idempotency_key,
                "state": replay_state,
                "replay_of": prior_lineage_id,
                "deterministic": True,
            },
        },
        "authority_limits": _authority_limits(),
    }
    _validate_envelope(envelope)
    return envelope


def write_ahrq_observation_envelope(path: str | Path, envelope: Mapping[str, object]) -> Path:
    """Validate and atomically write an observation envelope JSON file."""

    _validate_envelope(envelope)
    destination = Path(path)
    write_atomic_json(destination, dict(envelope))
    return destination


class InMemoryAhrqAcknowledgementStore:
    """Deterministic acknowledgement store used by tests and local dry-runs."""

    def __init__(self) -> None:
        self._records: dict[str, AhrqAcknowledgement] = {}
        self._lock = threading.Lock()

    @property
    def records(self) -> Mapping[str, AhrqAcknowledgement]:
        """Return a read-only view of acknowledgement records."""

        return self._records

    def acknowledge(self, envelope: Mapping[str, object]) -> AhrqAcknowledgement:
        """Durably record or replay one canonical envelope in memory."""

        values = _envelope_identity(envelope)
        with self._lock:
            existing = self._records.get(values["idempotency_key"])
            if existing is not None:
                if existing.envelope_sha256 != values["envelope_sha256"]:
                    raise AhrqReplayConflictError("idempotency key was reused for different envelope bytes")
                return AhrqAcknowledgement(
                    existing.acknowledgement_id,
                    existing.envelope_id,
                    existing.idempotency_key,
                    existing.envelope_sha256,
                    duplicate=True,
                )
            acknowledgement = AhrqAcknowledgement(
                acknowledgement_id=f"ack:{values['idempotency_key'].removeprefix('idempotency:')}",
                envelope_id=values["envelope_id"],
                idempotency_key=values["idempotency_key"],
                envelope_sha256=values["envelope_sha256"],
            )
            self._records[acknowledgement.idempotency_key] = acknowledgement
            return acknowledgement


class FileAhrqAcknowledgementStore:
    """Atomic JSON acknowledgement store for a bounded local producer run."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def acknowledge(self, envelope: Mapping[str, object]) -> AhrqAcknowledgement:
        """Persist or replay acknowledgement evidence without retaining payloads."""

        values = _envelope_identity(envelope)
        with self._lock, self._file_lock():
            records = self._read()
            existing = records.get(values["idempotency_key"])
            if existing is not None:
                if existing.envelope_sha256 != values["envelope_sha256"]:
                    raise AhrqReplayConflictError("idempotency key was reused for different envelope bytes")
                return AhrqAcknowledgement(
                    existing.acknowledgement_id,
                    existing.envelope_id,
                    existing.idempotency_key,
                    existing.envelope_sha256,
                    duplicate=True,
                )
            acknowledgement = AhrqAcknowledgement(
                acknowledgement_id=f"ack:{values['idempotency_key'].removeprefix('idempotency:')}",
                envelope_id=values["envelope_id"],
                idempotency_key=values["idempotency_key"],
                envelope_sha256=values["envelope_sha256"],
            )
            records[acknowledgement.idempotency_key] = acknowledgement
            write_atomic_json(self.path, {key: value.as_dict() for key, value in sorted(records.items())})
            return acknowledgement

    @contextmanager
    def _file_lock(self) -> Iterator[None]:
        """Hold an advisory OS lock shared by every process using this store."""

        lock_path = self.path.with_name(self.path.name + ".lock")
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = lock_path.open("a+", encoding="utf-8")
        except OSError as exc:
            raise AhrqAcknowledgementError("unable to open acknowledgement store lock") from exc
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            except OSError as exc:
                raise AhrqAcknowledgementError("unable to acquire acknowledgement store lock") from exc
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _read(self) -> dict[str, AhrqAcknowledgement]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise AhrqAcknowledgementError("unable to read acknowledgement store") from exc
        if not isinstance(raw, dict):
            raise AhrqAcknowledgementError("acknowledgement store must be an object")
        result: dict[str, AhrqAcknowledgement] = {}
        expected = {
            "acknowledgement_id",
            "envelope_id",
            "idempotency_key",
            "envelope_sha256",
            "durable",
            "duplicate",
        }
        for key, value in raw.items():
            if not isinstance(key, str) or not isinstance(value, Mapping):
                raise AhrqAcknowledgementError("acknowledgement store contains malformed record")
            if set(value) != expected:
                raise AhrqAcknowledgementError("acknowledgement store record has unknown or missing fields")
            if value["durable"] is not True or value["duplicate"] is not False:
                raise AhrqAcknowledgementError("acknowledgement store contains a non-durable record")
            record = AhrqAcknowledgement(
                acknowledgement_id=_required_text(value, "acknowledgement_id"),
                envelope_id=_required_text(value, "envelope_id"),
                idempotency_key=_required_text(value, "idempotency_key"),
                envelope_sha256=_required_text(value, "envelope_sha256"),
            )
            if key != record.idempotency_key:
                raise AhrqAcknowledgementError("acknowledgement store key does not match record")
            result[key] = record
        return result


class InMemoryAhrqCheckpointStore:
    """Thread-safe checkpoint CAS store for local deterministic delivery."""

    def __init__(self) -> None:
        self._records: dict[str, AhrqCheckpoint] = {}
        self._lock = threading.Lock()

    @property
    def records(self) -> Mapping[str, AhrqCheckpoint]:
        """Return the current checkpoint records."""

        return self._records

    def current(self, source_id: str = AHRQ_SOURCE_ID) -> AhrqCheckpoint | None:
        """Return the current checkpoint for one source."""

        _require_id(source_id, "source_id", _SOURCE_ID)
        return self._records.get(source_id)

    def compare_and_swap(
        self,
        precondition: AhrqCheckpointPrecondition,
        *,
        envelope: Mapping[str, object],
        acknowledgement: AhrqAcknowledgement,
    ) -> AhrqCheckpoint:
        """Advance exactly once when generation and cursor still match."""

        values = _envelope_identity(envelope)
        if precondition.source_id != values["source_id"]:
            raise AhrqCheckpointConflictError("checkpoint source_id does not match envelope")
        if (
            acknowledgement.envelope_id != values["envelope_id"]
            or acknowledgement.envelope_sha256 != values["envelope_sha256"]
        ):
            raise AhrqCheckpointConflictError("acknowledgement does not match envelope for checkpoint CAS")
        if acknowledgement.idempotency_key != values["idempotency_key"]:
            raise AhrqCheckpointConflictError("acknowledgement idempotency key does not match envelope")
        with self._lock:
            current = self._records.get(precondition.source_id)
            if current is not None and (
                current.envelope_id == values["envelope_id"]
                and current.acknowledgement_id == acknowledgement.acknowledgement_id
                and current.cursor == precondition.next_cursor
            ):
                return current
            if current is None:
                if precondition.expected_generation != 0 or precondition.expected_cursor is not None:
                    raise AhrqCheckpointConflictError("checkpoint does not exist at expected generation")
                generation = 1
            else:
                if (
                    current.generation != precondition.expected_generation
                    or current.cursor != precondition.expected_cursor
                ):
                    raise AhrqCheckpointConflictError(
                        f"stale checkpoint: expected generation {precondition.expected_generation}, "
                        f"cursor {precondition.expected_cursor!r}; found generation {current.generation}, "
                        f"cursor {current.cursor!r}"
                    )
                generation = current.generation + 1
            checkpoint = AhrqCheckpoint(
                source_id=precondition.source_id,
                generation=generation,
                cursor=precondition.next_cursor,
                envelope_id=values["envelope_id"],
                acknowledgement_id=acknowledgement.acknowledgement_id,
                envelope_sha256=values["envelope_sha256"],
                artifact_sha256=values["artifact_sha256"],
                updated_at=_canonical_timestamp(datetime.now(timezone.utc), "updated_at"),
            )
            self._records[precondition.source_id] = checkpoint
            return checkpoint


class FileAhrqCheckpointStore:
    """Atomic single-source checkpoint store with local compare-and-swap."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def current(self, source_id: str = AHRQ_SOURCE_ID) -> AhrqCheckpoint | None:
        """Read the checkpoint if present."""

        _require_id(source_id, "source_id", _SOURCE_ID)
        value = self._read()
        if value is None:
            return None
        if value.source_id != source_id:
            raise AhrqCheckpointConflictError("checkpoint source_id does not match requested source")
        return value

    def compare_and_swap(
        self,
        precondition: AhrqCheckpointPrecondition,
        *,
        envelope: Mapping[str, object],
        acknowledgement: AhrqAcknowledgement,
    ) -> AhrqCheckpoint:
        """Atomically verify and replace one checkpoint JSON record."""

        memory = InMemoryAhrqCheckpointStore()
        with self._lock:
            existing = self._read()
            if existing is not None and existing.source_id != precondition.source_id:
                raise AhrqCheckpointConflictError("checkpoint source_id does not match precondition")
            if existing is not None:
                memory._records[existing.source_id] = existing
            checkpoint = memory.compare_and_swap(
                precondition,
                envelope=envelope,
                acknowledgement=acknowledgement,
            )
            write_atomic_json(self.path, checkpoint.as_dict())
            return checkpoint

    def _read(self) -> AhrqCheckpoint | None:
        if not self.path.exists():
            return None
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise AhrqCheckpointConflictError("unable to read checkpoint") from exc
        if not isinstance(raw, Mapping):
            raise AhrqCheckpointConflictError("checkpoint must be an object")
        expected = {
            "schema_version",
            "record_type",
            "source_id",
            "generation",
            "cursor",
            "envelope_id",
            "acknowledgement_id",
            "envelope_sha256",
            "artifact_sha256",
            "updated_at",
        }
        if set(raw) != expected:
            raise AhrqCheckpointConflictError("checkpoint has unknown or missing fields")
        if (
            raw["schema_version"] != "hdp.ahrq-producer-checkpoint.v1"
            or raw["record_type"] != "ahrq_producer_checkpoint"
        ):
            raise AhrqCheckpointConflictError("checkpoint schema version is unsupported")
        generation = raw["generation"]
        if isinstance(generation, bool) or not isinstance(generation, int):
            raise AhrqCheckpointConflictError("checkpoint generation must be an integer")
        source_id = raw["source_id"]
        cursor = raw["cursor"]
        envelope_id = raw["envelope_id"]
        acknowledgement_id = raw["acknowledgement_id"]
        envelope_sha256 = raw["envelope_sha256"]
        artifact_sha256 = raw["artifact_sha256"]
        updated_at = raw["updated_at"]
        if not isinstance(source_id, str) or not isinstance(cursor, str):
            raise AhrqCheckpointConflictError("checkpoint source_id and cursor must be strings")
        if not isinstance(envelope_id, str) or not isinstance(acknowledgement_id, str):
            raise AhrqCheckpointConflictError("checkpoint envelope and acknowledgement IDs must be strings")
        if not isinstance(envelope_sha256, str) or not isinstance(artifact_sha256, str):
            raise AhrqCheckpointConflictError("checkpoint hashes must be strings")
        if not isinstance(updated_at, str):
            raise AhrqCheckpointConflictError("checkpoint updated_at must be a string")
        return AhrqCheckpoint(
            source_id=source_id,
            generation=generation,
            cursor=cursor,
            envelope_id=envelope_id,
            acknowledgement_id=acknowledgement_id,
            envelope_sha256=envelope_sha256,
            artifact_sha256=artifact_sha256,
            updated_at=updated_at,
        )


class AhrqObservationProducer:
    """Build and deliver AHRQ envelopes through acknowledgement then CAS."""

    def __init__(
        self,
        *,
        acknowledger: AhrqAcknowledgementSink | Callable[[Mapping[str, object]], AhrqAcknowledgement],
        checkpoint_store: AhrqCheckpointSink | None = None,
    ) -> None:
        self.acknowledger = acknowledger
        self.checkpoint_store = checkpoint_store

    def acknowledge_and_checkpoint(
        self,
        envelope: Mapping[str, object],
        *,
        checkpoint: AhrqCheckpointPrecondition | None = None,
    ) -> AhrqProducerReceipt:
        """Durably acknowledge one envelope, then optionally CAS its checkpoint."""

        _validate_envelope(envelope)
        values = _envelope_identity(envelope)
        acknowledgement = self._acknowledge(envelope)
        if not acknowledgement.durable:
            raise AhrqAcknowledgementError("producer checkpoint requires a durable acknowledgement")
        if (
            acknowledgement.envelope_id != values["envelope_id"]
            or acknowledgement.idempotency_key != values["idempotency_key"]
            or acknowledgement.envelope_sha256 != values["envelope_sha256"]
        ):
            raise AhrqAcknowledgementError("durable acknowledgement does not match envelope identity")
        generation: int | None = None
        if checkpoint is not None:
            if self.checkpoint_store is None:
                raise AhrqCheckpointConflictError("checkpoint store is required for checkpoint publication")
            updated = self.checkpoint_store.compare_and_swap(
                checkpoint,
                envelope=envelope,
                acknowledgement=acknowledgement,
            )
            generation = updated.generation
        artifact = _mapping(envelope.get("artifact"), "artifact")
        return AhrqProducerReceipt(
            state="duplicate" if acknowledgement.duplicate else "accepted",
            envelope_id=values["envelope_id"],
            idempotency_key=values["idempotency_key"],
            envelope_sha256=values["envelope_sha256"],
            acknowledgement_id=acknowledgement.acknowledgement_id,
            acknowledged=True,
            checkpoint_generation=generation,
            artifact_id=_required_text(artifact, "artifact_id"),
            artifact_sha256=_required_text(artifact, "content_sha256"),
            row_count=len(cast(list[object], envelope["observations"])),
        )

    def produce(
        self,
        parsed: AhrqParsedRows,
        release: DetectionReceipt,
        *,
        checkpoint: AhrqCheckpointPrecondition | None = None,
        normalized_artifact: AhrqArtifactLocator | Mapping[str, object] | None = None,
        normalized_store: RawArtifactStore | None = None,
        source_url: str = AHRQ_DEFAULT_SOURCE_URL,
        recorded_at: datetime | None = None,
        source_period: str = AHRQ_SOURCE_PERIOD,
        prior_lineage_id: str | None = None,
    ) -> AhrqProducerReceipt:
        """Build a validated envelope and run the acknowledgement/CAS protocol."""

        envelope = build_ahrq_observation_envelope(
            parsed,
            release,
            normalized_artifact=normalized_artifact,
            normalized_store=normalized_store,
            source_url=source_url,
            recorded_at=recorded_at,
            source_period=source_period,
            prior_lineage_id=prior_lineage_id,
        )
        return self.acknowledge_and_checkpoint(envelope, checkpoint=checkpoint)

    def _acknowledge(self, envelope: Mapping[str, object]) -> AhrqAcknowledgement:
        candidate = self.acknowledger
        try:
            if hasattr(candidate, "acknowledge"):
                acknowledgement = cast(AhrqAcknowledgementSink, candidate).acknowledge(envelope)
            else:
                acknowledgement = cast(
                    Callable[[Mapping[str, object]], AhrqAcknowledgement],
                    candidate,
                )(envelope)
        except AhrqProducerError:
            raise
        except Exception as exc:  # pragma: no cover - connector-specific failures
            raise AhrqAcknowledgementError("durable acknowledgement failed") from exc
        if not isinstance(acknowledgement, AhrqAcknowledgement):
            raise AhrqAcknowledgementError("acknowledger returned an unsupported result")
        return acknowledgement


def _parse_rows(
    path: Path,
    *,
    role: ArtifactRole,
    required_columns: frozenset[str],
    artifact: AhrqArtifactLocator,
    encoding: str,
    raw: bytes | None = None,
) -> tuple[AhrqSourceRow, ...]:
    try:
        source_bytes = path.read_bytes() if raw is None else raw
        text = source_bytes.decode(encoding, errors="strict")
    except (OSError, UnicodeError) as exc:
        raise AhrqRowParseError(f"unable to read {role} AHRQ CSV: {path.name}") from exc
    reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
    original_headers = reader.fieldnames
    if not original_headers:
        raise AhrqRowParseError(f"{role} AHRQ CSV has no header: {path.name}")
    headers: list[str] = []
    seen: set[str] = set()
    for index, header in enumerate(original_headers):
        canonical = (header or "").lstrip("\ufeff").strip().lower()
        if not canonical:
            raise AhrqRowParseError(f"{role} AHRQ CSV has a blank header at column {index + 1}")
        if canonical in seen:
            raise AhrqRowParseError(f"{role} AHRQ CSV has duplicate header: {canonical}")
        seen.add(canonical)
        headers.append(canonical)
    canonical_by_original = {original: canonical for original, canonical in zip(original_headers, headers)}
    missing = sorted(required_columns - set(headers))
    if missing:
        raise AhrqRowParseError(f"{role} AHRQ CSV is missing required columns: {', '.join(missing)}")
    rows: list[AhrqSourceRow] = []
    seen_ids: set[str] = set()
    try:
        for row_number, raw_row in enumerate(reader, start=2):
            if None in raw_row:
                raise AhrqRowParseError(f"{role} AHRQ CSV row {row_number} has too many fields")
            if not any(value not in (None, "") for value in raw_row.values()):
                continue
            fields: dict[str, str] = {}
            for original_header, header in canonical_by_original.items():
                value = raw_row.get(original_header)
                if value is None:
                    value = ""
                fields[header] = value.strip()
            id_column = "health_sys_id" if role == "system" else "compendium_hospital_id"
            source_native_id = fields.get(id_column, "")
            if not source_native_id:
                raise AhrqRowParseError(f"{role} AHRQ CSV row {row_number} has no {id_column}")
            if role == "facility" and not fields.get("health_sys_id", ""):
                raise AhrqRowParseError(f"facility AHRQ CSV row {row_number} has no health_sys_id")
            normalized_id = _slug(source_native_id)
            if normalized_id in seen_ids:
                raise AhrqRowParseError(f"duplicate {id_column} in {role} AHRQ CSV: {source_native_id}")
            seen_ids.add(normalized_id)
            row_id = f"{role}:{normalized_id}"
            row_bytes = _canonical_json_bytes({"headers": headers, "fields": fields})
            rows.append(
                AhrqSourceRow(
                    role=role,
                    row_number=row_number,
                    source_row_id=row_id,
                    fields=fields,
                    row_sha256=_sha256(row_bytes),
                    artifact=artifact,
                )
            )
    except csv.Error as exc:
        raise AhrqRowParseError(f"{role} AHRQ CSV is malformed: {path.name}") from exc
    if not rows:
        raise AhrqRowParseError(f"{role} AHRQ CSV has no data rows")
    return tuple(rows)


def _coerce_artifact(
    role: ArtifactRole, value: AhrqArtifactLocator | Mapping[str, object] | None
) -> AhrqArtifactLocator:
    if value is None:
        return AhrqArtifactLocator(
            role=role,
            artifact_id=f"artifact:ahrq:raw:{role}",
            custody_locator=f"object://ahrq/raw/{role}",
            content_sha256="sha256:" + "0" * 64,
            byte_length=1,
            verified=False,
        )
    if isinstance(value, AhrqArtifactLocator):
        if value.role != role:
            raise AhrqProducerError("artifact role does not match source file")
        return value
    return AhrqArtifactLocator.from_mapping(role, value)


def _coerce_normalized_artifact(
    value: AhrqArtifactLocator | Mapping[str, object] | None,
    content_sha256: str,
    byte_length: int,
    *,
    source_id: str,
    release_id: str,
) -> AhrqArtifactLocator | None:
    if value is None:
        return None
    locator = value if isinstance(value, AhrqArtifactLocator) else AhrqArtifactLocator.from_mapping("system", value)
    if locator.role != "system":
        raise AhrqProducerError("normalized artifact role must be system")
    if locator.source_id != AHRQ_SOURCE_ID or locator.source_id != source_id:
        raise AhrqProducerError("normalized artifact source_id does not match AHRQ")
    if locator.release_id != release_id:
        raise AhrqProducerError("normalized artifact release_id does not match detector receipt")
    if not locator.verified:
        raise AhrqProducerError("normalized artifact requires verified custody")
    if locator.content_sha256 != content_sha256 or locator.byte_length != byte_length:
        raise AhrqProducerError("normalized artifact hash or length does not match parsed rows")
    return locator


def _verify_file_claim(path: Path, artifact: AhrqArtifactLocator, role: str) -> bytes:
    if not path.is_file():
        raise AhrqRowParseError(f"{role} AHRQ artifact is missing: {path.name}")
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise AhrqRowParseError(f"unable to read {role} AHRQ artifact: {path.name}") from exc
    actual_hash = _sha256(content)
    if artifact.verified and (actual_hash != artifact.content_sha256 or len(content) != artifact.byte_length):
        raise AhrqProducerError(f"{role} raw artifact hash or length does not match custody claim")
    return content


def _validate_release(release: DetectionReceipt) -> None:
    if release.state not in {"changed", "unchanged"}:
        raise AhrqProducerError("AHRQ producer requires a successful detector receipt")
    if release.source_id != AHRQ_SOURCE_ID:
        raise AhrqProducerError("detector receipt source_id is not the AHRQ source")
    if (
        release.release_id is None
        or release.release_label is None
        or release.published_at is None
        or release.release_fingerprint is None
    ):
        raise AhrqProducerError("detector receipt is missing release metadata")
    _require_id(release.release_id, "release_id", _RELEASE_ID)
    _require_hash(release.release_fingerprint, "release_fingerprint")


def _validate_raw_artifact_lineage(rows: Sequence[AhrqSourceRow], release: DetectionReceipt) -> None:
    if not rows:
        raise AhrqProducerError("AHRQ producer cannot emit an empty observation batch")
    for row in rows:
        artifact = row.artifact
        if not artifact.verified:
            raise AhrqProducerError("AHRQ producer requires verified raw custody for every source row")
        if artifact.source_id != release.source_id:
            raise AhrqProducerError("raw artifact source_id does not match detector receipt")
        if artifact.release_id != release.release_id:
            raise AhrqProducerError("raw artifact release_id does not match detector receipt")


def _normalized_rows_bytes(parsed: AhrqParsedRows, release: DetectionReceipt, source_period: str) -> bytes:
    payload: dict[str, object] = {
        "schema_version": "hdp.ahrq-normalized-rows.v1",
        "source_id": release.source_id,
        "release_id": release.release_id,
        "source_period": source_period,
        "system_rows": [row.as_dict() for row in parsed.system_rows],
        "facility_rows": [row.as_dict() for row in parsed.facility_rows],
    }
    return _canonical_json_bytes(payload)


def _store_normalized_artifact(
    store: RawArtifactStore,
    *,
    release: DetectionReceipt,
    source_url: str,
    content: bytes,
    captured_at: str,
) -> AhrqArtifactLocator:
    content_hash = _sha256(content)
    material = f"{release.source_id}|{release.release_id}|{content_hash}".encode("utf-8")
    identity = sha256(material).hexdigest()[:32]
    chunk_size = min(65_536, len(content))
    metadata = RawArtifactMetadata(
        schema_version="hdp.raw-artifact.v1",
        record_type="raw_artifact",
        artifact_id=f"artifact:raw:{identity}",
        source_id=release.source_id,
        source_url=source_url,
        release_id=cast(str, release.release_id),
        media_type="application/json",
        content_sha256=content_hash,
        byte_length=len(content),
        chunk_count=(len(content) + chunk_size - 1) // chunk_size,
        chunk_size=chunk_size,
        idempotency_key=f"idempotency:raw:{identity}",
        captured_at=captured_at,
        rights_status="approved_public",
        response_fingerprint=release.response_fingerprint,
    )
    receipt = store.put(metadata, [content])
    return AhrqArtifactLocator(
        role="system",
        artifact_id=receipt.artifact_id,
        custody_locator=f"object://{receipt.object_key}",
        content_sha256=receipt.content_sha256,
        byte_length=receipt.byte_length,
        source_id=release.source_id,
        release_id=release.release_id,
        verified=True,
    )


def _observation_payload(
    row: AhrqSourceRow,
    *,
    release: DetectionReceipt,
    normalized: AhrqArtifactLocator,
    activity_id: str,
    receipt_id: str,
    observation_id: str,
    source_period: str,
    recorded_at: str,
) -> dict[str, object]:
    return {
        "observation_id": observation_id,
        "identity_key": f"identity:ahrq:{row.role}:{_slug(row.source_row_id)}",
        "subject_ref": f"source-record:ahrq-{row.role}-{_slug(row.source_row_id)}",
        "attribute_term_ref": "term:source-row",
        "value": row.as_dict(),
        "value_state": "observed",
        "source_scope": {
            "scope_id": f"scope:ahrq:{row.role}",
            "source_id": release.source_id,
            "release_ref": release.release_id,
            "artifact_ref": normalized.artifact_id,
            "custody_locator": normalized.custody_locator,
            "selector": f"row:{row.role}-{row.row_number}",
            "authority_state": "source_scoped",
        },
        "activity_ref": activity_id,
        "receipt_ref": receipt_id,
        "valid_time": {
            "precision": "year",
            "as_of": f"{source_period}-12-31",
            "valid_from": f"{source_period}-01-01",
            "valid_to": f"{source_period}-12-31",
        },
        "transaction_time": {"recorded_from": recorded_at, "recorded_to": None},
        "conflict": {
            "state": "none",
            "reason": "One verified source-native AHRQ row supplies this observation.",
            "resolution": "not_required",
            "competing_observation_refs": [],
        },
        "promotion_state": "unpromoted_observation",
    }


def _validate_envelope(envelope: Mapping[str, object]) -> None:
    try:
        from shared.contracts.healthcare_data_platform import (
            HealthcareDataPlatformContractError,
            validate_source_scoped_observation_envelope,
        )

        validate_source_scoped_observation_envelope(envelope)
    except HealthcareDataPlatformContractError as exc:
        raise AhrqProducerError(f"AHRQ observation envelope failed contract validation: {exc}") from exc
    except (ImportError, OSError) as exc:  # pragma: no cover - installation/repository failure
        raise AhrqProducerError("AHRQ observation envelope validator is unavailable") from exc


def _envelope_identity(envelope: Mapping[str, object]) -> dict[str, str]:
    envelope_id = _required_text(envelope, "record_id")
    source_release = _mapping(envelope.get("source_release"), "source_release")
    lineage = _mapping(envelope.get("lineage"), "lineage")
    replay = _mapping(lineage.get("replay"), "lineage.replay")
    idempotency_key = _required_text(replay, "idempotency_key")
    artifact = _mapping(envelope.get("artifact"), "artifact")
    artifact_id = _required_text(artifact, "artifact_id")
    if artifact_id.startswith(AHRQ_OFFLINE_ARTIFACT_PREFIX):
        raise AhrqAcknowledgementError("offline normalized artifact cannot be delivered")
    return {
        "envelope_id": envelope_id,
        "source_id": _required_text(source_release, "source_id"),
        "idempotency_key": idempotency_key,
        "envelope_sha256": _sha256(_canonical_json_bytes(envelope)),
        "artifact_sha256": _required_text(artifact, "content_sha256"),
    }


def _authority_limits() -> dict[str, bool]:
    return {
        "acquisition_allowed": False,
        "mutation_allowed": False,
        "deletion_allowed": False,
        "publication_allowed": False,
        "release_allowed": False,
        "production_allowed": False,
        "runtime_allowed": False,
        "current_projection_allowed": False,
        "identity_promotion_allowed": False,
    }


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AhrqProducerError("value is not canonical JSON") from exc


def _sha256(value: bytes) -> str:
    return f"sha256:{sha256(value).hexdigest()}"


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9._:-]+", "-", value.casefold()).strip("-")
    if not normalized:
        raise AhrqProducerError("value cannot be reduced to a safe identifier")
    return normalized


def _canonical_timestamp(value: datetime, label: str) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise AhrqProducerError(f"{label} must include a timezone")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_timestamp_text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AhrqProducerError(f"{label} must be a non-empty timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AhrqProducerError(f"{label} must be an ISO-8601 timestamp") from exc
    return _canonical_timestamp(parsed, label)


def _require_id(value: str, label: str, pattern: re.Pattern[str]) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise AhrqProducerError(f"{label} is malformed")


def _require_hash(value: str, label: str) -> None:
    _require_id(value, label, _SHA256)


def _require_locator(value: str, label: str) -> None:
    _require_id(value, label, _SAFE_LOCATOR)


def _required_text(value: Mapping[str, object], label: str) -> str:
    raw = value.get(label)
    if not isinstance(raw, str) or not raw.strip():
        raise AhrqProducerError(f"{label} must be a non-empty string")
    return raw


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise AhrqProducerError(f"{label} must be an object")
    return value


__all__ = [
    "AHRQ_DEFAULT_SOURCE_URL",
    "AHRQ_ENVELOPE_RECORD_TYPE",
    "AHRQ_ENVELOPE_SCHEMA_VERSION",
    "AHRQ_FROZEN_DISPATCH_BASE",
    "AHRQ_OFFLINE_ARTIFACT_PREFIX",
    "AHRQ_PACKET_ID",
    "AHRQ_PRODUCER_NAME",
    "AHRQ_RECEIPT_SCHEMA",
    "AHRQ_SOURCE_ID",
    "AHRQ_SOURCE_PERIOD",
    "AHRQ_TRACKING_BEAD",
    "AhrqAcknowledgement",
    "AhrqAcknowledgementError",
    "AhrqAcknowledgementSink",
    "AhrqArtifactLocator",
    "AhrqCheckpoint",
    "AhrqCheckpointConflictError",
    "AhrqCheckpointPrecondition",
    "AhrqCheckpointSink",
    "AhrqObservationProducer",
    "AhrqParsedRows",
    "AhrqProducerError",
    "AhrqProducerReceipt",
    "AhrqReplayConflictError",
    "AhrqRowParseError",
    "AhrqSourceRow",
    "FACILITY_REQUIRED_COLUMNS",
    "FileAhrqAcknowledgementStore",
    "FileAhrqCheckpointStore",
    "InMemoryAhrqAcknowledgementStore",
    "InMemoryAhrqCheckpointStore",
    "SYSTEM_REQUIRED_COLUMNS",
    "build_ahrq_observation_envelope",
    "parse_ahrq_facility_rows",
    "parse_ahrq_source_rows",
    "parse_ahrq_system_rows",
    "write_ahrq_observation_envelope",
]
