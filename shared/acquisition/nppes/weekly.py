"""Bounded, source-local producer for NPPES weekly changes.

This module consumes caller-owned update and deactivation streams.  It keeps
weekly observations source-native, applies deterministic merge ordering against
an optional source-local state mapping, and never writes a Toolkit projection
or promotes an NPI to canonical identity.
"""

from __future__ import annotations

import codecs
import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import ipaddress
import json
import re
import time
from typing import Callable, Iterable, Literal, Mapping, Protocol, TypeAlias, cast
from urllib.parse import urlsplit

from shared.acquisition.nppes.contracts import (
    MAX_DEACTIVATION_SAMPLES,
    MAX_IDENTIFIER_SAMPLES,
    MAX_NPPES_BYTES,
    MAX_NPPES_CHUNK_BYTES,
    MAX_NPPES_CHUNKS,
    MAX_NPPES_SECONDS,
    NPPES_SOURCE_ID,
    NppesCatalog,
    NppesCatalogEntry,
    NppesContractError,
    NppesDeactivationOpportunity,
    NppesFileDescriptor,
    NppesFileKind,
    NppesFileReceipt,
    NppesFileState,
    NppesReplayConflictError,
)
from shared.acquisition.nppes.producer import NppesStreamBudget


NPPES_WEEKLY_RELEASE_SCHEMA = "hdp.nppes-weekly-release.v1"
NPPES_WEEKLY_RECEIPT_SCHEMA = "hdp.nppes-weekly-receipt.v1"
NPPES_WEEKLY_PRODUCER = "healthcare-data-mcp:nppes-weekly-producer"
NppesWeeklyOperation: TypeAlias = Literal["upsert", "deactivate"]
NppesWeeklyProbeState: TypeAlias = Literal["changed", "not_modified", "failed_probe"]
NppesWeeklyOutcomeState: TypeAlias = Literal["changed", "no_op", "failed_probe", "schema_drift", "replayed", "blocked"]
WeeklyFileChunks: TypeAlias = Iterable[bytes]
WeeklyRowSink: TypeAlias = Callable[["NppesWeeklyObservation"], None]

_FILE_KINDS: tuple[NppesFileKind, ...] = (
    "provider",
    "location",
    "endpoint",
    "reference",
    "deactivation",
)
_NPI = re.compile(r"^[0-9]{10}$")
_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:[a-z0-9][a-z0-9._:-]*$")
_RECEIPT_ID = re.compile(r"^receipt:[a-z0-9][a-z0-9._:-]*$")
_SOURCE_ROW_ID = re.compile(r"^[a-z][a-z0-9._:-]{0,199}$")
_SOURCE_FILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_LOCATOR = re.compile(r"^(?:https://|object://|parquet://|docs/|contracts/)[A-Za-z0-9._:/-]+$")
_HEADER = re.compile(r"[^a-z0-9]+")
_MAX_ROW_BYTES = 1_048_576
_MAX_TEXT = 500
_MAX_SEQUENCE = 2_000_000_000


class NppesWeeklyProducerError(NppesContractError):
    """Raised when weekly producer input cannot be safely admitted."""


class NppesWeeklySchemaDriftError(NppesWeeklyProducerError):
    """Raised internally when a weekly source row violates its shape."""


class NppesWeeklyStreamInterrupted(NppesWeeklyProducerError):
    """Raised internally when a weekly source exceeds explicit bounds."""


class NppesWeeklyReplayConflictError(NppesReplayConflictError):
    """Raised when a successful weekly replay has conflicting file identity."""


class _Digest(Protocol):
    """Small structural type for hashlib-compatible streaming digests."""

    def update(self, value: bytes, /) -> None:
        """Add bytes to the digest."""

    def hexdigest(self) -> str:
        """Return the hexadecimal digest."""

        ...


def _text(value: object, name: str, *, maximum: int = _MAX_TEXT) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NppesWeeklyProducerError(f"{name} must be a non-empty string")
    if len(value) > maximum:
        raise NppesWeeklyProducerError(f"{name} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise NppesWeeklyProducerError(f"{name} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise NppesWeeklyProducerError(f"{name} contains malformed Unicode") from exc
    return value


def _https(value: object, name: str) -> str:
    text = _text(value, name, maximum=2048)
    try:
        parsed = urlsplit(text)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise NppesWeeklyProducerError(f"{name} has a malformed authority") from exc
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or hostname is None:
        raise NppesWeeklyProducerError(f"{name} must be an HTTPS URL without user information")
    host = hostname.casefold().rstrip(".")
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise NppesWeeklyProducerError(f"{name} has an unsafe local authority")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None and (
        address.is_private or address.is_loopback or address.is_link_local or address.is_reserved
    ):
        raise NppesWeeklyProducerError(f"{name} has an unsafe private authority")
    return text


def _locator(value: object, name: str) -> str:
    text = _text(value, name)
    if _SAFE_LOCATOR.fullmatch(text) is None:
        raise NppesWeeklyProducerError(f"{name} must be a bounded evidence locator")
    return text


def _date_value(value: object, name: str) -> str:
    text = _text(value, name, maximum=20)
    try:
        date.fromisoformat(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise NppesWeeklyProducerError(f"{name} must be an ISO-8601 date") from exc
    return text


def _timestamp(value: object, name: str, *, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    text = _text(value, name, maximum=80)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise NppesWeeklyProducerError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise NppesWeeklyProducerError(f"{name} must include a timezone")
    return text


def _integer(value: object, name: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise NppesWeeklyProducerError(f"{name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        bound = f" between {minimum} and {maximum}" if maximum is not None else f" at least {minimum}"
        raise NppesWeeklyProducerError(f"{name} must be{bound}")
    return value


def _sha(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise NppesWeeklyProducerError(f"{name} must be sha256:<64 lowercase hex>")
    return value


def _optional_sha(value: object, name: str) -> str | None:
    if value is None or value == "":
        return None
    return _sha(value, name)


def _canonical(value: Mapping[str, object]) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise NppesWeeklyProducerError("value cannot be canonically fingerprinted") from exc
    return "sha256:" + sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class NppesWeeklyRelease:
    """Weekly release/probe metadata independent of transport or custody."""

    release_id: str
    release_label: str
    source_id: str
    week_start: str
    week_end: str
    release_sequence: int
    source_url: str
    probe_state: NppesWeeklyProbeState
    final_url: str | None
    http_status: int | None
    published_at: str | None
    evidence_locator: str
    files: tuple[NppesFileDescriptor, ...] = ()
    failure_reason: str | None = None
    release_sha256: str | None = None
    schema_version: str = NPPES_WEEKLY_RELEASE_SCHEMA
    record_type: Literal["nppes_weekly_release"] = "nppes_weekly_release"

    def __post_init__(self) -> None:
        if _RELEASE_ID.fullmatch(self.release_id) is None:
            raise NppesWeeklyProducerError("release_id must be release-scoped")
        if self.schema_version != NPPES_WEEKLY_RELEASE_SCHEMA or self.record_type != "nppes_weekly_release":
            raise NppesWeeklyProducerError("unsupported NPPES weekly release schema")
        _text(self.release_label, "release_label", maximum=200)
        if self.source_id != NPPES_SOURCE_ID or _SOURCE_ID.fullmatch(self.source_id) is None:
            raise NppesWeeklyProducerError("source_id must be NPPES")
        start = _date_value(self.week_start, "week_start")
        end = _date_value(self.week_end, "week_end")
        if start > end:
            raise NppesWeeklyProducerError("week_start cannot be after week_end")
        _integer(self.release_sequence, "release_sequence", maximum=_MAX_SEQUENCE)
        _https(self.source_url, "source_url")
        if self.final_url is not None:
            _https(self.final_url, "final_url")
        if self.probe_state not in {"changed", "not_modified", "failed_probe"}:
            raise NppesWeeklyProducerError("unsupported weekly probe_state")
        if self.http_status is not None:
            _integer(self.http_status, "http_status", minimum=100, maximum=599)
        _timestamp(self.published_at, "published_at", allow_none=True)
        _locator(self.evidence_locator, "evidence_locator")
        seen: set[NppesFileKind] = set()
        present_count = 0
        for item in self.files:
            if not isinstance(item, NppesFileDescriptor):
                raise NppesWeeklyProducerError("weekly release files must be NppesFileDescriptor values")
            if item.file_kind in seen:
                raise NppesWeeklyProducerError(f"duplicate weekly file kind: {item.file_kind}")
            seen.add(item.file_kind)
            if item.availability == "present":
                present_count += 1
        if self.probe_state == "changed":
            if self.http_status is not None and not 200 <= self.http_status < 300:
                raise NppesWeeklyProducerError("changed weekly release requires a successful HTTP status")
            if self.final_url is None or self.published_at is None:
                raise NppesWeeklyProducerError("changed weekly release requires final_url and published_at")
            if present_count == 0:
                raise NppesWeeklyProducerError("changed weekly release requires a present update file")
            if self.failure_reason is not None:
                raise NppesWeeklyProducerError("changed weekly release cannot carry failure_reason")
        elif self.probe_state == "not_modified":
            if self.http_status != 304:
                raise NppesWeeklyProducerError("not_modified weekly release requires HTTP 304")
            if self.files:
                raise NppesWeeklyProducerError("not_modified weekly release cannot carry file descriptors")
            if self.failure_reason is not None:
                raise NppesWeeklyProducerError("not_modified weekly release cannot carry failure_reason")
        else:
            if self.failure_reason is None:
                raise NppesWeeklyProducerError("failed_probe weekly release requires failure_reason")
            _text(self.failure_reason, "failure_reason", maximum=300)
            if self.files:
                raise NppesWeeklyProducerError("failed_probe weekly release cannot carry file descriptors")
        computed = _canonical(self._semantic_payload())
        if self.release_sha256 is not None and self.release_sha256 != computed:
            raise NppesWeeklyProducerError("release_sha256 does not match weekly release metadata")
        object.__setattr__(self, "release_sha256", computed)

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "release_id": self.release_id,
            "release_label": self.release_label,
            "week_start": self.week_start,
            "week_end": self.week_end,
            "release_sequence": self.release_sequence,
            "source_url": self.source_url,
            "probe_state": self.probe_state,
            "final_url": self.final_url,
            "http_status": self.http_status,
            "published_at": self.published_at,
            "evidence_locator": self.evidence_locator,
            "files": [item.as_dict() for item in self.files],
        }

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "record_type": self.record_type,
            **self._semantic_payload(),
            "failure_reason": self.failure_reason,
            "release_sha256": self.release_sha256,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "NppesWeeklyRelease":
        allowed = {
            "schema_version",
            "record_type",
            "release_id",
            "release_label",
            "source_id",
            "week_start",
            "week_end",
            "release_sequence",
            "source_url",
            "probe_state",
            "final_url",
            "http_status",
            "published_at",
            "evidence_locator",
            "files",
            "failure_reason",
            "release_sha256",
        }
        unknown = set(value) - allowed
        if unknown:
            raise NppesWeeklyProducerError(f"unknown weekly release fields: {sorted(unknown)}")
        raw_files = value.get("files", [])
        if not isinstance(raw_files, list):
            raise NppesWeeklyProducerError("weekly release files must be an array")
        files = tuple(NppesFileDescriptor.from_mapping(item) for item in raw_files if isinstance(item, Mapping))
        if len(files) != len(raw_files):
            raise NppesWeeklyProducerError("weekly release files must contain mappings")
        return cls(
            release_id=_text(value.get("release_id"), "release_id", maximum=200),
            release_label=_text(value.get("release_label"), "release_label", maximum=200),
            source_id=_text(value.get("source_id"), "source_id", maximum=200),
            week_start=_date_value(value.get("week_start"), "week_start"),
            week_end=_date_value(value.get("week_end"), "week_end"),
            release_sequence=_integer(value.get("release_sequence"), "release_sequence", maximum=_MAX_SEQUENCE),
            source_url=_https(value.get("source_url"), "source_url"),
            probe_state=cast(NppesWeeklyProbeState, value.get("probe_state")),
            final_url=cast(str | None, value.get("final_url")),
            http_status=cast(int | None, value.get("http_status")),
            published_at=cast(str | None, value.get("published_at")),
            evidence_locator=_locator(value.get("evidence_locator"), "evidence_locator"),
            files=files,
            failure_reason=cast(str | None, value.get("failure_reason")),
            release_sha256=_optional_sha(value.get("release_sha256"), "release_sha256"),
            schema_version=cast(str, value.get("schema_version", NPPES_WEEKLY_RELEASE_SCHEMA)),
            record_type=cast(Literal["nppes_weekly_release"], value.get("record_type", "nppes_weekly_release")),
        )


@dataclass(frozen=True, slots=True)
class NppesWeeklyObservation:
    """One source-native weekly event or current source-local state value."""

    source_key: str
    file_kind: NppesFileKind
    source_file_name: str
    npi: str
    source_row_id: str
    row_number: int
    operation: NppesWeeklyOperation
    effective_date: str
    release_sequence: int
    release_id: str
    row_sha256: str
    evidence_locator: str

    def __post_init__(self) -> None:
        if self.file_kind not in _FILE_KINDS:
            raise NppesWeeklyProducerError("weekly observation file_kind is unsupported")
        if self.source_key != f"{self.file_kind}:{self.npi}":
            raise NppesWeeklyProducerError("weekly observation source_key is not source-local")
        if _NPI.fullmatch(self.npi) is None:
            raise NppesWeeklyProducerError("weekly observation NPI must be exactly ten ASCII digits")
        if _SOURCE_FILE.fullmatch(self.source_file_name) is None:
            raise NppesWeeklyProducerError("weekly observation source_file_name is malformed")
        if _SOURCE_ROW_ID.fullmatch(self.source_row_id) is None:
            raise NppesWeeklyProducerError("weekly observation source_row_id is malformed")
        _integer(self.row_number, "row_number", minimum=2)
        if self.operation not in {"upsert", "deactivate"}:
            raise NppesWeeklyProducerError("weekly observation operation is unsupported")
        _date_value(self.effective_date, "effective_date")
        _integer(self.release_sequence, "release_sequence", maximum=_MAX_SEQUENCE)
        if _RELEASE_ID.fullmatch(self.release_id) is None:
            raise NppesWeeklyProducerError("weekly observation release_id is malformed")
        _sha(self.row_sha256, "row_sha256")
        _locator(self.evidence_locator, "observation evidence_locator")

    def as_dict(self) -> dict[str, object]:
        return {
            "source_key": self.source_key,
            "file_kind": self.file_kind,
            "source_file_name": self.source_file_name,
            "npi": self.npi,
            "source_row_id": self.source_row_id,
            "row_number": self.row_number,
            "operation": self.operation,
            "effective_date": self.effective_date,
            "release_sequence": self.release_sequence,
            "release_id": self.release_id,
            "row_sha256": self.row_sha256,
            "evidence_locator": self.evidence_locator,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "NppesWeeklyObservation":
        allowed = {
            "source_key",
            "file_kind",
            "source_file_name",
            "npi",
            "source_row_id",
            "row_number",
            "operation",
            "effective_date",
            "release_sequence",
            "release_id",
            "row_sha256",
            "evidence_locator",
        }
        unknown = set(value) - allowed
        if unknown:
            raise NppesWeeklyProducerError(f"unknown weekly observation fields: {sorted(unknown)}")
        return cls(
            source_key=_text(value.get("source_key"), "source_key", maximum=256),
            file_kind=cast(NppesFileKind, value.get("file_kind")),
            source_file_name=_text(value.get("source_file_name"), "source_file_name", maximum=256),
            npi=_text(value.get("npi"), "npi", maximum=10),
            source_row_id=_text(value.get("source_row_id"), "source_row_id", maximum=200),
            row_number=_integer(value.get("row_number"), "row_number", minimum=2),
            operation=cast(NppesWeeklyOperation, value.get("operation")),
            effective_date=_date_value(value.get("effective_date"), "effective_date"),
            release_sequence=_integer(value.get("release_sequence"), "release_sequence", maximum=_MAX_SEQUENCE),
            release_id=_text(value.get("release_id"), "release_id", maximum=200),
            row_sha256=_sha(value.get("row_sha256"), "row_sha256"),
            evidence_locator=_locator(value.get("evidence_locator"), "observation evidence_locator"),
        )


# A source-local current state is represented by the same source-native shape
# as an event.  Its operation is the latest operation applied for the key.
NppesWeeklySourceState: TypeAlias = NppesWeeklyObservation


@dataclass(frozen=True, slots=True)
class NppesWeeklyReceipt:
    """Secret-free deterministic receipt for one weekly change attempt."""

    receipt_id: str
    source_id: str
    source_url: str
    release_id: str
    release_label: str
    week_start: str
    week_end: str
    release_sequence: int
    release_sha256: str
    probe_state: NppesWeeklyProbeState
    state: NppesWeeklyOutcomeState
    final_url: str | None
    http_status: int | None
    recorded_at: str
    evidence_locator: str
    files: tuple[NppesFileReceipt, ...]
    applied_count: int = 0
    late_count: int = 0
    out_of_order_count: int = 0
    unchanged_count: int = 0
    missing_file_count: int = 0
    deactivation_count: int = 0
    applied_samples: tuple[NppesWeeklyObservation, ...] = ()
    late_samples: tuple[NppesWeeklyObservation, ...] = ()
    deactivation_opportunities: tuple[NppesDeactivationOpportunity, ...] = ()
    current_projection_preserved: Literal[True] = True
    failure_code: str | None = None
    receipt_sha256: str | None = None
    schema_version: str = NPPES_WEEKLY_RECEIPT_SCHEMA
    record_type: Literal["nppes_weekly_receipt"] = "nppes_weekly_receipt"
    producer: str = NPPES_WEEKLY_PRODUCER

    def __post_init__(self) -> None:
        if self.schema_version != NPPES_WEEKLY_RECEIPT_SCHEMA or self.record_type != "nppes_weekly_receipt":
            raise NppesWeeklyProducerError("unsupported NPPES weekly receipt schema")
        if self.producer != NPPES_WEEKLY_PRODUCER:
            raise NppesWeeklyProducerError("unsupported NPPES weekly receipt producer")
        if _RECEIPT_ID.fullmatch(self.receipt_id) is None:
            raise NppesWeeklyProducerError("weekly receipt_id is malformed")
        if self.source_id != NPPES_SOURCE_ID:
            raise NppesWeeklyProducerError("weekly receipt source_id must be NPPES")
        _https(self.source_url, "receipt source_url")
        if _RELEASE_ID.fullmatch(self.release_id) is None:
            raise NppesWeeklyProducerError("weekly receipt release_id is malformed")
        _text(self.release_label, "release_label", maximum=200)
        start = _date_value(self.week_start, "week_start")
        end = _date_value(self.week_end, "week_end")
        if start > end:
            raise NppesWeeklyProducerError("week_start cannot be after week_end")
        _integer(self.release_sequence, "release_sequence", maximum=_MAX_SEQUENCE)
        _sha(self.release_sha256, "release_sha256")
        if self.probe_state not in {"changed", "not_modified", "failed_probe"}:
            raise NppesWeeklyProducerError("weekly receipt probe_state is unsupported")
        if self.state not in {"changed", "no_op", "failed_probe", "schema_drift", "replayed", "blocked"}:
            raise NppesWeeklyProducerError("weekly receipt state is unsupported")
        if self.final_url is not None:
            _https(self.final_url, "receipt final_url")
        if self.http_status is not None:
            _integer(self.http_status, "http_status", minimum=100, maximum=599)
        _timestamp(self.recorded_at, "recorded_at")
        _locator(self.evidence_locator, "receipt evidence_locator")
        if self.current_projection_preserved is not True:
            raise NppesWeeklyProducerError("weekly receipt must preserve current projection")
        seen: set[NppesFileKind] = set()
        for item in self.files:
            if not isinstance(item, NppesFileReceipt):
                raise NppesWeeklyProducerError("weekly receipt files must be typed values")
            if item.file_kind in seen:
                raise NppesWeeklyProducerError(f"duplicate weekly receipt file kind: {item.file_kind}")
            seen.add(item.file_kind)
        for item in self.applied_samples + self.late_samples:
            if not isinstance(item, NppesWeeklyObservation):
                raise NppesWeeklyProducerError("weekly receipt samples must be typed observations")
        if len(self.applied_samples) > MAX_IDENTIFIER_SAMPLES:
            raise NppesWeeklyProducerError("applied_samples exceeds its bound")
        if len(self.late_samples) > MAX_IDENTIFIER_SAMPLES:
            raise NppesWeeklyProducerError("late_samples exceeds its bound")
        _integer(self.applied_count, "applied_count")
        _integer(self.late_count, "late_count")
        _integer(self.out_of_order_count, "out_of_order_count")
        _integer(self.unchanged_count, "unchanged_count")
        _integer(self.missing_file_count, "missing_file_count")
        _integer(self.deactivation_count, "deactivation_count")
        if len(self.deactivation_opportunities) > MAX_DEACTIVATION_SAMPLES:
            raise NppesWeeklyProducerError("deactivation_opportunities exceeds its bound")
        for item in self.deactivation_opportunities:
            if not isinstance(item, NppesDeactivationOpportunity):
                raise NppesWeeklyProducerError("deactivation opportunities must be typed values")
        if self.deactivation_count < len(self.deactivation_opportunities):
            raise NppesWeeklyProducerError("deactivation_count cannot be below sampled opportunities")
        if self.failure_code is not None:
            _text(self.failure_code, "failure_code", maximum=120)
        computed = _canonical(self._payload())
        if self.receipt_sha256 is not None and self.receipt_sha256 != computed:
            raise NppesWeeklyProducerError("receipt_sha256 does not match weekly receipt payload")
        object.__setattr__(self, "receipt_sha256", computed)

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "record_type": self.record_type,
            "producer": self.producer,
            "receipt_id": self.receipt_id,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "release_id": self.release_id,
            "release_label": self.release_label,
            "week_start": self.week_start,
            "week_end": self.week_end,
            "release_sequence": self.release_sequence,
            "release_sha256": self.release_sha256,
            "probe_state": self.probe_state,
            "state": self.state,
            "final_url": self.final_url,
            "http_status": self.http_status,
            "recorded_at": self.recorded_at,
            "evidence_locator": self.evidence_locator,
            "files": [item.as_dict() for item in self.files],
            "applied_count": self.applied_count,
            "late_count": self.late_count,
            "out_of_order_count": self.out_of_order_count,
            "unchanged_count": self.unchanged_count,
            "missing_file_count": self.missing_file_count,
            "deactivation_count": self.deactivation_count,
            "applied_samples": [item.as_dict() for item in self.applied_samples],
            "late_samples": [item.as_dict() for item in self.late_samples],
            "deactivation_opportunities": [item.as_dict() for item in self.deactivation_opportunities],
            "current_projection_preserved": self.current_projection_preserved,
            "failure_code": self.failure_code,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self._payload(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "NppesWeeklyReceipt":
        allowed = {
            "schema_version",
            "record_type",
            "producer",
            "receipt_id",
            "source_id",
            "source_url",
            "release_id",
            "release_label",
            "week_start",
            "week_end",
            "release_sequence",
            "release_sha256",
            "probe_state",
            "state",
            "final_url",
            "http_status",
            "recorded_at",
            "evidence_locator",
            "files",
            "applied_count",
            "late_count",
            "out_of_order_count",
            "unchanged_count",
            "missing_file_count",
            "deactivation_count",
            "applied_samples",
            "late_samples",
            "deactivation_opportunities",
            "current_projection_preserved",
            "failure_code",
            "receipt_sha256",
        }
        unknown = set(value) - allowed
        if unknown:
            raise NppesWeeklyProducerError(f"unknown weekly receipt fields: {sorted(unknown)}")
        raw_files = value.get("files", [])
        if not isinstance(raw_files, list):
            raise NppesWeeklyProducerError("weekly receipt files must be an array")
        files = tuple(_file_receipt_from_mapping(item) for item in raw_files if isinstance(item, Mapping))
        if len(files) != len(raw_files):
            raise NppesWeeklyProducerError("weekly receipt files must contain mappings")
        applied_samples = _observations_from_mapping(value.get("applied_samples", []), "applied_samples")
        late_samples = _observations_from_mapping(value.get("late_samples", []), "late_samples")
        opportunities = _opportunities_from_mapping(value.get("deactivation_opportunities", []))
        return cls(
            schema_version=cast(str, value.get("schema_version", NPPES_WEEKLY_RECEIPT_SCHEMA)),
            record_type=cast(Literal["nppes_weekly_receipt"], value.get("record_type", "nppes_weekly_receipt")),
            producer=cast(str, value.get("producer", NPPES_WEEKLY_PRODUCER)),
            receipt_id=_text(value.get("receipt_id"), "receipt_id", maximum=200),
            source_id=_text(value.get("source_id"), "source_id", maximum=200),
            source_url=_https(value.get("source_url"), "receipt source_url"),
            release_id=_text(value.get("release_id"), "release_id", maximum=200),
            release_label=_text(value.get("release_label"), "release_label", maximum=200),
            week_start=_date_value(value.get("week_start"), "week_start"),
            week_end=_date_value(value.get("week_end"), "week_end"),
            release_sequence=_integer(value.get("release_sequence"), "release_sequence", maximum=_MAX_SEQUENCE),
            release_sha256=_sha(value.get("release_sha256"), "release_sha256"),
            probe_state=cast(NppesWeeklyProbeState, value.get("probe_state")),
            state=cast(NppesWeeklyOutcomeState, value.get("state")),
            final_url=cast(str | None, value.get("final_url")),
            http_status=cast(int | None, value.get("http_status")),
            recorded_at=_text(value.get("recorded_at"), "recorded_at", maximum=80),
            evidence_locator=_locator(value.get("evidence_locator"), "receipt evidence_locator"),
            files=files,
            applied_count=_integer(value.get("applied_count", 0), "applied_count"),
            late_count=_integer(value.get("late_count", 0), "late_count"),
            out_of_order_count=_integer(value.get("out_of_order_count", 0), "out_of_order_count"),
            unchanged_count=_integer(value.get("unchanged_count", 0), "unchanged_count"),
            missing_file_count=_integer(value.get("missing_file_count", 0), "missing_file_count"),
            deactivation_count=_integer(value.get("deactivation_count", 0), "deactivation_count"),
            applied_samples=applied_samples,
            late_samples=late_samples,
            deactivation_opportunities=opportunities,
            current_projection_preserved=cast(Literal[True], value.get("current_projection_preserved", True)),
            failure_code=cast(str | None, value.get("failure_code")),
            receipt_sha256=_optional_sha(value.get("receipt_sha256"), "receipt_sha256"),
        )


@dataclass(slots=True)
class _ParsedWeeklyFile:
    file_kind: NppesFileKind
    source_file_name: str
    evidence_locator: str
    release: NppesWeeklyRelease
    decoder: codecs.IncrementalDecoder
    text_buffer: str = ""
    header: tuple[str, ...] | None = None
    header_indexes: dict[str, int] | None = None
    line_number: int = 0
    row_count: int = 0
    identifier_count: int = 0
    deactivation_count: int = 0
    source_row_ids: set[str] | None = None
    observations: list[NppesWeeklyObservation] | None = None
    opportunities: list[NppesDeactivationOpportunity] | None = None
    row_digest: _Digest | None = None

    def __post_init__(self) -> None:
        self.source_row_ids = set()
        self.observations = []
        self.opportunities = []
        self.row_digest = sha256()

    def feed(self, chunk: bytes) -> None:
        try:
            decoded = self.decoder.decode(chunk, False)
        except UnicodeDecodeError as exc:
            raise NppesWeeklySchemaDriftError("weekly source file is not valid UTF-8") from exc
        self.text_buffer += decoded
        if len(self.text_buffer.encode("utf-8")) > _MAX_ROW_BYTES:
            raise NppesWeeklySchemaDriftError("weekly source row exceeds bounded parser line size")
        while "\n" in self.text_buffer:
            line, self.text_buffer = self.text_buffer.split("\n", 1)
            self._parse_line(line.rstrip("\r"))
            if len(self.text_buffer.encode("utf-8")) > _MAX_ROW_BYTES:
                raise NppesWeeklySchemaDriftError("weekly source row exceeds bounded parser line size")

    def finish(self) -> None:
        try:
            self.text_buffer += self.decoder.decode(b"", True)
        except UnicodeDecodeError as exc:
            raise NppesWeeklySchemaDriftError("weekly source file ends with incomplete UTF-8") from exc
        if self.text_buffer:
            self._parse_line(self.text_buffer.rstrip("\r"))
        if self.header is None:
            raise NppesWeeklySchemaDriftError("weekly source file is missing a CSV header")

    def _parse_line(self, line: str) -> None:
        self.line_number += 1
        if not line.strip():
            return
        try:
            values = next(csv.reader([line]))
        except (csv.Error, StopIteration) as exc:
            raise NppesWeeklySchemaDriftError(f"malformed weekly CSV row at line {self.line_number}") from exc
        if self.header is None:
            self._set_header(values)
            return
        if len(values) != len(self.header):
            raise NppesWeeklySchemaDriftError(f"weekly CSV column count changed at line {self.line_number}")
        assert self.header_indexes is not None
        npi = values[self.header_indexes["npi"]]
        if _NPI.fullmatch(npi) is None:
            raise NppesWeeklySchemaDriftError(f"weekly NPI is not ten ASCII digits at line {self.line_number}")
        source_row_id = self._value(values, "source_row_id") or f"{self.file_kind}:{self.line_number}"
        assert self.source_row_ids is not None
        if _SOURCE_ROW_ID.fullmatch(source_row_id) is None:
            raise NppesWeeklySchemaDriftError(f"weekly source_row_id is malformed at line {self.line_number}")
        if source_row_id in self.source_row_ids:
            raise NppesWeeklySchemaDriftError(f"duplicate weekly source_row_id at line {self.line_number}")
        self.source_row_ids.add(source_row_id)
        deactivation_date = self._value(values, "deactivation_date")
        effective_date = self._value(values, "effective_date") or deactivation_date or self.release.week_end
        operation = self._operation(values, deactivation_date)
        if operation == "deactivate":
            if deactivation_date is None:
                raise NppesWeeklySchemaDriftError(
                    f"weekly deactivation row lacks deactivation_date at line {self.line_number}"
                )
            effective_date = deactivation_date
        effective_date = _date_value(effective_date, "effective_date")
        row_payload = {
            "file_kind": self.file_kind,
            "source_file_name": self.source_file_name,
            "row_number": self.line_number,
            "source_row_id": source_row_id,
            "npi": npi,
            "operation": operation,
            "effective_date": effective_date,
            "release_sequence": self.release.release_sequence,
            "deactivation_date": deactivation_date,
            "reference_id": self._value(values, "reference_id"),
        }
        encoded = json.dumps(row_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        assert self.row_digest is not None
        self.row_digest.update(encoded + b"\n")
        observation = NppesWeeklyObservation(
            source_key=f"{self.file_kind}:{npi}",
            file_kind=self.file_kind,
            source_file_name=self.source_file_name,
            npi=npi,
            source_row_id=source_row_id,
            row_number=self.line_number,
            operation=operation,
            effective_date=effective_date,
            release_sequence=self.release.release_sequence,
            release_id=self.release.release_id,
            row_sha256="sha256:" + sha256(encoded).hexdigest(),
            evidence_locator=self.evidence_locator,
        )
        assert self.observations is not None
        self.observations.append(observation)
        self.row_count += 1
        self.identifier_count += 1
        if operation == "deactivate":
            self.deactivation_count += 1
            assert self.opportunities is not None
            self.opportunities.append(
                NppesDeactivationOpportunity(
                    npi=npi,
                    source_row_id=source_row_id,
                    deactivation_date=effective_date,
                    evidence_locator=self.evidence_locator,
                )
            )

    def _set_header(self, values: list[str]) -> None:
        normalized = tuple(_HEADER.sub("_", value.strip().casefold()).strip("_") for value in values)
        if any(not value for value in normalized):
            raise NppesWeeklySchemaDriftError("weekly source file contains a blank CSV header")
        if len(set(normalized)) != len(normalized):
            raise NppesWeeklySchemaDriftError("weekly source file contains duplicate CSV headers")
        if "npi" not in normalized and "npi_number" not in normalized:
            raise NppesWeeklySchemaDriftError(f"{self.file_kind} weekly file is missing the NPI column")
        npi_key = "npi" if "npi" in normalized else "npi_number"
        indexes = {"npi": normalized.index(npi_key)}
        aliases = {
            "source_row_id": ("source_row_id", "source_record_id", "record_id"),
            "effective_date": ("effective_date", "effective_dt", "update_date", "updated_date", "last_update_date"),
            "deactivation_date": ("deactivation_date", "deactivation_dt", "deactivated_date"),
            "operation": ("operation", "action", "change_type", "status"),
            "reference_id": ("reference_id", "other_name", "endpoint_id", "location_id"),
        }
        for logical, candidates in aliases.items():
            for candidate in candidates:
                if candidate in normalized:
                    indexes[logical] = normalized.index(candidate)
                    break
        if self.file_kind == "deactivation" and "deactivation_date" not in indexes:
            raise NppesWeeklySchemaDriftError("deactivation weekly file is missing deactivation_date")
        self.header = normalized
        self.header_indexes = indexes

    def _value(self, values: list[str], logical: str) -> str | None:
        assert self.header_indexes is not None
        index = self.header_indexes.get(logical)
        if index is None:
            return None
        value = values[index].strip()
        return value if value else None

    def _operation(self, values: list[str], deactivation_date: str | None) -> NppesWeeklyOperation:
        if self.file_kind == "deactivation" or deactivation_date is not None:
            return "deactivate"
        raw = self._value(values, "operation")
        if raw is None:
            return "upsert"
        normalized = raw.casefold().replace("-", "_").replace(" ", "_")
        if normalized in {"upsert", "update", "updated", "insert", "add", "added", "active", "changed"}:
            return "upsert"
        if normalized in {
            "deactivate",
            "deactivated",
            "deactivation",
            "delete",
            "deleted",
            "inactive",
            "terminated",
            "terminate",
            "remove",
            "removed",
        }:
            return "deactivate"
        raise NppesWeeklySchemaDriftError(f"unsupported weekly operation at line {self.line_number}")

    def row_digest_value(self) -> str:
        assert self.row_digest is not None
        return "sha256:" + self.row_digest.hexdigest()


@dataclass(frozen=True, slots=True)
class _MergeResult:
    applied_count: int
    late_count: int
    out_of_order_count: int
    unchanged_count: int
    applied_samples: tuple[NppesWeeklyObservation, ...]
    late_samples: tuple[NppesWeeklyObservation, ...]


def _event_order(observation: NppesWeeklyObservation) -> tuple[str, int, str, str, int]:
    return (
        observation.effective_date,
        observation.release_sequence,
        observation.source_row_id,
        observation.file_kind,
        observation.row_number,
    )


def _merge(
    observations: Iterable[NppesWeeklyObservation],
    previous_state: Mapping[str, NppesWeeklySourceState],
) -> _MergeResult:
    candidates: dict[str, NppesWeeklyObservation] = {}
    last_seen: dict[str, tuple[str, int, str, str, int]] = {}
    late_count = 0
    out_of_order_count = 0
    late_samples: list[NppesWeeklyObservation] = []
    for observation in observations:
        order = _event_order(observation)
        prior_seen = last_seen.get(observation.source_key)
        if prior_seen is not None and order < prior_seen:
            out_of_order_count += 1
        last_seen[observation.source_key] = order
        previous = previous_state.get(observation.source_key)
        if previous is not None and order < _event_order(previous):
            late_count += 1
            if len(late_samples) < MAX_IDENTIFIER_SAMPLES:
                late_samples.append(observation)
            continue
        candidate = candidates.get(observation.source_key)
        if candidate is None or order > _event_order(candidate):
            candidates[observation.source_key] = observation
    applied_count = 0
    unchanged_count = 0
    applied_samples: list[NppesWeeklyObservation] = []
    for key in sorted(candidates):
        candidate = candidates[key]
        previous = previous_state.get(key)
        if previous is not None:
            candidate_order = _event_order(candidate)
            previous_order = _event_order(previous)
            if candidate_order < previous_order:
                continue
            if candidate_order == previous_order:
                unchanged_count += 1
                continue
        applied_count += 1
        if len(applied_samples) < MAX_IDENTIFIER_SAMPLES:
            applied_samples.append(candidate)
    return _MergeResult(
        applied_count=applied_count,
        late_count=late_count,
        out_of_order_count=out_of_order_count,
        unchanged_count=unchanged_count,
        applied_samples=tuple(applied_samples),
        late_samples=tuple(late_samples),
    )


def _timestamp_for_receipt(value: datetime | str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise NppesWeeklyProducerError("recorded_at datetime must include a timezone")
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and value:
        return value
    raise NppesWeeklyProducerError("recorded_at must be a timezone-aware datetime or ISO timestamp")


def _coerce_catalog(value: NppesCatalog | NppesCatalogEntry | Mapping[str, object]) -> NppesCatalogEntry:
    if isinstance(value, NppesCatalogEntry):
        return value
    if isinstance(value, NppesCatalog):
        return value.source
    if not isinstance(value, Mapping):
        raise NppesWeeklyProducerError("catalog must be an NppesCatalog, NppesCatalogEntry, or mapping")
    if "sources" in value:
        return NppesCatalog.from_mapping(value).source
    return NppesCatalogEntry(
        source_id=cast(str, value.get("source_id", NPPES_SOURCE_ID)),
        title=cast(str, value.get("title", "")),
        source_url=cast(str, value.get("source_url", "")),
        release_locator=cast(str, value.get("release_locator", "")),
        change_mode=cast(Literal["release_metadata", "content_hash", "last_modified"], value.get("change_mode")),
        rights_status=cast(Literal["approved_public", "pending_review", "blocked"], value.get("rights_status")),
        enabled=cast(bool, value.get("enabled", True)),
        max_bytes=cast(int, value.get("max_bytes", MAX_NPPES_BYTES)),
        max_chunks=cast(int, value.get("max_chunks", MAX_NPPES_CHUNKS)),
        max_seconds=cast(int, value.get("max_seconds", MAX_NPPES_SECONDS)),
        max_chunk_bytes=cast(int, value.get("max_chunk_bytes", MAX_NPPES_CHUNK_BYTES)),
    )


def _coerce_release(value: NppesWeeklyRelease | Mapping[str, object]) -> NppesWeeklyRelease:
    if isinstance(value, NppesWeeklyRelease):
        return value
    if not isinstance(value, Mapping):
        raise NppesWeeklyProducerError("weekly release must be NppesWeeklyRelease or mapping")
    return NppesWeeklyRelease.from_mapping(value)


def _coerce_receipt(value: NppesWeeklyReceipt | Mapping[str, object] | None) -> NppesWeeklyReceipt | None:
    if value is None:
        return None
    if isinstance(value, NppesWeeklyReceipt):
        return value
    if not isinstance(value, Mapping):
        raise NppesWeeklyProducerError("previous weekly receipt must be typed or a mapping")
    return NppesWeeklyReceipt.from_mapping(value)


def _coerce_state(value: NppesWeeklySourceState | Mapping[str, object]) -> NppesWeeklySourceState:
    if isinstance(value, NppesWeeklyObservation):
        return value
    if not isinstance(value, Mapping):
        raise NppesWeeklyProducerError("previous weekly state values must be observations or mappings")
    return NppesWeeklyObservation.from_mapping(value)


def _same_file_identity(previous: tuple[NppesFileReceipt, ...], current: tuple[NppesFileReceipt, ...]) -> bool:
    old = {item.file_kind: item for item in previous}
    new = {item.file_kind: item for item in current}
    if old.keys() != new.keys():
        return False
    for kind in old:
        prior = old[kind]
        latest = new[kind]
        if (
            prior.state != latest.state
            or prior.content_sha256 != latest.content_sha256
            or prior.byte_length != latest.byte_length
            or prior.chunk_count != latest.chunk_count
            or prior.row_count != latest.row_count
            or prior.row_sha256 != latest.row_sha256
        ):
            return False
    return True


def _file_receipt_from_mapping(value: Mapping[str, object]) -> NppesFileReceipt:
    allowed = {
        "file_kind",
        "source_file_name",
        "source_url",
        "evidence_locator",
        "state",
        "content_sha256",
        "byte_length",
        "chunk_count",
        "row_count",
        "identifier_count",
        "row_sha256",
        "identifier_samples",
        "deactivation_count",
        "error_code",
    }
    unknown = set(value) - allowed
    if unknown:
        raise NppesWeeklyProducerError(f"unknown weekly file receipt fields: {sorted(unknown)}")
    samples = value.get("identifier_samples", [])
    if not isinstance(samples, list):
        raise NppesWeeklyProducerError("weekly identifier_samples must be an array")
    return NppesFileReceipt(
        file_kind=cast(NppesFileKind, value.get("file_kind")),
        source_file_name=_text(value.get("source_file_name"), "source_file_name", maximum=256),
        source_url=_https(value.get("source_url"), "file receipt source_url"),
        evidence_locator=_locator(value.get("evidence_locator"), "file receipt evidence_locator"),
        state=cast(NppesFileState, value.get("state")),
        content_sha256=_optional_sha(value.get("content_sha256"), "content_sha256"),
        byte_length=_integer(value.get("byte_length", 0), "byte_length"),
        chunk_count=_integer(value.get("chunk_count", 0), "chunk_count"),
        row_count=_integer(value.get("row_count", 0), "row_count"),
        identifier_count=_integer(value.get("identifier_count", 0), "identifier_count"),
        row_sha256=_optional_sha(value.get("row_sha256"), "row_sha256"),
        identifier_samples=tuple(
            _text(item, f"identifier_samples[{index}]", maximum=20) for index, item in enumerate(samples)
        ),
        deactivation_count=_integer(value.get("deactivation_count", 0), "deactivation_count"),
        error_code=cast(str | None, value.get("error_code")),
    )


def _observations_from_mapping(value: object, name: str) -> tuple[NppesWeeklyObservation, ...]:
    if not isinstance(value, list):
        raise NppesWeeklyProducerError(f"weekly {name} must be an array")
    result: list[NppesWeeklyObservation] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise NppesWeeklyProducerError(f"weekly {name} must contain mappings")
        result.append(NppesWeeklyObservation.from_mapping(item))
    return tuple(result)


def _opportunities_from_mapping(value: object) -> tuple[NppesDeactivationOpportunity, ...]:
    if not isinstance(value, list):
        raise NppesWeeklyProducerError("weekly deactivation_opportunities must be an array")
    result: list[NppesDeactivationOpportunity] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise NppesWeeklyProducerError("weekly deactivation opportunities must contain mappings")
        allowed = {"npi", "source_row_id", "deactivation_date", "evidence_locator", "review_state"}
        unknown = set(item) - allowed
        if unknown:
            raise NppesWeeklyProducerError(f"unknown weekly deactivation fields: {sorted(unknown)}")
        result.append(
            NppesDeactivationOpportunity(
                npi=_text(item.get("npi"), "npi", maximum=10),
                source_row_id=_text(item.get("source_row_id"), "source_row_id", maximum=200),
                deactivation_date=_date_value(item.get("deactivation_date"), "deactivation_date"),
                evidence_locator=_locator(item.get("evidence_locator"), "evidence_locator"),
                review_state=cast(Literal["review_required"], item.get("review_state", "review_required")),
            )
        )
    return tuple(result)


def _host(value: str, name: str) -> str:
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise NppesWeeklyProducerError(f"{name} has a malformed authority") from exc
    if parsed.scheme != "https" or hostname is None:
        raise NppesWeeklyProducerError(f"{name} must be an HTTPS URL")
    return hostname.casefold().rstrip(".")


def _validate_catalog_binding(catalog: NppesCatalogEntry, release: NppesWeeklyRelease) -> None:
    approved_hosts = {
        _host(catalog.source_url, "catalog source_url"),
        _host(catalog.release_locator, "catalog release_locator"),
    }
    urls: list[tuple[str, str]] = [("weekly release source_url", release.source_url)]
    if release.final_url is not None:
        urls.append(("weekly release final_url", release.final_url))
    if release.evidence_locator.startswith("https://"):
        urls.append(("weekly release evidence_locator", release.evidence_locator))
    for descriptor in release.files:
        urls.append((f"{descriptor.file_kind} weekly file source_url", descriptor.source_url))
        if descriptor.evidence_locator.startswith("https://"):
            urls.append((f"{descriptor.file_kind} weekly file evidence_locator", descriptor.evidence_locator))
    for name, value in urls:
        if _host(value, name) not in approved_hosts:
            raise NppesWeeklyProducerError(f"{name} host is not approved by the NPPES catalog")


def _unavailable_receipt(descriptor: NppesFileDescriptor) -> NppesFileReceipt:
    return NppesFileReceipt(
        file_kind=descriptor.file_kind,
        source_file_name=descriptor.source_file_name,
        source_url=descriptor.source_url,
        evidence_locator=descriptor.evidence_locator,
        state=cast(NppesFileState, descriptor.availability),
        content_sha256=None,
        byte_length=0,
        chunk_count=0,
        row_count=0,
        identifier_count=0,
        row_sha256=None,
    )


def _omitted_receipt(kind: NppesFileKind, release: NppesWeeklyRelease) -> NppesFileReceipt:
    return NppesFileReceipt(
        file_kind=kind,
        source_file_name=f"omitted_weekly_{kind}.csv",
        source_url=release.source_url,
        evidence_locator=release.evidence_locator,
        state="unavailable_public",
        content_sha256=None,
        byte_length=0,
        chunk_count=0,
        row_count=0,
        identifier_count=0,
        row_sha256=None,
        error_code="file_descriptor_omitted",
    )


def _not_consumed_receipt(descriptor: NppesFileDescriptor) -> NppesFileReceipt:
    return NppesFileReceipt(
        file_kind=descriptor.file_kind,
        source_file_name=descriptor.source_file_name,
        source_url=descriptor.source_url,
        evidence_locator=descriptor.evidence_locator,
        state="not_consumed",
        content_sha256=None,
        byte_length=0,
        chunk_count=0,
        row_count=0,
        identifier_count=0,
        row_sha256=None,
        error_code=f"{descriptor.file_kind}_stream_missing",
    )


def _consume_file(
    descriptor: NppesFileDescriptor,
    release: NppesWeeklyRelease,
    chunks: WeeklyFileChunks,
    budget: NppesStreamBudget,
    *,
    clock: Callable[[], float],
) -> tuple[NppesFileReceipt, tuple[NppesWeeklyObservation, ...], tuple[NppesDeactivationOpportunity, ...]]:
    parsed = _ParsedWeeklyFile(
        file_kind=descriptor.file_kind,
        source_file_name=descriptor.source_file_name,
        evidence_locator=descriptor.evidence_locator,
        release=release,
        decoder=codecs.getincrementaldecoder("utf-8-sig")(),
    )
    started = clock()
    digest = sha256()
    byte_length = 0
    chunk_count = 0
    try:
        for chunk in chunks:
            if not isinstance(chunk, bytes):
                raise NppesWeeklySchemaDriftError("weekly source chunks must be bytes")
            if not chunk:
                raise NppesWeeklySchemaDriftError("weekly source chunks must not be empty")
            if clock() - started > budget.max_seconds:
                raise NppesWeeklyStreamInterrupted("weekly source exceeded its time bound")
            if chunk_count >= budget.max_chunks or len(chunk) > budget.max_chunk_bytes:
                raise NppesWeeklyStreamInterrupted("weekly source exceeded its chunk bound")
            if byte_length + len(chunk) > budget.max_bytes:
                raise NppesWeeklyStreamInterrupted("weekly source exceeded its byte bound")
            parsed.feed(chunk)
            digest.update(chunk)
            byte_length += len(chunk)
            chunk_count += 1
            if clock() - started > budget.max_seconds:
                raise NppesWeeklyStreamInterrupted("weekly source exceeded its time bound")
        parsed.finish()
    except NppesWeeklyStreamInterrupted:
        return (
            _file_receipt(
                descriptor,
                state="interrupted",
                digest=digest,
                byte_length=byte_length,
                chunk_count=chunk_count,
                parsed=parsed,
                error_code="stream_interrupted",
            ),
            tuple(parsed.observations or ()),
            tuple(parsed.opportunities or ()),
        )
    except NppesContractError as exc:
        return (
            _file_receipt(
                descriptor,
                state="schema_drift",
                digest=digest,
                byte_length=byte_length,
                chunk_count=chunk_count,
                parsed=parsed,
                error_code=_error_code(exc),
            ),
            tuple(parsed.observations or ()),
            tuple(parsed.opportunities or ()),
        )
    except Exception:  # pragma: no cover - caller-owned iterable failures vary by transport
        return (
            _file_receipt(
                descriptor,
                state="schema_drift",
                digest=digest,
                byte_length=byte_length,
                chunk_count=chunk_count,
                parsed=parsed,
                error_code="stream_error",
            ),
            tuple(parsed.observations or ()),
            tuple(parsed.opportunities or ()),
        )
    content_sha256 = "sha256:" + digest.hexdigest()
    if descriptor.expected_sha256 is not None and descriptor.expected_sha256 != content_sha256:
        return (
            _file_receipt(
                descriptor,
                state="schema_drift",
                digest=digest,
                byte_length=byte_length,
                chunk_count=chunk_count,
                parsed=parsed,
                error_code="content_hash_mismatch",
            ),
            tuple(parsed.observations or ()),
            tuple(parsed.opportunities or ()),
        )
    return (
        _file_receipt(
            descriptor,
            state="accepted",
            digest=digest,
            byte_length=byte_length,
            chunk_count=chunk_count,
            parsed=parsed,
            error_code=None,
        ),
        tuple(parsed.observations or ()),
        tuple(parsed.opportunities or ()),
    )


def _file_receipt(
    descriptor: NppesFileDescriptor,
    *,
    state: NppesFileState,
    digest: _Digest,
    byte_length: int,
    chunk_count: int,
    parsed: _ParsedWeeklyFile,
    error_code: str | None,
) -> NppesFileReceipt:
    return NppesFileReceipt(
        file_kind=descriptor.file_kind,
        source_file_name=descriptor.source_file_name,
        source_url=descriptor.source_url,
        evidence_locator=descriptor.evidence_locator,
        state=state,
        content_sha256="sha256:" + digest.hexdigest(),
        byte_length=byte_length,
        chunk_count=chunk_count,
        row_count=parsed.row_count,
        identifier_count=parsed.identifier_count,
        row_sha256=parsed.row_digest_value(),
        identifier_samples=tuple(item.npi for item in (parsed.observations or ())[:MAX_IDENTIFIER_SAMPLES]),
        deactivation_count=parsed.deactivation_count,
        error_code=error_code,
    )


def _error_code(exc: BaseException) -> str:
    message = str(exc).casefold()
    if "npi" in message:
        return "invalid_npi"
    if "header" in message or "column" in message:
        return "schema_drift"
    if "deactivation" in message:
        return "deactivation_schema_drift"
    if "operation" in message:
        return "operation_schema_drift"
    if "row" in message:
        return "row_schema_drift"
    return "schema_drift"


def _receipt_id(
    release: NppesWeeklyRelease,
    state: NppesWeeklyOutcomeState,
    files: tuple[NppesFileReceipt, ...],
    merge: _MergeResult,
) -> str:
    payload = {
        "release_sha256": release.release_sha256,
        "state": state,
        "files": [item.as_dict() for item in files],
        "applied_count": merge.applied_count,
        "late_count": merge.late_count,
        "out_of_order_count": merge.out_of_order_count,
        "unchanged_count": merge.unchanged_count,
        "applied_samples": [item.as_dict() for item in merge.applied_samples],
        "late_samples": [item.as_dict() for item in merge.late_samples],
    }
    return "receipt:nppes-weekly:" + _canonical(payload).removeprefix("sha256:")[:32]


def _empty_receipt(
    release: NppesWeeklyRelease,
    *,
    state: NppesWeeklyOutcomeState,
    recorded_at: str,
    failure_code: str | None = None,
) -> NppesWeeklyReceipt:
    empty_merge = _MergeResult(0, 0, 0, 0, (), ())
    return NppesWeeklyReceipt(
        receipt_id=_receipt_id(release, state, (), empty_merge),
        source_id=release.source_id,
        source_url=release.source_url,
        release_id=release.release_id,
        release_label=release.release_label,
        week_start=release.week_start,
        week_end=release.week_end,
        release_sequence=release.release_sequence,
        release_sha256=cast(str, release.release_sha256),
        probe_state=release.probe_state,
        state=state,
        final_url=release.final_url,
        http_status=release.http_status,
        recorded_at=recorded_at,
        evidence_locator=release.evidence_locator,
        files=(),
        failure_code=failure_code,
    )


class NppesWeeklyProducer:
    """Produce source-local weekly changes from bounded caller-owned streams."""

    def __init__(self, catalog: NppesCatalog | NppesCatalogEntry | Mapping[str, object]) -> None:
        self.catalog = _coerce_catalog(catalog)
        if not self.catalog.enabled or self.catalog.rights_status != "approved_public":
            raise NppesWeeklyProducerError("NPPES catalog registration is not enabled with approved public rights")

    def produce(
        self,
        release: NppesWeeklyRelease | Mapping[str, object],
        files: Mapping[NppesFileKind, WeeklyFileChunks] | None = None,
        *,
        previous: NppesWeeklyReceipt | Mapping[str, object] | None = None,
        previous_state: Mapping[str, NppesWeeklySourceState | Mapping[str, object]] | None = None,
        recorded_at: datetime | str | None = None,
        row_sink: WeeklyRowSink | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> NppesWeeklyReceipt:
        """Consume one weekly release without canonical or projection authority."""

        descriptor = _coerce_release(release)
        if descriptor.source_id != self.catalog.source_id:
            raise NppesWeeklyProducerError("weekly release source_id does not match catalog source_id")
        _validate_catalog_binding(self.catalog, descriptor)
        prior_receipt = _coerce_receipt(previous)
        timestamp = _timestamp_for_receipt(recorded_at)
        if descriptor.probe_state == "failed_probe":
            return _empty_receipt(descriptor, state="failed_probe", recorded_at=timestamp, failure_code="failed_probe")
        if descriptor.probe_state == "not_modified":
            return _empty_receipt(descriptor, state="no_op", recorded_at=timestamp)
        if files is None:
            raise NppesWeeklyProducerError("changed weekly release requires file streams")
        unknown = set(files) - set(_FILE_KINDS)
        if unknown:
            raise NppesWeeklyProducerError(f"unknown weekly file stream kinds: {sorted(unknown)}")
        descriptors = {item.file_kind: item for item in descriptor.files}
        undeclared = set(files).difference(descriptors.keys())
        if undeclared:
            raise NppesWeeklyProducerError(f"weekly file stream lacks a release descriptor: {sorted(undeclared)}")
        prior_successful_same_release = (
            prior_receipt is not None
            and prior_receipt.state in {"changed", "replayed"}
            and prior_receipt.release_sha256 == descriptor.release_sha256
        )
        receipts: list[NppesFileReceipt] = []
        observations: list[NppesWeeklyObservation] = []
        opportunities: list[NppesDeactivationOpportunity] = []
        first_failure: str | None = None
        budget = NppesStreamBudget(
            max_bytes=self.catalog.max_bytes,
            max_chunks=self.catalog.max_chunks,
            max_seconds=self.catalog.max_seconds,
            max_chunk_bytes=self.catalog.max_chunk_bytes,
        )
        for kind in _FILE_KINDS:
            file_descriptor = descriptors.get(kind)
            stream = files.get(kind)
            if file_descriptor is None:
                receipts.append(_omitted_receipt(kind, descriptor))
                continue
            if file_descriptor.availability != "present":
                if stream is not None:
                    raise NppesWeeklyProducerError(f"{kind} weekly file is unavailable but a stream was supplied")
                receipts.append(_unavailable_receipt(file_descriptor))
                continue
            if stream is None:
                receipts.append(_not_consumed_receipt(file_descriptor))
                first_failure = first_failure or f"{kind}_stream_missing"
                continue
            file_receipt, file_observations, file_opportunities = _consume_file(
                file_descriptor,
                descriptor,
                stream,
                budget,
                clock=clock,
            )
            receipts.append(file_receipt)
            observations.extend(file_observations)
            opportunities.extend(file_opportunities)
            if file_receipt.state != "accepted":
                first_failure = first_failure or file_receipt.error_code or "file_rejected"
        file_receipts = tuple(receipts)
        if first_failure is not None:
            outcome: NppesWeeklyOutcomeState = (
                "blocked"
                if any(item.state in {"interrupted", "not_consumed"} for item in file_receipts)
                else "schema_drift"
            )
            empty_merge = _MergeResult(0, 0, 0, 0, (), ())
            return self._receipt(
                descriptor,
                outcome=outcome,
                recorded_at=timestamp,
                files=file_receipts,
                opportunities=tuple(opportunities),
                merge=empty_merge,
                failure_code=first_failure,
            )
        state = _coerce_previous_state(previous_state)
        merge = _merge(observations, state)
        if row_sink is not None and not prior_successful_same_release:
            for observation in observations:
                try:
                    row_sink(observation)
                except Exception as exc:  # pragma: no cover - caller-owned sink behavior
                    raise NppesWeeklyProducerError("weekly row sink rejected source observation") from exc
        if prior_successful_same_release and prior_receipt is not None:
            if not _same_file_identity(prior_receipt.files, file_receipts):
                raise NppesWeeklyReplayConflictError("replayed weekly release has different file identity")
            outcome = "replayed"
        else:
            outcome = "changed"
        return self._receipt(
            descriptor,
            outcome=outcome,
            recorded_at=timestamp,
            files=file_receipts,
            opportunities=tuple(opportunities),
            merge=merge,
            failure_code=None,
        )

    def _receipt(
        self,
        release: NppesWeeklyRelease,
        *,
        outcome: NppesWeeklyOutcomeState,
        recorded_at: str,
        files: tuple[NppesFileReceipt, ...],
        opportunities: tuple[NppesDeactivationOpportunity, ...],
        merge: _MergeResult,
        failure_code: str | None,
    ) -> NppesWeeklyReceipt:
        deactivation_count = sum(item.deactivation_count for item in files)
        missing_file_count = sum(item.state in {"unavailable_public", "not_applicable"} for item in files)
        return NppesWeeklyReceipt(
            receipt_id=_receipt_id(release, outcome, files, merge),
            source_id=release.source_id,
            source_url=release.source_url,
            release_id=release.release_id,
            release_label=release.release_label,
            week_start=release.week_start,
            week_end=release.week_end,
            release_sequence=release.release_sequence,
            release_sha256=cast(str, release.release_sha256),
            probe_state=release.probe_state,
            state=outcome,
            final_url=release.final_url,
            http_status=release.http_status,
            recorded_at=recorded_at,
            evidence_locator=release.evidence_locator,
            files=files,
            applied_count=merge.applied_count,
            late_count=merge.late_count,
            out_of_order_count=merge.out_of_order_count,
            unchanged_count=merge.unchanged_count,
            missing_file_count=missing_file_count,
            deactivation_count=deactivation_count,
            applied_samples=merge.applied_samples,
            late_samples=merge.late_samples,
            deactivation_opportunities=opportunities[:MAX_DEACTIVATION_SAMPLES],
            failure_code=failure_code,
        )


def _coerce_previous_state(
    value: Mapping[str, NppesWeeklySourceState | Mapping[str, object]] | None,
) -> dict[str, NppesWeeklySourceState]:
    if value is None:
        return {}
    result: dict[str, NppesWeeklySourceState] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise NppesWeeklyProducerError("previous weekly state keys must be non-empty source keys")
        observation = _coerce_state(item)
        if observation.source_key != key:
            raise NppesWeeklyProducerError("previous weekly state key does not match observation source_key")
        result[key] = observation
    return result


def produce_nppes_weekly(
    catalog: NppesCatalog | NppesCatalogEntry | Mapping[str, object],
    release: NppesWeeklyRelease | Mapping[str, object],
    files: Mapping[NppesFileKind, WeeklyFileChunks] | None = None,
    *,
    previous: NppesWeeklyReceipt | Mapping[str, object] | None = None,
    previous_state: Mapping[str, NppesWeeklySourceState | Mapping[str, object]] | None = None,
    recorded_at: datetime | str | None = None,
    row_sink: WeeklyRowSink | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> NppesWeeklyReceipt:
    """Convenience wrapper for :class:`NppesWeeklyProducer`."""

    return NppesWeeklyProducer(catalog).produce(
        release,
        files,
        previous=previous,
        previous_state=previous_state,
        recorded_at=recorded_at,
        row_sink=row_sink,
        clock=clock,
    )


def validate_nppes_weekly_receipt(value: Mapping[str, object]) -> NppesWeeklyReceipt:
    """Parse and strictly validate a serialized weekly receipt."""

    return NppesWeeklyReceipt.from_mapping(value)


__all__ = [
    "NPPES_WEEKLY_PRODUCER",
    "NPPES_WEEKLY_RECEIPT_SCHEMA",
    "NPPES_WEEKLY_RELEASE_SCHEMA",
    "NppesWeeklyObservation",
    "NppesWeeklyOutcomeState",
    "NppesWeeklyProducer",
    "NppesWeeklyProducerError",
    "NppesWeeklyProbeState",
    "NppesWeeklyReceipt",
    "NppesWeeklyRelease",
    "NppesWeeklyReplayConflictError",
    "NppesWeeklySchemaDriftError",
    "NppesWeeklySourceState",
    "NppesWeeklyStreamInterrupted",
    "NppesWeeklyOperation",
    "WeeklyFileChunks",
    "WeeklyRowSink",
    "produce_nppes_weekly",
    "validate_nppes_weekly_receipt",
]
