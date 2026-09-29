"""Strict, source-scoped contracts for the NPPES V2 baseline producer.

The contracts in this module describe public NPPES release metadata and
bounded row evidence.  They deliberately stop short of a Toolkit provider or
facility identity: an NPI, endpoint, location, reference name, or
deactivation row remains a source-native observation for a later review lane.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
import ipaddress
import json
import re
from typing import Literal, Mapping, TypeAlias, cast
from urllib.parse import urlsplit


NppesFileKind: TypeAlias = Literal["provider", "location", "endpoint", "reference", "deactivation"]
NppesAvailability: TypeAlias = Literal["present", "unavailable_public", "not_applicable"]
NppesProbeState: TypeAlias = Literal["changed", "not_modified", "failed_probe"]
NppesOutcomeState: TypeAlias = Literal["changed", "no_op", "failed_probe", "schema_drift", "replayed", "blocked"]
NppesFileState: TypeAlias = Literal[
    "accepted", "unavailable_public", "not_applicable", "not_consumed", "interrupted", "schema_drift"
]

NPPES_SOURCE_ID = "source:nppes:registry"
NPPES_SOURCE_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination.html"
NPPES_SCHEMA_VERSION = "hdp.nppes-baseline.v2"
NPPES_RECORD_TYPE = "nppes_baseline_receipt"
NPPES_PRODUCER = "healthcare-data-mcp:nppes-baseline-producer"
NPPES_RECEIPT_SCHEMA = "hdp.nppes-baseline-receipt.v2"
NPPES_CATALOG_SCHEMA = "hdp.nppes-catalog.v2"
NPPES_RELEASE_SCHEMA = "hdp.nppes-release.v2"
MAX_NPPES_BYTES = 8 * 1024 * 1024 * 1024
MAX_NPPES_CHUNKS = 2_000_000
MAX_NPPES_SECONDS = 4 * 60 * 60
MAX_NPPES_CHUNK_BYTES = 8 * 1024 * 1024
MAX_IDENTIFIER_SAMPLES = 64
MAX_DEACTIVATION_SAMPLES = 256

_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:[a-z0-9][a-z0-9._:-]*$")
_RECEIPT_ID = re.compile(r"^receipt:[a-z0-9][a-z0-9._:-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_NPI = re.compile(r"^[0-9]{10}$")
_SOURCE_ROW_ID = re.compile(r"^[a-z][a-z0-9._:-]{0,199}$")
_SOURCE_FILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
_SCHEMA_VERSION = re.compile(r"^hdp\.[a-z0-9-]+\.v[0-9]+$")
_SAFE_LOCATOR = re.compile(r"^(?:https://|object://|parquet://|docs/|contracts/)[A-Za-z0-9._:/-]+$")
_MAX_TEXT = 500


class NppesContractError(ValueError):
    """Raised when a NPPES catalog, release, row, or receipt is unsafe."""


class NppesSchemaDriftError(NppesContractError):
    """Raised when a source file does not match the declared row contract."""


class NppesReplayConflictError(NppesContractError):
    """Raised when a release is replayed with different source bytes."""


def _text(value: object, name: str, *, maximum: int = _MAX_TEXT) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NppesContractError(f"{name} must be a non-empty string")
    if len(value) > maximum:
        raise NppesContractError(f"{name} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise NppesContractError(f"{name} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise NppesContractError(f"{name} contains malformed Unicode") from exc
    return value


def _https(value: object, name: str) -> str:
    text = _text(value, name, maximum=2048)
    parsed = urlsplit(text)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise NppesContractError(f"{name} must be an HTTPS URL without user information")
    try:
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise NppesContractError(f"{name} has a malformed authority") from exc
    if hostname is None:
        raise NppesContractError(f"{name} has a malformed authority")
    host = hostname.casefold().rstrip(".")
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise NppesContractError(f"{name} has an unsafe local authority")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None and (
        address.is_private or address.is_loopback or address.is_link_local or address.is_reserved
    ):
        raise NppesContractError(f"{name} has an unsafe private authority")
    return text


def _locator(value: object, name: str) -> str:
    text = _text(value, name)
    if _SAFE_LOCATOR.fullmatch(text) is None:
        raise NppesContractError(f"{name} must be a bounded evidence locator")
    return text


def _sha(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise NppesContractError(f"{name} must be sha256:<64 lowercase hex>")
    return value


def _optional_sha(value: object, name: str) -> str | None:
    if value is None or value == "":
        return None
    return _sha(value, name)


def _timestamp(value: object, name: str, *, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    text = _text(value, name, maximum=80)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise NppesContractError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise NppesContractError(f"{name} must include a timezone")
    return text


def _date(value: object, name: str, *, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    text = _text(value, name, maximum=20)
    try:
        date.fromisoformat(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise NppesContractError(f"{name} must be an ISO-8601 date") from exc
    return text


def _integer(value: object, name: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise NppesContractError(f"{name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        bound = f" between {minimum} and {maximum}" if maximum is not None else f" at least {minimum}"
        raise NppesContractError(f"{name} must be{bound}")
    return value


def _canonical_digest(value: Mapping[str, object]) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise NppesContractError("value cannot be canonically fingerprinted") from exc
    return f"sha256:{sha256(encoded).hexdigest()}"


def _tuple_of_strings(value: object, name: str, *, maximum: int) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise NppesContractError(f"{name} must be an array")
    if len(value) > maximum:
        raise NppesContractError(f"{name} exceeds its {maximum}-item bound")
    result: list[str] = []
    for index, item in enumerate(value):
        result.append(_text(item, f"{name}[{index}]", maximum=200))
    return tuple(result)


@dataclass(frozen=True, slots=True)
class NppesCatalogEntry:
    """One approved-public NPPES source registration with explicit bounds."""

    source_id: str = NPPES_SOURCE_ID
    title: str = "NPPES NPI Registry V2 full baseline"
    source_url: str = NPPES_SOURCE_URL
    release_locator: str = NPPES_SOURCE_URL
    change_mode: Literal["release_metadata", "content_hash", "last_modified"] = "release_metadata"
    rights_status: Literal["approved_public", "pending_review", "blocked"] = "approved_public"
    enabled: bool = True
    max_bytes: int = MAX_NPPES_BYTES
    max_chunks: int = MAX_NPPES_CHUNKS
    max_seconds: int = MAX_NPPES_SECONDS
    max_chunk_bytes: int = MAX_NPPES_CHUNK_BYTES

    def __post_init__(self) -> None:
        if self.source_id != NPPES_SOURCE_ID or _SOURCE_ID.fullmatch(self.source_id) is None:
            raise NppesContractError(f"source_id must be {NPPES_SOURCE_ID}")
        _text(self.title, "title", maximum=200)
        _https(self.source_url, "source_url")
        _https(self.release_locator, "release_locator")
        if self.change_mode not in {"release_metadata", "content_hash", "last_modified"}:
            raise NppesContractError("change_mode is unsupported")
        if self.rights_status not in {"approved_public", "pending_review", "blocked"}:
            raise NppesContractError("rights_status is unsupported")
        if not isinstance(self.enabled, bool):
            raise NppesContractError("enabled must be boolean")
        if self.enabled and self.rights_status != "approved_public":
            raise NppesContractError("enabled NPPES source requires approved public rights")
        _integer(self.max_bytes, "max_bytes", minimum=1, maximum=MAX_NPPES_BYTES)
        _integer(self.max_chunks, "max_chunks", minimum=1, maximum=MAX_NPPES_CHUNKS)
        _integer(self.max_seconds, "max_seconds", minimum=1, maximum=MAX_NPPES_SECONDS)
        _integer(self.max_chunk_bytes, "max_chunk_bytes", minimum=1, maximum=MAX_NPPES_CHUNK_BYTES)
        if self.max_chunk_bytes > self.max_bytes:
            raise NppesContractError("max_chunk_bytes cannot exceed max_bytes")

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "title": self.title,
            "source_url": self.source_url,
            "release_locator": self.release_locator,
            "change_mode": self.change_mode,
            "rights_status": self.rights_status,
            "enabled": self.enabled,
            "max_bytes": self.max_bytes,
            "max_chunks": self.max_chunks,
            "max_seconds": self.max_seconds,
            "max_chunk_bytes": self.max_chunk_bytes,
        }


@dataclass(frozen=True, slots=True)
class NppesCatalog:
    """Strict catalog document containing the single NPPES registration."""

    catalog_id: str = "catalog:nppes:registry"
    source: NppesCatalogEntry = NppesCatalogEntry()
    schema_version: str = NPPES_CATALOG_SCHEMA
    record_type: Literal["nppes_catalog"] = "nppes_catalog"

    def __post_init__(self) -> None:
        if _SOURCE_ID.fullmatch(self.source.source_id) is None or self.source.source_id != NPPES_SOURCE_ID:
            raise NppesContractError("catalog source must be NPPES")
        if re.fullmatch(r"^catalog:[a-z0-9][a-z0-9._:-]*$", self.catalog_id) is None:
            raise NppesContractError("catalog_id must be catalog-scoped")
        if self.schema_version != NPPES_CATALOG_SCHEMA or self.record_type != "nppes_catalog":
            raise NppesContractError("unsupported NPPES catalog schema")

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "record_type": self.record_type,
            "catalog_id": self.catalog_id,
            "sources": [self.source.as_dict()],
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "NppesCatalog":
        allowed = {"schema_version", "record_type", "catalog_id", "sources"}
        unknown = set(value) - allowed
        if unknown:
            raise NppesContractError(f"unknown catalog fields: {sorted(unknown)}")
        raw_sources = value.get("sources")
        if not isinstance(raw_sources, list) or len(raw_sources) != 1 or not isinstance(raw_sources[0], Mapping):
            raise NppesContractError("catalog must contain exactly one source mapping")
        raw = raw_sources[0]
        return cls(
            catalog_id=_text(value.get("catalog_id"), "catalog_id", maximum=200),
            source=NppesCatalogEntry(
                source_id=cast(str, raw.get("source_id", NPPES_SOURCE_ID)),
                title=cast(str, raw.get("title", "")),
                source_url=cast(str, raw.get("source_url", "")),
                release_locator=cast(str, raw.get("release_locator", "")),
                change_mode=cast(Literal["release_metadata", "content_hash", "last_modified"], raw.get("change_mode")),
                rights_status=cast(Literal["approved_public", "pending_review", "blocked"], raw.get("rights_status")),
                enabled=cast(bool, raw.get("enabled", True)),
                max_bytes=cast(int, raw.get("max_bytes", MAX_NPPES_BYTES)),
                max_chunks=cast(int, raw.get("max_chunks", MAX_NPPES_CHUNKS)),
                max_seconds=cast(int, raw.get("max_seconds", MAX_NPPES_SECONDS)),
                max_chunk_bytes=cast(int, raw.get("max_chunk_bytes", MAX_NPPES_CHUNK_BYTES)),
            ),
            schema_version=cast(str, value.get("schema_version", NPPES_CATALOG_SCHEMA)),
            record_type=cast(Literal["nppes_catalog"], value.get("record_type", "nppes_catalog")),
        )


@dataclass(frozen=True, slots=True)
class NppesFileDescriptor:
    """Declared source file and whether its bytes are available to consume."""

    file_kind: NppesFileKind
    source_file_name: str
    source_url: str
    evidence_locator: str
    availability: NppesAvailability = "present"
    expected_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.file_kind not in {"provider", "location", "endpoint", "reference", "deactivation"}:
            raise NppesContractError("unsupported NPPES file_kind")
        if _SOURCE_FILE.fullmatch(self.source_file_name) is None:
            raise NppesContractError("source_file_name must be a safe basename")
        _https(self.source_url, "file source_url")
        _locator(self.evidence_locator, "file evidence_locator")
        if self.availability not in {"present", "unavailable_public", "not_applicable"}:
            raise NppesContractError("unsupported file availability")
        if self.availability != "present" and self.expected_sha256 is not None:
            raise NppesContractError("unavailable file cannot declare a content digest")
        if self.expected_sha256 is not None:
            _sha(self.expected_sha256, "expected_sha256")

    def as_dict(self) -> dict[str, object]:
        return {
            "file_kind": self.file_kind,
            "source_file_name": self.source_file_name,
            "source_url": self.source_url,
            "evidence_locator": self.evidence_locator,
            "availability": self.availability,
            "expected_sha256": self.expected_sha256,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "NppesFileDescriptor":
        allowed = {
            "file_kind",
            "source_file_name",
            "source_url",
            "evidence_locator",
            "availability",
            "expected_sha256",
        }
        unknown = set(value) - allowed
        if unknown:
            raise NppesContractError(f"unknown file descriptor fields: {sorted(unknown)}")
        return cls(
            file_kind=cast(NppesFileKind, value.get("file_kind")),
            source_file_name=_text(value.get("source_file_name"), "source_file_name", maximum=256),
            source_url=_https(value.get("source_url"), "file source_url"),
            evidence_locator=_locator(value.get("evidence_locator"), "file evidence_locator"),
            availability=cast(NppesAvailability, value.get("availability", "present")),
            expected_sha256=_optional_sha(value.get("expected_sha256"), "expected_sha256"),
        )


@dataclass(frozen=True, slots=True)
class NppesReleaseDescriptor:
    """One NPPES release/probe descriptor with stable semantic fingerprint."""

    release_id: str
    release_label: str
    source_id: str
    source_url: str
    probe_state: NppesProbeState
    final_url: str | None
    http_status: int | None
    published_at: str | None
    evidence_locator: str
    files: tuple[NppesFileDescriptor, ...] = ()
    failure_reason: str | None = None
    release_sha256: str | None = None
    schema_version: str = NPPES_RELEASE_SCHEMA
    record_type: Literal["nppes_release"] = "nppes_release"

    def __post_init__(self) -> None:
        if _RELEASE_ID.fullmatch(self.release_id) is None:
            raise NppesContractError("release_id must be release-scoped")
        if self.schema_version != NPPES_RELEASE_SCHEMA or self.record_type != "nppes_release":
            raise NppesContractError("unsupported NPPES release schema")
        _text(self.release_label, "release_label", maximum=200)
        if self.source_id != NPPES_SOURCE_ID:
            raise NppesContractError("source_id must be NPPES")
        _https(self.source_url, "source_url")
        if self.final_url is not None:
            _https(self.final_url, "final_url")
        if self.probe_state not in {"changed", "not_modified", "failed_probe"}:
            raise NppesContractError("unsupported probe_state")
        if self.http_status is not None:
            _integer(self.http_status, "http_status", minimum=100, maximum=599)
        if self.published_at is not None:
            _timestamp(self.published_at, "published_at")
        _locator(self.evidence_locator, "evidence_locator")
        seen: set[NppesFileKind] = set()
        for item in self.files:
            if not isinstance(item, NppesFileDescriptor):
                raise NppesContractError("release files must be NppesFileDescriptor values")
            if item.file_kind in seen:
                raise NppesContractError(f"duplicate NPPES file kind: {item.file_kind}")
            seen.add(item.file_kind)
        if self.probe_state == "changed":
            if self.http_status is not None and not 200 <= self.http_status < 300:
                raise NppesContractError("changed release requires a successful HTTP status")
            if self.final_url is None or self.published_at is None:
                raise NppesContractError("changed release requires final_url and published_at")
            provider = next((item for item in self.files if item.file_kind == "provider"), None)
            if provider is None or provider.availability != "present":
                raise NppesContractError("changed release requires a present provider file")
            if self.failure_reason is not None:
                raise NppesContractError("changed release cannot carry failure_reason")
        elif self.probe_state == "not_modified":
            if self.http_status != 304:
                raise NppesContractError("not_modified release requires HTTP 304")
            if self.files:
                raise NppesContractError("not_modified release cannot carry file descriptors")
            if self.failure_reason is not None:
                raise NppesContractError("not_modified release cannot carry failure_reason")
        else:
            if self.failure_reason is None:
                raise NppesContractError("failed_probe release requires failure_reason")
            _text(self.failure_reason, "failure_reason", maximum=300)
            if self.files:
                raise NppesContractError("failed_probe release cannot carry file descriptors")
        computed = _canonical_digest(self._semantic_payload())
        if self.release_sha256 is not None and self.release_sha256 != computed:
            raise NppesContractError("release_sha256 does not match semantic release metadata")
        object.__setattr__(self, "release_sha256", computed)

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "release_id": self.release_id,
            "release_label": self.release_label,
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
    def from_mapping(cls, value: Mapping[str, object]) -> "NppesReleaseDescriptor":
        allowed = {
            "schema_version",
            "record_type",
            "release_id",
            "release_label",
            "source_id",
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
            raise NppesContractError(f"unknown release fields: {sorted(unknown)}")
        raw_files = value.get("files", [])
        if not isinstance(raw_files, list):
            raise NppesContractError("release files must be an array")
        files = tuple(NppesFileDescriptor.from_mapping(item) for item in raw_files if isinstance(item, Mapping))
        if len(files) != len(raw_files):
            raise NppesContractError("release files must contain mappings")
        return cls(
            release_id=_text(value.get("release_id"), "release_id", maximum=200),
            release_label=_text(value.get("release_label"), "release_label", maximum=200),
            source_id=_text(value.get("source_id"), "source_id", maximum=200),
            source_url=_https(value.get("source_url"), "source_url"),
            probe_state=cast(NppesProbeState, value.get("probe_state")),
            final_url=cast(str | None, value.get("final_url")),
            http_status=cast(int | None, value.get("http_status")),
            published_at=cast(str | None, value.get("published_at")),
            evidence_locator=_locator(value.get("evidence_locator"), "evidence_locator"),
            files=files,
            failure_reason=cast(str | None, value.get("failure_reason")),
            release_sha256=_optional_sha(value.get("release_sha256"), "release_sha256"),
            schema_version=cast(str, value.get("schema_version", NPPES_RELEASE_SCHEMA)),
            record_type=cast(Literal["nppes_release"], value.get("record_type", "nppes_release")),
        )


@dataclass(frozen=True, slots=True)
class NppesSourceRow:
    """Bounded source-row identity metadata; no source payload is retained."""

    file_kind: NppesFileKind
    row_number: int
    source_row_id: str
    npi: str
    row_sha256: str
    reference_id: str | None = None
    deactivation_date: str | None = None

    def __post_init__(self) -> None:
        if self.file_kind not in {"provider", "location", "endpoint", "reference", "deactivation"}:
            raise NppesSchemaDriftError("source row file_kind is unsupported")
        _integer(self.row_number, "row_number", minimum=2)
        if _SOURCE_ROW_ID.fullmatch(self.source_row_id) is None:
            raise NppesSchemaDriftError("source_row_id is malformed")
        if _NPI.fullmatch(self.npi) is None:
            raise NppesSchemaDriftError("NPPES NPI must be exactly ten ASCII digits")
        _sha(self.row_sha256, "row_sha256")
        if self.reference_id is not None:
            _text(self.reference_id, "reference_id", maximum=200)
        if self.deactivation_date is not None:
            _date(self.deactivation_date, "deactivation_date")
        if self.file_kind == "deactivation" and self.deactivation_date is None:
            raise NppesSchemaDriftError("deactivation row requires deactivation_date")

    def as_dict(self) -> dict[str, object]:
        return {
            "file_kind": self.file_kind,
            "row_number": self.row_number,
            "source_row_id": self.source_row_id,
            "npi": self.npi,
            "row_sha256": self.row_sha256,
            "reference_id": self.reference_id,
            "deactivation_date": self.deactivation_date,
        }


@dataclass(frozen=True, slots=True)
class NppesDeactivationOpportunity:
    """A bounded exact identifier for later deactivation review."""

    npi: str
    source_row_id: str
    deactivation_date: str
    evidence_locator: str
    review_state: Literal["review_required"] = "review_required"

    def __post_init__(self) -> None:
        if _NPI.fullmatch(self.npi) is None:
            raise NppesContractError("deactivation opportunity NPI is malformed")
        if _SOURCE_ROW_ID.fullmatch(self.source_row_id) is None:
            raise NppesContractError("deactivation opportunity source_row_id is malformed")
        _date(self.deactivation_date, "deactivation_date")
        _locator(self.evidence_locator, "evidence_locator")
        if self.review_state != "review_required":
            raise NppesContractError("deactivation opportunities require later review")

    def as_dict(self) -> dict[str, object]:
        return {
            "npi": self.npi,
            "source_row_id": self.source_row_id,
            "deactivation_date": self.deactivation_date,
            "evidence_locator": self.evidence_locator,
            "review_state": self.review_state,
        }


@dataclass(frozen=True, slots=True)
class NppesFileReceipt:
    """Secret-free counters and fingerprints for one NPPES source file."""

    file_kind: NppesFileKind
    source_file_name: str
    source_url: str
    evidence_locator: str
    state: NppesFileState
    content_sha256: str | None
    byte_length: int
    chunk_count: int
    row_count: int
    identifier_count: int
    row_sha256: str | None
    identifier_samples: tuple[str, ...] = ()
    deactivation_count: int = 0
    error_code: str | None = None

    def __post_init__(self) -> None:
        if self.file_kind not in {"provider", "location", "endpoint", "reference", "deactivation"}:
            raise NppesContractError("file receipt kind is unsupported")
        if _SOURCE_FILE.fullmatch(self.source_file_name) is None:
            raise NppesContractError("file receipt source_file_name is malformed")
        _https(self.source_url, "file receipt source_url")
        _locator(self.evidence_locator, "file receipt evidence_locator")
        if self.state not in {
            "accepted",
            "unavailable_public",
            "not_applicable",
            "not_consumed",
            "interrupted",
            "schema_drift",
        }:
            raise NppesContractError("file receipt state is unsupported")
        if self.content_sha256 is not None:
            _sha(self.content_sha256, "content_sha256")
        if self.row_sha256 is not None:
            _sha(self.row_sha256, "row_sha256")
        _integer(self.byte_length, "byte_length")
        _integer(self.chunk_count, "chunk_count")
        _integer(self.row_count, "row_count")
        _integer(self.identifier_count, "identifier_count")
        _integer(self.deactivation_count, "deactivation_count")
        if len(self.identifier_samples) > MAX_IDENTIFIER_SAMPLES:
            raise NppesContractError("identifier_samples exceeds its bound")
        for sample in self.identifier_samples:
            if _NPI.fullmatch(sample) is None:
                raise NppesContractError("identifier_samples must contain exact NPIs")
        if self.error_code is not None:
            _text(self.error_code, "error_code", maximum=120)
        if self.state in {"unavailable_public", "not_applicable", "not_consumed"} and self.content_sha256 is not None:
            raise NppesContractError("unconsumed file cannot carry content digest")

    def as_dict(self) -> dict[str, object]:
        return {
            "file_kind": self.file_kind,
            "source_file_name": self.source_file_name,
            "source_url": self.source_url,
            "evidence_locator": self.evidence_locator,
            "state": self.state,
            "content_sha256": self.content_sha256,
            "byte_length": self.byte_length,
            "chunk_count": self.chunk_count,
            "row_count": self.row_count,
            "identifier_count": self.identifier_count,
            "row_sha256": self.row_sha256,
            "identifier_samples": list(self.identifier_samples),
            "deactivation_count": self.deactivation_count,
            "error_code": self.error_code,
        }


@dataclass(frozen=True, slots=True)
class NppesBaselineReceipt:
    """Deterministic baseline result that never grants canonical authority."""

    receipt_id: str
    source_id: str
    source_url: str
    release_id: str
    release_label: str
    release_sha256: str
    probe_state: NppesProbeState
    state: NppesOutcomeState
    final_url: str | None
    http_status: int | None
    recorded_at: str
    evidence_locator: str
    files: tuple[NppesFileReceipt, ...]
    deactivation_opportunities: tuple[NppesDeactivationOpportunity, ...] = ()
    deactivation_count: int = 0
    current_projection_preserved: Literal[True] = True
    failure_code: str | None = None
    receipt_sha256: str | None = None
    schema_version: str = NPPES_SCHEMA_VERSION
    record_type: Literal["nppes_baseline_receipt"] = NPPES_RECORD_TYPE
    producer: str = NPPES_PRODUCER
    receipt_schema: str = NPPES_RECEIPT_SCHEMA

    def __post_init__(self) -> None:
        if _RECEIPT_ID.fullmatch(self.receipt_id) is None:
            raise NppesContractError("receipt_id must be receipt-scoped")
        if (
            self.schema_version != NPPES_SCHEMA_VERSION
            or self.record_type != NPPES_RECORD_TYPE
            or self.producer != NPPES_PRODUCER
            or self.receipt_schema != NPPES_RECEIPT_SCHEMA
        ):
            raise NppesContractError("unsupported NPPES receipt schema or producer")
        if self.source_id != NPPES_SOURCE_ID:
            raise NppesContractError("receipt source_id must be NPPES")
        _https(self.source_url, "source_url")
        if _RELEASE_ID.fullmatch(self.release_id) is None:
            raise NppesContractError("receipt release_id is malformed")
        _text(self.release_label, "release_label", maximum=200)
        _sha(self.release_sha256, "release_sha256")
        if self.probe_state not in {"changed", "not_modified", "failed_probe"}:
            raise NppesContractError("receipt probe_state is unsupported")
        if self.state not in {"changed", "no_op", "failed_probe", "schema_drift", "replayed", "blocked"}:
            raise NppesContractError("receipt state is unsupported")
        if self.final_url is not None:
            _https(self.final_url, "final_url")
        if self.http_status is not None:
            _integer(self.http_status, "http_status", minimum=100, maximum=599)
        _timestamp(self.recorded_at, "recorded_at")
        _locator(self.evidence_locator, "evidence_locator")
        if self.current_projection_preserved is not True:
            raise NppesContractError("NPPES receipt must preserve current projection")
        seen: set[NppesFileKind] = set()
        for item in self.files:
            if item.file_kind in seen:
                raise NppesContractError(f"duplicate file receipt kind: {item.file_kind}")
            seen.add(item.file_kind)
        _integer(self.deactivation_count, "deactivation_count")
        if len(self.deactivation_opportunities) > MAX_DEACTIVATION_SAMPLES:
            raise NppesContractError("deactivation_opportunities exceeds its bound")
        for item in self.deactivation_opportunities:
            if not isinstance(item, NppesDeactivationOpportunity):
                raise NppesContractError("deactivation opportunities must be typed values")
        if self.deactivation_count < len(self.deactivation_opportunities):
            raise NppesContractError("deactivation_count cannot be below sampled opportunities")
        if self.failure_code is not None:
            _text(self.failure_code, "failure_code", maximum=120)
        computed = _canonical_digest(self._payload())
        if self.receipt_sha256 is not None and self.receipt_sha256 != computed:
            raise NppesContractError("receipt_sha256 does not match receipt payload")
        object.__setattr__(self, "receipt_sha256", computed)

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "record_type": self.record_type,
            "receipt_id": self.receipt_id,
            "producer": self.producer,
            "receipt_schema": self.receipt_schema,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "release_id": self.release_id,
            "release_label": self.release_label,
            "release_sha256": self.release_sha256,
            "probe_state": self.probe_state,
            "state": self.state,
            "final_url": self.final_url,
            "http_status": self.http_status,
            "recorded_at": self.recorded_at,
            "evidence_locator": self.evidence_locator,
            "files": [item.as_dict() for item in self.files],
            "deactivation_opportunities": [item.as_dict() for item in self.deactivation_opportunities],
            "deactivation_count": self.deactivation_count,
            "current_projection_preserved": self.current_projection_preserved,
            "failure_code": self.failure_code,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self._payload(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "NppesBaselineReceipt":
        allowed = {
            "schema_version",
            "record_type",
            "receipt_id",
            "producer",
            "receipt_schema",
            "source_id",
            "source_url",
            "release_id",
            "release_label",
            "release_sha256",
            "probe_state",
            "state",
            "final_url",
            "http_status",
            "recorded_at",
            "evidence_locator",
            "files",
            "deactivation_opportunities",
            "deactivation_count",
            "current_projection_preserved",
            "failure_code",
            "receipt_sha256",
        }
        unknown = set(value) - allowed
        if unknown:
            raise NppesContractError(f"unknown receipt fields: {sorted(unknown)}")
        raw_files = value.get("files")
        if not isinstance(raw_files, list):
            raise NppesContractError("receipt files must be an array")
        files = tuple(_file_receipt_from_mapping(item) for item in raw_files if isinstance(item, Mapping))
        if len(files) != len(raw_files):
            raise NppesContractError("receipt files must contain mappings")
        raw_opportunities = value.get("deactivation_opportunities", [])
        if not isinstance(raw_opportunities, list):
            raise NppesContractError("deactivation_opportunities must be an array")
        opportunities = tuple(
            _deactivation_from_mapping(item) for item in raw_opportunities if isinstance(item, Mapping)
        )
        if len(opportunities) != len(raw_opportunities):
            raise NppesContractError("deactivation opportunities must contain mappings")
        return cls(
            receipt_id=_text(value.get("receipt_id"), "receipt_id", maximum=200),
            source_id=_text(value.get("source_id"), "source_id", maximum=200),
            source_url=_https(value.get("source_url"), "source_url"),
            release_id=_text(value.get("release_id"), "release_id", maximum=200),
            release_label=_text(value.get("release_label"), "release_label", maximum=200),
            release_sha256=_sha(value.get("release_sha256"), "release_sha256"),
            probe_state=cast(NppesProbeState, value.get("probe_state")),
            state=cast(NppesOutcomeState, value.get("state")),
            final_url=cast(str | None, value.get("final_url")),
            http_status=cast(int | None, value.get("http_status")),
            recorded_at=_text(value.get("recorded_at"), "recorded_at", maximum=80),
            evidence_locator=_locator(value.get("evidence_locator"), "evidence_locator"),
            files=files,
            deactivation_opportunities=opportunities,
            deactivation_count=cast(int, value.get("deactivation_count", 0)),
            current_projection_preserved=cast(Literal[True], value.get("current_projection_preserved", True)),
            failure_code=cast(str | None, value.get("failure_code")),
            receipt_sha256=_optional_sha(value.get("receipt_sha256"), "receipt_sha256"),
            schema_version=cast(str, value.get("schema_version", NPPES_SCHEMA_VERSION)),
            record_type=cast(Literal["nppes_baseline_receipt"], value.get("record_type", NPPES_RECORD_TYPE)),
            producer=cast(str, value.get("producer", NPPES_PRODUCER)),
            receipt_schema=cast(str, value.get("receipt_schema", NPPES_RECEIPT_SCHEMA)),
        )


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
        raise NppesContractError(f"unknown file receipt fields: {sorted(unknown)}")
    return NppesFileReceipt(
        file_kind=cast(NppesFileKind, value.get("file_kind")),
        source_file_name=_text(value.get("source_file_name"), "source_file_name", maximum=256),
        source_url=_https(value.get("source_url"), "file source_url"),
        evidence_locator=_locator(value.get("evidence_locator"), "file evidence_locator"),
        state=cast(NppesFileState, value.get("state")),
        content_sha256=_optional_sha(value.get("content_sha256"), "content_sha256"),
        byte_length=cast(int, value.get("byte_length", 0)),
        chunk_count=cast(int, value.get("chunk_count", 0)),
        row_count=cast(int, value.get("row_count", 0)),
        identifier_count=cast(int, value.get("identifier_count", 0)),
        row_sha256=_optional_sha(value.get("row_sha256"), "row_sha256"),
        identifier_samples=_tuple_of_strings(
            value.get("identifier_samples", []), "identifier_samples", maximum=MAX_IDENTIFIER_SAMPLES
        ),
        deactivation_count=cast(int, value.get("deactivation_count", 0)),
        error_code=cast(str | None, value.get("error_code")),
    )


def _deactivation_from_mapping(value: Mapping[str, object]) -> NppesDeactivationOpportunity:
    allowed = {"npi", "source_row_id", "deactivation_date", "evidence_locator", "review_state"}
    unknown = set(value) - allowed
    if unknown:
        raise NppesContractError(f"unknown deactivation opportunity fields: {sorted(unknown)}")
    return NppesDeactivationOpportunity(
        npi=_text(value.get("npi"), "npi", maximum=10),
        source_row_id=_text(value.get("source_row_id"), "source_row_id", maximum=200),
        deactivation_date=_text(value.get("deactivation_date"), "deactivation_date", maximum=20),
        evidence_locator=_locator(value.get("evidence_locator"), "evidence_locator"),
        review_state=cast(Literal["review_required"], value.get("review_state", "review_required")),
    )


def validate_nppes_catalog(value: Mapping[str, object]) -> NppesCatalog:
    """Parse and validate a strict NPPES catalog document."""

    return NppesCatalog.from_mapping(value)


def validate_nppes_release(value: Mapping[str, object]) -> NppesReleaseDescriptor:
    """Parse and validate a strict NPPES release document."""

    return NppesReleaseDescriptor.from_mapping(value)


def validate_nppes_receipt(value: Mapping[str, object]) -> NppesBaselineReceipt:
    """Parse and validate a strict NPPES baseline receipt."""

    return NppesBaselineReceipt.from_mapping(value)


__all__ = [
    "MAX_DEACTIVATION_SAMPLES",
    "MAX_IDENTIFIER_SAMPLES",
    "MAX_NPPES_BYTES",
    "MAX_NPPES_CHUNK_BYTES",
    "MAX_NPPES_CHUNKS",
    "MAX_NPPES_SECONDS",
    "NPPES_CATALOG_SCHEMA",
    "NPPES_PRODUCER",
    "NPPES_RECORD_TYPE",
    "NPPES_RECEIPT_SCHEMA",
    "NPPES_RELEASE_SCHEMA",
    "NPPES_SCHEMA_VERSION",
    "NPPES_SOURCE_ID",
    "NPPES_SOURCE_URL",
    "NppesAvailability",
    "NppesBaselineReceipt",
    "NppesCatalog",
    "NppesCatalogEntry",
    "NppesContractError",
    "NppesDeactivationOpportunity",
    "NppesFileDescriptor",
    "NppesFileKind",
    "NppesFileReceipt",
    "NppesFileState",
    "NppesOutcomeState",
    "NppesProbeState",
    "NppesReplayConflictError",
    "NppesReleaseDescriptor",
    "NppesSchemaDriftError",
    "NppesSourceRow",
    "validate_nppes_catalog",
    "validate_nppes_receipt",
    "validate_nppes_release",
]
