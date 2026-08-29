"""Release-aware, source-scoped CMS Provider of Services producer.

The producer accepts metadata and byte chunks that were resolved by a caller.
It deliberately performs no network, filesystem, queue, object-store, or
database work.  The shared adapter SDK supplies the catalog, conditional
probe, byte fingerprint, and bounded stream contracts; this module adds only
CMS POS release semantics and source-native CSV rows.

``PRVDR_NUM`` remains an opaque CMS identifier.  It is never converted into a
Toolkit entity, canonical identity, metric, or fact.  A caller must explicitly
request an authorized source preview before facility fields are returned by
``authorized_source_preview``.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Iterable, Literal, Mapping, TypeAlias, cast

from shared.adapters import (
    AdapterCatalog,
    AdapterCatalogEntry,
    AdapterContractError,
    ConditionalResponse,
    JsonValue,
    StreamBudget,
    classify_conditional_response,
    fingerprint_bytes,
    fingerprint_json,
    stream_bounded,
)


CMS_POS_SOURCE_ID = "source:cms:pos"
CMS_POS_SCHEMA_VERSION = "hdp.cms-pos-producer.v1"
CMS_POS_RECORD_TYPE = "cms_pos_source_result"
CMS_POS_IDENTIFIER_COLUMN = "PRVDR_NUM"
CMS_POS_MEDIA_TYPE = "text/csv"
CMS_POS_MAX_ROWS = 10_000
CMS_POS_MAX_PREVIEW_ROWS = 100

CmsPosState = Literal["no_op", "changed", "drift", "replayed", "failed_probe"]
StreamState = Literal["not_started", "completed", "interrupted"]

_ID = re.compile(r"^[a-z0-9][a-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:cms:pos:[a-z0-9][a-z0-9._:-]*$")
_DISTRIBUTION_ID = re.compile(r"^distribution:cms:pos:[a-z0-9][a-z0-9._:-]*$")
_RECORD_ID = re.compile(r"^record:cms:pos:[a-f0-9]{32}$")
_ARTIFACT_ID = re.compile(r"^artifact:cms:pos:[a-f0-9]{32}$")
_RECEIPT_ID = re.compile(r"^receipt:cms:pos:[a-f0-9]{24}$")
_IDEMPOTENCY_KEY = re.compile(r"^idempotency:cms:pos:[a-f0-9]{32}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_PERIOD = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._:/-]{0,63}$")
_SOURCE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SELECTOR = re.compile(r"^csv:PRVDR_NUM=[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

_RESERVED_CANONICAL_FIELDS = frozenset(
    {
        "canonical_id",
        "canonical_entity_id",
        "entity_id",
        "identity_key",
        "profile_entity_id",
        "toolkit_entity_id",
    }
)

JsonObject: TypeAlias = dict[str, JsonValue]


class CmsPosError(ValueError):
    """Raised when CMS POS metadata, bytes, or state transitions fail closed."""


class CmsPosAuthorizationError(CmsPosError):
    """Raised when a source preview was requested without explicit authority."""


def _text(value: object, label: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CmsPosError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise CmsPosError(f"{label} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise CmsPosError(f"{label} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise CmsPosError(f"{label} contains malformed Unicode") from exc
    return value


def _id(value: object, label: str, pattern: re.Pattern[str] = _ID) -> str:
    text = _text(value, label)
    if pattern.fullmatch(text) is None:
        raise CmsPosError(f"{label} has an invalid format")
    return text


def _url(value: object, label: str) -> str:
    try:
        from shared.adapters.contracts import ConditionalRequest

        return ConditionalRequest(_text(value, label, maximum=2048)).url
    except AdapterContractError as exc:
        raise CmsPosError(f"{label} must be an HTTPS URL") from exc


def _sha(value: object, label: str) -> str:
    text = _text(value, label, maximum=71)
    if _SHA256.fullmatch(text) is None:
        raise CmsPosError(f"{label} must be a sha256 fingerprint")
    return text


def _timestamp(value: object, label: str) -> str:
    text = _text(value, label, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CmsPosError(f"{label} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CmsPosError(f"{label} must include a timezone")
    normalized = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _canonical(value: JsonValue, label: str) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, UnicodeError, ValueError) as exc:
        raise CmsPosError(f"{label} is not canonically serializable") from exc


def _fingerprint(value: JsonValue, label: str) -> str:
    try:
        return fingerprint_json(value)
    except AdapterContractError as exc:
        raise CmsPosError(f"{label} cannot be fingerprinted") from exc


def _digest_id(prefix: str, material: str, length: int = 32) -> str:
    digest = sha256(material.encode("utf-8")).hexdigest()
    return f"{prefix}{digest[:length]}"


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise CmsPosError(f"{label} must be an object")
    result: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise CmsPosError(f"{label} keys must be strings")
        result[key] = item
    return result


def _optional_sha(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _sha(value, label)


def _optional_header(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label, maximum=512)


@dataclass(frozen=True, slots=True)
class CmsPosRelease:
    """Resolved semantic identity for one CMS POS official release."""

    source_id: str
    release_id: str
    release_label: str
    source_period: str
    source_url: str
    release_locator: str
    landing_page: str
    published_at: str

    def __post_init__(self) -> None:
        source_id = _id(self.source_id, "source_id")
        if source_id != CMS_POS_SOURCE_ID:
            raise CmsPosError(f"unsupported CMS POS source_id: {source_id}")
        _id(self.release_id, "release_id", _RELEASE_ID)
        _text(self.release_label, "release_label", maximum=200)
        source_period = _text(self.source_period, "source_period", maximum=64)
        if _PERIOD.fullmatch(source_period) is None:
            raise CmsPosError("source_period has an invalid format")
        _url(self.source_url, "source_url")
        _url(self.release_locator, "release_locator")
        _url(self.landing_page, "landing_page")
        _timestamp(self.published_at, "published_at")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CmsPosRelease":
        raw = _mapping(value, "release")
        release_locator = raw.get("release_locator", raw.get("release_url"))
        if release_locator is None:
            raise CmsPosError("release.release_locator is required")
        result = cls(
            source_id=_id(raw.get("source_id"), "release.source_id"),
            release_id=_id(raw.get("release_id"), "release.release_id", _RELEASE_ID),
            release_label=_text(raw.get("release_label"), "release.release_label", maximum=200),
            source_period=_text(raw.get("source_period"), "release.source_period", maximum=64),
            source_url=_url(raw.get("source_url"), "release.source_url"),
            release_locator=_url(release_locator, "release.release_locator"),
            landing_page=_url(raw.get("landing_page"), "release.landing_page"),
            published_at=_timestamp(raw.get("published_at"), "release.published_at"),
        )
        supplied = raw.get("release_fingerprint")
        if supplied is not None and _sha(supplied, "release.release_fingerprint") != result.release_fingerprint:
            raise CmsPosError("release semantic fingerprint drift")
        return result

    @property
    def release_fingerprint(self) -> str:
        """Return the semantic fingerprint, independent of mapping key order."""

        return _fingerprint(cast(JsonValue, self.as_dict(include_fingerprint=False)), "release")

    def as_dict(self, *, include_fingerprint: bool = True) -> dict[str, str]:
        result = {
            "source_id": self.source_id,
            "release_id": self.release_id,
            "release_label": self.release_label,
            "source_period": self.source_period,
            "source_url": self.source_url,
            "release_locator": self.release_locator,
            "landing_page": self.landing_page,
            "published_at": _timestamp(self.published_at, "published_at"),
        }
        if include_fingerprint:
            result["release_fingerprint"] = self.release_fingerprint
        return result


@dataclass(frozen=True, slots=True)
class CmsPosDistribution:
    """Resolved identity and conditional validators for one CSV distribution."""

    distribution_id: str
    url: str
    media_type: str = CMS_POS_MEDIA_TYPE
    etag: str | None = None
    last_modified: str | None = None
    content_sha256: str | None = None
    byte_length: int | None = None

    def __post_init__(self) -> None:
        _id(self.distribution_id, "distribution_id", _DISTRIBUTION_ID)
        _url(self.url, "distribution.url")
        media_type = _text(self.media_type, "distribution.media_type", maximum=100)
        if media_type != CMS_POS_MEDIA_TYPE:
            raise CmsPosError("distribution.media_type must be text/csv")
        # ConditionalResponse performs the shared ETag/Last-Modified syntax
        # checks while remaining transport-neutral.
        try:
            ConditionalResponse(200, etag=self.etag, last_modified=self.last_modified)
        except AdapterContractError as exc:
            raise CmsPosError("distribution conditional validator is invalid") from exc
        if self.content_sha256 is not None:
            _sha(self.content_sha256, "distribution.content_sha256")
        if self.byte_length is not None and (
            isinstance(self.byte_length, bool) or not isinstance(self.byte_length, int) or self.byte_length < 0
        ):
            raise CmsPosError("distribution.byte_length must be a non-negative integer")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CmsPosDistribution":
        raw = _mapping(value, "distribution")
        return cls(
            distribution_id=_id(raw.get("distribution_id"), "distribution.distribution_id", _DISTRIBUTION_ID),
            url=_url(raw.get("url", raw.get("distribution_url")), "distribution.url"),
            media_type=_text(raw.get("media_type", CMS_POS_MEDIA_TYPE), "distribution.media_type", maximum=100),
            etag=_optional_header(raw.get("etag"), "distribution.etag"),
            last_modified=_optional_header(raw.get("last_modified"), "distribution.last_modified"),
            content_sha256=_optional_sha(raw.get("content_sha256"), "distribution.content_sha256"),
            byte_length=cast(int | None, raw.get("byte_length")),
        )

    @property
    def distribution_fingerprint(self) -> str:
        """Return a semantic distribution fingerprint without payload bytes."""

        value: JsonObject = {
            "distribution_id": self.distribution_id,
            "url": self.url,
            "media_type": self.media_type,
            "etag": self.etag,
            "last_modified": self.last_modified,
        }
        return _fingerprint(value, "distribution")

    def with_content(self, content_sha256: str, byte_length: int) -> "CmsPosDistribution":
        """Return this distribution bound to verified stream content."""

        return CmsPosDistribution(
            distribution_id=self.distribution_id,
            url=self.url,
            media_type=self.media_type,
            etag=self.etag,
            last_modified=self.last_modified,
            content_sha256=_sha(content_sha256, "distribution.content_sha256"),
            byte_length=byte_length,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "distribution_id": self.distribution_id,
            "url": self.url,
            "media_type": self.media_type,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "distribution_fingerprint": self.distribution_fingerprint,
            "content_sha256": self.content_sha256,
            "byte_length": self.byte_length,
        }


@dataclass(frozen=True, slots=True)
class CmsPosSourceRow:
    """One source-native CMS row with an opaque provider identifier."""

    source_record_id: str
    source_identifier: str
    row_number: int
    fields: Mapping[str, str]
    source_selector: str
    row_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _id(self.source_record_id, "source_record_id", _RECORD_ID)
        source_identifier = _text(self.source_identifier, "source_identifier", maximum=128)
        if _SOURCE_IDENTIFIER.fullmatch(source_identifier) is None:
            raise CmsPosError("source_identifier has an invalid format")
        if isinstance(self.row_number, bool) or not isinstance(self.row_number, int) or self.row_number < 2:
            raise CmsPosError("row_number must be an integer at least 2")
        source_selector = _text(self.source_selector, "source_selector", maximum=200)
        if _SELECTOR.fullmatch(source_selector) is None:
            raise CmsPosError("source_selector has an invalid format")
        if source_selector != f"csv:{CMS_POS_IDENTIFIER_COLUMN}={source_identifier}":
            raise CmsPosError("source_selector does not match source_identifier")
        if not isinstance(self.fields, Mapping) or not self.fields:
            raise CmsPosError("source row fields must be a non-empty object")
        fields: dict[str, str] = {}
        for key, value in self.fields.items():
            if not isinstance(key, str) or not key:
                raise CmsPosError("source row field names must be non-empty strings")
            _text(key, "source row field name", maximum=200)
            if key.lower() in _RESERVED_CANONICAL_FIELDS or key.lower().endswith("_entity_id"):
                raise CmsPosError("source row contains a reserved canonical identity field")
            if not isinstance(value, str):
                raise CmsPosError("source row field values must be strings")
            try:
                value.encode("utf-8")
            except UnicodeError as exc:
                raise CmsPosError("source row contains malformed Unicode") from exc
            if len(value) > 16_384:
                raise CmsPosError("source row field value exceeds the 16384-character bound")
            fields[key] = value
        if fields.get(CMS_POS_IDENTIFIER_COLUMN) != source_identifier:
            raise CmsPosError("source_identifier must equal the PRVDR_NUM field")
        if CMS_POS_IDENTIFIER_COLUMN not in fields:
            raise CmsPosError("source row must preserve PRVDR_NUM")
        object.__setattr__(self, "fields", MappingProxyType(fields))
        expected_record = _digest_id("record:cms:pos:", source_identifier)
        if self.source_record_id != expected_record:
            raise CmsPosError("source_record_id does not match source identifier")
        object.__setattr__(self, "row_sha256", _fingerprint(cast(JsonValue, fields), "source row"))

    @classmethod
    def from_fields(cls, fields: Mapping[str, str], *, row_number: int) -> "CmsPosSourceRow":
        if not isinstance(fields, Mapping):
            raise CmsPosError("source row fields must be an object")
        values = dict(fields)
        identifier = values.get(CMS_POS_IDENTIFIER_COLUMN)
        if not isinstance(identifier, str) or not identifier:
            raise CmsPosError("source row must contain a non-empty PRVDR_NUM")
        if identifier != identifier.strip() or _SOURCE_IDENTIFIER.fullmatch(identifier) is None:
            raise CmsPosError("PRVDR_NUM has an invalid source identifier")
        return cls(
            source_record_id=_digest_id("record:cms:pos:", identifier),
            source_identifier=identifier,
            row_number=row_number,
            fields=values,
            source_selector=f"csv:{CMS_POS_IDENTIFIER_COLUMN}={identifier}",
        )

    @property
    def source_native_id(self) -> str:
        """Alias making the source-local nature explicit to callers."""

        return self.source_identifier

    def as_dict(self) -> dict[str, object]:
        return {
            "source_record_id": self.source_record_id,
            "source_identifier_column": CMS_POS_IDENTIFIER_COLUMN,
            "source_identifier": self.source_identifier,
            "row_number": self.row_number,
            "row_sha256": self.row_sha256,
            "source_selector": self.source_selector,
            "fields": dict(self.fields),
        }


@dataclass(frozen=True, slots=True)
class CmsPosReceipt:
    """Secret-free evidence for one CMS POS probe and parse attempt."""

    probe_state: CmsPosState
    source_id: str
    release_id: str
    release_fingerprint: str
    distribution_id: str
    distribution_fingerprint: str
    content_sha256: str | None
    artifact_id: str | None
    idempotency_key: str | None
    receipt_id: str
    acknowledged: bool
    stream_state: StreamState
    received_bytes: int
    chunk_count: int
    row_count: int
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        if self.probe_state not in {"no_op", "changed", "drift", "replayed", "failed_probe"}:
            raise CmsPosError("unsupported CMS POS probe state")
        if self.source_id != CMS_POS_SOURCE_ID:
            raise CmsPosError("receipt source_id is not CMS POS")
        _id(self.source_id, "receipt.source_id")
        _id(self.release_id, "receipt.release_id", _RELEASE_ID)
        _sha(self.release_fingerprint, "receipt.release_fingerprint")
        _id(self.distribution_id, "receipt.distribution_id", _DISTRIBUTION_ID)
        _sha(self.distribution_fingerprint, "receipt.distribution_fingerprint")
        _optional_sha(self.content_sha256, "receipt.content_sha256")
        if self.artifact_id is not None:
            _id(self.artifact_id, "receipt.artifact_id", _ARTIFACT_ID)
        if self.idempotency_key is not None:
            _id(self.idempotency_key, "receipt.idempotency_key", _IDEMPOTENCY_KEY)
        _id(self.receipt_id, "receipt.receipt_id", _RECEIPT_ID)
        if not isinstance(self.acknowledged, bool):
            raise CmsPosError("receipt.acknowledged must be boolean")
        if self.probe_state in {"no_op", "drift", "failed_probe"} and self.acknowledged:
            raise CmsPosError("quarantined or no-op receipt cannot be acknowledged")
        if self.probe_state in {"changed", "replayed"} and not self.acknowledged:
            raise CmsPosError("accepted or replayed receipt must be acknowledged")
        if self.stream_state not in {"not_started", "completed", "interrupted"}:
            raise CmsPosError("unsupported stream state")
        if isinstance(self.received_bytes, bool) or not isinstance(self.received_bytes, int) or self.received_bytes < 0:
            raise CmsPosError("receipt.received_bytes must be non-negative")
        if isinstance(self.chunk_count, bool) or not isinstance(self.chunk_count, int) or self.chunk_count < 0:
            raise CmsPosError("receipt.chunk_count must be non-negative")
        if isinstance(self.row_count, bool) or not isinstance(self.row_count, int) or self.row_count < 0:
            raise CmsPosError("receipt.row_count must be non-negative")
        if self.failure_reason is not None:
            _text(self.failure_reason, "receipt.failure_reason", maximum=120)
        if self.probe_state in {"changed", "replayed"}:
            if self.content_sha256 is None or self.artifact_id is None or self.idempotency_key is None:
                raise CmsPosError("accepted receipt is missing content lineage")
            if self.stream_state != "completed":
                raise CmsPosError("accepted receipt requires a completed stream")
        if self.probe_state == "no_op" and self.stream_state != "not_started":
            raise CmsPosError("no-op receipt must not start a stream")

    @property
    def state(self) -> CmsPosState:
        """Compatibility alias for callers that use ``state`` terminology."""

        return self.probe_state

    @property
    def no_op(self) -> bool:
        """Whether this receipt represents a conditional no-op."""

        return self.probe_state == "no_op"

    def as_dict(self) -> dict[str, object]:
        return {
            "receipt_id": self.receipt_id,
            "probe_state": self.probe_state,
            "state": self.probe_state,
            "source_id": self.source_id,
            "release_id": self.release_id,
            "release_fingerprint": self.release_fingerprint,
            "distribution_id": self.distribution_id,
            "distribution_fingerprint": self.distribution_fingerprint,
            "content_sha256": self.content_sha256,
            "artifact_id": self.artifact_id,
            "idempotency_key": self.idempotency_key,
            "acknowledged": self.acknowledged,
            "stream_state": self.stream_state,
            "received_bytes": self.received_bytes,
            "chunk_count": self.chunk_count,
            "row_count": self.row_count,
            "failure_reason": self.failure_reason,
        }


@dataclass(frozen=True, slots=True)
class CmsPosResult:
    """Source-scoped result containing a receipt and optional source rows."""

    release: CmsPosRelease
    distribution: CmsPosDistribution
    receipt: CmsPosReceipt
    rows: tuple[CmsPosSourceRow, ...]
    budget: StreamBudget
    preview_authorized: bool = False

    def __post_init__(self) -> None:
        if self.receipt.release_id != self.release.release_id:
            raise CmsPosError("result receipt/release identity mismatch")
        if self.receipt.release_fingerprint != self.release.release_fingerprint:
            raise CmsPosError("result receipt/release fingerprint mismatch")
        if self.receipt.distribution_id != self.distribution.distribution_id:
            raise CmsPosError("result receipt/distribution identity mismatch")
        if self.receipt.probe_state in {"changed", "replayed"}:
            if self.distribution.content_sha256 != self.receipt.content_sha256:
                raise CmsPosError("result distribution/content fingerprint mismatch")
            if len(self.rows) != self.receipt.row_count:
                raise CmsPosError("result row count mismatch")
        elif self.rows:
            raise CmsPosError("no-op or quarantined result cannot carry rows")
        if not isinstance(self.preview_authorized, bool):
            raise CmsPosError("preview_authorized must be boolean")

    @property
    def source_preview(self) -> tuple[CmsPosSourceRow, ...]:
        """Return rows only when the producer was explicitly authorized."""

        if not self.preview_authorized:
            raise CmsPosAuthorizationError("source preview requires explicit authorization")
        return self.rows[:CMS_POS_MAX_PREVIEW_ROWS]

    def authorized_source_preview(self, *, limit: int = 25) -> dict[str, object]:
        """Expose bounded source-native rows and provenance to an authorized caller."""

        if not self.preview_authorized:
            raise CmsPosAuthorizationError("source preview requires explicit authorization")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= CMS_POS_MAX_PREVIEW_ROWS:
            raise CmsPosAuthorizationError("preview limit must be between 1 and 100")
        return {
            "source_id": self.release.source_id,
            "release_id": self.release.release_id,
            "distribution_id": self.distribution.distribution_id,
            "receipt_id": self.receipt.receipt_id,
            "authorized": True,
            "rows": [row.as_dict() for row in self.rows[:limit]],
        }

    def as_dict(self, *, include_rows: bool = True) -> dict[str, object]:
        """Return a JSON-safe source result; no payload is put in the receipt."""

        result: dict[str, object] = {
            "schema_version": CMS_POS_SCHEMA_VERSION,
            "record_type": CMS_POS_RECORD_TYPE,
            "source_id": self.release.source_id,
            "release": self.release.as_dict(),
            "distribution": self.distribution.as_dict(),
            "receipt": self.receipt.as_dict(),
            "rows": [row.as_dict() for row in self.rows] if include_rows else [],
            "budget": {
                "max_bytes": self.budget.max_bytes,
                "max_chunks": self.budget.max_chunks,
                "max_seconds": self.budget.max_seconds,
                "max_chunk_bytes": self.budget.max_chunk_bytes,
            },
            "authority_limits": {
                "source_scoped": True,
                "acquisition_allowed": False,
                "mutation_allowed": False,
                "canonical_identity_admission_allowed": False,
                "fact_admission_allowed": False,
                "publication_allowed": False,
                "production_allowed": False,
            },
            "preview": {
                "authorized": self.preview_authorized,
                "max_rows": CMS_POS_MAX_PREVIEW_ROWS,
            },
        }
        return result


class CmsPosProducer:
    """Produce bounded, source-native CMS POS rows from caller-owned bytes."""

    def __init__(self, catalog: AdapterCatalog, *, budget: StreamBudget | None = None) -> None:
        if not isinstance(catalog, AdapterCatalog):
            # Protocol runtime checks do not guarantee methods are callable,
            # but this catches accidental ``None``/mapping use early.
            raise CmsPosError("catalog must implement the adapter catalog protocol")
        if budget is not None and not isinstance(budget, StreamBudget):
            raise CmsPosError("budget must be a StreamBudget")
        self._catalog = catalog
        self._budget = budget or StreamBudget()

    @property
    def budget(self) -> StreamBudget:
        """Return the bounded stream budget used by this producer."""

        return self._budget

    def produce(
        self,
        release: CmsPosRelease | Mapping[str, object],
        distribution: CmsPosDistribution | Mapping[str, object],
        chunks: Iterable[bytes] | None,
        *,
        conditional_response: ConditionalResponse | None = None,
        prior: CmsPosReceipt | None = None,
        preview_authorized: bool = False,
    ) -> CmsPosResult:
        """Classify one conditional probe and parse only a completed stream.

        ``chunks`` is consumed only for a successful ``2xx`` probe.  A `304`
        response is a no-op and does not touch the iterable, so callers can
        safely pass a lazy body reader without accidentally downloading it.
        """

        resolved_release = release if isinstance(release, CmsPosRelease) else CmsPosRelease.from_mapping(release)
        resolved_distribution = (
            distribution
            if isinstance(distribution, CmsPosDistribution)
            else CmsPosDistribution.from_mapping(distribution)
        )
        self._validate_catalog(resolved_release)
        if not isinstance(preview_authorized, bool):
            raise CmsPosError("preview_authorized must be boolean")
        if prior is not None:
            self._validate_prior(prior, resolved_release, resolved_distribution)
        response = conditional_response or ConditionalResponse(200)
        if not isinstance(response, ConditionalResponse):
            raise CmsPosError("conditional_response must be ConditionalResponse")
        probe_state = classify_conditional_response(response.status_code)

        if probe_state == "not_modified":
            if (
                prior is not None
                and self._same_release_identity(prior, resolved_release, resolved_distribution) is False
            ):
                return self._result(
                    resolved_release,
                    resolved_distribution,
                    self._receipt(
                        resolved_release,
                        resolved_distribution,
                        probe_state="drift",
                        content_sha256=prior.content_sha256,
                        received_bytes=0,
                        chunk_count=0,
                        row_count=0,
                        stream_state="not_started",
                        acknowledged=False,
                        failure_reason="not_modified_identity_mismatch",
                    ),
                    (),
                    preview_authorized,
                )
            return self._result(
                resolved_release,
                resolved_distribution,
                self._receipt(
                    resolved_release,
                    resolved_distribution,
                    probe_state="no_op",
                    content_sha256=prior.content_sha256 if prior is not None else None,
                    received_bytes=0,
                    chunk_count=0,
                    row_count=0,
                    stream_state="not_started",
                    acknowledged=False,
                    failure_reason=None,
                ),
                (),
                preview_authorized,
            )

        if probe_state == "failed_probe":
            return self._result(
                resolved_release,
                resolved_distribution,
                self._receipt(
                    resolved_release,
                    resolved_distribution,
                    probe_state="failed_probe",
                    content_sha256=None,
                    received_bytes=0,
                    chunk_count=0,
                    row_count=0,
                    stream_state="not_started",
                    acknowledged=False,
                    failure_reason=f"conditional_probe_status_{response.status_code}",
                ),
                (),
                preview_authorized,
            )

        if chunks is None:
            return self._result(
                resolved_release,
                resolved_distribution,
                self._receipt(
                    resolved_release,
                    resolved_distribution,
                    probe_state="failed_probe",
                    content_sha256=None,
                    received_bytes=0,
                    chunk_count=0,
                    row_count=0,
                    stream_state="not_started",
                    acknowledged=False,
                    failure_reason="changed_probe_missing_body",
                ),
                (),
                preview_authorized,
            )

        received: list[bytes] = []
        try:
            stream = stream_bounded(chunks, self._budget, on_chunk=received.append)
        except (AdapterContractError, TypeError) as exc:
            raise CmsPosError("CMS POS byte stream violates adapter bounds") from exc
        content_sha256 = stream.content_sha256
        if stream.state != "completed":
            return self._result(
                resolved_release,
                resolved_distribution.with_content(content_sha256, stream.received_bytes),
                self._receipt(
                    resolved_release,
                    resolved_distribution,
                    probe_state="failed_probe",
                    content_sha256=content_sha256,
                    received_bytes=stream.received_bytes,
                    chunk_count=stream.chunk_count,
                    row_count=0,
                    stream_state="interrupted",
                    acknowledged=False,
                    failure_reason="stream_interrupted",
                ),
                (),
                preview_authorized,
            )
        body = b"".join(received)
        if fingerprint_bytes(body) != content_sha256:
            raise CmsPosError("adapter stream fingerprint is inconsistent")
        if response.response_fingerprint is not None and response.response_fingerprint != content_sha256:
            bound_distribution = resolved_distribution.with_content(content_sha256, len(body))
            return self._result(
                resolved_release,
                bound_distribution,
                self._receipt(
                    resolved_release,
                    bound_distribution,
                    probe_state="drift",
                    content_sha256=content_sha256,
                    received_bytes=stream.received_bytes,
                    chunk_count=stream.chunk_count,
                    row_count=0,
                    stream_state="completed",
                    acknowledged=False,
                    failure_reason="response_fingerprint_mismatch",
                ),
                (),
                preview_authorized,
            )
        bound_distribution = resolved_distribution.with_content(content_sha256, len(body))
        drift_reason = self._drift_reason(prior, resolved_release, bound_distribution)
        if drift_reason is not None:
            return self._result(
                resolved_release,
                bound_distribution,
                self._receipt(
                    resolved_release,
                    bound_distribution,
                    probe_state="drift",
                    content_sha256=content_sha256,
                    received_bytes=stream.received_bytes,
                    chunk_count=stream.chunk_count,
                    row_count=0,
                    stream_state="completed",
                    acknowledged=False,
                    failure_reason=drift_reason,
                ),
                (),
                preview_authorized,
            )
        try:
            rows = _parse_rows(body)
        except CmsPosError as exc:
            return self._result(
                resolved_release,
                bound_distribution,
                self._receipt(
                    resolved_release,
                    bound_distribution,
                    probe_state="failed_probe",
                    content_sha256=content_sha256,
                    received_bytes=stream.received_bytes,
                    chunk_count=stream.chunk_count,
                    row_count=0,
                    stream_state="completed",
                    acknowledged=False,
                    failure_reason=_failure_code(str(exc)),
                ),
                (),
                preview_authorized,
            )
        replayed = self._is_replay(prior, resolved_release, bound_distribution)
        state: Literal["changed", "replayed"] = "replayed" if replayed else "changed"
        return self._result(
            resolved_release,
            bound_distribution,
            self._receipt(
                resolved_release,
                bound_distribution,
                probe_state=state,
                content_sha256=content_sha256,
                received_bytes=stream.received_bytes,
                chunk_count=stream.chunk_count,
                row_count=len(rows),
                stream_state="completed",
                acknowledged=True,
                failure_reason=None,
            ),
            rows,
            preview_authorized,
        )

    def _validate_catalog(self, release: CmsPosRelease) -> AdapterCatalogEntry:
        try:
            entry = self._catalog.registration(CMS_POS_SOURCE_ID)
        except (AdapterContractError, AttributeError, TypeError) as exc:
            raise CmsPosError("CMS POS catalog lookup failed") from exc
        if entry is None:
            raise CmsPosError("CMS POS source is not registered in the adapter catalog")
        if not isinstance(entry, AdapterCatalogEntry):
            raise CmsPosError("CMS POS catalog returned an invalid registration")
        if not entry.enabled or entry.rights_status != "approved_public":
            raise CmsPosError("CMS POS source is not enabled with approved public rights")
        if entry.source_url != release.source_url:
            raise CmsPosError("release source_url does not match catalog")
        if entry.release_locator != release.release_locator:
            raise CmsPosError("release_locator does not match catalog")
        return entry

    @staticmethod
    def _validate_prior(
        prior: CmsPosReceipt,
        release: CmsPosRelease,
        distribution: CmsPosDistribution,
    ) -> None:
        if not isinstance(prior, CmsPosReceipt):
            raise CmsPosError("prior must be a CmsPosReceipt")
        if prior.source_id != release.source_id:
            raise CmsPosError("prior receipt source_id does not match release")
        if prior.release_id != release.release_id:
            return
        if prior.probe_state not in {"changed", "replayed", "no_op", "drift", "failed_probe"}:
            raise CmsPosError("prior receipt state is unsupported")
        if prior.distribution_id != distribution.distribution_id:
            raise CmsPosError("prior receipt distribution_id does not match distribution")

    @staticmethod
    def _same_release_identity(
        prior: CmsPosReceipt,
        release: CmsPosRelease,
        distribution: CmsPosDistribution,
    ) -> bool:
        return (
            prior.release_id == release.release_id
            and prior.release_fingerprint == release.release_fingerprint
            and prior.distribution_id == distribution.distribution_id
            and prior.distribution_fingerprint == distribution.distribution_fingerprint
        )

    @classmethod
    def _drift_reason(
        cls,
        prior: CmsPosReceipt | None,
        release: CmsPosRelease,
        distribution: CmsPosDistribution,
    ) -> str | None:
        if prior is None or prior.probe_state not in {"changed", "replayed"}:
            return None
        if prior.release_id != release.release_id:
            return None
        if prior.release_fingerprint != release.release_fingerprint:
            return "release_metadata_drift"
        if prior.distribution_id != distribution.distribution_id:
            return "distribution_identity_drift"
        if prior.distribution_fingerprint != distribution.distribution_fingerprint:
            return "distribution_metadata_drift"
        if prior.content_sha256 != distribution.content_sha256:
            return "distribution_content_drift"
        return None

    @classmethod
    def _is_replay(
        cls,
        prior: CmsPosReceipt | None,
        release: CmsPosRelease,
        distribution: CmsPosDistribution,
    ) -> bool:
        return (
            prior is not None
            and prior.probe_state in {"changed", "replayed"}
            and cls._same_release_identity(prior, release, distribution)
            and prior.content_sha256 == distribution.content_sha256
        )

    @staticmethod
    def _receipt(
        release: CmsPosRelease,
        distribution: CmsPosDistribution,
        *,
        probe_state: CmsPosState,
        content_sha256: str | None,
        received_bytes: int,
        chunk_count: int,
        row_count: int,
        stream_state: StreamState,
        acknowledged: bool,
        failure_reason: str | None,
    ) -> CmsPosReceipt:
        material = "|".join(
            (
                release.source_id,
                release.release_id,
                release.release_fingerprint,
                distribution.distribution_id,
                distribution.distribution_fingerprint,
                content_sha256 or "none",
            )
        )
        digest = sha256(material.encode("utf-8")).hexdigest()
        artifact_id = f"artifact:cms:pos:{content_sha256.removeprefix('sha256:')[:32]}" if content_sha256 else None
        idempotency_key = f"idempotency:cms:pos:{digest[:32]}" if content_sha256 else None
        receipt_id = f"receipt:cms:pos:{digest[:24]}"
        return CmsPosReceipt(
            probe_state=probe_state,
            source_id=release.source_id,
            release_id=release.release_id,
            release_fingerprint=release.release_fingerprint,
            distribution_id=distribution.distribution_id,
            distribution_fingerprint=distribution.distribution_fingerprint,
            content_sha256=content_sha256,
            artifact_id=artifact_id if probe_state in {"changed", "replayed"} else None,
            idempotency_key=idempotency_key if probe_state in {"changed", "replayed"} else None,
            receipt_id=receipt_id,
            acknowledged=acknowledged,
            stream_state=stream_state,
            received_bytes=received_bytes,
            chunk_count=chunk_count,
            row_count=row_count,
            failure_reason=failure_reason,
        )

    def _result(
        self,
        release: CmsPosRelease,
        distribution: CmsPosDistribution,
        receipt: CmsPosReceipt,
        rows: tuple[CmsPosSourceRow, ...],
        preview_authorized: bool,
    ) -> CmsPosResult:
        return CmsPosResult(
            release=release,
            distribution=distribution,
            receipt=receipt,
            rows=rows,
            budget=self._budget,
            preview_authorized=preview_authorized,
        )


def _failure_code(message: str) -> str:
    """Convert parser detail into a stable, payload-free failure code."""

    normalized = message.lower()
    if "duplicate source identifier" in normalized:
        return "duplicate_source_identifier"
    if "reserved canonical" in normalized:
        return "reserved_canonical_field"
    if "row width" in normalized or "csv row" in normalized:
        return "malformed_csv_row"
    if "header" in normalized:
        return "malformed_csv_header"
    if "identifier" in normalized or "prvdr_num" in normalized:
        return "invalid_source_identifier"
    if "duplicate" in normalized:
        return "duplicate_source_identifier"
    if "canonical" in normalized:
        return "reserved_canonical_field"
    if "row" in normalized:
        return "malformed_csv_row"
    return "malformed_csv"


def _parse_rows(body: bytes) -> tuple[CmsPosSourceRow, ...]:
    if not body:
        raise CmsPosError("CSV body is empty")
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CmsPosError("CSV body is not valid UTF-8") from exc
    try:
        reader = csv.reader(StringIO(text, newline=""), strict=True)
        header = next(reader)
    except (StopIteration, csv.Error) as exc:
        raise CmsPosError("CSV header is missing or malformed") from exc
    if not header or any(not isinstance(item, str) or not item for item in header):
        raise CmsPosError("CSV header contains an empty field")
    if len(set(header)) != len(header):
        raise CmsPosError("CSV header contains duplicate fields")
    if CMS_POS_IDENTIFIER_COLUMN not in header:
        raise CmsPosError("CSV header does not contain PRVDR_NUM")
    for name in header:
        if name.lower() in _RESERVED_CANONICAL_FIELDS or name.lower().endswith("_entity_id"):
            raise CmsPosError("CSV header contains a reserved canonical identity field")
        _text(name, "CSV header field", maximum=200)
    rows: list[CmsPosSourceRow] = []
    seen: set[str] = set()
    try:
        for values in reader:
            if not values:
                continue
            if len(values) != len(header):
                raise CmsPosError("CSV row width does not match header")
            fields = dict(zip(header, values, strict=True))
            row = CmsPosSourceRow.from_fields(fields, row_number=reader.line_num)
            if row.source_identifier in seen:
                raise CmsPosError("duplicate source identifier")
            seen.add(row.source_identifier)
            rows.append(row)
            if len(rows) > CMS_POS_MAX_ROWS:
                raise CmsPosError("CSV row count exceeds the bounded limit")
    except csv.Error as exc:
        raise CmsPosError("CSV row is malformed") from exc
    if not rows:
        raise CmsPosError("CSV contains no source rows")
    return tuple(rows)


def validate_cms_pos_result(value: Mapping[str, object]) -> None:
    """Validate a serialized CMS POS result against the pinned JSON Schema."""

    if not isinstance(value, Mapping):
        raise CmsPosError("CMS POS result must be an object")
    schema_path = (
        Path(__file__).resolve().parents[2] / "contracts/healthcare-data-platform/cms-pos/v1/cms-pos.schema.json"
    )
    try:
        from jsonschema import Draft202012Validator, FormatChecker

        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, dict(value))),
            key=lambda error: list(error.absolute_path),
        )
    except (ImportError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CmsPosError("CMS POS JSON Schema validation is unavailable") from exc
    if errors:
        error = errors[0]
        location = ".".join(str(item) for item in error.absolute_path)
        suffix = f" at {location}" if location else ""
        raise CmsPosError(f"CMS POS result failed schema validation{suffix}: {error.message}")


def load_cms_pos_fixture(path: str | Path) -> dict[str, object]:
    """Load a strict, schema-validated synthetic CMS POS fixture."""

    fixture_path = Path(path)
    try:
        value = json.loads(fixture_path.read_text(encoding="utf-8"), object_pairs_hook=_strict_pairs)
    except CmsPosError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CmsPosError(f"unable to read CMS POS fixture: {fixture_path}") from exc
    if not isinstance(value, dict):
        raise CmsPosError("CMS POS fixture must be an object")
    validate_cms_pos_result(value)
    return value


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CmsPosError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def build_cms_pos_catalog(
    *,
    source_url: str = "https://data.cms.gov/provider-data/dataset/cms-pos",
    release_locator: str = "https://data.cms.gov/provider-data/dataset/cms-pos/releases",
) -> AdapterCatalog:
    """Build a safe in-memory catalog for local fixtures and dry-runs."""

    from shared.adapters import InMemoryAdapterCatalog

    return InMemoryAdapterCatalog(
        [
            AdapterCatalogEntry(
                source_id=CMS_POS_SOURCE_ID,
                source_url=source_url,
                change_mode="release_metadata",
                release_locator=release_locator,
                rights_status="approved_public",
            )
        ]
    )


__all__ = [
    "CMS_POS_IDENTIFIER_COLUMN",
    "CMS_POS_MEDIA_TYPE",
    "CMS_POS_RECORD_TYPE",
    "CMS_POS_SCHEMA_VERSION",
    "CMS_POS_SOURCE_ID",
    "CMS_POS_MAX_PREVIEW_ROWS",
    "CMS_POS_MAX_ROWS",
    "CmsPosAuthorizationError",
    "CmsPosDistribution",
    "CmsPosError",
    "CmsPosProducer",
    "CmsPosReceipt",
    "CmsPosRelease",
    "CmsPosResult",
    "CmsPosSourceRow",
    "CmsPosState",
    "build_cms_pos_catalog",
    "load_cms_pos_fixture",
    "validate_cms_pos_result",
]
