"""Transport-neutral contracts for source adapter implementations.

These contracts intentionally contain no HTTP client, database, queue, or
object-store dependency.  An adapter can use them with any transport while
keeping validators, rights metadata, checkpoint compare-and-swap, and
fingerprint identity consistent across sources.
"""

from __future__ import annotations

from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from hashlib import sha256
import json
import math
import re
import threading
from types import MappingProxyType
from typing import Iterable, Literal, Mapping, Protocol, TypeAlias, runtime_checkable
from urllib.parse import urlsplit


JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
AdapterChangeMode: TypeAlias = Literal["etag", "last_modified", "release_metadata", "content_hash"]
ProbeState: TypeAlias = Literal["changed", "not_modified", "failed_probe"]
RightsStatus: TypeAlias = Literal["approved_public", "pending_review", "blocked"]

_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_HTTPS = re.compile(r"^https://[A-Za-z0-9._:/-]+$")
_HOST = re.compile(
    r"^(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
)
_ETAG = re.compile(r'^(?:W/)?"[\x21\x23-\x7e]*"$')
_IMF_FIXDATE = re.compile(
    r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), [0-9]{2} "
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{4} "
    r"[0-9]{2}:[0-9]{2}:[0-9]{2} GMT$"
)
_MAX_SOURCE_ID = 200
_MAX_URL = 2048
_MAX_HEADER_VALUE = 512
_MAX_CURSOR = 512


class AdapterContractError(ValueError):
    """Raised when a source adapter value violates a shared contract."""


class CursorConflictError(AdapterContractError):
    """Raised when a cursor compare-and-swap precondition is stale."""


def _required_text(value: object, label: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AdapterContractError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise AdapterContractError(f"{label} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise AdapterContractError(f"{label} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise AdapterContractError(f"{label} contains malformed Unicode") from exc
    return value


def _required_source_id(value: object, label: str = "source_id") -> str:
    text = _required_text(value, label, maximum=_MAX_SOURCE_ID)
    if _SOURCE_ID.fullmatch(text) is None:
        raise AdapterContractError(f"{label} has an invalid format")
    return text


def _required_https(value: object, label: str) -> str:
    text = _required_text(value, label, maximum=_MAX_URL)
    if _HTTPS.fullmatch(text) is None:
        raise AdapterContractError(f"{label} must be an HTTPS URL")
    try:
        parsed = urlsplit(text)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise AdapterContractError(f"{label} has a malformed authority") from exc
    if parsed.scheme != "https" or not parsed.netloc or hostname is None or _HOST.fullmatch(hostname) is None:
        raise AdapterContractError(f"{label} has a malformed authority")
    if parsed.username is not None or parsed.password is not None:
        raise AdapterContractError(f"{label} must not contain user information")
    return text


def _optional_header(value: object, label: str) -> str | None:
    if value is None:
        return None
    text = _required_text(value, label, maximum=_MAX_HEADER_VALUE)
    if not text.strip():
        raise AdapterContractError(f"{label} must not be blank")
    return text


def _optional_etag(value: object, label: str, *, allow_wildcard: bool) -> str | None:
    text = _optional_header(value, label)
    if text is None:
        return None
    if allow_wildcard and text == "*":
        return text
    if _ETAG.fullmatch(text) is None:
        raise AdapterContractError(f"{label} must be an HTTP entity-tag")
    return text


def _optional_last_modified(value: object, label: str) -> str | None:
    text = _optional_header(value, label)
    if text is None:
        return None
    if _IMF_FIXDATE.fullmatch(text) is None:
        raise AdapterContractError(f"{label} must be an IMF-fixdate")
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise AdapterContractError(f"{label} must be an IMF-fixdate") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AdapterContractError(f"{label} must be an IMF-fixdate")
    if parsed.strftime("%a, %d %b %Y %H:%M:%S GMT") != text:
        raise AdapterContractError(f"{label} must be an IMF-fixdate")
    return text


def _optional_fingerprint(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise AdapterContractError(f"{label} must be a sha256 fingerprint")
    return value


@dataclass(frozen=True, slots=True)
class Fingerprint:
    """A typed SHA-256 content identity with its wire representation."""

    value: str

    def __post_init__(self) -> None:
        if _SHA256.fullmatch(self.value) is None:
            raise AdapterContractError("fingerprint must be sha256:<64 lowercase hex characters>")

    @classmethod
    def from_bytes(cls, value: bytes) -> "Fingerprint":
        if not isinstance(value, bytes):
            raise AdapterContractError("fingerprint input must be bytes")
        return cls(fingerprint_bytes(value))

    @classmethod
    def from_json(cls, value: JsonValue) -> "Fingerprint":
        return cls(fingerprint_json(value))

    @property
    def algorithm(self) -> Literal["sha256"]:
        return "sha256"

    @property
    def digest(self) -> str:
        return self.value.removeprefix("sha256:")

    def __str__(self) -> str:
        return self.value


def fingerprint_bytes(value: bytes) -> str:
    """Return a stable, prefixed SHA-256 digest for exact bytes."""

    if not isinstance(value, bytes):
        raise AdapterContractError("fingerprint input must be bytes")
    return "sha256:" + sha256(value).hexdigest()


def _canonical_json_bytes(value: JsonValue) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, UnicodeError, ValueError) as exc:
        raise AdapterContractError("value cannot be canonically fingerprinted as JSON") from exc


def fingerprint_json(value: JsonValue) -> str:
    """Return a deterministic SHA-256 fingerprint for JSON-compatible data."""

    return fingerprint_bytes(_canonical_json_bytes(value))


@dataclass(frozen=True, slots=True)
class ConditionalRequest:
    """An HTTPS URL plus safe validators for a conditional source probe."""

    url: str
    etag: str | None = None
    last_modified: str | None = None

    def __post_init__(self) -> None:
        _required_https(self.url, "url")
        object.__setattr__(self, "etag", _optional_etag(self.etag, "etag", allow_wildcard=True))
        object.__setattr__(self, "last_modified", _optional_last_modified(self.last_modified, "last_modified"))

    @property
    def headers(self) -> Mapping[str, str]:
        """Return only conditional headers, never credentials or payload data."""

        headers: dict[str, str] = {}
        if self.etag is not None:
            headers["If-None-Match"] = self.etag
        if self.last_modified is not None:
            headers["If-Modified-Since"] = self.last_modified
        return headers

    @property
    def has_validator(self) -> bool:
        return bool(self.etag or self.last_modified)


@dataclass(frozen=True, slots=True)
class ConditionalResponse:
    """Minimal response metadata required to classify a conditional probe."""

    status_code: int
    etag: str | None = None
    last_modified: str | None = None
    response_fingerprint: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.status_code, bool) or not isinstance(self.status_code, int):
            raise AdapterContractError("status_code must be an integer")
        if not 100 <= self.status_code <= 599:
            raise AdapterContractError("status_code must be between 100 and 599")
        object.__setattr__(self, "etag", _optional_etag(self.etag, "etag", allow_wildcard=False))
        object.__setattr__(self, "last_modified", _optional_last_modified(self.last_modified, "last_modified"))
        object.__setattr__(
            self,
            "response_fingerprint",
            _optional_fingerprint(self.response_fingerprint, "response_fingerprint"),
        )

    @property
    def not_modified(self) -> bool:
        return self.status_code == 304


def classify_conditional_response(status_code: int) -> ProbeState:
    """Classify an HTTP status without coupling the SDK to an HTTP client."""

    if isinstance(status_code, bool) or not isinstance(status_code, int):
        raise AdapterContractError("status_code must be an integer")
    if status_code == 304:
        return "not_modified"
    if 200 <= status_code < 300:
        return "changed"
    return "failed_probe"


@dataclass(frozen=True, slots=True)
class AdapterCatalogEntry:
    """Validated catalog metadata and explicit bounds for one adapter source."""

    source_id: str
    source_url: str
    change_mode: AdapterChangeMode
    release_locator: str
    rights_status: RightsStatus
    enabled: bool = True
    max_bytes: int = 131_072
    max_chunks: int = 128
    max_seconds: float = 60.0

    def __post_init__(self) -> None:
        _required_source_id(self.source_id)
        _required_https(self.source_url, "source_url")
        _required_https(self.release_locator, "release_locator")
        if self.change_mode not in {"etag", "last_modified", "release_metadata", "content_hash"}:
            raise AdapterContractError("change_mode is unsupported")
        if self.rights_status not in {"approved_public", "pending_review", "blocked"}:
            raise AdapterContractError("rights_status is unsupported")
        if not isinstance(self.enabled, bool):
            raise AdapterContractError("enabled must be boolean")
        if (
            isinstance(self.max_bytes, bool)
            or not isinstance(self.max_bytes, int)
            or not 1 <= self.max_bytes <= 1_073_741_824
        ):
            raise AdapterContractError("max_bytes must be an integer between 1 and 1073741824")
        if (
            isinstance(self.max_chunks, bool)
            or not isinstance(self.max_chunks, int)
            or not 1 <= self.max_chunks <= 1_000_000
        ):
            raise AdapterContractError("max_chunks must be an integer between 1 and 1000000")
        if isinstance(self.max_seconds, bool) or not isinstance(self.max_seconds, (int, float)):
            raise AdapterContractError("max_seconds must be a number")
        if not math.isfinite(self.max_seconds) or not 0 < self.max_seconds <= 86_400:
            raise AdapterContractError("max_seconds must be greater than 0 and at most 86400")
        if self.enabled and self.rights_status != "approved_public":
            raise AdapterContractError("enabled source lacks approved public rights")

    def conditional_request(
        self,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> ConditionalRequest:
        """Build a conditional request while preserving the catalog URL."""

        return ConditionalRequest(self.source_url, etag=etag, last_modified=last_modified)


@runtime_checkable
class AdapterCatalog(Protocol):
    """Read-only catalog seam used by source adapters."""

    def registration(self, source_id: str) -> AdapterCatalogEntry | None:
        """Return a source registration or ``None`` when it is unknown."""

        ...

    def get(self, source_id: str) -> AdapterCatalogEntry | None:
        """Alias for registration used by mapping-like adapter callers."""

        ...


class InMemoryAdapterCatalog:
    """Read-only catalog implementation for tests and local adapter runs."""

    def __init__(self, entries: Iterable[AdapterCatalogEntry]) -> None:
        registrations: dict[str, AdapterCatalogEntry] = {}
        for entry in entries:
            if not isinstance(entry, AdapterCatalogEntry):
                raise AdapterContractError("catalog entries must be AdapterCatalogEntry values")
            if entry.source_id in registrations:
                raise AdapterContractError(f"duplicate source_id: {entry.source_id}")
            registrations[entry.source_id] = entry
        self._registrations = registrations

    @property
    def entries(self) -> Mapping[str, AdapterCatalogEntry]:
        """Return an immutable view of the registered source entries."""

        return MappingProxyType(self._registrations)

    def registration(self, source_id: str) -> AdapterCatalogEntry | None:
        """Return an entry by validated source identity."""

        return self._registrations.get(_required_source_id(source_id))

    def get(self, source_id: str) -> AdapterCatalogEntry | None:
        """Return an entry by validated source identity."""

        return self.registration(source_id)


@dataclass(frozen=True, slots=True)
class SourceCursor:
    """Opaque source cursor plus a monotonic generation for CAS."""

    source_id: str
    token: str
    generation: int

    def __post_init__(self) -> None:
        _required_source_id(self.source_id)
        _required_text(self.token, "cursor token", maximum=_MAX_CURSOR)
        if isinstance(self.generation, bool) or not isinstance(self.generation, int) or self.generation < 1:
            raise CursorConflictError("cursor generation must be a positive integer")


@dataclass(frozen=True, slots=True)
class CursorPrecondition:
    """Expected cursor state and the next token to publish after durable work."""

    source_id: str
    expected_generation: int
    expected_token: str | None
    next_token: str

    def __post_init__(self) -> None:
        _required_source_id(self.source_id)
        if (
            isinstance(self.expected_generation, bool)
            or not isinstance(self.expected_generation, int)
            or self.expected_generation < 0
        ):
            raise CursorConflictError("expected_generation must be a non-negative integer")
        if self.expected_token is not None:
            _required_text(self.expected_token, "expected_token", maximum=_MAX_CURSOR)
        _required_text(self.next_token, "next_token", maximum=_MAX_CURSOR)


@runtime_checkable
class CursorStore(Protocol):
    """Durable cursor seam; implementations must provide atomic CAS."""

    def current(self, source_id: str) -> SourceCursor | None:
        """Return the current cursor for one source."""

        ...

    def compare_and_swap(self, precondition: CursorPrecondition) -> SourceCursor:
        """Advance the cursor only when its expected state still matches."""

        ...


class InMemoryCursorStore:
    """Thread-safe cursor CAS implementation for conformance tests and dry-runs."""

    def __init__(self) -> None:
        self._records: dict[str, SourceCursor] = {}
        self._lock = threading.Lock()

    def current(self, source_id: str) -> SourceCursor | None:
        source = _required_source_id(source_id)
        with self._lock:
            return self._records.get(source)

    def compare_and_swap(self, precondition: CursorPrecondition) -> SourceCursor:
        if not isinstance(precondition, CursorPrecondition):
            raise CursorConflictError("cursor precondition is unsupported")
        with self._lock:
            current = self._records.get(precondition.source_id)
            if current is None:
                if precondition.expected_generation != 0 or precondition.expected_token is not None:
                    raise CursorConflictError("cursor does not exist at expected generation")
                next_cursor = SourceCursor(precondition.source_id, precondition.next_token, 1)
            else:
                if (
                    current.generation != precondition.expected_generation
                    or current.token != precondition.expected_token
                ):
                    if (
                        current.generation == precondition.expected_generation + 1
                        and current.token == precondition.next_token
                    ):
                        return current
                    raise CursorConflictError("stale cursor compare-and-swap precondition")
                next_cursor = SourceCursor(
                    precondition.source_id,
                    precondition.next_token,
                    current.generation + 1,
                )
            self._records[precondition.source_id] = next_cursor
            return next_cursor


__all__ = [
    "AdapterCatalog",
    "AdapterCatalogEntry",
    "AdapterChangeMode",
    "AdapterContractError",
    "ConditionalRequest",
    "ConditionalResponse",
    "CursorConflictError",
    "CursorPrecondition",
    "CursorStore",
    "Fingerprint",
    "InMemoryAdapterCatalog",
    "InMemoryCursorStore",
    "JsonValue",
    "ProbeState",
    "SourceCursor",
    "fingerprint_bytes",
    "fingerprint_json",
    "classify_conditional_response",
]
