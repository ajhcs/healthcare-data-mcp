"""Bounded, source-safe correlation telemetry for data-platform runs.

The recorder accepts one immutable run telemetry event at a time and stores
only identifiers, counters, timestamps, bounded dimensions, and redacted
failure evidence.  SQLite admission is append-only and idempotent by
``telemetry_id``; it never stores source payloads, request content, or
credentials.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
from types import MappingProxyType
from typing import Literal, TypeAlias, cast


TELEMETRY_SCHEMA_VERSION = "hdp.telemetry.v1"
TELEMETRY_RECORD_TYPE = "run_telemetry"
TELEMETRY_RECEIPT_RECORD_TYPE = "telemetry_receipt"
TELEMETRY_SUMMARY_RECORD_TYPE = "run_telemetry_summary"
MAX_RUN_ID_LENGTH = 128
MAX_SOURCE_ID_LENGTH = 200
MAX_ARTIFACT_ID_LENGTH = 256
MAX_ENVELOPE_ID_LENGTH = 256
MAX_TELEMETRY_ID_LENGTH = 160
MAX_FAILURE_CODE_LENGTH = 96
MAX_FAILURE_LENGTH = 512
MAX_DIMENSION_KEY_LENGTH = 32
MAX_DIMENSION_VALUE_LENGTH = 128
MAX_DIMENSIONS = 8
MAX_METRIC_SECONDS = 315_360_000.0
MAX_METRIC_VALUE = 1_000_000_000_000
MAX_RETRY_COUNT = 100
MAX_EVENTS = 100_000
MAX_DIMENSION_VALUES = 128

TelemetryStatus: TypeAlias = Literal["started", "succeeded", "failed", "partial", "dead_lettered"]
FreshnessState: TypeAlias = Literal["fresh", "stale", "unknown"]
DlqState: TypeAlias = Literal["none", "queued", "resolved"]
TelemetryAction: TypeAlias = Literal["recorded", "duplicate"]
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
Clock: TypeAlias = Callable[[], datetime]

DEFAULT_DIMENSION_KEYS = frozenset(
    {
        "dataset",
        "environment",
        "operation",
        "release",
        "source_kind",
        "stage",
        "worker_class",
    }
)

_RUN_ID = re.compile(r"^run:[a-z0-9][a-z0-9._:/-]*$")
_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_ARTIFACT_ID = re.compile(r"^artifact:[a-z0-9][a-z0-9._:-]*$")
_ENVELOPE_ID = re.compile(r"^hdp:observation-envelope:[a-z0-9][a-z0-9._:-]*$")
_TELEMETRY_ID = re.compile(r"^telemetry:run:[a-z0-9][a-z0-9._:/-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_FAILURE_CODE = re.compile(r"^[a-z0-9][a-z0-9._:-]*$")
_DIMENSION_KEY = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_DIMENSION_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_SENSITIVE_FAILURE = re.compile(
    r"(?i)(?:authorization|proxy-authorization|cookie|set-cookie|x-api-key|api[_-]?key|password|private[_-]?key|"
    r"secret|token)\s*[:=]\s*(?:(?:bearer|basic)\s+)?\S+|(?:bearer|basic)\s+\S+"
)
_CREDENTIAL_URL = re.compile(r"(?i)(https?://[^:/\s]+:)[^@/\s]+@")
_SENSITIVE_DIMENSION = re.compile(r"(?i)(?:authorization|cookie|key|password|secret|token|url|query|path)")


class TelemetryError(ValueError):
    """Raised when telemetry metadata or an operation violates the contract."""


class TelemetryValidationError(TelemetryError):
    """Raised when telemetry input fails closed validation."""


class TelemetryCollisionError(TelemetryError):
    """Raised when an immutable telemetry identity is reused with new data."""


class TelemetryCapacityError(TelemetryError):
    """Raised when a configured event or dimension cardinality bound is full."""


class TelemetryStateError(TelemetryError):
    """Raised when a recorder is closed or a stored record is unavailable."""


class TelemetryNotFoundError(TelemetryStateError):
    """Raised when a requested telemetry identity or run has no record."""


def _required_text(value: object, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TelemetryValidationError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise TelemetryValidationError(f"{label} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise TelemetryValidationError(f"{label} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise TelemetryValidationError(f"{label} contains malformed Unicode") from exc
    return value


def _identifier(value: object, label: str, pattern: re.Pattern[str], maximum: int) -> str:
    result = _required_text(value, label, maximum)
    if pattern.fullmatch(result) is None:
        raise TelemetryValidationError(f"{label} has an invalid format")
    return result


def _bounded_int(value: object, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise TelemetryValidationError(f"{label} must be an integer between 0 and {maximum}")
    return value


def _bounded_seconds(value: object, label: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TelemetryValidationError(f"{label} must be a finite number")
    try:
        result = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise TelemetryValidationError(f"{label} must be a finite number") from exc
    if not math.isfinite(result) or result < 0 or result > maximum:
        raise TelemetryValidationError(f"{label} must be finite, non-negative, and at most {maximum}")
    return result


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise TelemetryValidationError(f"{label} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _parse_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise TelemetryValidationError(f"{label} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TelemetryValidationError(f"{label} must be an ISO-8601 timestamp") from exc
    return _utc(parsed, label)


def _format_timestamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: object, label: str) -> datetime | None:
    if value is None:
        return None
    return _parse_timestamp(value, label)


def _fingerprint(value: object, label: str = "telemetry_sha256") -> str:
    if not isinstance(value, str):
        raise TelemetryValidationError(f"{label} must be a sha256 fingerprint")
    candidate = value if value.startswith("sha256:") else "sha256:" + value
    if _SHA256.fullmatch(candidate) is None:
        raise TelemetryValidationError(f"{label} must be a sha256 fingerprint")
    return candidate


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _redact_failure(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TelemetryValidationError(f"{label} must be a string or null")
    normalized = "".join(character if ord(character) >= 0x20 and ord(character) != 0x7F else " " for character in value)
    normalized = _SENSITIVE_FAILURE.sub("[REDACTED]", normalized)
    normalized = _CREDENTIAL_URL.sub(r"\1[REDACTED]@", normalized).strip()
    if not normalized:
        raise TelemetryValidationError(f"{label} must not be blank when supplied")
    return normalized[:MAX_FAILURE_LENGTH]


def _dimensions(value: object) -> Mapping[str, str]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise TelemetryValidationError("dimensions must be an object")
    if len(value) > MAX_DIMENSIONS:
        raise TelemetryValidationError(f"dimensions cannot contain more than {MAX_DIMENSIONS} labels")
    normalized: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = _required_text(raw_key, "dimension key", MAX_DIMENSION_KEY_LENGTH)
        if _DIMENSION_KEY.fullmatch(key) is None:
            raise TelemetryValidationError("dimension key has an invalid format")
        if _SENSITIVE_DIMENSION.search(key):
            raise TelemetryValidationError("sensitive dimension keys are not allowed")
        item = _required_text(raw_value, f"dimension {key}", MAX_DIMENSION_VALUE_LENGTH)
        if _DIMENSION_VALUE.fullmatch(item) is None:
            raise TelemetryValidationError(f"dimension {key} must be a safe bounded value")
        if _SENSITIVE_FAILURE.search(item):
            raise TelemetryValidationError(f"dimension {key} contains sensitive material")
        normalized[key] = item
    return MappingProxyType(dict(sorted(normalized.items())))


def _schema_path() -> Path:
    return Path(__file__).resolve().parents[2] / "contracts/healthcare-data-platform/telemetry/v1/telemetry.schema.json"


def _validate_schema(value: object, label: str) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker

        schema = json.loads(_schema_path().read_text(encoding="utf-8"))
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, value)),
            key=lambda error: str(error.absolute_path),
        )
    except ImportError as exc:  # pragma: no cover - environment setup failure
        raise TelemetryValidationError("jsonschema is required for telemetry validation") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise TelemetryValidationError("unable to read telemetry schema") from exc
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.absolute_path)
        suffix = f" at {location}" if location else ""
        raise TelemetryValidationError(f"{label} failed schema validation{suffix}: {error.message}")


def _telemetry_material(event: "RunTelemetry") -> dict[str, object]:
    return {
        "schema_version": TELEMETRY_SCHEMA_VERSION,
        "record_type": TELEMETRY_RECORD_TYPE,
        "telemetry_id": event.telemetry_id,
        "run_id": event.run_id,
        "source_id": event.source_id,
        "artifact_id": event.artifact_id,
        "envelope_id": event.envelope_id,
        "observed_at": _format_timestamp(event.observed_at),
        "status": event.status,
        "freshness_state": event.freshness_state,
        "freshness_seconds": event.freshness_seconds,
        "source_observed_at": _format_timestamp(event.source_observed_at) if event.source_observed_at else None,
        "failure_code": event.failure_code,
        "failure_message": event.failure_message,
        "failure_retryable": event.failure_retryable,
        "lag_seconds": event.lag_seconds,
        "bytes_in": event.bytes_in,
        "bytes_out": event.bytes_out,
        "rows_in": event.rows_in,
        "rows_out": event.rows_out,
        "retry_count": event.retry_count,
        "dlq_state": event.dlq_state,
        "dlq_count": event.dlq_count,
        "dlq_reason": event.dlq_reason,
        "dimensions": dict(event.dimensions),
    }


@dataclass(frozen=True, slots=True)
class RunTelemetry:
    """One immutable, correlated telemetry event for a data-platform run."""

    telemetry_id: str
    run_id: str
    source_id: str
    artifact_id: str
    envelope_id: str
    observed_at: datetime
    status: TelemetryStatus
    freshness_state: FreshnessState = "unknown"
    freshness_seconds: float | None = None
    source_observed_at: datetime | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    failure_retryable: bool | None = None
    lag_seconds: float = 0.0
    bytes_in: int = 0
    bytes_out: int = 0
    rows_in: int = 0
    rows_out: int = 0
    retry_count: int = 0
    dlq_state: DlqState = "none"
    dlq_count: int = 0
    dlq_reason: str | None = None
    dimensions: Mapping[str, str] = field(default_factory=dict)
    telemetry_sha256: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "telemetry_id", _identifier(self.telemetry_id, "telemetry_id", _TELEMETRY_ID, MAX_TELEMETRY_ID_LENGTH)
        )
        object.__setattr__(self, "run_id", _identifier(self.run_id, "run_id", _RUN_ID, MAX_RUN_ID_LENGTH))
        object.__setattr__(
            self, "source_id", _identifier(self.source_id, "source_id", _SOURCE_ID, MAX_SOURCE_ID_LENGTH)
        )
        object.__setattr__(
            self, "artifact_id", _identifier(self.artifact_id, "artifact_id", _ARTIFACT_ID, MAX_ARTIFACT_ID_LENGTH)
        )
        object.__setattr__(
            self, "envelope_id", _identifier(self.envelope_id, "envelope_id", _ENVELOPE_ID, MAX_ENVELOPE_ID_LENGTH)
        )
        object.__setattr__(self, "observed_at", _utc(self.observed_at, "observed_at"))
        if self.status not in {"started", "succeeded", "failed", "partial", "dead_lettered"}:
            raise TelemetryValidationError("status is unsupported")
        if self.freshness_state not in {"fresh", "stale", "unknown"}:
            raise TelemetryValidationError("freshness_state is unsupported")
        if self.freshness_state == "unknown":
            if self.freshness_seconds is not None or self.source_observed_at is not None:
                raise TelemetryValidationError("unknown freshness cannot include age or source timestamp")
        else:
            if self.freshness_seconds is None:
                raise TelemetryValidationError("known freshness requires freshness_seconds")
            object.__setattr__(
                self,
                "freshness_seconds",
                _bounded_seconds(self.freshness_seconds, "freshness_seconds", MAX_METRIC_SECONDS),
            )
            if self.source_observed_at is not None:
                object.__setattr__(self, "source_observed_at", _utc(self.source_observed_at, "source_observed_at"))
        if self.failure_code is not None:
            code = _required_text(self.failure_code, "failure_code", MAX_FAILURE_CODE_LENGTH)
            if _FAILURE_CODE.fullmatch(code) is None:
                raise TelemetryValidationError("failure_code has an invalid format")
            object.__setattr__(self, "failure_code", code)
        redacted_message = _redact_failure(self.failure_message, "failure_message")
        object.__setattr__(self, "failure_message", redacted_message)
        redacted_dlq_reason = _redact_failure(self.dlq_reason, "dlq_reason")
        object.__setattr__(self, "dlq_reason", redacted_dlq_reason)
        if self.status in {"failed", "dead_lettered"} and self.failure_code is None:
            raise TelemetryValidationError("failed or dead-lettered telemetry requires failure_code")
        if self.failure_code is None and (self.failure_message is not None or self.failure_retryable is not None):
            raise TelemetryValidationError("failure details require failure_code")
        if self.status in {"started", "succeeded"} and self.failure_code is not None:
            raise TelemetryValidationError("successful telemetry cannot include failure details")
        if self.failure_retryable is not None and not isinstance(self.failure_retryable, bool):
            raise TelemetryValidationError("failure_retryable must be boolean or null")
        object.__setattr__(self, "lag_seconds", _bounded_seconds(self.lag_seconds, "lag_seconds", MAX_METRIC_SECONDS))
        for label in ("bytes_in", "bytes_out", "rows_in", "rows_out"):
            object.__setattr__(self, label, _bounded_int(getattr(self, label), label, MAX_METRIC_VALUE))
        object.__setattr__(self, "retry_count", _bounded_int(self.retry_count, "retry_count", MAX_RETRY_COUNT))
        if self.dlq_state not in {"none", "queued", "resolved"}:
            raise TelemetryValidationError("dlq_state is unsupported")
        object.__setattr__(self, "dlq_count", _bounded_int(self.dlq_count, "dlq_count", MAX_RETRY_COUNT))
        if self.dlq_state == "none" and (self.dlq_count != 0 or self.dlq_reason is not None):
            raise TelemetryValidationError("dlq_state none requires zero count and no reason")
        if self.dlq_state == "queued" and self.dlq_count < 1:
            raise TelemetryValidationError("queued DLQ telemetry requires a positive count")
        if self.status == "dead_lettered" and self.dlq_state != "queued":
            raise TelemetryValidationError("dead-lettered telemetry requires queued DLQ state")
        normalized_dimensions = _dimensions(self.dimensions)
        object.__setattr__(self, "dimensions", normalized_dimensions)
        material_hash = "sha256:" + sha256(_canonical(_telemetry_material(self))).hexdigest()
        if self.telemetry_sha256 is None:
            object.__setattr__(self, "telemetry_sha256", material_hash)
        elif _fingerprint(self.telemetry_sha256) != material_hash:
            raise TelemetryValidationError("telemetry_sha256 does not match canonical telemetry")

    @property
    def input_bytes(self) -> int:
        """Alias for bytes received from the source plane."""

        return self.bytes_in

    @property
    def output_bytes(self) -> int:
        """Alias for bytes emitted to the next data-platform plane."""

        return self.bytes_out

    @property
    def input_rows(self) -> int:
        """Alias for rows received from the source plane."""

        return self.rows_in

    @property
    def output_rows(self) -> int:
        """Alias for rows emitted to the next data-platform plane."""

        return self.rows_out

    def as_dict(self) -> dict[str, object]:
        """Return a strict JSON-safe telemetry event."""

        payload = {**_telemetry_material(self), "telemetry_sha256": self.telemetry_sha256}
        _validate_schema(payload, "run telemetry")
        return payload

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "RunTelemetry":
        """Parse a schema-validated event without accepting unknown fields."""

        _validate_schema(dict(value), "run telemetry")
        expected = {
            "schema_version",
            "record_type",
            "telemetry_id",
            "telemetry_sha256",
            "run_id",
            "source_id",
            "artifact_id",
            "envelope_id",
            "observed_at",
            "status",
            "freshness_state",
            "freshness_seconds",
            "source_observed_at",
            "failure_code",
            "failure_message",
            "failure_retryable",
            "lag_seconds",
            "bytes_in",
            "bytes_out",
            "rows_in",
            "rows_out",
            "retry_count",
            "dlq_state",
            "dlq_count",
            "dlq_reason",
            "dimensions",
        }
        unknown = set(value) - expected
        if unknown:
            raise TelemetryValidationError(f"run telemetry has unknown fields: {sorted(unknown)}")
        raw_dimensions = value.get("dimensions")
        if not isinstance(raw_dimensions, Mapping):
            raise TelemetryValidationError("dimensions must be an object")
        raw_status = value.get("status")
        raw_freshness = value.get("freshness_state")
        raw_dlq = value.get("dlq_state")
        if raw_status not in {"started", "succeeded", "failed", "partial", "dead_lettered"}:
            raise TelemetryValidationError("status is unsupported")
        if raw_freshness not in {"fresh", "stale", "unknown"}:
            raise TelemetryValidationError("freshness_state is unsupported")
        if raw_dlq not in {"none", "queued", "resolved"}:
            raise TelemetryValidationError("dlq_state is unsupported")
        raw_retryable = value.get("failure_retryable")
        if raw_retryable is not None and not isinstance(raw_retryable, bool):
            raise TelemetryValidationError("failure_retryable must be boolean or null")
        return cls(
            telemetry_id=cast(str, value.get("telemetry_id")),
            run_id=cast(str, value.get("run_id")),
            source_id=cast(str, value.get("source_id")),
            artifact_id=cast(str, value.get("artifact_id")),
            envelope_id=cast(str, value.get("envelope_id")),
            observed_at=_parse_timestamp(value.get("observed_at"), "observed_at"),
            status=cast(TelemetryStatus, raw_status),
            freshness_state=cast(FreshnessState, raw_freshness),
            freshness_seconds=cast(float | None, value.get("freshness_seconds")),
            source_observed_at=_optional_timestamp(value.get("source_observed_at"), "source_observed_at"),
            failure_code=cast(str | None, value.get("failure_code")),
            failure_message=cast(str | None, value.get("failure_message")),
            failure_retryable=cast(bool | None, raw_retryable),
            lag_seconds=cast(float, value.get("lag_seconds")),
            bytes_in=cast(int, value.get("bytes_in")),
            bytes_out=cast(int, value.get("bytes_out")),
            rows_in=cast(int, value.get("rows_in")),
            rows_out=cast(int, value.get("rows_out")),
            retry_count=cast(int, value.get("retry_count")),
            dlq_state=cast(DlqState, raw_dlq),
            dlq_count=cast(int, value.get("dlq_count")),
            dlq_reason=cast(str | None, value.get("dlq_reason")),
            dimensions=cast(Mapping[str, str], raw_dimensions),
            telemetry_sha256=cast(str, value.get("telemetry_sha256")),
        )


TelemetryEvent = RunTelemetry


def build_run_telemetry(
    telemetry_id: str,
    run_id: str,
    source_id: str,
    artifact_id: str,
    envelope_id: str,
    observed_at: datetime,
    status: TelemetryStatus,
    *,
    freshness_state: FreshnessState = "unknown",
    freshness_seconds: float | None = None,
    source_observed_at: datetime | None = None,
    failure_code: str | None = None,
    failure_message: str | None = None,
    failure_retryable: bool | None = None,
    lag_seconds: float = 0.0,
    bytes_in: int = 0,
    bytes_out: int = 0,
    rows_in: int = 0,
    rows_out: int = 0,
    retry_count: int = 0,
    dlq_state: DlqState = "none",
    dlq_count: int = 0,
    dlq_reason: str | None = None,
    dimensions: Mapping[str, str] | None = None,
) -> RunTelemetry:
    """Build one event from caller-owned run measurements."""

    return RunTelemetry(
        telemetry_id=telemetry_id,
        run_id=run_id,
        source_id=source_id,
        artifact_id=artifact_id,
        envelope_id=envelope_id,
        observed_at=observed_at,
        status=status,
        freshness_state=freshness_state,
        freshness_seconds=freshness_seconds,
        source_observed_at=source_observed_at,
        failure_code=failure_code,
        failure_message=failure_message,
        failure_retryable=failure_retryable,
        lag_seconds=lag_seconds,
        bytes_in=bytes_in,
        bytes_out=bytes_out,
        rows_in=rows_in,
        rows_out=rows_out,
        retry_count=retry_count,
        dlq_state=dlq_state,
        dlq_count=dlq_count,
        dlq_reason=dlq_reason,
        dimensions={} if dimensions is None else dimensions,
    )


@dataclass(frozen=True, slots=True)
class TelemetryReceipt:
    """Durable admission receipt for one telemetry event."""

    action: TelemetryAction
    telemetry_id: str
    run_id: str
    telemetry_sha256: str
    durable: bool = True

    def __post_init__(self) -> None:
        if self.action not in {"recorded", "duplicate"}:
            raise TelemetryValidationError("telemetry receipt action is unsupported")
        object.__setattr__(
            self, "telemetry_id", _identifier(self.telemetry_id, "telemetry_id", _TELEMETRY_ID, MAX_TELEMETRY_ID_LENGTH)
        )
        object.__setattr__(self, "run_id", _identifier(self.run_id, "run_id", _RUN_ID, MAX_RUN_ID_LENGTH))
        object.__setattr__(self, "telemetry_sha256", _fingerprint(self.telemetry_sha256))
        if self.durable is not True:
            raise TelemetryValidationError("telemetry receipt must be durable")

    def as_dict(self) -> dict[str, object]:
        payload = {
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "record_type": TELEMETRY_RECEIPT_RECORD_TYPE,
            "action": self.action,
            "telemetry_id": self.telemetry_id,
            "run_id": self.run_id,
            "telemetry_sha256": self.telemetry_sha256,
            "durable": self.durable,
        }
        _validate_schema(payload, "telemetry receipt")
        return payload


@dataclass(frozen=True, slots=True)
class RunTelemetrySummary:
    """Deterministic aggregate over events sharing one run correlation ID."""

    run_id: str
    source_id: str | None
    artifact_id: str | None
    envelope_id: str | None
    event_count: int
    status_counts: Mapping[str, int]
    failure_count: int
    first_observed_at: datetime
    last_observed_at: datetime
    freshness_max_seconds: float | None
    lag_max_seconds: float
    bytes_in: int
    bytes_out: int
    rows_in: int
    rows_out: int
    retry_count: int
    dlq_count: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _identifier(self.run_id, "run_id", _RUN_ID, MAX_RUN_ID_LENGTH))
        for label, pattern, maximum in (
            ("source_id", _SOURCE_ID, MAX_SOURCE_ID_LENGTH),
            ("artifact_id", _ARTIFACT_ID, MAX_ARTIFACT_ID_LENGTH),
            ("envelope_id", _ENVELOPE_ID, MAX_ENVELOPE_ID_LENGTH),
        ):
            value = getattr(self, label)
            if value is not None:
                object.__setattr__(self, label, _identifier(value, label, pattern, maximum))
        object.__setattr__(self, "event_count", _bounded_int(self.event_count, "event_count", MAX_EVENTS))
        if self.event_count < 1:
            raise TelemetryValidationError("event_count must be positive")
        if not isinstance(self.status_counts, Mapping):
            raise TelemetryValidationError("status_counts must be an object")
        statuses: dict[str, int] = {}
        for key, value in self.status_counts.items():
            if key not in {"started", "succeeded", "failed", "partial", "dead_lettered"}:
                raise TelemetryValidationError("status_counts contains an unsupported status")
            statuses[key] = _bounded_int(value, f"status_counts[{key}]", MAX_EVENTS)
        if sum(statuses.values()) != self.event_count:
            raise TelemetryValidationError("status_counts must sum to event_count")
        object.__setattr__(self, "status_counts", MappingProxyType(dict(sorted(statuses.items()))))
        object.__setattr__(self, "failure_count", _bounded_int(self.failure_count, "failure_count", MAX_EVENTS))
        if self.failure_count > self.event_count:
            raise TelemetryValidationError("failure_count cannot exceed event_count")
        object.__setattr__(self, "first_observed_at", _utc(self.first_observed_at, "first_observed_at"))
        object.__setattr__(self, "last_observed_at", _utc(self.last_observed_at, "last_observed_at"))
        if self.last_observed_at < self.first_observed_at:
            raise TelemetryValidationError("last_observed_at cannot precede first_observed_at")
        if self.freshness_max_seconds is not None:
            object.__setattr__(
                self,
                "freshness_max_seconds",
                _bounded_seconds(self.freshness_max_seconds, "freshness_max_seconds", MAX_METRIC_SECONDS),
            )
        object.__setattr__(
            self, "lag_max_seconds", _bounded_seconds(self.lag_max_seconds, "lag_max_seconds", MAX_METRIC_SECONDS)
        )
        for label in ("bytes_in", "bytes_out", "rows_in", "rows_out"):
            object.__setattr__(self, label, _bounded_int(getattr(self, label), label, MAX_METRIC_VALUE * MAX_EVENTS))
        object.__setattr__(
            self, "retry_count", _bounded_int(self.retry_count, "retry_count", MAX_RETRY_COUNT * MAX_EVENTS)
        )
        object.__setattr__(self, "dlq_count", _bounded_int(self.dlq_count, "dlq_count", MAX_RETRY_COUNT * MAX_EVENTS))

    def as_dict(self) -> dict[str, object]:
        payload = {
            "schema_version": TELEMETRY_SCHEMA_VERSION,
            "record_type": TELEMETRY_SUMMARY_RECORD_TYPE,
            "run_id": self.run_id,
            "source_id": self.source_id,
            "artifact_id": self.artifact_id,
            "envelope_id": self.envelope_id,
            "event_count": self.event_count,
            "status_counts": dict(self.status_counts),
            "failure_count": self.failure_count,
            "first_observed_at": _format_timestamp(self.first_observed_at),
            "last_observed_at": _format_timestamp(self.last_observed_at),
            "freshness_max_seconds": self.freshness_max_seconds,
            "lag_max_seconds": self.lag_max_seconds,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "retry_count": self.retry_count,
            "dlq_count": self.dlq_count,
        }
        _validate_schema(payload, "run telemetry summary")
        return payload


class TelemetryRecorder:
    """Append-only SQLite recorder for bounded correlated telemetry."""

    def __init__(
        self,
        database: str | Path | sqlite3.Connection,
        *,
        max_events: int = MAX_EVENTS,
        max_dimension_values: int = MAX_DIMENSION_VALUES,
        dimension_allowlist: Collection[str] | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.max_events = _bounded_int(max_events, "max_events", MAX_EVENTS)
        if self.max_events < 1:
            raise TelemetryValidationError("max_events must be positive")
        self.max_dimension_values = _bounded_int(max_dimension_values, "max_dimension_values", MAX_DIMENSION_VALUES)
        if self.max_dimension_values < 1:
            raise TelemetryValidationError("max_dimension_values must be positive")
        raw_allowlist = DEFAULT_DIMENSION_KEYS if dimension_allowlist is None else frozenset(dimension_allowlist)
        if not raw_allowlist or not raw_allowlist <= DEFAULT_DIMENSION_KEYS:
            raise TelemetryValidationError("dimension_allowlist must be a non-empty subset of approved keys")
        self.dimension_allowlist = frozenset(raw_allowlist)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        if isinstance(database, sqlite3.Connection):
            self._connection = database
            self._owns_connection = False
        else:
            self._connection = sqlite3.connect(str(database), timeout=30, isolation_level=None, check_same_thread=False)
            self._owns_connection = True
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout=30000")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._initialize()

    def __enter__(self) -> "TelemetryRecorder":
        self._ensure_open()
        return self

    def __exit__(self, _exc_type: object, _exc_value: object, _traceback: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                if self._owns_connection:
                    self._connection.close()
                self._closed = True

    def record(self, event: RunTelemetry | Mapping[str, object]) -> TelemetryReceipt:
        """Durably record or replay one canonical event by telemetry identity."""

        item = event if isinstance(event, RunTelemetry) else RunTelemetry.from_mapping(event)
        if item.telemetry_sha256 is None:  # pragma: no cover - __post_init__ always derives the hash
            raise TelemetryStateError("telemetry event has no canonical fingerprint")
        telemetry_hash = item.telemetry_sha256
        with self._transaction():
            existing = self._connection.execute(
                "SELECT run_id, telemetry_sha256 FROM telemetry_events WHERE telemetry_id = ?",
                (item.telemetry_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["telemetry_sha256"]) != telemetry_hash:
                    raise TelemetryCollisionError("telemetry_id was reused for different canonical data")
                return TelemetryReceipt("duplicate", item.telemetry_id, item.run_id, telemetry_hash)
            count = int(self._connection.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0])
            if count >= self.max_events:
                raise TelemetryCapacityError(f"telemetry event capacity {self.max_events} is exhausted")
            for key, value in item.dimensions.items():
                if key not in self.dimension_allowlist:
                    raise TelemetryValidationError(f"dimension {key} is not in the recorder allowlist")
                known = self._connection.execute(
                    "SELECT 1 FROM telemetry_dimension_values WHERE dimension_key = ? AND dimension_value = ? LIMIT 1",
                    (key, value),
                ).fetchone()
                if known is None:
                    distinct = int(
                        self._connection.execute(
                            "SELECT COUNT(DISTINCT dimension_value) FROM telemetry_dimension_values WHERE dimension_key = ?",
                            (key,),
                        ).fetchone()[0]
                    )
                    if distinct >= self.max_dimension_values:
                        raise TelemetryCapacityError(
                            f"dimension {key} cardinality {self.max_dimension_values} is exhausted"
                        )
            self._connection.execute(
                """
                INSERT INTO telemetry_events (
                    telemetry_id, run_id, source_id, artifact_id, envelope_id, observed_at, status,
                    freshness_state, freshness_seconds, source_observed_at, failure_code, failure_message,
                    failure_retryable, lag_seconds, bytes_in, bytes_out, rows_in, rows_out, retry_count,
                    dlq_state, dlq_count, dlq_reason, dimensions_json, telemetry_sha256, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.telemetry_id,
                    item.run_id,
                    item.source_id,
                    item.artifact_id,
                    item.envelope_id,
                    _format_timestamp(item.observed_at),
                    item.status,
                    item.freshness_state,
                    item.freshness_seconds,
                    _format_timestamp(item.source_observed_at) if item.source_observed_at else None,
                    item.failure_code,
                    item.failure_message,
                    item.failure_retryable,
                    item.lag_seconds,
                    item.bytes_in,
                    item.bytes_out,
                    item.rows_in,
                    item.rows_out,
                    item.retry_count,
                    item.dlq_state,
                    item.dlq_count,
                    item.dlq_reason,
                    json.dumps(dict(item.dimensions), sort_keys=True, separators=(",", ":")),
                    telemetry_hash,
                    _format_timestamp(_utc(self.clock(), "clock")),
                ),
            )
            self._connection.executemany(
                "INSERT INTO telemetry_dimension_values (telemetry_id, dimension_key, dimension_value) VALUES (?, ?, ?)",
                [(item.telemetry_id, key, value) for key, value in item.dimensions.items()],
            )
            return TelemetryReceipt("recorded", item.telemetry_id, item.run_id, telemetry_hash)

    def record_event(self, event: RunTelemetry | Mapping[str, object]) -> TelemetryReceipt:
        """Explicit alias for :meth:`record` used by event-oriented callers."""

        return self.record(event)

    def append(self, event: RunTelemetry | Mapping[str, object]) -> TelemetryReceipt:
        """Append one event, retaining the idempotent admission behavior."""

        return self.record(event)

    def read(self, telemetry_id: str) -> RunTelemetry:
        """Read one durable event by its immutable identity."""

        identity = _identifier(telemetry_id, "telemetry_id", _TELEMETRY_ID, MAX_TELEMETRY_ID_LENGTH)
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT * FROM telemetry_events WHERE telemetry_id = ?", (identity,)
            ).fetchone()
        if row is None:
            raise TelemetryNotFoundError(f"unknown telemetry_id: {identity}")
        return self._row_to_event(row)

    def list_events(
        self, run_id: str, *, source_id: str | None = None, limit: int = MAX_EVENTS
    ) -> tuple[RunTelemetry, ...]:
        """Return deterministic events for one run without exposing payload data."""

        run = _identifier(run_id, "run_id", _RUN_ID, MAX_RUN_ID_LENGTH)
        source = None if source_id is None else _identifier(source_id, "source_id", _SOURCE_ID, MAX_SOURCE_ID_LENGTH)
        bounded_limit = _bounded_int(limit, "limit", MAX_EVENTS)
        if bounded_limit < 1:
            raise TelemetryValidationError("limit must be positive")
        with self._lock:
            self._ensure_open()
            if source is None:
                rows = self._connection.execute(
                    "SELECT * FROM telemetry_events WHERE run_id = ? ORDER BY observed_at, telemetry_id LIMIT ?",
                    (run, bounded_limit),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM telemetry_events WHERE run_id = ? AND source_id = ? ORDER BY observed_at, telemetry_id LIMIT ?",
                    (run, source, bounded_limit),
                ).fetchall()
        return tuple(self._row_to_event(row) for row in rows)

    def summarize(self, run_id: str, *, source_id: str | None = None) -> RunTelemetrySummary:
        """Aggregate bounded counters and outcomes for one correlated run."""

        events = self.list_events(run_id, source_id=source_id)
        if not events:
            raise TelemetryNotFoundError(f"no telemetry for run_id: {run_id}")
        statuses: dict[str, int] = {}
        for event in events:
            statuses[event.status] = statuses.get(event.status, 0) + 1
        unique_sources = {event.source_id for event in events}
        unique_artifacts = {event.artifact_id for event in events}
        unique_envelopes = {event.envelope_id for event in events}
        freshness = [event.freshness_seconds for event in events if event.freshness_seconds is not None]
        return RunTelemetrySummary(
            run_id=events[0].run_id,
            source_id=next(iter(unique_sources)) if len(unique_sources) == 1 else None,
            artifact_id=next(iter(unique_artifacts)) if len(unique_artifacts) == 1 else None,
            envelope_id=next(iter(unique_envelopes)) if len(unique_envelopes) == 1 else None,
            event_count=len(events),
            status_counts=statuses,
            failure_count=sum(event.status in {"failed", "dead_lettered"} for event in events),
            first_observed_at=min(event.observed_at for event in events),
            last_observed_at=max(event.observed_at for event in events),
            freshness_max_seconds=max(freshness) if freshness else None,
            lag_max_seconds=max(event.lag_seconds for event in events),
            bytes_in=sum(event.bytes_in for event in events),
            bytes_out=sum(event.bytes_out for event in events),
            rows_in=sum(event.rows_in for event in events),
            rows_out=sum(event.rows_out for event in events),
            retry_count=sum(event.retry_count for event in events),
            dlq_count=sum(event.dlq_count for event in events),
        )

    def summary(self, run_id: str, *, source_id: str | None = None) -> RunTelemetrySummary:
        """Alias for :meth:`summarize`."""

        return self.summarize(run_id, source_id=source_id)

    def count(self, run_id: str | None = None) -> int:
        """Return the number of durable events, optionally scoped to a run."""

        with self._lock:
            self._ensure_open()
            if run_id is None:
                return int(self._connection.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0])
            run = _identifier(run_id, "run_id", _RUN_ID, MAX_RUN_ID_LENGTH)
            return int(
                self._connection.execute("SELECT COUNT(*) FROM telemetry_events WHERE run_id = ?", (run,)).fetchone()[0]
            )

    def _row_to_event(self, row: sqlite3.Row) -> RunTelemetry:
        try:
            dimensions = json.loads(str(row["dimensions_json"]))
        except (TypeError, json.JSONDecodeError) as exc:
            raise TelemetryStateError("stored telemetry dimensions are malformed") from exc
        if not isinstance(dimensions, Mapping):
            raise TelemetryStateError("stored telemetry dimensions are not an object")
        return RunTelemetry(
            telemetry_id=str(row["telemetry_id"]),
            run_id=str(row["run_id"]),
            source_id=str(row["source_id"]),
            artifact_id=str(row["artifact_id"]),
            envelope_id=str(row["envelope_id"]),
            observed_at=_parse_timestamp(row["observed_at"], "observed_at"),
            status=cast(TelemetryStatus, str(row["status"])),
            freshness_state=cast(FreshnessState, str(row["freshness_state"])),
            freshness_seconds=None if row["freshness_seconds"] is None else float(row["freshness_seconds"]),
            source_observed_at=_optional_timestamp(row["source_observed_at"], "source_observed_at"),
            failure_code=None if row["failure_code"] is None else str(row["failure_code"]),
            failure_message=None if row["failure_message"] is None else str(row["failure_message"]),
            failure_retryable=None if row["failure_retryable"] is None else bool(row["failure_retryable"]),
            lag_seconds=float(row["lag_seconds"]),
            bytes_in=int(row["bytes_in"]),
            bytes_out=int(row["bytes_out"]),
            rows_in=int(row["rows_in"]),
            rows_out=int(row["rows_out"]),
            retry_count=int(row["retry_count"]),
            dlq_state=cast(DlqState, str(row["dlq_state"])),
            dlq_count=int(row["dlq_count"]),
            dlq_reason=None if row["dlq_reason"] is None else str(row["dlq_reason"]),
            dimensions=cast(Mapping[str, str], dimensions),
            telemetry_sha256=str(row["telemetry_sha256"]),
        )

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS telemetry_events (
                telemetry_id TEXT PRIMARY KEY NOT NULL,
                run_id TEXT NOT NULL,
                source_id TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                freshness_state TEXT NOT NULL,
                freshness_seconds REAL,
                source_observed_at TEXT,
                failure_code TEXT,
                failure_message TEXT,
                failure_retryable INTEGER,
                lag_seconds REAL NOT NULL,
                bytes_in INTEGER NOT NULL,
                bytes_out INTEGER NOT NULL,
                rows_in INTEGER NOT NULL,
                rows_out INTEGER NOT NULL,
                retry_count INTEGER NOT NULL,
                dlq_state TEXT NOT NULL,
                dlq_count INTEGER NOT NULL,
                dlq_reason TEXT,
                dimensions_json TEXT NOT NULL,
                telemetry_sha256 TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS telemetry_dimension_values (
                telemetry_id TEXT NOT NULL REFERENCES telemetry_events(telemetry_id) ON DELETE CASCADE,
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL,
                PRIMARY KEY (telemetry_id, dimension_key)
            );
            CREATE INDEX IF NOT EXISTS telemetry_events_run_idx
                ON telemetry_events (run_id, observed_at, telemetry_id);
            CREATE INDEX IF NOT EXISTS telemetry_dimensions_key_idx
                ON telemetry_dimension_values (dimension_key, dimension_value);
            """
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                yield
                self._connection.execute("COMMIT")
            except Exception:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise TelemetryStateError("telemetry recorder is closed")


CorrelatedRunTelemetryRecorder = TelemetryRecorder
