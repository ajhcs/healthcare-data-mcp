"""Bounded, restart-safe replay and backfill control.

The replay controller owns a small local SQLite control store.  It plans
version identities without doing network work, records an immutable plan
identity, and lets a caller-owned runner process one item at a time.  Runner
outputs are reduced to a validated SHA-256 fingerprint and opaque reference;
payload bytes, credentials, and arbitrary runner output never enter the store.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import re
import secrets
import sqlite3
import threading
from typing import Literal, TypeAlias, cast


REPLAY_SCHEMA_VERSION = "hdp.replay.v1"
REPLAY_RECORD_TYPE = "replay_plan"
MAX_SOURCE_ID_LENGTH = 200
MAX_VERSION_LENGTH = 256
MAX_CONTRACT_VERSION_LENGTH = 64
MAX_IDEMPOTENCY_KEY_LENGTH = 256
MAX_PLAN_ITEMS = 100_000
MAX_PLAN_BYTES = 1_073_741_824
MAX_ITEM_BYTES = 1_073_741_824
MAX_ATTEMPTS = 3
MAX_ERROR_LENGTH = 512
MAX_REFERENCE_LENGTH = 512
MAX_CLAIM_LEASE_SECONDS = 86_400.0
DEFAULT_MAX_ITEMS = 1_024
DEFAULT_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_ITEM_BYTES = 1
DEFAULT_CLAIM_LEASE_SECONDS = 300.0

ReplayState: TypeAlias = Literal["planned", "running", "cancel_requested", "cancelled", "completed", "failed"]
ReplayItemState: TypeAlias = Literal["pending", "in_progress", "completed", "failed"]
SubmissionAction: TypeAlias = Literal["created", "duplicate"]
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
Runner: TypeAlias = Callable[["ReplayItem"], object]
Cancellation: TypeAlias = Callable[[], bool] | object

_SOURCE_ID = re.compile(r"^source:[A-Za-z0-9][A-Za-z0-9._:-]*$")
_VERSION_ID = re.compile(r"^(?:version|release):[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_CONTRACT_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_REPLAY_ID = re.compile(r"^replay:(?:plan|item|work):[0-9a-f]{64}$")
_IDEMPOTENCY_KEY = re.compile(r"^replay:[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}$")
_TRAILING_NUMBER = re.compile(r"^(?P<prefix>.*?)(?P<number>[0-9]+)$")
_DATE_VERSION = re.compile(r"^(?P<prefix>.*?)(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})$")
_SECRET_MESSAGE = re.compile(
    r"(?i)(?:authorization\s*[:=]\s*\S+|bearer\s+\S+|(?:api[_-]?key|password|private[_-]?key|secret|token)\s*[:=]\s*\S+)"
)


class ReplayError(ValueError):
    """Raised when a replay plan or operation violates the contract."""


class ReplayValidationError(ReplayError):
    """Raised when a plan or runner result fails closed validation."""


class ReplayCollisionError(ReplayError):
    """Raised when an idempotency key is reused with different plan bytes."""


class ReplayStateError(ReplayError):
    """Raised when an operation does not match the durable replay state."""


def _required_text(value: object, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReplayValidationError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise ReplayValidationError(f"{label} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ReplayValidationError(f"{label} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ReplayValidationError(f"{label} contains malformed Unicode") from exc
    return value


def _source_id(value: object) -> str:
    result = _required_text(value, "source_id", MAX_SOURCE_ID_LENGTH)
    if _SOURCE_ID.fullmatch(result) is None:
        raise ReplayValidationError("source_id has an invalid format")
    return result


def _version_id(value: object, label: str = "version") -> str:
    result = _required_text(value, label, MAX_VERSION_LENGTH)
    if _VERSION_ID.fullmatch(result) is None:
        raise ReplayValidationError(f"{label} has an invalid format")
    return result


def _contract_version(value: object) -> str:
    result = _required_text(value, "contract_version", MAX_CONTRACT_VERSION_LENGTH)
    if _CONTRACT_VERSION.fullmatch(result) is None:
        raise ReplayValidationError("contract_version has an invalid format")
    return result


def _idempotency_key(value: object) -> str:
    result = _required_text(value, "idempotency_key", MAX_IDEMPOTENCY_KEY_LENGTH)
    if _IDEMPOTENCY_KEY.fullmatch(result) is None:
        raise ReplayValidationError("idempotency_key has an invalid format")
    return result


def _replay_id(value: object, kind: Literal["plan", "item", "work"]) -> str:
    result = _required_text(value, f"{kind}_id", 78)
    if _REPLAY_ID.fullmatch(result) is None or not result.startswith(f"replay:{kind}:"):
        raise ReplayValidationError(f"{kind}_id has an invalid format")
    return result


def _fingerprint(value: object, label: str = "sha256") -> str:
    if isinstance(value, bytes):
        return "sha256:" + sha256(value).hexdigest()
    if not isinstance(value, str):
        raise ReplayValidationError(f"{label} must be a sha256 fingerprint")
    candidate = value if value.startswith("sha256:") else "sha256:" + value
    if _SHA256.fullmatch(candidate) is None:
        raise ReplayValidationError(f"{label} must be a sha256 fingerprint")
    return candidate


def _bounded_int(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ReplayValidationError(f"{label} must be an integer between {minimum} and {maximum}")
    return value


def _bounded_seconds(value: object, label: str, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReplayValidationError(f"{label} must be a finite number")
    result = float(value)
    if not math.isfinite(result) or result <= 0 or result > maximum:
        raise ReplayValidationError(f"{label} must be finite, greater than 0, and at most {maximum}")
    return result


def _utc(value: datetime, label: str = "timestamp") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ReplayValidationError(f"{label} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object, label: str = "timestamp") -> datetime:
    if not isinstance(value, str) or not value:
        raise ReplayValidationError(f"{label} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReplayValidationError(f"{label} must be an ISO-8601 timestamp") from exc
    return _utc(parsed, label)


def _optional_timestamp(value: object, label: str) -> datetime | None:
    if value is None:
        return None
    return _parse_timestamp(value, label)


def _canonical(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReplayValidationError("replay material is not canonical JSON") from exc


def _safe_error(value: BaseException | object) -> str:
    if isinstance(value, BaseException):
        name = type(value).__name__
        try:
            detail = str(value)
        except Exception:  # pragma: no cover - hostile exception stringification
            detail = "runner failure"
        text = f"{name}: {detail}"
    else:
        text = str(value)
    lowered = text.lower()
    if _SECRET_MESSAGE.search(text) is not None or any(
        marker in lowered
        for marker in ("authorization", "bearer", "api_key", "api-key", "password", "private_key", "secret", "token")
    ):
        text = f"{type(value).__name__ if isinstance(value, BaseException) else 'runner'}: [redacted]"
    text = " ".join(character if ord(character) >= 0x20 and ord(character) != 0x7F else " " for character in text)
    text = text.strip()
    if not text:
        text = "runner failure"
    return text[:MAX_ERROR_LENGTH]


def _safe_reference(value: object, label: str = "result_ref") -> str:
    result = _required_text(value, label, MAX_REFERENCE_LENGTH)
    if _SAFE_REFERENCE.fullmatch(result) is None:
        raise ReplayValidationError(f"{label} must be a bounded opaque reference")
    return result


def _schema_path() -> Path:
    return Path(__file__).resolve().parents[2] / "contracts/healthcare-data-platform/replay/v1/replay.schema.json"


def _validate_schema(value: object, label: str) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker

        schema = json.loads(_schema_path().read_text(encoding="utf-8"))
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, value)),
            key=lambda error: str(error.absolute_path),
        )
    except ImportError as exc:  # pragma: no cover - environment setup failure
        raise ReplayValidationError("jsonschema is required for replay validation") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayValidationError("unable to read replay schema") from exc
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.absolute_path)
        suffix = f" at {location}" if location else ""
        raise ReplayValidationError(f"{label} failed schema validation{suffix}: {error.message}")


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReplayValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Validated result metadata returned by a caller-owned runner."""

    result_sha256: str
    result_ref: str
    result_bytes: int

    @property
    def sha256(self) -> str:
        return self.result_sha256

    @property
    def reference(self) -> str:
        return self.result_ref

    @property
    def byte_size(self) -> int:
        return self.result_bytes

    def as_dict(self) -> dict[str, object]:
        return {
            "result_sha256": self.result_sha256,
            "result_ref": self.result_ref,
            "result_bytes": self.result_bytes,
        }

    @classmethod
    def from_value(cls, value: object, item_id: str, *, maximum_bytes: int = MAX_ITEM_BYTES) -> "ReplayResult":
        if isinstance(value, cls):
            fingerprint = _fingerprint(value.result_sha256, "result_sha256")
            reference = _safe_reference(value.result_ref)
            size = _bounded_int(value.result_bytes, "result_bytes", 1, maximum_bytes)
            return cls(fingerprint, reference, size)
        if isinstance(value, bytes):
            if len(value) == 0 or len(value) > maximum_bytes:
                raise ReplayValidationError(f"runner bytes must be between 1 and {maximum_bytes}")
            return cls(_fingerprint(value), f"result:{item_id}", len(value))
        if isinstance(value, str):
            return cls(_fingerprint(value, "result_sha256"), f"result:{item_id}", 1)
        if not isinstance(value, Mapping):
            raise ReplayValidationError("runner must return a result fingerprint object")
        expected = {"result_sha256", "result_ref", "result_bytes"}
        unknown = set(value) - expected
        if unknown:
            raise ReplayValidationError(f"runner result has unknown fields: {sorted(unknown)}")
        return cls(
            _fingerprint(value.get("result_sha256"), "result_sha256"),
            _safe_reference(value.get("result_ref")),
            _bounded_int(value.get("result_bytes"), "result_bytes", 1, maximum_bytes),
        )


@dataclass(frozen=True, slots=True)
class ReplayItem:
    """One deterministic version work item and bounded execution metadata."""

    item_id: str
    work_identity: str
    source_id: str
    version: str
    ordinal: int
    estimated_bytes: int
    state: ReplayItemState = "pending"
    attempts: int = 0
    result_sha256: str | None = None
    result_ref: str | None = None
    result_bytes: int | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def version_id(self) -> str:
        return self.version

    @property
    def byte_size(self) -> int:
        return self.estimated_bytes

    def as_dict(self) -> dict[str, object]:
        return {
            "item_id": self.item_id,
            "work_identity": self.work_identity,
            "source_id": self.source_id,
            "version": self.version,
            "ordinal": self.ordinal,
            "estimated_bytes": self.estimated_bytes,
            "state": self.state,
            "attempts": self.attempts,
            "result_sha256": self.result_sha256,
            "result_ref": self.result_ref,
            "result_bytes": self.result_bytes,
            "error": self.error,
            "started_at": _format_timestamp(self.started_at) if self.started_at else None,
            "completed_at": _format_timestamp(self.completed_at) if self.completed_at else None,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "ReplayItem":
        _validate_schema(
            {
                "schema_version": REPLAY_SCHEMA_VERSION,
                "record_type": "replay_plan",
                "plan_id": "replay:plan:" + "0" * 64,
                "idempotency_key": "replay:idem:" + "0" * 64,
                "plan_sha256": "sha256:" + "0" * 64,
                "source_id": "source:test",
                "from_version": "version:1",
                "to_version": "version:1",
                "contract_version": REPLAY_SCHEMA_VERSION,
                "max_items": 1,
                "max_bytes": 1,
                "state": "planned",
                "cancel_requested": False,
                "created_at": None,
                "updated_at": None,
                "items": [dict(value)],
            },
            "replay item",
        )
        item_id = _replay_id(value.get("item_id"), "item")
        work_identity = _replay_id(value.get("work_identity"), "work")
        source_id = _source_id(value.get("source_id"))
        version = _version_id(value.get("version"))
        ordinal = _bounded_int(value.get("ordinal"), "ordinal", 0, MAX_PLAN_ITEMS)
        estimated_bytes = _bounded_int(value.get("estimated_bytes"), "estimated_bytes", 1, MAX_ITEM_BYTES)
        raw_state = value.get("state")
        if raw_state not in {"pending", "in_progress", "completed", "failed"}:
            raise ReplayValidationError("replay item state is unsupported")
        attempts = _bounded_int(value.get("attempts"), "attempts", 0, 100)
        raw_sha = value.get("result_sha256")
        result_sha = None if raw_sha is None else _fingerprint(raw_sha, "result_sha256")
        raw_ref = value.get("result_ref")
        result_ref = None if raw_ref is None else _safe_reference(raw_ref)
        raw_bytes = value.get("result_bytes")
        result_bytes = None if raw_bytes is None else _bounded_int(raw_bytes, "result_bytes", 1, MAX_ITEM_BYTES)
        raw_error = value.get("error")
        error = None if raw_error is None else _safe_error(raw_error)
        started_at = _optional_timestamp(value.get("started_at"), "started_at")
        completed_at = _optional_timestamp(value.get("completed_at"), "completed_at")
        if raw_state == "completed" and (result_sha is None or result_ref is None or result_bytes is None):
            raise ReplayValidationError("completed replay item requires a result fingerprint")
        return cls(
            item_id,
            work_identity,
            source_id,
            version,
            ordinal,
            estimated_bytes,
            cast(ReplayItemState, raw_state),
            attempts,
            result_sha,
            result_ref,
            result_bytes,
            error,
            started_at,
            completed_at,
        )


@dataclass(frozen=True, slots=True)
class ReplayPlan:
    """Immutable replay identity plus its current durable state."""

    plan_id: str
    idempotency_key: str
    plan_sha256: str
    source_id: str
    from_version: str
    to_version: str
    contract_version: str
    max_items: int
    max_bytes: int
    items: tuple[ReplayItem, ...]
    state: ReplayState = "planned"
    cancel_requested: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def version_range(self) -> tuple[str, str]:
        return self.from_version, self.to_version

    @property
    def completed_items(self) -> tuple[ReplayItem, ...]:
        return tuple(item for item in self.items if item.state == "completed")

    @property
    def remaining_items(self) -> tuple[ReplayItem, ...]:
        return tuple(item for item in self.items if item.state != "completed")

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "record_type": REPLAY_RECORD_TYPE,
            "plan_id": self.plan_id,
            "idempotency_key": self.idempotency_key,
            "plan_sha256": self.plan_sha256,
            "source_id": self.source_id,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "contract_version": self.contract_version,
            "max_items": self.max_items,
            "max_bytes": self.max_bytes,
            "state": self.state,
            "cancel_requested": self.cancel_requested,
            "created_at": _format_timestamp(self.created_at) if self.created_at else None,
            "updated_at": _format_timestamp(self.updated_at) if self.updated_at else None,
            "items": [item.as_dict() for item in self.items],
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "ReplayPlan":
        _validate_schema(dict(value), "replay plan")
        expected = {
            "schema_version",
            "record_type",
            "plan_id",
            "idempotency_key",
            "plan_sha256",
            "source_id",
            "from_version",
            "to_version",
            "contract_version",
            "max_items",
            "max_bytes",
            "state",
            "cancel_requested",
            "created_at",
            "updated_at",
            "items",
        }
        unknown = set(value) - expected
        if unknown:
            raise ReplayValidationError(f"replay plan has unknown fields: {sorted(unknown)}")
        raw_items = value.get("items")
        if not isinstance(raw_items, list):
            raise ReplayValidationError("replay plan items must be an array")
        items = tuple(ReplayItem.from_mapping(cast(Mapping[str, object], item)) for item in raw_items)
        raw_state = value.get("state")
        if raw_state not in {"planned", "running", "cancel_requested", "cancelled", "completed", "failed"}:
            raise ReplayValidationError("replay plan state is unsupported")
        plan = cls(
            _replay_id(value.get("plan_id"), "plan"),
            _idempotency_key(value.get("idempotency_key")),
            _fingerprint(value.get("plan_sha256"), "plan_sha256"),
            _source_id(value.get("source_id")),
            _version_id(value.get("from_version"), "from_version"),
            _version_id(value.get("to_version"), "to_version"),
            _contract_version(value.get("contract_version")),
            _bounded_int(value.get("max_items"), "max_items", 1, MAX_PLAN_ITEMS),
            _bounded_int(value.get("max_bytes"), "max_bytes", 1, MAX_PLAN_BYTES),
            items,
            cast(ReplayState, raw_state),
            value.get("cancel_requested") is True,
            _optional_timestamp(value.get("created_at"), "created_at"),
            _optional_timestamp(value.get("updated_at"), "updated_at"),
        )
        _validate_plan(plan)
        return plan


@dataclass(frozen=True, slots=True)
class ReplayCheckpoint:
    """Monotonic contiguous completion cursor persisted with each result."""

    plan_id: str
    next_ordinal: int
    completed_items: int
    completed_bytes: int
    last_item_id: str | None
    checkpoint_sha256: str
    updated_at: datetime

    @property
    def next_item(self) -> int:
        return self.next_ordinal

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "record_type": "replay_checkpoint",
            "plan_id": self.plan_id,
            "next_ordinal": self.next_ordinal,
            "completed_items": self.completed_items,
            "completed_bytes": self.completed_bytes,
            "last_item_id": self.last_item_id,
            "checkpoint_sha256": self.checkpoint_sha256,
            "updated_at": _format_timestamp(self.updated_at),
        }


@dataclass(frozen=True, slots=True)
class ReplaySubmission:
    """Result of idempotent plan admission."""

    action: SubmissionAction
    plan: ReplayPlan

    @property
    def plan_id(self) -> str:
        return self.plan.plan_id

    @property
    def state(self) -> ReplayState:
        return self.plan.state

    @property
    def items(self) -> tuple[ReplayItem, ...]:
        return self.plan.items

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "record_type": "replay_submission",
            "action": self.action,
            "plan": self.plan.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class ReplayDryRunDiff:
    """Read-only classification of candidate work against durable state."""

    plan_id: str
    plan_sha256: str
    new_items: tuple[str, ...] = ()
    already_completed: tuple[str, ...] = ()
    in_progress: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()

    @property
    def new(self) -> tuple[str, ...]:
        return self.new_items

    @property
    def completed(self) -> tuple[str, ...]:
        return self.already_completed

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "record_type": "replay_dry_run_diff",
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "new_items": list(self.new_items),
            "already_completed": list(self.already_completed),
            "in_progress": list(self.in_progress),
            "conflicts": list(self.conflicts),
        }


@dataclass(frozen=True, slots=True)
class ReplayExecution:
    """Bounded execution receipt with the durable checkpoint snapshot."""

    plan_id: str
    state: ReplayState
    attempted_items: int
    completed_items: int
    remaining_items: int
    checkpoint: ReplayCheckpoint
    error: str | None = None

    @property
    def processed_items(self) -> int:
        return self.attempted_items

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "record_type": "replay_execution",
            "plan_id": self.plan_id,
            "state": self.state,
            "attempted_items": self.attempted_items,
            "completed_items": self.completed_items,
            "remaining_items": self.remaining_items,
            "checkpoint": self.checkpoint.as_dict(),
            "error": self.error,
        }


def _range_versions(from_version: str, to_version: str) -> tuple[str, ...]:
    if from_version == to_version:
        return (from_version,)
    start_match = _TRAILING_NUMBER.fullmatch(from_version)
    end_match = _TRAILING_NUMBER.fullmatch(to_version)
    if start_match and end_match and start_match.group("prefix") == end_match.group("prefix"):
        start = int(start_match.group("number"))
        end = int(end_match.group("number"))
        if end < start or end - start + 1 > MAX_PLAN_ITEMS:
            raise ReplayValidationError("version range exceeds the item bound")
        start_digits = start_match.group("number")
        end_digits = end_match.group("number")
        if len(start_digits) == len(end_digits):
            width = len(start_digits)
        elif start_digits.startswith("0") or end_digits.startswith("0"):
            raise ReplayValidationError("numeric version range uses inconsistent zero padding")
        else:
            width = 0
        if width:
            return tuple(f"{start_match.group('prefix')}{index:0{width}d}" for index in range(start, end + 1))
        return tuple(f"{start_match.group('prefix')}{index}" for index in range(start, end + 1))
    start_date = _DATE_VERSION.fullmatch(from_version)
    end_date = _DATE_VERSION.fullmatch(to_version)
    if start_date and end_date and start_date.group("prefix") == end_date.group("prefix"):
        try:
            first = date(int(start_date.group("year")), int(start_date.group("month")), int(start_date.group("day")))
            last = date(int(end_date.group("year")), int(end_date.group("month")), int(end_date.group("day")))
        except ValueError as exc:
            raise ReplayValidationError("version range contains an invalid date") from exc
        days = (last - first).days
        if days < 0 or days + 1 > MAX_PLAN_ITEMS:
            raise ReplayValidationError("version range exceeds the item bound")
        return tuple(
            f"{start_date.group('prefix')}{(first + timedelta(days=offset)).isoformat()}" for offset in range(days + 1)
        )
    if from_version > to_version:
        raise ReplayValidationError("from_version must not be after to_version")
    raise ReplayValidationError("non-contiguous ranges require an explicit versions list")


def build_replay_plan(
    source_id: str,
    from_version: str | None = None,
    to_version: str | None = None,
    *,
    contract_version: str = REPLAY_SCHEMA_VERSION,
    versions: Sequence[str] | None = None,
    version_ids: Sequence[str] | None = None,
    from_release: str | None = None,
    to_release: str | None = None,
    max_items: int = DEFAULT_MAX_ITEMS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    estimated_bytes: int = DEFAULT_ITEM_BYTES,
    item_bytes: int | None = None,
    bytes_by_version: Mapping[str, int] | None = None,
    idempotency_key: str | None = None,
) -> ReplayPlan:
    """Build a deterministic, network-free replay plan.

    Numeric/date ranges are expanded inclusively.  Opaque version sequences
    must be supplied through ``versions`` (or its ``version_ids`` alias) so a
    controller can never silently skip an unknown release between bounds.
    """

    source = _source_id(source_id)
    if from_version is not None and from_release is not None and from_version != from_release:
        raise ReplayValidationError("from_version and from_release are aliases; supply one")
    if to_version is not None and to_release is not None and to_version != to_release:
        raise ReplayValidationError("to_version and to_release are aliases; supply one")
    first_raw = from_version if from_version is not None else from_release
    last_raw = to_version if to_version is not None else to_release
    if versions is not None and version_ids is not None:
        if tuple(versions) != tuple(version_ids):
            raise ReplayValidationError("versions and version_ids are aliases; supply one")
    supplied_versions = versions if versions is not None else version_ids
    if supplied_versions is not None:
        raw_versions = tuple(supplied_versions)
        if not raw_versions:
            raise ReplayValidationError("versions must contain at least one version")
        expanded = tuple(_version_id(value, "version") for value in raw_versions)
        if len(set(expanded)) != len(expanded):
            raise ReplayValidationError("versions must not contain duplicates")
        if first_raw is None:
            first_raw = expanded[0]
        if last_raw is None:
            last_raw = expanded[-1]
    if first_raw is None or last_raw is None:
        raise ReplayValidationError("from_version and to_version are required")
    first = _version_id(first_raw, "from_version")
    last = _version_id(last_raw, "to_version")
    if supplied_versions is None:
        expanded = _range_versions(first, last)
    else:
        if expanded[0] != first or expanded[-1] != last:
            raise ReplayValidationError("versions must include the inclusive range boundaries")
        if any(version < first or version > last for version in expanded):
            raise ReplayValidationError("versions must fall inside the requested range")
    maximum_items = _bounded_int(max_items, "max_items", 1, MAX_PLAN_ITEMS)
    maximum_bytes = _bounded_int(max_bytes, "max_bytes", 1, MAX_PLAN_BYTES)
    if item_bytes is not None and item_bytes != estimated_bytes:
        raise ReplayValidationError("estimated_bytes and item_bytes are aliases; supply one")
    default_bytes = _bounded_int(
        item_bytes if item_bytes is not None else estimated_bytes, "estimated_bytes", 1, MAX_ITEM_BYTES
    )
    byte_map: dict[str, int] = {}
    if bytes_by_version is not None:
        for key, value in bytes_by_version.items():
            normalized_key = _version_id(key, "bytes_by_version version")
            byte_map[normalized_key] = _bounded_int(value, f"bytes_by_version[{normalized_key}]", 1, MAX_ITEM_BYTES)
        unknown_versions = set(byte_map) - set(expanded)
        if unknown_versions:
            raise ReplayValidationError(f"bytes_by_version references unknown versions: {sorted(unknown_versions)}")
    if len(expanded) > maximum_items:
        raise ReplayValidationError("replay plan exceeds max_items")
    item_defs: list[dict[str, object]] = []
    items: list[ReplayItem] = []
    total_bytes = 0
    normalized_contract = _contract_version(contract_version)
    for ordinal, version in enumerate(expanded):
        size = byte_map.get(version, default_bytes)
        total_bytes += size
        work_digest = sha256(f"work|{source}|{normalized_contract}|{version}".encode("utf-8")).hexdigest()
        item_digest = sha256(f"item|{source}|{normalized_contract}|{version}".encode("utf-8")).hexdigest()
        work_identity = f"replay:work:{work_digest}"
        item_id = f"replay:item:{item_digest}"
        item_defs.append(
            {
                "item_id": item_id,
                "work_identity": work_identity,
                "source_id": source,
                "version": version,
                "ordinal": ordinal,
                "estimated_bytes": size,
            }
        )
        items.append(ReplayItem(item_id, work_identity, source, version, ordinal, size))
    if total_bytes > maximum_bytes:
        raise ReplayValidationError("replay plan exceeds max_bytes")
    material = {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "record_type": REPLAY_RECORD_TYPE,
        "source_id": source,
        "from_version": first,
        "to_version": last,
        "contract_version": normalized_contract,
        "max_items": maximum_items,
        "max_bytes": maximum_bytes,
        "items": item_defs,
    }
    plan_digest = "sha256:" + sha256(_canonical(material)).hexdigest()
    plan_id = "replay:plan:" + plan_digest.removeprefix("sha256:")
    key = (
        _idempotency_key(idempotency_key)
        if idempotency_key is not None
        else f"replay:idem:{plan_digest.removeprefix('sha256:')}"
    )
    return ReplayPlan(
        plan_id,
        key,
        plan_digest,
        source,
        first,
        last,
        normalized_contract,
        maximum_items,
        maximum_bytes,
        tuple(items),
    )


class ReplayController:
    """SQLite-backed replay plan, checkpoint, and bounded runner controller."""

    def __init__(
        self,
        database: str | Path | sqlite3.Connection,
        *,
        clock: Callable[[], datetime] | None = None,
        claim_lease_seconds: float = DEFAULT_CLAIM_LEASE_SECONDS,
    ) -> None:
        if clock is not None and not callable(clock):
            raise ReplayValidationError("clock must be callable")
        self.claim_lease_seconds = _bounded_seconds(claim_lease_seconds, "claim_lease_seconds", MAX_CLAIM_LEASE_SECONDS)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._owns_connection = not isinstance(database, sqlite3.Connection)
        if isinstance(database, sqlite3.Connection):
            self._connection = database
        else:
            path = Path(database)
            if str(path) != ":memory:":
                try:
                    path.parent.mkdir(parents=True, exist_ok=True)
                except OSError as exc:
                    raise ReplayError(f"unable to create replay database directory: {path.parent}") from exc
            try:
                self._connection = sqlite3.connect(
                    str(database), isolation_level=None, check_same_thread=False, timeout=5.0
                )
            except sqlite3.Error as exc:
                raise ReplayError("unable to open replay database") from exc
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA busy_timeout = 5000")
            self._initialize()
        except sqlite3.Error as exc:
            self.close()
            raise ReplayError("unable to initialize replay database") from exc

    def close(self) -> None:
        """Close an owned database connection."""

        if self._owns_connection:
            with self._lock:
                try:
                    self._connection.close()
                except sqlite3.Error:
                    pass

    def __enter__(self) -> "ReplayController":
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    @staticmethod
    def build_plan(*args: object, **kwargs: object) -> ReplayPlan:
        """Build a plan without changing durable state."""

        builder = cast(Callable[..., ReplayPlan], build_replay_plan)
        return builder(*args, **kwargs)

    plan = build_plan

    def submit(self, plan: ReplayPlan) -> ReplaySubmission:
        """Persist one plan idempotently, returning the existing plan on retry."""

        _validate_plan(plan)
        current = _utc(self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM replay_plans WHERE idempotency_key = ?", (plan.idempotency_key,)
            ).fetchone()
            if existing is not None:
                if str(existing["plan_sha256"]) != plan.plan_sha256:
                    raise ReplayCollisionError("idempotency key already exists with a different plan")
                if str(existing["plan_id"]) != plan.plan_id:
                    raise ReplayCollisionError("plan identity collides with an existing idempotency record")
                return ReplaySubmission("duplicate", self._read_plan_locked(plan.plan_id))
            identity = self._connection.execute(
                "SELECT * FROM replay_plans WHERE plan_id = ?", (plan.plan_id,)
            ).fetchone()
            if identity is not None:
                raise ReplayCollisionError("plan id already exists with a different idempotency key")
            try:
                self._connection.execute(
                    """
                    INSERT INTO replay_plans (
                        plan_id, idempotency_key, plan_sha256, source_id, from_version,
                        to_version, contract_version, max_items, max_bytes, state,
                        cancel_requested, created_at, updated_at, error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', 0, ?, ?, NULL)
                    """,
                    (
                        plan.plan_id,
                        plan.idempotency_key,
                        plan.plan_sha256,
                        plan.source_id,
                        plan.from_version,
                        plan.to_version,
                        plan.contract_version,
                        plan.max_items,
                        plan.max_bytes,
                        stamp,
                        stamp,
                    ),
                )
                for item in plan.items:
                    self._connection.execute(
                        """
                        INSERT INTO replay_items (
                            plan_id, ordinal, item_id, work_identity, source_id, version,
                            estimated_bytes, state, attempts, result_sha256, result_ref,
                            result_bytes, error, started_at, completed_at, claim_owner, claim_expires_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                        """,
                        (
                            plan.plan_id,
                            item.ordinal,
                            item.item_id,
                            item.work_identity,
                            item.source_id,
                            item.version,
                            item.estimated_bytes,
                        ),
                    )
                self._sync_previously_completed_locked(plan.plan_id, stamp)
                self._rebuild_checkpoint_locked(plan.plan_id, current)
            except sqlite3.IntegrityError as exc:
                raise ReplayCollisionError("replay plan collides with an existing work record") from exc
            return ReplaySubmission("created", self._read_plan_locked(plan.plan_id))

    submit_plan = submit

    def create_plan(self, *args: object, **kwargs: object) -> ReplayPlan:
        """Build and admit a plan; retries return the same durable identity."""

        return self.submit(self.build_plan(*args, **kwargs)).plan

    create = create_plan

    def get_plan(self, plan_id: str) -> ReplayPlan:
        identifier = _replay_id(plan_id, "plan")
        with self._lock:
            row = self._connection.execute("SELECT 1 FROM replay_plans WHERE plan_id = ?", (identifier,)).fetchone()
            if row is None:
                raise ReplayError(f"unknown plan_id: {identifier}")
            return self._read_plan_locked(identifier)

    def list_plans(self, *, source_id: str | None = None, state: ReplayState | None = None) -> tuple[ReplayPlan, ...]:
        source = None if source_id is None else _source_id(source_id)
        if state is not None and state not in {
            "planned",
            "running",
            "cancel_requested",
            "cancelled",
            "completed",
            "failed",
        }:
            raise ReplayValidationError("unsupported replay plan state")
        clauses: list[str] = []
        params: list[str] = []
        if source is not None:
            clauses.append("source_id = ?")
            params.append(source)
        if state is not None:
            clauses.append("state = ?")
            params.append(state)
        query = "SELECT plan_id FROM replay_plans"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, plan_id"
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
            return tuple(self._read_plan_locked(str(row["plan_id"])) for row in rows)

    def get_items(self, plan_id: str) -> tuple[ReplayItem, ...]:
        return self.get_plan(plan_id).items

    items = get_items

    def get_checkpoint(self, plan_id: str) -> ReplayCheckpoint:
        identifier = _replay_id(plan_id, "plan")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM replay_checkpoints WHERE plan_id = ?", (identifier,)
            ).fetchone()
            if row is None:
                raise ReplayError(f"unknown plan_id: {identifier}")
            return self._row_to_checkpoint(row)

    checkpoint = get_checkpoint

    def dry_run(
        self,
        plan: ReplayPlan | None = None,
        *,
        source_id: str | None = None,
        from_version: str | None = None,
        to_version: str | None = None,
        **kwargs: object,
    ) -> ReplayDryRunDiff:
        """Classify candidate work without inserting a plan or checkpoint."""

        if plan is not None and (source_id is not None or from_version is not None or to_version is not None or kwargs):
            raise ReplayValidationError("dry_run accepts either plan or plan arguments, not both")
        candidate = plan or self.build_plan(source_id, from_version, to_version, **kwargs)
        _validate_plan(candidate)
        new: list[str] = []
        completed: list[str] = []
        in_progress: list[str] = []
        conflicts: list[str] = []
        with self._lock:
            existing = self._connection.execute(
                "SELECT plan_sha256 FROM replay_plans WHERE idempotency_key = ?", (candidate.idempotency_key,)
            ).fetchone()
            if existing is not None and str(existing["plan_sha256"]) != candidate.plan_sha256:
                conflicts.extend(item.item_id for item in candidate.items)
            else:
                for item in candidate.items:
                    if existing is not None:
                        row = self._connection.execute(
                            "SELECT state FROM replay_items WHERE plan_id = ? AND item_id = ?",
                            (candidate.plan_id, item.item_id),
                        ).fetchone()
                    else:
                        row = self._connection.execute(
                            """
                            SELECT state FROM replay_items
                            WHERE work_identity = ?
                            ORDER BY CASE state WHEN 'completed' THEN 0 WHEN 'in_progress' THEN 1 ELSE 2 END
                            LIMIT 1
                            """,
                            (item.work_identity,),
                        ).fetchone()
                    state = None if row is None else str(row["state"])
                    if state == "completed":
                        completed.append(item.item_id)
                    elif state == "in_progress":
                        in_progress.append(item.item_id)
                    else:
                        new.append(item.item_id)
        return ReplayDryRunDiff(
            candidate.plan_id, candidate.plan_sha256, tuple(new), tuple(completed), tuple(in_progress), tuple(conflicts)
        )

    dry_run_plan = dry_run

    def cancel(self, plan_id: str, *, reason: str | None = None) -> ReplayPlan:
        """Request cancellation; an active item finishes before the state changes."""

        identifier = _replay_id(plan_id, "plan")
        current = _utc(self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            row = self._connection.execute("SELECT * FROM replay_plans WHERE plan_id = ?", (identifier,)).fetchone()
            if row is None:
                raise ReplayError(f"unknown plan_id: {identifier}")
            current_state = str(row["state"])
            if current_state == "completed":
                return self._read_plan_locked(identifier)
            active = self._connection.execute(
                "SELECT 1 FROM replay_items WHERE plan_id = ? AND state = 'in_progress' LIMIT 1", (identifier,)
            ).fetchone()
            next_state: ReplayState = "cancel_requested" if active is not None else "cancelled"
            detail = None if reason is None else _safe_error(reason)
            self._connection.execute(
                "UPDATE replay_plans SET state = ?, cancel_requested = 1, updated_at = ?, error = COALESCE(?, error) WHERE plan_id = ?",
                (next_state, stamp, detail, identifier),
            )
            return self._read_plan_locked(identifier)

    request_cancel = cancel

    def resume(
        self,
        plan_id: str,
        runner: Runner | None = None,
        *,
        max_items: int | None = None,
        max_bytes: int | None = None,
        cancel: Cancellation | None = None,
        cancellation: Cancellation | None = None,
    ) -> ReplayPlan | ReplayExecution:
        """Clear cancellation and optionally continue execution from its checkpoint."""

        if runner is not None:
            return self.execute(
                plan_id,
                runner,
                max_items=max_items,
                max_bytes=max_bytes,
                cancel=cancel,
                cancellation=cancellation,
                resume=True,
            )
        identifier = _replay_id(plan_id, "plan")
        current = _utc(self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            row = self._connection.execute("SELECT state FROM replay_plans WHERE plan_id = ?", (identifier,)).fetchone()
            if row is None:
                raise ReplayError(f"unknown plan_id: {identifier}")
            if str(row["state"]) == "completed":
                return self._read_plan_locked(identifier)
            self._recover_expired_claims_locked(identifier, stamp)
            active = self._connection.execute(
                "SELECT 1 FROM replay_items WHERE plan_id = ? AND state = 'in_progress' AND claim_expires_at > ? LIMIT 1",
                (identifier, stamp),
            ).fetchone()
            if active is not None:
                raise ReplayStateError("active replay claim has not expired")
            self._connection.execute(
                "UPDATE replay_plans SET state = 'planned', cancel_requested = 0, updated_at = ?, error = NULL WHERE plan_id = ?",
                (stamp, identifier),
            )
            return self._read_plan_locked(identifier)

    def execute(
        self,
        plan_id: str | ReplayPlan,
        runner: Runner,
        *,
        max_items: int | None = None,
        max_bytes: int | None = None,
        cancel: Cancellation | None = None,
        cancellation: Cancellation | None = None,
        resume: bool = False,
    ) -> ReplayExecution:
        """Run a bounded batch and advance the checkpoint after each valid result."""

        if not callable(runner):
            raise ReplayValidationError("runner must be callable")
        if cancel is not None and cancellation is not None and cancel is not cancellation:
            raise ReplayValidationError("cancel and cancellation are aliases; supply one")
        cancellation_signal = cancel if cancel is not None else cancellation
        identifier = _replay_id(plan_id.plan_id if isinstance(plan_id, ReplayPlan) else plan_id, "plan")
        plan = self._prepare_execution(identifier, resume=resume)
        item_limit = plan.max_items if max_items is None else _bounded_int(max_items, "max_items", 1, plan.max_items)
        byte_limit = plan.max_bytes if max_bytes is None else _bounded_int(max_bytes, "max_bytes", 1, plan.max_bytes)
        attempted = 0
        spent_bytes = 0
        last_error: str | None = None
        owner = f"replay-runner:{secrets.token_hex(16)}"
        while attempted < item_limit:
            if self._cancel_signal_set(cancellation_signal):
                self.cancel(identifier)
                break
            claimed = self._claim_next(identifier, owner)
            if claimed is None:
                break
            if spent_bytes + claimed.estimated_bytes > byte_limit:
                self._release_claim(identifier, claimed.item_id, owner)
                break
            attempted += 1
            spent_bytes += claimed.estimated_bytes
            try:
                result = ReplayResult.from_value(runner(claimed), claimed.item_id, maximum_bytes=MAX_ITEM_BYTES)
                self._complete_item(identifier, claimed, owner, result)
            except Exception as exc:
                last_error = _safe_error(exc)
                try:
                    self._fail_item(identifier, claimed, owner, last_error)
                except ReplayStateError:
                    # A stale owner must not overwrite the newer fenced claim.
                    pass
                break
            if self._cancel_signal_set(cancellation_signal):
                self.cancel(identifier)
                break
        return self._execution_receipt(identifier, attempted, last_error)

    run = execute

    def _prepare_execution(self, plan_id: str, *, resume: bool) -> ReplayPlan:
        current = _utc(self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            row = self._connection.execute("SELECT * FROM replay_plans WHERE plan_id = ?", (plan_id,)).fetchone()
            if row is None:
                raise ReplayError(f"unknown plan_id: {plan_id}")
            state = str(row["state"])
            if state == "completed":
                return self._read_plan_locked(plan_id)
            if state in {"cancelled", "cancel_requested"}:
                if not resume:
                    raise ReplayStateError("cancelled replay requires resume=True")
                self._recover_expired_claims_locked(plan_id, stamp)
                active = self._connection.execute(
                    "SELECT 1 FROM replay_items WHERE plan_id = ? AND state = 'in_progress' AND claim_expires_at > ? LIMIT 1",
                    (plan_id, stamp),
                ).fetchone()
                if active is not None:
                    raise ReplayStateError("active replay claim has not expired")
                self._connection.execute(
                    "UPDATE replay_plans SET state = 'planned', cancel_requested = 0, updated_at = ?, error = NULL WHERE plan_id = ?",
                    (stamp, plan_id),
                )
            self._connection.execute(
                "UPDATE replay_plans SET state = 'running', updated_at = ? WHERE plan_id = ? AND cancel_requested = 0",
                (stamp, plan_id),
            )
            return self._read_plan_locked(plan_id)

    def _claim_next(self, plan_id: str, owner: str) -> ReplayItem | None:
        current = _utc(self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            plan_row = self._connection.execute(
                "SELECT cancel_requested, state FROM replay_plans WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            if plan_row is None:
                raise ReplayError(f"unknown plan_id: {plan_id}")
            if int(plan_row["cancel_requested"]) != 0:
                active = self._connection.execute(
                    "SELECT 1 FROM replay_items WHERE plan_id = ? AND state = 'in_progress' AND claim_expires_at > ? LIMIT 1",
                    (plan_id, stamp),
                ).fetchone()
                if active is None:
                    self._connection.execute(
                        "UPDATE replay_plans SET state = 'cancelled', updated_at = ? WHERE plan_id = ? AND state != 'completed'",
                        (stamp, plan_id),
                    )
                return None
            row = self._connection.execute(
                """
                SELECT * FROM replay_items
                WHERE plan_id = ? AND state IN ('pending', 'failed', 'in_progress')
                ORDER BY ordinal
                LIMIT 1
                """,
                (plan_id,),
            ).fetchone()
            if row is None:
                self._rebuild_checkpoint_locked(plan_id, current)
                return None
            if str(row["state"]) == "in_progress":
                expires = row["claim_expires_at"]
                if expires is not None and _parse_timestamp(expires, "claim_expires_at") > current:
                    return None
            attempts = int(row["attempts"])
            if attempts >= MAX_ATTEMPTS:
                self._connection.execute(
                    "UPDATE replay_items SET state = 'failed', error = ?, claim_owner = NULL, claim_expires_at = NULL WHERE plan_id = ? AND item_id = ?",
                    ("maximum replay attempts reached", stamp, plan_id, row["item_id"]),
                )
                self._connection.execute(
                    "UPDATE replay_plans SET state = 'failed', error = ?, updated_at = ? WHERE plan_id = ?",
                    ("maximum replay attempts reached", stamp, plan_id),
                )
                return None
            updated = self._connection.execute(
                """
                UPDATE replay_items SET state = 'in_progress', attempts = ?, started_at = ?,
                    completed_at = NULL, error = NULL, claim_owner = ?, claim_expires_at = ?
                WHERE plan_id = ? AND item_id = ?
                  AND (state IN ('pending', 'failed') OR (state = 'in_progress' AND (claim_expires_at IS NULL OR claim_expires_at <= ?)))
                """,
                (
                    attempts + 1,
                    stamp,
                    owner,
                    _format_timestamp(current + timedelta(seconds=self.claim_lease_seconds)),
                    plan_id,
                    row["item_id"],
                    stamp,
                ),
            )
            if updated.rowcount != 1:
                return None
            fresh = self._connection.execute(
                "SELECT * FROM replay_items WHERE plan_id = ? AND item_id = ?", (plan_id, row["item_id"])
            ).fetchone()
            if fresh is None:  # pragma: no cover - guarded by update
                raise ReplayError("claimed replay item disappeared")
            return self._row_to_item(fresh)

    def _release_claim(self, plan_id: str, item_id: str, owner: str) -> None:
        with self._transaction():
            self._connection.execute(
                "UPDATE replay_items SET state = 'pending', attempts = attempts - 1, claim_owner = NULL, claim_expires_at = NULL, started_at = NULL WHERE plan_id = ? AND item_id = ? AND state = 'in_progress' AND claim_owner = ?",
                (plan_id, item_id, owner),
            )

    def _complete_item(self, plan_id: str, item: ReplayItem, owner: str, result: ReplayResult) -> None:
        current = _utc(self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            row = self._connection.execute(
                "SELECT state, claim_owner, claim_expires_at FROM replay_items WHERE plan_id = ? AND item_id = ?",
                (plan_id, item.item_id),
            ).fetchone()
            if row is None:
                raise ReplayError("replay item disappeared before completion")
            if row["state"] != "in_progress" or row["claim_owner"] != owner:
                raise ReplayStateError("replay item is not owned by this execution")
            expires = _parse_timestamp(row["claim_expires_at"], "claim_expires_at")
            if expires <= current:
                raise ReplayStateError("replay item claim has expired")
            self._connection.execute(
                """
                UPDATE replay_items SET state = 'completed', result_sha256 = ?, result_ref = ?, result_bytes = ?,
                    error = NULL, completed_at = ?, claim_owner = NULL, claim_expires_at = NULL
                WHERE plan_id = ? AND item_id = ? AND state = 'in_progress' AND claim_owner = ?
                """,
                (result.result_sha256, result.result_ref, result.result_bytes, stamp, plan_id, item.item_id, owner),
            )
            self._rebuild_checkpoint_locked(plan_id, current)

    def _fail_item(self, plan_id: str, item: ReplayItem, owner: str, error: str) -> None:
        current = _utc(self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            row = self._connection.execute(
                "SELECT state, claim_owner, claim_expires_at FROM replay_items WHERE plan_id = ? AND item_id = ?",
                (plan_id, item.item_id),
            ).fetchone()
            if row is None:
                raise ReplayError("replay item disappeared before failure update")
            if row["state"] != "in_progress" or row["claim_owner"] != owner:
                raise ReplayStateError("replay item is not owned by this execution")
            expires = _parse_timestamp(row["claim_expires_at"], "claim_expires_at")
            if expires <= current:
                raise ReplayStateError("replay item claim has expired")
            self._connection.execute(
                "UPDATE replay_items SET state = 'failed', error = ?, claim_owner = NULL, claim_expires_at = NULL WHERE plan_id = ? AND item_id = ? AND state = 'in_progress' AND claim_owner = ?",
                (error[:MAX_ERROR_LENGTH], plan_id, item.item_id, owner),
            )
            self._connection.execute(
                "UPDATE replay_plans SET state = 'failed', error = ?, updated_at = ? WHERE plan_id = ?",
                (error[:MAX_ERROR_LENGTH], stamp, plan_id),
            )

    def _execution_receipt(self, plan_id: str, attempted: int, error: str | None) -> ReplayExecution:
        plan = self.get_plan(plan_id)
        checkpoint = self.get_checkpoint(plan_id)
        receipt = ReplayExecution(
            plan_id,
            plan.state,
            attempted,
            checkpoint.completed_items,
            len(plan.remaining_items),
            checkpoint,
            error or (plan.items[0].error if plan.items and plan.state == "failed" else None),
        )
        _validate_schema(receipt.as_dict(), "replay execution")
        return receipt

    @staticmethod
    def _cancel_signal_set(signal: Cancellation | None) -> bool:
        if signal is None:
            return False
        if callable(signal):
            return bool(signal())
        method = getattr(signal, "is_set", None)
        if callable(method):
            return bool(method())
        raise ReplayValidationError("cancel must be callable or expose is_set()")

    def _sync_previously_completed_locked(self, plan_id: str, stamp: str) -> None:
        rows = self._connection.execute(
            "SELECT item_id, work_identity FROM replay_items WHERE plan_id = ?", (plan_id,)
        ).fetchall()
        for row in rows:
            prior = self._connection.execute(
                """
                SELECT result_sha256, result_ref, result_bytes, completed_at
                FROM replay_items
                WHERE work_identity = ? AND state = 'completed' AND plan_id != ?
                ORDER BY completed_at DESC, plan_id
                LIMIT 1
                """,
                (row["work_identity"], plan_id),
            ).fetchone()
            if prior is not None:
                self._connection.execute(
                    """
                    UPDATE replay_items SET state = 'completed', result_sha256 = ?, result_ref = ?, result_bytes = ?,
                        completed_at = COALESCE(?, ?)
                    WHERE plan_id = ? AND item_id = ? AND state = 'pending'
                    """,
                    (
                        prior["result_sha256"],
                        prior["result_ref"],
                        prior["result_bytes"],
                        prior["completed_at"],
                        stamp,
                        plan_id,
                        row["item_id"],
                    ),
                )

    def _recover_expired_claims_locked(self, plan_id: str, stamp: str) -> None:
        """Make only stale claims resumable; live owners retain their fence."""

        self._connection.execute(
            """
            UPDATE replay_items
            SET state = 'pending', claim_owner = NULL, claim_expires_at = NULL
            WHERE plan_id = ? AND state = 'in_progress'
              AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
            """,
            (plan_id, stamp),
        )

    def _rebuild_checkpoint_locked(self, plan_id: str, current: datetime) -> ReplayCheckpoint:
        rows = self._connection.execute(
            "SELECT * FROM replay_items WHERE plan_id = ? ORDER BY ordinal", (plan_id,)
        ).fetchall()
        next_ordinal = 0
        completed_items = 0
        completed_bytes = 0
        last_item_id: str | None = None
        for row in rows:
            if str(row["state"]) != "completed":
                break
            next_ordinal += 1
            completed_items += 1
            completed_bytes += int(row["estimated_bytes"])
            last_item_id = str(row["item_id"])
        stamp = _format_timestamp(current)
        digest_material = {
            "plan_id": plan_id,
            "next_ordinal": next_ordinal,
            "completed_items": completed_items,
            "completed_bytes": completed_bytes,
            "last_item_id": last_item_id,
        }
        digest = "sha256:" + sha256(_canonical(digest_material)).hexdigest()
        self._connection.execute(
            """
            INSERT INTO replay_checkpoints (
                plan_id, next_ordinal, completed_items, completed_bytes, last_item_id,
                checkpoint_sha256, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(plan_id) DO UPDATE SET next_ordinal = excluded.next_ordinal,
                completed_items = excluded.completed_items, completed_bytes = excluded.completed_bytes,
                last_item_id = excluded.last_item_id, checkpoint_sha256 = excluded.checkpoint_sha256,
                updated_at = excluded.updated_at
            """,
            (plan_id, next_ordinal, completed_items, completed_bytes, last_item_id, digest, stamp),
        )
        if rows and next_ordinal == len(rows):
            self._connection.execute(
                "UPDATE replay_plans SET state = 'completed', cancel_requested = 0, updated_at = ? WHERE plan_id = ?",
                (stamp, plan_id),
            )
        else:
            row = self._connection.execute(
                "SELECT state, cancel_requested FROM replay_plans WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            if row is not None and int(row["cancel_requested"]) != 0:
                self._connection.execute(
                    "UPDATE replay_plans SET state = 'cancel_requested', updated_at = ? WHERE plan_id = ? AND state != 'completed'",
                    (stamp, plan_id),
                )
        return ReplayCheckpoint(plan_id, next_ordinal, completed_items, completed_bytes, last_item_id, digest, current)

    def _read_plan_locked(self, plan_id: str) -> ReplayPlan:
        row = self._connection.execute("SELECT * FROM replay_plans WHERE plan_id = ?", (plan_id,)).fetchone()
        if row is None:
            raise ReplayError(f"unknown plan_id: {plan_id}")
        item_rows = self._connection.execute(
            "SELECT * FROM replay_items WHERE plan_id = ? ORDER BY ordinal", (plan_id,)
        ).fetchall()
        items = tuple(self._row_to_item(item) for item in item_rows)
        state = str(row["state"])
        if state not in {"planned", "running", "cancel_requested", "cancelled", "completed", "failed"}:
            raise ReplayError("database contains unsupported replay plan state")
        plan = ReplayPlan(
            str(row["plan_id"]),
            str(row["idempotency_key"]),
            str(row["plan_sha256"]),
            str(row["source_id"]),
            str(row["from_version"]),
            str(row["to_version"]),
            str(row["contract_version"]),
            int(row["max_items"]),
            int(row["max_bytes"]),
            items,
            cast(ReplayState, state),
            bool(int(row["cancel_requested"])),
            _parse_timestamp(row["created_at"], "created_at"),
            _parse_timestamp(row["updated_at"], "updated_at"),
        )
        _validate_plan(plan)
        return plan

    @staticmethod
    def _row_to_item(row: sqlite3.Row) -> ReplayItem:
        state = str(row["state"])
        if state not in {"pending", "in_progress", "completed", "failed"}:
            raise ReplayError("database contains unsupported replay item state")
        result_sha = cast(str | None, row["result_sha256"])
        result_ref = cast(str | None, row["result_ref"])
        result_bytes = None if row["result_bytes"] is None else int(row["result_bytes"])
        return ReplayItem(
            str(row["item_id"]),
            str(row["work_identity"]),
            str(row["source_id"]),
            str(row["version"]),
            int(row["ordinal"]),
            int(row["estimated_bytes"]),
            cast(ReplayItemState, state),
            int(row["attempts"]),
            result_sha,
            result_ref,
            result_bytes,
            cast(str | None, row["error"]),
            _optional_timestamp(row["started_at"], "started_at"),
            _optional_timestamp(row["completed_at"], "completed_at"),
        )

    @staticmethod
    def _row_to_checkpoint(row: sqlite3.Row) -> ReplayCheckpoint:
        last = row["last_item_id"]
        return ReplayCheckpoint(
            str(row["plan_id"]),
            int(row["next_ordinal"]),
            int(row["completed_items"]),
            int(row["completed_bytes"]),
            None if last is None else str(last),
            str(row["checkpoint_sha256"]),
            _parse_timestamp(row["updated_at"], "updated_at"),
        )

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS replay_plans (
                plan_id TEXT PRIMARY KEY NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                plan_sha256 TEXT NOT NULL,
                source_id TEXT NOT NULL,
                from_version TEXT NOT NULL,
                to_version TEXT NOT NULL,
                contract_version TEXT NOT NULL,
                max_items INTEGER NOT NULL CHECK (max_items >= 1 AND max_items <= 100000),
                max_bytes INTEGER NOT NULL CHECK (max_bytes >= 1 AND max_bytes <= 1073741824),
                state TEXT NOT NULL CHECK (state IN ('planned', 'running', 'cancel_requested', 'cancelled', 'completed', 'failed')),
                cancel_requested INTEGER NOT NULL CHECK (cancel_requested IN (0, 1)),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS replay_items (
                plan_id TEXT NOT NULL REFERENCES replay_plans(plan_id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal <= 100000),
                item_id TEXT NOT NULL,
                work_identity TEXT NOT NULL,
                source_id TEXT NOT NULL,
                version TEXT NOT NULL,
                estimated_bytes INTEGER NOT NULL CHECK (estimated_bytes >= 1 AND estimated_bytes <= 1073741824),
                state TEXT NOT NULL CHECK (state IN ('pending', 'in_progress', 'completed', 'failed')),
                attempts INTEGER NOT NULL CHECK (attempts >= 0 AND attempts <= 100),
                result_sha256 TEXT,
                result_ref TEXT,
                result_bytes INTEGER CHECK (result_bytes IS NULL OR (result_bytes >= 1 AND result_bytes <= 1073741824)),
                error TEXT,
                started_at TEXT,
                completed_at TEXT,
                claim_owner TEXT,
                claim_expires_at TEXT,
                PRIMARY KEY (plan_id, ordinal),
                UNIQUE (plan_id, item_id)
            );
            CREATE TABLE IF NOT EXISTS replay_checkpoints (
                plan_id TEXT PRIMARY KEY NOT NULL REFERENCES replay_plans(plan_id) ON DELETE CASCADE,
                next_ordinal INTEGER NOT NULL CHECK (next_ordinal >= 0 AND next_ordinal <= 100000),
                completed_items INTEGER NOT NULL CHECK (completed_items >= 0 AND completed_items <= 100000),
                completed_bytes INTEGER NOT NULL CHECK (completed_bytes >= 0 AND completed_bytes <= 1073741824),
                last_item_id TEXT,
                checkpoint_sha256 TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS replay_items_work_idx ON replay_items (work_identity, state);
            CREATE INDEX IF NOT EXISTS replay_plans_source_idx ON replay_plans (source_id, state, created_at);
            """
        )
        columns = {str(row[1]) for row in self._connection.execute("PRAGMA table_info(replay_items)").fetchall()}
        if "claim_expires_at" not in columns:
            self._connection.execute("ALTER TABLE replay_items ADD COLUMN claim_expires_at TEXT")
            self._connection.execute(
                "UPDATE replay_items SET claim_expires_at = started_at WHERE state = 'in_progress' AND claim_expires_at IS NULL"
            )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
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


def _plan_material(plan: ReplayPlan) -> dict[str, object]:
    return {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "record_type": REPLAY_RECORD_TYPE,
        "source_id": plan.source_id,
        "from_version": plan.from_version,
        "to_version": plan.to_version,
        "contract_version": plan.contract_version,
        "max_items": plan.max_items,
        "max_bytes": plan.max_bytes,
        "items": [
            {
                "item_id": item.item_id,
                "work_identity": item.work_identity,
                "source_id": item.source_id,
                "version": item.version,
                "ordinal": item.ordinal,
                "estimated_bytes": item.estimated_bytes,
            }
            for item in plan.items
        ],
    }


def _validate_plan(plan: ReplayPlan) -> None:
    if not isinstance(plan, ReplayPlan):
        raise ReplayValidationError("plan must be a ReplayPlan")
    _validate_schema(plan.as_dict(), "replay plan")
    _replay_id(plan.plan_id, "plan")
    _idempotency_key(plan.idempotency_key)
    expected_hash = "sha256:" + sha256(_canonical(_plan_material(plan))).hexdigest()
    if expected_hash != plan.plan_sha256:
        raise ReplayValidationError("plan_sha256 does not match canonical plan material")
    if plan.plan_id != "replay:plan:" + expected_hash.removeprefix("sha256:"):
        raise ReplayValidationError("plan_id does not match canonical plan material")
    if not plan.items:
        raise ReplayValidationError("replay plan must contain at least one item")
    if len(plan.items) > plan.max_items:
        raise ReplayValidationError("replay plan exceeds max_items")
    if any(item.ordinal != index for index, item in enumerate(plan.items)):
        raise ReplayValidationError("replay item ordinals must be contiguous")
    total = 0
    seen: set[str] = set()
    for item in plan.items:
        if item.item_id in seen:
            raise ReplayValidationError("replay plan contains duplicate item_id")
        seen.add(item.item_id)
        if item.source_id != plan.source_id:
            raise ReplayValidationError("replay item source_id does not match plan")
        _replay_id(item.item_id, "item")
        _replay_id(item.work_identity, "work")
        _source_id(item.source_id)
        _version_id(item.version)
        total += _bounded_int(item.estimated_bytes, "estimated_bytes", 1, MAX_ITEM_BYTES)
        if item.state == "completed" and (
            item.result_sha256 is None or item.result_ref is None or item.result_bytes is None
        ):
            raise ReplayValidationError("completed replay item requires a result fingerprint")
    if total > plan.max_bytes:
        raise ReplayValidationError("replay plan exceeds max_bytes")


# Discoverable aliases used by adjacent control-plane lanes.
DurableReplayController = ReplayController
ReplayPlanController = ReplayController
ReplayResultFingerprint = ReplayResult
ResultFingerprint = ReplayResult
DryRunDiff = ReplayDryRunDiff
ExecutionReceipt = ReplayExecution


__all__ = [
    "DEFAULT_CLAIM_LEASE_SECONDS",
    "DEFAULT_ITEM_BYTES",
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_ITEMS",
    "DryRunDiff",
    "ExecutionReceipt",
    "MAX_ATTEMPTS",
    "MAX_CLAIM_LEASE_SECONDS",
    "MAX_ITEM_BYTES",
    "MAX_PLAN_BYTES",
    "MAX_PLAN_ITEMS",
    "ReplayCheckpoint",
    "ReplayCollisionError",
    "ReplayController",
    "DurableReplayController",
    "ReplayDryRunDiff",
    "ReplayError",
    "ReplayExecution",
    "ReplayItem",
    "ReplayItemState",
    "ReplayPlan",
    "ReplayPlanController",
    "ReplayResult",
    "ReplayResultFingerprint",
    "ReplayState",
    "ReplayStateError",
    "ReplaySubmission",
    "ReplayValidationError",
    "ResultFingerprint",
    "build_replay_plan",
]
