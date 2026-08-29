"""SQLite-backed source-plane work queue with renewable owner-bound leases.

This module deliberately keeps the queue independent from source adapters and
raw custody.  A row contains an opaque work reference, a content fingerprint,
and a byte estimate; it never contains source payload bytes or credentials.
SQLite ``BEGIN IMMEDIATE`` transactions make admission, expiry recovery, and
claim budget accounting atomic for multiple queue instances sharing a file.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import math
from pathlib import Path
import re
import secrets
import sqlite3
import threading
from typing import Callable, Iterator, Literal, Mapping, TypeAlias, cast


WorkItemState: TypeAlias = Literal["queued", "leased", "retry_wait", "completed", "poison"]
DecisionState: TypeAlias = Literal["claimed", "backpressure", "empty"]
RecoveryState: TypeAlias = Literal["requeued", "poisoned"]

QUEUE_SCHEMA_VERSION = "hdp.queue.v1"
QUEUE_RECORD_TYPE = "queue_work_item"
QUEUE_POLICY_RECORD_TYPE = "queue_policy"

# These are intentionally finite hard ceilings.  Callers may configure lower
# limits, but cannot accidentally turn a queue into an unbounded worker pool.
MAX_SOURCE_ID_LENGTH = 200
MAX_WORK_ID_LENGTH = 256
MAX_REFERENCE_LENGTH = 512
MAX_REASON_LENGTH = 512
MAX_OWNER_LENGTH = 128
MAX_ACTIVE_ITEMS = 100_000
MAX_ACTIVE_BYTES = 1_073_741_824
MAX_WORK_BYTES = 1_073_741_824
MAX_ATTEMPTS = 100
MAX_PRIORITY = 100
MAX_LEASE_TTL_SECONDS = 86_400.0
MAX_RETRY_DELAY_SECONDS = 86_400.0

_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}$")
_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts/healthcare-data-platform/queue/v1/queue.schema.json"


class QueueError(ValueError):
    """Raised when queue metadata or an operation violates the contract."""


class QueueCollisionError(QueueError):
    """Raised when an immutable work identity is reused with another hash."""


class QueueStateError(QueueError):
    """Raised when an operation does not match the durable work state."""


class LeaseExpiredError(QueueStateError):
    """Raised when a worker presents an expired or otherwise stale lease."""


def _required_text(value: object, label: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QueueError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise QueueError(f"{label} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise QueueError(f"{label} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise QueueError(f"{label} contains malformed Unicode") from exc
    return value


def _source_id(value: object) -> str:
    result = _required_text(value, "source_id", maximum=MAX_SOURCE_ID_LENGTH)
    if _SOURCE_ID.fullmatch(result) is None:
        raise QueueError("source_id has an invalid format")
    return result


def _work_id(value: object, label: str = "work_id") -> str:
    return _required_text(value, label, maximum=MAX_WORK_ID_LENGTH)


def _safe_reference(value: object, label: str = "payload_ref") -> str:
    result = _required_text(value, label, maximum=MAX_REFERENCE_LENGTH)
    if _SAFE_REFERENCE.fullmatch(result) is None:
        raise QueueError(f"{label} must be a bounded opaque reference")
    return result


def _reason(value: object, label: str = "reason") -> str:
    return _required_text(value, label, maximum=MAX_REASON_LENGTH)


def _owner(value: object) -> str:
    return _required_text(value, "owner", maximum=MAX_OWNER_LENGTH)


def _positive_int(value: object, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise QueueError(f"{label} must be an integer between 1 and {maximum}")
    return value


def _nonnegative_int(value: object, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise QueueError(f"{label} must be an integer between 0 and {maximum}")
    return value


def _seconds(value: object, label: str, maximum: float, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise QueueError(f"{label} must be a finite number")
    result = float(value)
    lower_ok = result > 0 if positive else result >= 0
    if not math.isfinite(result) or not lower_ok or result > maximum:
        lower = "greater than 0" if positive else "at least 0"
        raise QueueError(f"{label} must be finite, {lower}, and at most {maximum}")
    return result


def _fingerprint(value: object, label: str = "payload_sha256") -> str:
    if isinstance(value, bytes):
        return "sha256:" + sha256(value).hexdigest()
    if not isinstance(value, str):
        raise QueueError(f"{label} must be a sha256 fingerprint")
    candidate = value if value.startswith("sha256:") else "sha256:" + value
    if _SHA256.fullmatch(candidate) is None:
        raise QueueError(f"{label} must be a sha256 fingerprint")
    return candidate


def _utc(value: datetime, label: str = "timestamp") -> datetime:
    if not isinstance(value, datetime):
        raise QueueError(f"{label} must be a timezone-aware datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise QueueError(f"{label} must include a timezone")
    return value.astimezone(timezone.utc)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object, label: str = "timestamp") -> datetime:
    if not isinstance(value, str) or not value:
        raise QueueError(f"{label} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QueueError(f"{label} must be an ISO-8601 timestamp") from exc
    return _utc(parsed, label)


def _optional_timestamp(value: object, label: str) -> datetime | None:
    if value is None:
        return None
    return _parse_timestamp(value, label)


@dataclass(frozen=True, slots=True)
class SourceBudget:
    """Per-source active item and estimated-byte ceilings."""

    max_active_items: int
    max_active_bytes: int

    def __post_init__(self) -> None:
        _positive_int(self.max_active_items, "max_active_items", MAX_ACTIVE_ITEMS)
        _positive_int(self.max_active_bytes, "max_active_bytes", MAX_ACTIVE_BYTES)

    @classmethod
    def from_value(cls, value: object, label: str = "source budget") -> "SourceBudget":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise QueueError(f"{label} must be an object")
        expected = {"max_active_items", "max_active_bytes"}
        unknown = set(value) - expected
        if unknown:
            raise QueueError(f"{label} has unknown fields: {sorted(unknown)}")
        return cls(
            max_active_items=_positive_int(
                value.get("max_active_items"), f"{label}.max_active_items", MAX_ACTIVE_ITEMS
            ),
            max_active_bytes=_positive_int(
                value.get("max_active_bytes"), f"{label}.max_active_bytes", MAX_ACTIVE_BYTES
            ),
        )

    def as_dict(self) -> dict[str, int]:
        return {
            "max_active_items": self.max_active_items,
            "max_active_bytes": self.max_active_bytes,
        }


@dataclass(frozen=True, slots=True)
class QueuePolicy:
    """Finite global and source-specific limits for claim admission."""

    max_active_items: int = 64
    max_active_bytes: int = 16 * 1024 * 1024
    lease_ttl_seconds: float = 300.0
    heartbeat_extension_seconds: float | None = None
    retry_delay_seconds: float = 30.0
    max_attempts: int = 3
    source_budgets: Mapping[str, SourceBudget] | None = None

    def __post_init__(self) -> None:
        _positive_int(self.max_active_items, "max_active_items", MAX_ACTIVE_ITEMS)
        _positive_int(self.max_active_bytes, "max_active_bytes", MAX_ACTIVE_BYTES)
        _seconds(self.lease_ttl_seconds, "lease_ttl_seconds", MAX_LEASE_TTL_SECONDS, positive=True)
        heartbeat = (
            self.lease_ttl_seconds if self.heartbeat_extension_seconds is None else self.heartbeat_extension_seconds
        )
        _seconds(heartbeat, "heartbeat_extension_seconds", MAX_LEASE_TTL_SECONDS, positive=True)
        _seconds(self.retry_delay_seconds, "retry_delay_seconds", MAX_RETRY_DELAY_SECONDS)
        _positive_int(self.max_attempts, "max_attempts", MAX_ATTEMPTS)
        raw_budgets = self.source_budgets or {}
        if not isinstance(raw_budgets, Mapping):
            raise QueueError("source_budgets must be an object")
        normalized: dict[str, SourceBudget] = {}
        for source, budget in raw_budgets.items():
            source_name = _source_id(source)
            normalized[source_name] = SourceBudget.from_value(budget, f"{source_name} budget")
        object.__setattr__(self, "source_budgets", normalized)
        object.__setattr__(self, "heartbeat_extension_seconds", float(heartbeat))
        object.__setattr__(self, "lease_ttl_seconds", float(self.lease_ttl_seconds))
        object.__setattr__(self, "retry_delay_seconds", float(self.retry_delay_seconds))

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "QueuePolicy":
        expected = {
            "schema_version",
            "record_type",
            "max_active_items",
            "max_active_bytes",
            "lease_ttl_seconds",
            "heartbeat_extension_seconds",
            "retry_delay_seconds",
            "max_attempts",
            "source_budgets",
        }
        unknown = set(value) - expected
        if unknown:
            raise QueueError(f"queue policy has unknown fields: {sorted(unknown)}")
        if value.get("schema_version") != QUEUE_SCHEMA_VERSION:
            raise QueueError("queue policy schema_version is unsupported")
        if value.get("record_type") != QUEUE_POLICY_RECORD_TYPE:
            raise QueueError("queue policy record_type is unsupported")
        raw_budgets = value.get("source_budgets", {})
        if not isinstance(raw_budgets, Mapping):
            raise QueueError("source_budgets must be an object")
        return cls(
            max_active_items=_positive_int(value.get("max_active_items"), "max_active_items", MAX_ACTIVE_ITEMS),
            max_active_bytes=_positive_int(value.get("max_active_bytes"), "max_active_bytes", MAX_ACTIVE_BYTES),
            lease_ttl_seconds=_seconds(
                value.get("lease_ttl_seconds"), "lease_ttl_seconds", MAX_LEASE_TTL_SECONDS, positive=True
            ),
            heartbeat_extension_seconds=_seconds(
                value.get("heartbeat_extension_seconds"),
                "heartbeat_extension_seconds",
                MAX_LEASE_TTL_SECONDS,
                positive=True,
            ),
            retry_delay_seconds=_seconds(
                value.get("retry_delay_seconds"), "retry_delay_seconds", MAX_RETRY_DELAY_SECONDS
            ),
            max_attempts=_positive_int(value.get("max_attempts"), "max_attempts", MAX_ATTEMPTS),
            source_budgets={key: SourceBudget.from_value(item, f"{key} budget") for key, item in raw_budgets.items()},
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "record_type": QUEUE_POLICY_RECORD_TYPE,
            "max_active_items": self.max_active_items,
            "max_active_bytes": self.max_active_bytes,
            "lease_ttl_seconds": self.lease_ttl_seconds,
            "heartbeat_extension_seconds": self.heartbeat_extension_seconds,
            "retry_delay_seconds": self.retry_delay_seconds,
            "max_attempts": self.max_attempts,
            "source_budgets": {key: budget.as_dict() for key, budget in sorted((self.source_budgets or {}).items())},
        }

    def budget_for(self, source_id: str) -> SourceBudget:
        configured = (self.source_budgets or {}).get(source_id)
        if configured is not None:
            return configured
        return SourceBudget(self.max_active_items, self.max_active_bytes)


@dataclass(frozen=True, slots=True)
class WorkItem:
    """Durable work metadata, with no source payload bytes."""

    work_id: str
    source_id: str
    work_identity: str
    payload_sha256: str
    payload_ref: str
    byte_size: int
    state: WorkItemState
    priority: int
    available_at: datetime
    attempt_count: int
    max_attempts: int
    created_at: datetime
    updated_at: datetime
    claimed_at: datetime | None = None
    lease_expires_at: datetime | None = None
    lease_owner: str | None = None
    last_error: str | None = None
    blocked_reason: str | None = None
    poison_reason: str | None = None
    completed_at: datetime | None = None

    @property
    def item_id(self) -> str:
        """Alias used by callers that call queue rows items."""

        return self.work_id

    @property
    def content_sha256(self) -> str:
        """Alias matching raw-custody terminology."""

        return self.payload_sha256

    @property
    def byte_length(self) -> int:
        return self.byte_size

    @property
    def attempts(self) -> int:
        return self.attempt_count

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "record_type": QUEUE_RECORD_TYPE,
            "work_id": self.work_id,
            "source_id": self.source_id,
            "work_identity": self.work_identity,
            "payload_sha256": self.payload_sha256,
            "payload_ref": self.payload_ref,
            "byte_size": self.byte_size,
            "state": self.state,
            "priority": self.priority,
            "available_at": _format_timestamp(self.available_at),
            "attempt_count": self.attempt_count,
            "max_attempts": self.max_attempts,
            "created_at": _format_timestamp(self.created_at),
            "updated_at": _format_timestamp(self.updated_at),
            "claimed_at": _format_timestamp(self.claimed_at) if self.claimed_at else None,
            "lease_expires_at": _format_timestamp(self.lease_expires_at) if self.lease_expires_at else None,
            "lease_owner": self.lease_owner,
            "last_error": self.last_error,
            "blocked_reason": self.blocked_reason,
            "poison_reason": self.poison_reason,
            "completed_at": _format_timestamp(self.completed_at) if self.completed_at else None,
        }


@dataclass(frozen=True, slots=True)
class EnqueueReceipt:
    """Secret-free result of admitting one work identity."""

    action: Literal["enqueued", "duplicate"]
    item: WorkItem

    @property
    def state(self) -> Literal["enqueued", "duplicate"]:
        return self.action

    @property
    def work_id(self) -> str:
        return self.item.work_id

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "record_type": "queue_enqueue_receipt",
            "action": self.action,
            "work_id": self.item.work_id,
            "source_id": self.item.source_id,
            "work_identity": self.item.work_identity,
            "payload_sha256": self.item.payload_sha256,
            "byte_size": self.item.byte_size,
        }


@dataclass(frozen=True, slots=True)
class LeaseReceipt:
    """Worker-facing lease token and bounded work metadata."""

    work_id: str
    source_id: str
    work_identity: str
    payload_sha256: str
    payload_ref: str
    byte_size: int
    owner: str
    lease_token: str
    attempt: int
    leased_at: datetime
    lease_expires_at: datetime

    @property
    def item_id(self) -> str:
        return self.work_id

    @property
    def token(self) -> str:
        return self.lease_token

    @property
    def expires_at(self) -> datetime:
        return self.lease_expires_at

    def as_dict(self, *, include_token: bool = False) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "record_type": "queue_lease_receipt",
            "work_id": self.work_id,
            "source_id": self.source_id,
            "work_identity": self.work_identity,
            "payload_sha256": self.payload_sha256,
            "payload_ref": self.payload_ref,
            "byte_size": self.byte_size,
            "owner": self.owner,
            "attempt": self.attempt,
            "leased_at": _format_timestamp(self.leased_at),
            "lease_expires_at": _format_timestamp(self.lease_expires_at),
        }
        if include_token:
            value["lease_token"] = self.lease_token
        return value


@dataclass(frozen=True, slots=True)
class BackpressureDecision:
    """Explicit reason a queued item was not admitted in a claim pass."""

    work_id: str
    source_id: str
    reason: Literal["global_item_budget", "global_byte_budget", "source_item_budget", "source_byte_budget"]
    active_items: int
    active_bytes: int

    def as_dict(self) -> dict[str, object]:
        return {
            "work_id": self.work_id,
            "source_id": self.source_id,
            "reason": self.reason,
            "active_items": self.active_items,
            "active_bytes": self.active_bytes,
        }


@dataclass(frozen=True, slots=True)
class ClaimResult:
    """Claim-pass outcome including explicit backpressure evidence."""

    state: DecisionState
    leases: tuple[LeaseReceipt, ...] = ()
    decisions: tuple[BackpressureDecision, ...] = ()

    @property
    def blocked(self) -> tuple[BackpressureDecision, ...]:
        return self.decisions

    @property
    def lease(self) -> LeaseReceipt | None:
        return self.leases[0] if self.leases else None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "record_type": "queue_claim_result",
            "state": self.state,
            "leases": [lease.as_dict() for lease in self.leases],
            "decisions": [decision.as_dict() for decision in self.decisions],
        }


@dataclass(frozen=True, slots=True)
class RecoveryReceipt:
    """Idempotent recovery evidence for one expired lease."""

    work_id: str
    source_id: str
    state: RecoveryState
    attempt: int
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": QUEUE_SCHEMA_VERSION,
            "record_type": "queue_recovery_receipt",
            "work_id": self.work_id,
            "source_id": self.source_id,
            "state": self.state,
            "attempt": self.attempt,
            "reason": self.reason,
        }


class DurableQueue:
    """Restart-safe SQLite queue with atomic budget-aware claims."""

    def __init__(
        self,
        database: str | Path | sqlite3.Connection,
        *,
        policy: QueuePolicy | None = None,
        max_active_items: int | None = None,
        max_active_bytes: int | None = None,
        lease_ttl_seconds: float | None = None,
        heartbeat_extension_seconds: float | None = None,
        retry_delay_seconds: float | None = None,
        max_attempts: int | None = None,
        source_budgets: Mapping[str, SourceBudget | Mapping[str, object]] | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if policy is not None and any(
            value is not None
            for value in (
                max_active_items,
                max_active_bytes,
                lease_ttl_seconds,
                heartbeat_extension_seconds,
                retry_delay_seconds,
                max_attempts,
                source_budgets,
            )
        ):
            raise QueueError("policy cannot be combined with individual queue limits")
        if policy is None:
            raw_budgets = source_budgets or {}
            policy = QueuePolicy(
                max_active_items=64 if max_active_items is None else max_active_items,
                max_active_bytes=16 * 1024 * 1024 if max_active_bytes is None else max_active_bytes,
                lease_ttl_seconds=300.0 if lease_ttl_seconds is None else lease_ttl_seconds,
                heartbeat_extension_seconds=heartbeat_extension_seconds,
                retry_delay_seconds=30.0 if retry_delay_seconds is None else retry_delay_seconds,
                max_attempts=3 if max_attempts is None else max_attempts,
                source_budgets={
                    key: SourceBudget.from_value(value, f"{key} budget") for key, value in raw_budgets.items()
                },
            )
        if not callable(clock):
            raise QueueError("clock must be callable")
        self.policy = policy
        self._clock = clock
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
                    raise QueueError(f"unable to create queue database directory: {path.parent}") from exc
            try:
                self._connection = sqlite3.connect(
                    str(database),
                    isolation_level=None,
                    check_same_thread=False,
                    timeout=5.0,
                )
            except sqlite3.Error as exc:
                raise QueueError("unable to open queue database") from exc
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA busy_timeout = 5000")
            self._initialize()
        except sqlite3.Error as exc:
            self.close()
            raise QueueError("unable to initialize queue database") from exc
        self._last_claim_result = ClaimResult("empty")

    def close(self) -> None:
        """Close an owned connection; caller-owned DB-API connections remain open."""

        if self._owns_connection:
            with self._lock:
                try:
                    self._connection.close()
                except sqlite3.Error:
                    pass

    def __enter__(self) -> "DurableQueue":
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    @property
    def last_claim_result(self) -> ClaimResult:
        """Return the most recent claim-pass decision, including backpressure."""

        return self._last_claim_result

    def enqueue(
        self,
        source_id: str,
        work_id: str | None = None,
        payload_sha256: str | bytes | None = None,
        byte_size: int | None = None,
        payload_ref: str | None = None,
        *,
        work_identity: str | None = None,
        content_sha256: str | bytes | None = None,
        byte_length: int | None = None,
        available_at: datetime | None = None,
        priority: int = 0,
        max_attempts: int | None = None,
        now: datetime | None = None,
    ) -> EnqueueReceipt:
        """Admit one work identity idempotently without retaining source bytes."""

        source = _source_id(source_id)
        identity = _work_id(work_identity if work_identity is not None else work_id, "work_identity")
        item_name = _work_id(work_id if work_id is not None else identity)
        if payload_sha256 is not None and content_sha256 is not None:
            raise QueueError("payload_sha256 and content_sha256 are aliases; supply one")
        fingerprint = _fingerprint(
            payload_sha256 if payload_sha256 is not None else content_sha256,
            "payload_sha256",
        )
        if byte_size is not None and byte_length is not None:
            raise QueueError("byte_size and byte_length are aliases; supply one")
        size = _positive_int(
            byte_size if byte_size is not None else byte_length,
            "byte_size",
            MAX_WORK_BYTES,
        )
        reference = _safe_reference(payload_ref if payload_ref is not None else item_name)
        if isinstance(priority, bool) or not isinstance(priority, int) or not 0 <= priority <= MAX_PRIORITY:
            raise QueueError(f"priority must be an integer between 0 and {MAX_PRIORITY}")
        attempts_limit = (
            self.policy.max_attempts
            if max_attempts is None
            else _positive_int(max_attempts, "max_attempts", MAX_ATTEMPTS)
        )
        current = _utc(now if now is not None else self._clock(), "now")
        available = _utc(available_at, "available_at") if available_at is not None else current
        created = _format_timestamp(current)
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM queue_work_items WHERE source_id = ? AND work_identity = ?",
                (source, identity),
            ).fetchone()
            if existing is not None:
                existing_hash = str(existing["payload_sha256"])
                if existing_hash != fingerprint:
                    raise QueueCollisionError("work identity already exists with a different payload hash")
                existing_item = self._row_to_item(existing)
                return EnqueueReceipt("duplicate", existing_item)
            try:
                self._connection.execute(
                    """
                    INSERT INTO queue_work_items (
                        work_id, source_id, work_identity, payload_sha256, payload_ref,
                        byte_size, state, priority, available_at, attempt_count,
                        max_attempts, created_at, updated_at, claimed_at,
                        lease_expires_at, lease_owner, lease_token_sha256, last_error,
                        blocked_reason, poison_reason, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, NULL, NULL,
                              NULL, NULL, NULL, NULL, NULL, NULL)
                    """,
                    (
                        item_name,
                        source,
                        identity,
                        fingerprint,
                        reference,
                        size,
                        priority,
                        _format_timestamp(available),
                        attempts_limit,
                        created,
                        created,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise QueueCollisionError("work identity collides with an existing queue row") from exc
            row = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (item_name,)).fetchone()
            if row is None:  # pragma: no cover - guarded by the insert
                raise QueueError("queue insert did not produce a row")
            return EnqueueReceipt("enqueued", self._row_to_item(row))

    def get(self, work_id: str) -> WorkItem:
        """Read one work item, including terminal and poison states."""

        name = _work_id(work_id)
        with self._lock:
            row = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
        if row is None:
            raise QueueError(f"unknown work_id: {name}")
        return self._row_to_item(row)

    def list_items(
        self,
        *,
        source_id: str | None = None,
        state: WorkItemState | None = None,
    ) -> tuple[WorkItem, ...]:
        """Return deterministic metadata-only queue rows."""

        if source_id is not None:
            source_id = _source_id(source_id)
        if state is not None and state not in {"queued", "leased", "retry_wait", "completed", "poison"}:
            raise QueueError("unsupported work item state")
        query = "SELECT * FROM queue_work_items"
        params: list[str] = []
        clauses: list[str] = []
        if source_id is not None:
            clauses.append("source_id = ?")
            params.append(source_id)
        if state is not None:
            clauses.append("state = ?")
            params.append(state)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY source_id, priority DESC, available_at, created_at, work_id"
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return tuple(self._row_to_item(row) for row in rows)

    def recover_expired(self, *, now: datetime | None = None) -> tuple[RecoveryReceipt, ...]:
        """Requeue or isolate expired claims exactly once per lease expiry."""

        current = _utc(now if now is not None else self._clock(), "now")
        stamp = _format_timestamp(current)
        receipts: list[RecoveryReceipt] = []
        with self._transaction():
            rows = self._connection.execute(
                """
                SELECT work_id, source_id, attempt_count, max_attempts
                FROM queue_work_items
                WHERE state = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
                ORDER BY lease_expires_at, work_id
                """,
                (stamp,),
            ).fetchall()
            for row in rows:
                work_id = str(row["work_id"])
                source_id = str(row["source_id"])
                attempt = int(row["attempt_count"])
                max_attempt = int(row["max_attempts"])
                if attempt >= max_attempt:
                    state: RecoveryState = "poisoned"
                    reason = "lease expired after maximum attempts"
                    self._connection.execute(
                        """
                        UPDATE queue_work_items
                        SET state = 'poison', updated_at = ?, lease_expires_at = NULL,
                            lease_owner = NULL, lease_token_sha256 = NULL,
                            poison_reason = ?, blocked_reason = NULL
                        WHERE work_id = ? AND state = 'leased'
                        """,
                        (stamp, reason, work_id),
                    )
                else:
                    state = "requeued"
                    reason = "lease expired; work made visible for retry"
                    self._connection.execute(
                        """
                        UPDATE queue_work_items
                        SET state = 'queued', available_at = ?, updated_at = ?,
                            lease_expires_at = NULL, lease_owner = NULL,
                            lease_token_sha256 = NULL, claimed_at = NULL,
                            blocked_reason = ?
                        WHERE work_id = ? AND state = 'leased'
                        """,
                        (stamp, stamp, reason, work_id),
                    )
                receipts.append(RecoveryReceipt(work_id, source_id, state, attempt, reason))
        return tuple(receipts)

    def claim(
        self,
        owner: str,
        *,
        now: datetime | None = None,
        limit: int = 1,
    ) -> LeaseReceipt | None:
        """Claim one work item; use ``claim_many`` for an explicit batch."""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit != 1:
            raise QueueError("claim() accepts limit=1; use claim_many() for a batch")
        result = self.claim_many(owner, now=now, limit=1)
        return result.lease

    def claim_many(
        self,
        owner: str,
        *,
        now: datetime | None = None,
        limit: int = 1,
    ) -> ClaimResult:
        """Atomically claim eligible work while honoring all active budgets."""

        worker = _owner(owner)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_ACTIVE_ITEMS:
            raise QueueError(f"limit must be an integer between 1 and {MAX_ACTIVE_ITEMS}")
        current = _utc(now if now is not None else self._clock(), "now")
        stamp = _format_timestamp(current)
        leases: list[LeaseReceipt] = []
        decisions: list[BackpressureDecision] = []
        with self._transaction():
            self._recover_expired_locked(stamp)
            active_items, active_bytes = self._active_totals_locked(stamp)
            rows = self._connection.execute(
                """
                SELECT * FROM queue_work_items
                WHERE state IN ('queued', 'retry_wait') AND available_at <= ?
                ORDER BY priority DESC, available_at, created_at, work_id
                """,
                (stamp,),
            ).fetchall()
            for row in rows:
                if len(leases) >= limit:
                    break
                item = self._row_to_item(row)
                source_budget = self.policy.budget_for(item.source_id)
                reason: (
                    Literal["global_item_budget", "global_byte_budget", "source_item_budget", "source_byte_budget"]
                    | None
                ) = None
                source_items, source_bytes = self._active_totals_locked(stamp, source_id=item.source_id)
                if active_items >= self.policy.max_active_items:
                    reason = "global_item_budget"
                elif active_bytes + item.byte_size > self.policy.max_active_bytes:
                    reason = "global_byte_budget"
                elif source_items >= source_budget.max_active_items:
                    reason = "source_item_budget"
                elif source_bytes + item.byte_size > source_budget.max_active_bytes:
                    reason = "source_byte_budget"
                if reason is not None:
                    decisions.append(
                        BackpressureDecision(item.work_id, item.source_id, reason, active_items, active_bytes)
                    )
                    self._connection.execute(
                        "UPDATE queue_work_items SET blocked_reason = ?, updated_at = ? WHERE work_id = ? AND state IN ('queued', 'retry_wait')",
                        (reason, stamp, item.work_id),
                    )
                    continue
                token = secrets.token_urlsafe(32)
                token_hash = "sha256:" + sha256(token.encode("utf-8")).hexdigest()
                expires = current + timedelta(seconds=self.policy.lease_ttl_seconds)
                expires_stamp = _format_timestamp(expires)
                attempt = item.attempt_count + 1
                updated = self._connection.execute(
                    """
                    UPDATE queue_work_items
                    SET state = 'leased', attempt_count = ?, updated_at = ?, claimed_at = ?,
                        lease_expires_at = ?, lease_owner = ?, lease_token_sha256 = ?,
                        blocked_reason = NULL, last_error = NULL
                    WHERE work_id = ? AND state IN ('queued', 'retry_wait') AND available_at <= ?
                    """,
                    (attempt, stamp, stamp, expires_stamp, worker, token_hash, item.work_id, stamp),
                )
                if updated.rowcount != 1:  # pragma: no cover - transaction serializes claimers
                    continue
                active_items += 1
                active_bytes += item.byte_size
                leases.append(
                    LeaseReceipt(
                        work_id=item.work_id,
                        source_id=item.source_id,
                        work_identity=item.work_identity,
                        payload_sha256=item.payload_sha256,
                        payload_ref=item.payload_ref,
                        byte_size=item.byte_size,
                        owner=worker,
                        lease_token=token,
                        attempt=attempt,
                        leased_at=current,
                        lease_expires_at=expires,
                    )
                )
            state: DecisionState
            if leases:
                state = "claimed"
            elif decisions:
                state = "backpressure"
            else:
                state = "empty"
            result = ClaimResult(state, tuple(leases), tuple(decisions))
        self._last_claim_result = result
        return result

    def claim_with_result(
        self,
        owner: str,
        *,
        now: datetime | None = None,
        limit: int = 1,
    ) -> ClaimResult:
        """Alias emphasizing that backpressure evidence is part of the result."""

        return self.claim_many(owner, now=now, limit=limit)

    def heartbeat(
        self,
        work_id: str,
        owner: str,
        lease_token: str,
        *,
        now: datetime | None = None,
    ) -> LeaseReceipt:
        """Renew an active lease only when owner and random token both match."""

        name = _work_id(work_id)
        worker = _owner(owner)
        token = _required_text(lease_token, "lease_token", maximum=256)
        current = _utc(now if now is not None else self._clock(), "now")
        stamp = _format_timestamp(current)
        token_hash = "sha256:" + sha256(token.encode("utf-8")).hexdigest()
        expires = current + timedelta(seconds=cast(float, self.policy.heartbeat_extension_seconds))
        expires_stamp = _format_timestamp(expires)
        with self._transaction():
            row = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            self._assert_active_lease(row, worker, token_hash, current)
            updated = self._connection.execute(
                """
                UPDATE queue_work_items SET lease_expires_at = ?, updated_at = ?
                WHERE work_id = ? AND state = 'leased' AND lease_owner = ? AND lease_token_sha256 = ?
                """,
                (expires_stamp, stamp, name, worker, token_hash),
            )
            if updated.rowcount != 1:  # pragma: no cover - guarded by the predicate and lock
                raise LeaseExpiredError("lease could not be renewed")
            fresh = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            if fresh is None:  # pragma: no cover - guarded by the update
                raise QueueError("renewed queue row disappeared")
            item = self._row_to_item(fresh)
            return self._lease_from_item(item, worker, token, current, expires)

    renew = heartbeat

    def complete(
        self,
        work_id: str,
        owner: str,
        lease_token: str,
        *,
        now: datetime | None = None,
    ) -> WorkItem:
        """Acknowledge successful work only through its active lease."""

        return self._finish(work_id, owner, lease_token, state="completed", now=now)

    acknowledge = complete

    def fail(
        self,
        work_id: str,
        owner: str,
        lease_token: str,
        reason: str,
        *,
        retryable: bool = True,
        now: datetime | None = None,
    ) -> WorkItem:
        """Retry a bounded failure or isolate it as poison work."""

        if not isinstance(retryable, bool):
            raise QueueError("retryable must be a boolean")
        detail = _reason(reason)
        name = _work_id(work_id)
        worker = _owner(owner)
        token = _required_text(lease_token, "lease_token", maximum=256)
        current = _utc(now if now is not None else self._clock(), "now")
        stamp = _format_timestamp(current)
        token_hash = "sha256:" + sha256(token.encode("utf-8")).hexdigest()
        with self._transaction():
            row = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            self._assert_active_lease(row, worker, token_hash, current)
            if row is None:  # pragma: no cover - _assert_active_lease always raises
                raise QueueError("unknown work item")
            item = self._row_to_item(row)
            poison = (not retryable) or item.attempt_count >= item.max_attempts
            if poison:
                self._connection.execute(
                    """
                    UPDATE queue_work_items SET state = 'poison', updated_at = ?,
                        lease_expires_at = NULL, lease_owner = NULL, lease_token_sha256 = NULL,
                        poison_reason = ?, last_error = ?, blocked_reason = NULL
                    WHERE work_id = ? AND state = 'leased' AND lease_owner = ? AND lease_token_sha256 = ?
                    """,
                    (stamp, detail, detail, name, worker, token_hash),
                )
            else:
                available = current + timedelta(seconds=self.policy.retry_delay_seconds)
                self._connection.execute(
                    """
                    UPDATE queue_work_items SET state = 'retry_wait', available_at = ?, updated_at = ?,
                        lease_expires_at = NULL, lease_owner = NULL, lease_token_sha256 = NULL,
                        last_error = ?, blocked_reason = NULL
                    WHERE work_id = ? AND state = 'leased' AND lease_owner = ? AND lease_token_sha256 = ?
                    """,
                    (_format_timestamp(available), stamp, detail, name, worker, token_hash),
                )
            fresh = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            if fresh is None:  # pragma: no cover - guarded by the update
                raise QueueError("failed queue row disappeared")
            return self._row_to_item(fresh)

    def mark_poison(
        self,
        work_id: str,
        reason: str,
        *,
        owner: str | None = None,
        lease_token: str | None = None,
        now: datetime | None = None,
    ) -> WorkItem:
        """Isolate queued work, or active work with an owner-bound lease."""

        detail = _reason(reason)
        name = _work_id(work_id)
        current = _utc(now if now is not None else self._clock(), "now")
        stamp = _format_timestamp(current)
        with self._transaction():
            row = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            if row is None:
                raise QueueError(f"unknown work_id: {name}")
            item = self._row_to_item(row)
            if item.state == "leased":
                if owner is None or lease_token is None:
                    raise QueueStateError("leased work requires owner and lease_token to poison")
                worker = _owner(owner)
                token = _required_text(lease_token, "lease_token", maximum=256)
                self._assert_active_lease(row, worker, "sha256:" + sha256(token.encode()).hexdigest(), current)
                predicate = " AND lease_owner = ? AND lease_token_sha256 = ?"
                params: tuple[object, ...] = (
                    stamp,
                    detail,
                    name,
                    worker,
                    "sha256:" + sha256(token.encode()).hexdigest(),
                )
            elif item.state in {"queued", "retry_wait"}:
                predicate = ""
                params = (stamp, detail, name)
            else:
                raise QueueStateError("only queued or leased work can be marked poison")
            self._connection.execute(
                """
                UPDATE queue_work_items SET state = 'poison', updated_at = ?, poison_reason = ?,
                    lease_expires_at = NULL, lease_owner = NULL, lease_token_sha256 = NULL,
                    blocked_reason = NULL
                WHERE work_id = ? AND state IN ('queued', 'retry_wait', 'leased')
                """
                + predicate,
                params,
            )
            fresh = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            if fresh is None:  # pragma: no cover
                raise QueueError("poison queue row disappeared")
            return self._row_to_item(fresh)

    poison = mark_poison
    quarantine = mark_poison

    def policy_record(self) -> dict[str, object]:
        """Return the validated queue policy in schema form."""

        return self.policy.as_dict()

    def _finish(
        self,
        work_id: str,
        owner: str,
        lease_token: str,
        *,
        state: Literal["completed"],
        now: datetime | None,
    ) -> WorkItem:
        name = _work_id(work_id)
        worker = _owner(owner)
        token = _required_text(lease_token, "lease_token", maximum=256)
        current = _utc(now if now is not None else self._clock(), "now")
        stamp = _format_timestamp(current)
        token_hash = "sha256:" + sha256(token.encode("utf-8")).hexdigest()
        with self._transaction():
            row = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            self._assert_active_lease(row, worker, token_hash, current)
            self._connection.execute(
                """
                UPDATE queue_work_items SET state = ?, updated_at = ?, completed_at = ?,
                    lease_expires_at = NULL, lease_owner = NULL, lease_token_sha256 = NULL,
                    blocked_reason = NULL
                WHERE work_id = ? AND state = 'leased' AND lease_owner = ? AND lease_token_sha256 = ?
                """,
                (state, stamp, stamp, name, worker, token_hash),
            )
            fresh = self._connection.execute("SELECT * FROM queue_work_items WHERE work_id = ?", (name,)).fetchone()
            if fresh is None:  # pragma: no cover
                raise QueueError("completed queue row disappeared")
            return self._row_to_item(fresh)

    def _assert_active_lease(
        self,
        row: sqlite3.Row | None,
        owner: str,
        token_hash: str,
        now: datetime,
    ) -> None:
        if row is None:
            raise QueueError("unknown work_id")
        if row["state"] != "leased":
            raise QueueStateError("work item does not have an active lease")
        expires = _parse_timestamp(row["lease_expires_at"], "lease_expires_at")
        if expires <= now:
            raise LeaseExpiredError("lease has expired")
        if row["lease_owner"] != owner or row["lease_token_sha256"] != token_hash:
            raise QueueStateError("lease owner or token does not match")

    def _lease_from_item(
        self,
        item: WorkItem,
        owner: str,
        token: str,
        leased_at: datetime,
        expires: datetime,
    ) -> LeaseReceipt:
        return LeaseReceipt(
            work_id=item.work_id,
            source_id=item.source_id,
            work_identity=item.work_identity,
            payload_sha256=item.payload_sha256,
            payload_ref=item.payload_ref,
            byte_size=item.byte_size,
            owner=owner,
            lease_token=token,
            attempt=item.attempt_count,
            leased_at=leased_at,
            lease_expires_at=expires,
        )

    def _active_totals_locked(self, stamp: str, *, source_id: str | None = None) -> tuple[int, int]:
        if source_id is None:
            row = self._connection.execute(
                """
                SELECT COUNT(*) AS active_items, COALESCE(SUM(byte_size), 0) AS active_bytes
                FROM queue_work_items WHERE state = 'leased' AND lease_expires_at > ?
                """,
                (stamp,),
            ).fetchone()
        else:
            row = self._connection.execute(
                """
                SELECT COUNT(*) AS active_items, COALESCE(SUM(byte_size), 0) AS active_bytes
                FROM queue_work_items
                WHERE state = 'leased' AND lease_expires_at > ? AND source_id = ?
                """,
                (stamp, source_id),
            ).fetchone()
        if row is None:  # pragma: no cover
            return 0, 0
        return int(row["active_items"]), int(row["active_bytes"])

    def _recover_expired_locked(self, stamp: str) -> None:
        rows = self._connection.execute(
            "SELECT work_id, attempt_count, max_attempts FROM queue_work_items WHERE state = 'leased' AND lease_expires_at <= ?",
            (stamp,),
        ).fetchall()
        for row in rows:
            if int(row["attempt_count"]) >= int(row["max_attempts"]):
                self._connection.execute(
                    """
                    UPDATE queue_work_items SET state = 'poison', updated_at = ?,
                        poison_reason = 'lease expired after maximum attempts',
                        lease_expires_at = NULL, lease_owner = NULL, lease_token_sha256 = NULL,
                        blocked_reason = NULL WHERE work_id = ? AND state = 'leased'
                    """,
                    (stamp, row["work_id"]),
                )
            else:
                self._connection.execute(
                    """
                    UPDATE queue_work_items SET state = 'queued', available_at = ?, updated_at = ?,
                        claimed_at = NULL, lease_expires_at = NULL, lease_owner = NULL,
                        lease_token_sha256 = NULL, blocked_reason = 'lease expired; work made visible for retry'
                    WHERE work_id = ? AND state = 'leased'
                    """,
                    (stamp, stamp, row["work_id"]),
                )

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS queue_work_items (
                work_id TEXT PRIMARY KEY NOT NULL,
                source_id TEXT NOT NULL,
                work_identity TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                payload_ref TEXT NOT NULL,
                byte_size INTEGER NOT NULL CHECK (byte_size > 0),
                state TEXT NOT NULL CHECK (state IN ('queued', 'leased', 'retry_wait', 'completed', 'poison')),
                priority INTEGER NOT NULL CHECK (priority >= 0 AND priority <= 100),
                available_at TEXT NOT NULL,
                attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
                max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                claimed_at TEXT,
                lease_expires_at TEXT,
                lease_owner TEXT,
                lease_token_sha256 TEXT,
                last_error TEXT,
                blocked_reason TEXT,
                poison_reason TEXT,
                completed_at TEXT,
                UNIQUE (source_id, work_identity)
            );
            CREATE INDEX IF NOT EXISTS queue_work_items_claim_idx
                ON queue_work_items (state, available_at, priority DESC, created_at, work_id);
            CREATE INDEX IF NOT EXISTS queue_work_items_lease_idx
                ON queue_work_items (state, lease_expires_at);
            CREATE INDEX IF NOT EXISTS queue_work_items_source_idx
                ON queue_work_items (source_id, state, available_at);
            """
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

    @staticmethod
    def _row_to_item(row: sqlite3.Row) -> WorkItem:
        state = str(row["state"])
        if state not in {"queued", "leased", "retry_wait", "completed", "poison"}:
            raise QueueError("database contains unsupported work item state")
        return WorkItem(
            work_id=str(row["work_id"]),
            source_id=str(row["source_id"]),
            work_identity=str(row["work_identity"]),
            payload_sha256=str(row["payload_sha256"]),
            payload_ref=str(row["payload_ref"]),
            byte_size=int(row["byte_size"]),
            state=cast(WorkItemState, state),
            priority=int(row["priority"]),
            available_at=_parse_timestamp(row["available_at"], "available_at"),
            attempt_count=int(row["attempt_count"]),
            max_attempts=int(row["max_attempts"]),
            created_at=_parse_timestamp(row["created_at"], "created_at"),
            updated_at=_parse_timestamp(row["updated_at"], "updated_at"),
            claimed_at=_optional_timestamp(row["claimed_at"], "claimed_at"),
            lease_expires_at=_optional_timestamp(row["lease_expires_at"], "lease_expires_at"),
            lease_owner=cast(str | None, row["lease_owner"]),
            last_error=cast(str | None, row["last_error"]),
            blocked_reason=cast(str | None, row["blocked_reason"]),
            poison_reason=cast(str | None, row["poison_reason"]),
            completed_at=_optional_timestamp(row["completed_at"], "completed_at"),
        )


# Public aliases keep terminology easy to discover for callers from scheduler
# and custody lanes without introducing duplicate implementations.
QueueBudget = SourceBudget
WorkLease = LeaseReceipt


__all__ = [
    "BackpressureDecision",
    "ClaimResult",
    "DurableQueue",
    "EnqueueReceipt",
    "LeaseExpiredError",
    "LeaseReceipt",
    "QueueBudget",
    "QueueCollisionError",
    "QueueError",
    "QueuePolicy",
    "QueueStateError",
    "RecoveryReceipt",
    "SourceBudget",
    "WorkItem",
    "WorkItemState",
    "WorkLease",
]
