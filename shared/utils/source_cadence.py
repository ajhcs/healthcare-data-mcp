"""Stable source registrations and bounded poll-state transitions.

The catalog is declarative and network-free. It gives later scheduler and
producer lanes one source identity, release/change mode, cadence, and explicit
rights boundary. It never performs a poll or writes a queue.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Literal, Mapping, Sequence, TypeAlias, cast


ChangeMode = Literal["etag", "last_modified", "release_metadata", "content_hash"]
PollResult = Literal["no_op", "changed", "failed_probe", "interrupted"]
PollStateName = Literal[
    "never_run",
    "succeeded",
    "no_op",
    "failed",
    "missed",
    "backfill_pending",
    "backfill_running",
    "blocked",
]
PollIntentReason = Literal["cadence", "retry", "operator_replay", "backfill"]

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]

_ROOT = Path(__file__).resolve().parents[2]
_CATALOG_SCHEMA = _ROOT / "contracts/healthcare-data-platform/catalog/v1/source-catalog.schema.json"
_POLL_STATE_SCHEMA = _ROOT / "contracts/healthcare-data-platform/catalog/v1/poll-state.schema.json"
_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:[a-z0-9][a-z0-9._:-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class SourceCatalogError(ValueError):
    """Raised when catalog data or a poll transition violates the contract."""


def _required_string(value: object, label: str, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value:
        raise SourceCatalogError(f"{label} must be a non-empty string")
    if pattern is not None and pattern.fullmatch(value) is None:
        raise SourceCatalogError(f"{label} has an invalid format")
    return value


def _required_int(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise SourceCatalogError(f"{label} must be an integer between {minimum} and {maximum}")
    return value


def _required_datetime(value: object, label: str, *, allow_none: bool = False) -> datetime | None:
    if value is None and allow_none:
        return None
    raw = _required_string(value, label)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SourceCatalogError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise SourceCatalogError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _schema_validate(value: object, path: Path, label: str) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as exc:  # pragma: no cover - dependency installation failure
        raise SourceCatalogError("jsonschema is required for catalog validation") from exc
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SourceCatalogError(f"unable to read {label} schema") from exc
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, value)),
        key=lambda error: str(error.absolute_path),
    )
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.absolute_path)
        suffix = f" at {location}" if location else ""
        raise SourceCatalogError(f"{label} failed schema validation{suffix}: {error.message}")


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SourceCatalogError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class SourceCadence:
    """A bounded cadence and late-run grace window."""

    interval_seconds: int
    jitter_seconds: int
    missed_run_grace_seconds: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "SourceCadence":
        return cls(
            interval_seconds=_required_int(value.get("interval_seconds"), "interval_seconds", 60, 2678400),
            jitter_seconds=_required_int(value.get("jitter_seconds"), "jitter_seconds", 0, 3600),
            missed_run_grace_seconds=_required_int(
                value.get("missed_run_grace_seconds"), "missed_run_grace_seconds", 60, 604800
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "interval_seconds": self.interval_seconds,
            "jitter_seconds": self.jitter_seconds,
            "missed_run_grace_seconds": self.missed_run_grace_seconds,
        }


@dataclass(frozen=True, slots=True)
class SourceRegistration:
    """One stable source identity and its change-detection policy."""

    source_id: str
    title: str
    family: str
    source_url: str
    change_mode: ChangeMode
    cadence: SourceCadence
    release_locator: str
    rights_status: Literal["approved_public", "pending_review", "blocked"]
    enabled: bool
    owner: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "SourceRegistration":
        source_id = _required_string(value.get("source_id"), "source_id", _SOURCE_ID)
        title = _required_string(value.get("title"), f"{source_id}.title")
        family = _required_string(value.get("family"), f"{source_id}.family")
        source_url = _required_string(value.get("source_url"), f"{source_id}.source_url")
        release_locator = _required_string(value.get("release_locator"), f"{source_id}.release_locator")
        raw_mode = _required_string(value.get("change_mode"), f"{source_id}.change_mode")
        if raw_mode not in {"etag", "last_modified", "release_metadata", "content_hash"}:
            raise SourceCatalogError(f"{source_id}.change_mode is unsupported")
        raw_rights = _required_string(value.get("rights_status"), f"{source_id}.rights_status")
        if raw_rights not in {"approved_public", "pending_review", "blocked"}:
            raise SourceCatalogError(f"{source_id}.rights_status is unsupported")
        enabled = value.get("enabled")
        if not isinstance(enabled, bool):
            raise SourceCatalogError(f"{source_id}.enabled must be boolean")
        return cls(
            source_id=source_id,
            title=title,
            family=family,
            source_url=source_url,
            change_mode=cast(ChangeMode, raw_mode),
            cadence=SourceCadence.from_mapping(_mapping(value.get("cadence"), f"{source_id}.cadence")),
            release_locator=release_locator,
            rights_status=cast(Literal["approved_public", "pending_review", "blocked"], raw_rights),
            enabled=enabled,
            owner=_required_string(value.get("owner"), f"{source_id}.owner"),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "title": self.title,
            "family": self.family,
            "source_url": self.source_url,
            "change_mode": self.change_mode,
            "cadence": self.cadence.as_dict(),
            "release_locator": self.release_locator,
            "rights_status": self.rights_status,
            "enabled": self.enabled,
            "owner": self.owner,
        }


@dataclass(frozen=True, slots=True)
class PollState:
    """Durable state needed to schedule and replay one source."""

    source_id: str
    state: PollStateName
    generation: int
    consecutive_failures: int
    next_due_at: datetime
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_release_id: str | None = None
    last_release_fingerprint: str | None = None
    backfill_from: str | None = None
    backfill_to: str | None = None

    @classmethod
    def initial(cls, source_id: str, *, next_due_at: datetime) -> "PollState":
        _required_string(source_id, "source_id", _SOURCE_ID)
        return cls(
            source_id=source_id, state="never_run", generation=0, consecutive_failures=0, next_due_at=_utc(next_due_at)
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "PollState":
        _schema_validate(value, _POLL_STATE_SCHEMA, "poll state")
        source_id = _required_string(value.get("source_id"), "source_id", _SOURCE_ID)
        raw_state = _required_string(value.get("state"), f"{source_id}.state")
        allowed = {
            "never_run",
            "succeeded",
            "no_op",
            "failed",
            "missed",
            "backfill_pending",
            "backfill_running",
            "blocked",
        }
        if raw_state not in allowed:
            raise SourceCatalogError(f"{source_id}.state is unsupported")
        generation = _required_int(value.get("generation"), f"{source_id}.generation", 0, 1000000)
        failures = _required_int(value.get("consecutive_failures"), f"{source_id}.consecutive_failures", 0, 1000)
        next_due = _required_datetime(value.get("next_due_at"), f"{source_id}.next_due_at")
        assert next_due is not None
        last_attempt = _required_datetime(value.get("last_attempt_at"), f"{source_id}.last_attempt_at", allow_none=True)
        last_success = _required_datetime(value.get("last_success_at"), f"{source_id}.last_success_at", allow_none=True)
        last_release_id = value.get("last_release_id")
        if last_release_id is not None:
            _required_string(last_release_id, f"{source_id}.last_release_id", _RELEASE_ID)
        last_fingerprint = value.get("last_release_fingerprint")
        if last_fingerprint is not None:
            _required_string(last_fingerprint, f"{source_id}.last_release_fingerprint", _SHA256)
        backfill_from = value.get("backfill_from")
        if backfill_from is not None:
            _required_string(backfill_from, f"{source_id}.backfill_from", _RELEASE_ID)
        backfill_to = value.get("backfill_to")
        if backfill_to is not None:
            _required_string(backfill_to, f"{source_id}.backfill_to", _RELEASE_ID)
        return cls(
            source_id=source_id,
            state=cast(PollStateName, raw_state),
            generation=generation,
            consecutive_failures=failures,
            next_due_at=next_due,
            last_attempt_at=last_attempt,
            last_success_at=last_success,
            last_release_id=cast(str | None, last_release_id),
            last_release_fingerprint=cast(str | None, last_fingerprint),
            backfill_from=cast(str | None, backfill_from),
            backfill_to=cast(str | None, backfill_to),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": "hdp.source-poll-state.v1",
            "record_type": "source_poll_state",
            "source_id": self.source_id,
            "state": self.state,
            "generation": self.generation,
            "consecutive_failures": self.consecutive_failures,
            "next_due_at": _iso(self.next_due_at),
            "last_attempt_at": _iso(self.last_attempt_at) if self.last_attempt_at else None,
            "last_success_at": _iso(self.last_success_at) if self.last_success_at else None,
            "last_release_id": self.last_release_id,
            "last_release_fingerprint": self.last_release_fingerprint,
            "backfill_from": self.backfill_from,
            "backfill_to": self.backfill_to,
        }


@dataclass(frozen=True, slots=True)
class PollIntent:
    """Deterministic, durable intent emitted for a due source."""

    intent_id: str
    source_id: str
    scheduled_at: datetime
    reason: PollIntentReason
    generation: int

    def as_dict(self) -> dict[str, object]:
        return {
            "intent_id": self.intent_id,
            "source_id": self.source_id,
            "scheduled_at": _iso(self.scheduled_at),
            "reason": self.reason,
            "generation": self.generation,
        }


def load_catalog(path: str | Path) -> tuple[SourceRegistration, ...]:
    """Load and validate a catalog fixture without network access."""

    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=_strict_pairs)
    except SourceCatalogError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise SourceCatalogError(f"unable to read catalog: {path}") from exc
    if not isinstance(raw, dict):
        raise SourceCatalogError("source catalog must be an object")
    _schema_validate(raw, _CATALOG_SCHEMA, "source catalog")
    sources = raw.get("sources")
    if not isinstance(sources, list):
        raise SourceCatalogError("source catalog sources must be an array")
    registrations = tuple(SourceRegistration.from_mapping(_mapping(item, "source registration")) for item in sources)
    validate_registrations(registrations)
    return registrations


def validate_registrations(registrations: Sequence[SourceRegistration]) -> None:
    """Reject duplicate identities and enabled sources without public rights."""

    seen: set[str] = set()
    for registration in registrations:
        if registration.source_id in seen:
            raise SourceCatalogError(f"duplicate source_id: {registration.source_id}")
        seen.add(registration.source_id)
        if registration.enabled and registration.rights_status != "approved_public":
            raise SourceCatalogError(f"enabled source lacks approved public rights: {registration.source_id}")


def validate_poll_state(state: PollState) -> None:
    """Validate the serialized poll state against the v1 schema."""

    _schema_validate(state.as_dict(), _POLL_STATE_SCHEMA, "poll state")
    if state.state in {"backfill_pending", "backfill_running"} and (
        state.backfill_from is None or state.backfill_to is None
    ):
        raise SourceCatalogError(f"{state.state} state requires a release range")
    if state.state not in {"backfill_pending", "backfill_running"} and (state.backfill_from or state.backfill_to):
        raise SourceCatalogError("backfill release range is only valid while backfill is active")


def schedule_due(
    registrations: Sequence[SourceRegistration],
    states: Mapping[str, PollState],
    *,
    now: datetime,
) -> tuple[PollIntent, ...]:
    """Emit deterministic intents for enabled, approved, due sources."""

    current = _utc(now)
    intents: list[PollIntent] = []
    for registration in sorted(registrations, key=lambda item: item.source_id):
        state = states.get(registration.source_id)
        if state is None or not registration.enabled or registration.rights_status != "approved_public":
            continue
        validate_poll_state(state)
        if state.next_due_at > current or state.state == "blocked":
            continue
        reason: PollIntentReason
        if state.state in {"backfill_pending", "backfill_running"}:
            reason = "backfill"
        elif state.state in {"failed", "missed"}:
            reason = "retry"
        else:
            reason = "cadence"
        material = f"{state.source_id}|{_iso(state.next_due_at)}|{state.generation}|{reason}".encode()
        intent_id = "intent:poll:" + sha256(material).hexdigest()[:24]
        intents.append(PollIntent(intent_id, state.source_id, current, reason, state.generation))
    return tuple(intents)


def mark_missed(state: PollState, *, now: datetime, cadence: SourceCadence) -> PollState:
    """Mark a due state missed after its explicit grace window."""

    current = _utc(now)
    if state.state in {"blocked", "backfill_pending", "backfill_running"}:
        return state
    if current <= state.next_due_at + timedelta(seconds=cadence.missed_run_grace_seconds):
        return state
    updated = PollState(
        source_id=state.source_id,
        state="missed",
        generation=state.generation + 1,
        consecutive_failures=state.consecutive_failures,
        next_due_at=current,
        last_attempt_at=state.last_attempt_at,
        last_success_at=state.last_success_at,
        last_release_id=state.last_release_id,
        last_release_fingerprint=state.last_release_fingerprint,
        backfill_from=state.backfill_from,
        backfill_to=state.backfill_to,
    )
    validate_poll_state(updated)
    return updated


def apply_probe_result(
    state: PollState,
    result: PollResult,
    *,
    attempted_at: datetime,
    next_due_at: datetime,
    release_id: str | None = None,
    release_fingerprint: str | None = None,
) -> PollState:
    """Advance state after a bounded probe, preserving prior release on failure."""

    attempted = _utc(attempted_at)
    next_due = _utc(next_due_at)
    if result == "changed":
        if release_id is None or release_fingerprint is None:
            raise SourceCatalogError("changed result requires release identity and fingerprint")
        _required_string(release_id, "release_id", _RELEASE_ID)
        _required_string(release_fingerprint, "release_fingerprint", _SHA256)
        next_state: PollStateName = "succeeded"
        failures = 0
        success = attempted
    elif result == "no_op":
        next_state = "no_op"
        failures = 0
        success = attempted
    elif result in {"failed_probe", "interrupted"}:
        next_state = "failed"
        failures = min(1000, state.consecutive_failures + 1)
        success = state.last_success_at
    else:  # pragma: no cover - Literal exhaustiveness guard
        raise SourceCatalogError(f"unsupported probe result: {result}")
    updated = PollState(
        source_id=state.source_id,
        state=next_state,
        generation=state.generation + 1,
        consecutive_failures=failures,
        next_due_at=next_due,
        last_attempt_at=attempted,
        last_success_at=success,
        last_release_id=release_id if result == "changed" else state.last_release_id,
        last_release_fingerprint=release_fingerprint if result == "changed" else state.last_release_fingerprint,
        backfill_from=None if result in {"changed", "no_op"} else state.backfill_from,
        backfill_to=None if result in {"changed", "no_op"} else state.backfill_to,
    )
    validate_poll_state(updated)
    return updated


def request_backfill(state: PollState, *, from_release: str, to_release: str) -> PollState:
    """Create an explicit bounded backfill state; no queue work is started."""

    _required_string(from_release, "from_release", _RELEASE_ID)
    _required_string(to_release, "to_release", _RELEASE_ID)
    updated = PollState(
        source_id=state.source_id,
        state="backfill_pending",
        generation=state.generation + 1,
        consecutive_failures=state.consecutive_failures,
        next_due_at=state.next_due_at,
        last_attempt_at=state.last_attempt_at,
        last_success_at=state.last_success_at,
        last_release_id=state.last_release_id,
        last_release_fingerprint=state.last_release_fingerprint,
        backfill_from=from_release,
        backfill_to=to_release,
    )
    validate_poll_state(updated)
    return updated


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise SourceCatalogError(f"{label} must be an object")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise SourceCatalogError("timestamps must include a timezone")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


__all__ = [
    "PollIntent",
    "PollState",
    "SourceCadence",
    "SourceCatalogError",
    "SourceRegistration",
    "apply_probe_result",
    "load_catalog",
    "mark_missed",
    "request_backfill",
    "schedule_due",
    "validate_poll_state",
    "validate_registrations",
]
