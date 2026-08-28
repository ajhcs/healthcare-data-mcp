"""Durable-record scheduler built on the Phase 1 source catalog contract.

The scheduler emits deterministic poll intentions and serializes state. It
does not perform acquisition, enqueue an external queue, or mutate runtime
infrastructure; a later worker lane consumes its records explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Mapping, Sequence, TypeAlias, cast

from shared.utils.cache import write_atomic_json
from shared.utils.source_cadence import (
    PollIntent,
    PollIntentReason,
    PollResult,
    PollState,
    SourceCatalogError,
    SourceRegistration,
    apply_probe_result,
    mark_missed,
    request_backfill,
    schedule_due,
    validate_registrations,
    validate_poll_state,
)


class SchedulerError(SourceCatalogError):
    """Raised when serialized scheduler state or an operator request is unsafe."""


JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
_MAX_INTENTS = 1024


@dataclass(frozen=True, slots=True)
class SchedulerSnapshot:
    """Serializable scheduler state suitable for a durable checkpoint."""

    state_id: str
    states: tuple[PollState, ...]
    intents: tuple[PollIntent, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": "hdp.scheduler-state.v1",
            "record_type": "scheduler_state",
            "state_id": self.state_id,
            "states": [state.as_dict() for state in self.states],
            "intents": [intent.as_dict() for intent in self.intents],
        }


class DurableScheduler:
    """Bounded scheduler with deterministic, restart-safe intent records."""

    def __init__(
        self,
        registrations: Sequence[SourceRegistration],
        states: Sequence[PollState],
        *,
        state_id: str = "scheduler:healthcare-data-platform",
        intents: Sequence[PollIntent] = (),
    ) -> None:
        if not state_id.startswith("scheduler:"):
            raise SchedulerError("state_id must use the scheduler namespace")
        registration_items = tuple(registrations)
        state_items = tuple(states)
        intent_items = tuple(intents)
        self._registrations = {registration.source_id: registration for registration in registration_items}
        validate_registrations(registration_items)
        self._states = {state.source_id: state for state in state_items}
        if len(self._states) != len(state_items):
            raise SchedulerError("duplicate poll state source_id")
        if set(self._states) - set(self._registrations):
            raise SchedulerError("poll state references an unregistered source")
        for state in self._states.values():
            try:
                validate_poll_state(state)
            except SourceCatalogError as exc:
                raise SchedulerError(str(exc)) from exc
        self._intents = {intent.intent_id: intent for intent in intent_items}
        if len(self._intents) != len(intent_items):
            raise SchedulerError("duplicate poll intent id")
        for intent in self._intents.values():
            registration = self._registrations.get(intent.source_id)
            if registration is None:
                raise SchedulerError(f"poll intent references an unregistered source: {intent.source_id}")
            if not registration.enabled or registration.rights_status != "approved_public":
                raise SchedulerError(f"poll intent references an ineligible source: {intent.source_id}")
            if intent.source_id not in self._states:
                raise SchedulerError(f"poll intent has no poll state: {intent.source_id}")
        if len(self._intents) > _MAX_INTENTS:
            raise SchedulerError(f"poll intent history exceeds {_MAX_INTENTS} records")
        self.state_id = state_id

    @property
    def states(self) -> Mapping[str, PollState]:
        """Read-only view of current source poll states."""

        return self._states

    @property
    def intents(self) -> Mapping[str, PollIntent]:
        """Read-only view of durable intent records."""

        return self._intents

    def snapshot(self) -> SchedulerSnapshot:
        """Return a deterministically ordered snapshot."""

        return SchedulerSnapshot(
            state_id=self.state_id,
            states=tuple(self._states[key] for key in sorted(self._states)),
            intents=tuple(self._intents[key] for key in sorted(self._intents)),
        )

    def checkpoint(self, path: str | Path) -> Path:
        """Atomically persist scheduler state for restart."""

        destination = Path(path)
        write_atomic_json(destination, self.snapshot().as_dict())
        return destination

    @classmethod
    def restore(
        cls,
        path: str | Path,
        registrations: Sequence[SourceRegistration],
    ) -> "DurableScheduler":
        """Restore a strict checkpoint without replaying or acquiring data."""

        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=_strict_pairs)
        except SchedulerError:
            raise
        except (OSError, json.JSONDecodeError) as exc:
            raise SchedulerError(f"unable to read scheduler checkpoint: {path}") from exc
        if not isinstance(raw, dict):
            raise SchedulerError("scheduler checkpoint must be an object")
        _validate_schema(raw, _state_schema_path(), "scheduler state")
        states_raw = raw.get("states")
        intents_raw = raw.get("intents")
        if not isinstance(states_raw, list) or not isinstance(intents_raw, list):
            raise SchedulerError("scheduler checkpoint states and intents must be arrays")
        states = tuple(PollState.from_mapping(_mapping(item, "poll state")) for item in states_raw)
        intents = tuple(_intent_from_mapping(_mapping(item, "poll intent")) for item in intents_raw)
        return cls(registrations, states, state_id=str(raw["state_id"]), intents=intents)

    def schedule(self, *, now: datetime) -> tuple[PollIntent, ...]:
        """Persist newly due intents; repeated calls are idempotent."""

        candidates = schedule_due(tuple(self._registrations.values()), self._states, now=now)
        fresh: list[PollIntent] = []
        for intent in candidates:
            if any(
                existing.source_id == intent.source_id and existing.generation == intent.generation
                for existing in self._intents.values()
            ):
                continue
            if intent.intent_id not in self._intents:
                self._intents[intent.intent_id] = intent
                self._trim_intents()
                fresh.append(intent)
        return tuple(fresh)

    def mark_missed(self, *, now: datetime) -> tuple[PollState, ...]:
        """Record all source runs beyond their configured grace windows."""

        changed: list[PollState] = []
        for source_id, state in sorted(self._states.items()):
            registration = self._registrations[source_id]
            if not registration.enabled or registration.rights_status != "approved_public":
                continue
            updated = mark_missed(state, now=now, cadence=registration.cadence)
            if updated != state:
                self._states[source_id] = updated
                changed.append(updated)
        return tuple(changed)

    def replay(
        self,
        source_id: str,
        *,
        from_release: str,
        to_release: str,
        now: datetime,
    ) -> PollIntent:
        """Create one bounded operator replay intent without starting work."""

        state = self._states.get(source_id)
        registration = self._registrations.get(source_id)
        if state is None or registration is None:
            raise SchedulerError(f"unknown source_id: {source_id}")
        if not registration.enabled or registration.rights_status != "approved_public":
            raise SchedulerError(f"source is not eligible for replay: {source_id}")
        pending = request_backfill(state, from_release=from_release, to_release=to_release)
        self._states[source_id] = pending
        scheduled = _utc(now)
        material = f"{source_id}|{from_release}|{to_release}|{pending.generation}".encode()
        from hashlib import sha256

        intent = PollIntent(
            intent_id="intent:poll:" + sha256(material).hexdigest()[:24],
            source_id=source_id,
            scheduled_at=scheduled,
            reason="operator_replay",
            generation=pending.generation,
        )
        existing = self._intents.get(intent.intent_id)
        if existing is not None and existing != intent:
            raise SchedulerError("operator replay intent collision")
        self._intents[intent.intent_id] = intent
        return intent

    def apply_result(
        self,
        source_id: str,
        result: str,
        *,
        attempted_at: datetime,
        next_due_at: datetime,
        release_id: str | None = None,
        release_fingerprint: str | None = None,
    ) -> PollState:
        """Apply a probe result to one source's durable state."""

        state = self._states.get(source_id)
        if state is None:
            raise SchedulerError(f"unknown source_id: {source_id}")
        registration = self._registrations[source_id]
        if not registration.enabled or registration.rights_status != "approved_public":
            raise SchedulerError(f"source is not eligible for result application: {source_id}")
        if result not in {"no_op", "changed", "failed_probe", "interrupted"}:
            raise SchedulerError(f"unsupported probe result: {result}")
        try:
            updated = apply_probe_result(
                state,
                cast(PollResult, result),
                attempted_at=attempted_at,
                next_due_at=next_due_at,
                release_id=release_id,
                release_fingerprint=release_fingerprint,
            )
        except SourceCatalogError as exc:
            raise SchedulerError(str(exc)) from exc
        self._states[source_id] = updated
        return updated

    def _trim_intents(self) -> None:
        """Retain a bounded recent history so checkpoints remain restorable."""

        while len(self._intents) > _MAX_INTENTS:
            oldest = min(self._intents.values(), key=lambda item: (item.scheduled_at, item.intent_id))
            del self._intents[oldest.intent_id]


def _state_schema_path() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "contracts/healthcare-data-platform/scheduler/v1/scheduler-state.schema.json"
    )


def _validate_schema(value: object, path: Path, label: str) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker

        schema = json.loads(path.read_text(encoding="utf-8"))
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, value)),
            key=lambda error: str(error.absolute_path),
        )
    except ImportError as exc:  # pragma: no cover - dependency installation failure
        raise SchedulerError("jsonschema is required for scheduler validation") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise SchedulerError(f"unable to read {label} schema") from exc
    if errors:
        error = errors[0]
        raise SchedulerError(f"{label} failed schema validation: {error.message}")


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SchedulerError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise SchedulerError(f"{label} must be an object")
    return value


def _intent_from_mapping(value: Mapping[str, object]) -> PollIntent:
    required = ("intent_id", "source_id", "scheduled_at", "reason", "generation")
    if any(key not in value for key in required):
        raise SchedulerError("poll intent is incomplete")
    try:
        scheduled = datetime.fromisoformat(str(value["scheduled_at"]).replace("Z", "+00:00"))
        raw_generation = value["generation"]
        if isinstance(raw_generation, bool) or not isinstance(raw_generation, (str, int)):
            raise SchedulerError("poll intent generation must be an integer")
        generation = int(raw_generation)
    except SchedulerError:
        raise
    except (TypeError, ValueError) as exc:
        raise SchedulerError("poll intent has invalid timestamp or generation") from exc
    if scheduled.tzinfo is None:
        raise SchedulerError("poll intent timestamp must include a timezone")
    reason = str(value["reason"])
    if reason not in {"cadence", "retry", "operator_replay", "backfill"}:
        raise SchedulerError("poll intent reason is unsupported")
    return PollIntent(
        str(value["intent_id"]),
        str(value["source_id"]),
        scheduled.astimezone(timezone.utc),
        cast(PollIntentReason, reason),
        generation,
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise SchedulerError("timestamp must include a timezone")
    return value.astimezone(timezone.utc)


__all__ = ["DurableScheduler", "SchedulerError", "SchedulerSnapshot"]
