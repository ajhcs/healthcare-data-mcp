"""Durable scheduler contract, restart, and operator-control tests."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from shared.utils.source_cadence import PollState, load_catalog
from shared.utils.source_scheduler import DurableScheduler, SchedulerError


ROOT = Path(__file__).resolve().parents[1]
CATALOG_FIXTURE = ROOT / "contracts/healthcare-data-platform/catalog/v1/fixtures/valid-source-catalog.json"
SCHEDULER_SCHEMA = ROOT / "contracts/healthcare-data-platform/scheduler/v1/scheduler-state.schema.json"
SCHEDULER_FIXTURE = ROOT / "contracts/healthcare-data-platform/scheduler/v1/fixtures/valid-scheduler-state.json"


def _utc(hour: int) -> datetime:
    return datetime(2026, 8, 29, hour, 0, tzinfo=timezone.utc)


def _scheduler() -> DurableScheduler:
    registrations = load_catalog(CATALOG_FIXTURE)
    states = tuple(
        PollState.initial(registration.source_id, next_due_at=_utc(0))
        for registration in registrations
    )
    return DurableScheduler(registrations, states)


def test_schedule_due_is_idempotent_and_respects_catalog_rights() -> None:
    scheduler = _scheduler()

    first = scheduler.schedule(now=_utc(1))
    second = scheduler.schedule(now=_utc(1))

    assert [intent.source_id for intent in first] == ["source:ahrq:lighthouse", "source:cms:pdc"]
    assert all(intent.reason == "cadence" for intent in first)
    assert second == ()
    assert all("fast" not in intent.source_id for intent in scheduler.intents.values())


def test_checkpoint_and_restore_preserve_exact_snapshot(tmp_path: Path) -> None:
    scheduler = _scheduler()
    scheduler.schedule(now=_utc(1))
    path = scheduler.checkpoint(tmp_path / "scheduler-state.json")

    restored = DurableScheduler.restore(path, load_catalog(CATALOG_FIXTURE))

    assert restored.snapshot() == scheduler.snapshot()
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == "hdp.scheduler-state.v1"


def test_fixture_validates_against_scheduler_schema() -> None:
    from jsonschema import Draft202012Validator, FormatChecker

    schema = json.loads(SCHEDULER_SCHEMA.read_text(encoding="utf-8"))
    fixture = json.loads(SCHEDULER_FIXTURE.read_text(encoding="utf-8"))

    assert list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(fixture)) == []


def test_mark_missed_emits_explicit_state_and_retry_intent() -> None:
    scheduler = _scheduler()

    late = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)
    changed = scheduler.mark_missed(now=late)
    intents = scheduler.schedule(now=late)

    assert {state.source_id for state in changed} == {"source:ahrq:lighthouse", "source:cms:pdc"}
    assert all(state.state == "missed" for state in changed)
    assert all(intent.reason == "retry" for intent in intents)


def test_operator_replay_is_bounded_and_not_duplicated_by_schedule() -> None:
    scheduler = _scheduler()

    intent = scheduler.replay(
        "source:ahrq:lighthouse",
        from_release="release:ahrq:2026-08-01",
        to_release="release:ahrq:2026-08-22",
        now=_utc(2),
    )

    assert intent.reason == "operator_replay"
    assert scheduler.states["source:ahrq:lighthouse"].state == "backfill_pending"
    assert [item.source_id for item in scheduler.schedule(now=_utc(2))] == ["source:cms:pdc"]
    assert scheduler.intents[intent.intent_id] == intent


def test_replay_rejects_unapproved_source() -> None:
    scheduler = _scheduler()

    with pytest.raises(SchedulerError, match="not eligible"):
        scheduler.replay(
            "source:fast:identity-snapshot",
            from_release="release:fast:old",
            to_release="release:fast:new",
            now=_utc(2),
        )


def test_apply_result_preserves_prior_release_on_failure() -> None:
    scheduler = _scheduler()
    scheduler.apply_result(
        "source:ahrq:lighthouse",
        "changed",
        attempted_at=_utc(1),
        next_due_at=_utc(2),
        release_id="release:ahrq:2026-08-22",
        release_fingerprint="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )

    failed = scheduler.apply_result(
        "source:ahrq:lighthouse",
        "failed_probe",
        attempted_at=_utc(2),
        next_due_at=_utc(3),
    )

    assert failed.state == "failed"
    assert failed.consecutive_failures == 1
    assert failed.last_release_id == "release:ahrq:2026-08-22"


def test_invalid_checkpoint_is_rejected_before_restore(tmp_path: Path) -> None:
    scheduler = _scheduler()
    path = scheduler.checkpoint(tmp_path / "scheduler-state.json")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["unexpected"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(SchedulerError, match="schema validation"):
        DurableScheduler.restore(path, load_catalog(CATALOG_FIXTURE))


def test_duplicate_checkpoint_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"schema_version":"hdp.scheduler-state.v1","schema_version":"bad"}', encoding="utf-8")

    with pytest.raises(SchedulerError, match="duplicate JSON key"):
        DurableScheduler.restore(path, load_catalog(CATALOG_FIXTURE))


def test_apply_result_rejects_unknown_result() -> None:
    scheduler = _scheduler()

    with pytest.raises(SchedulerError, match="unsupported probe result"):
        scheduler.apply_result("source:ahrq:lighthouse", "unknown", attempted_at=_utc(1), next_due_at=_utc(2))


def test_changed_result_requires_release_identity() -> None:
    scheduler = _scheduler()
    with pytest.raises(SchedulerError, match="changed result requires"):
        scheduler.apply_result(
            "source:ahrq:lighthouse",
            "changed",
            attempted_at=_utc(1),
            next_due_at=_utc(2),
        )
