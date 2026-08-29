"""Focused replay/backfill contract, durability, and operator-control tests."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from threading import Event
from typing import TypeAlias, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from shared.replay import (
    ReplayCollisionError,
    ReplayController,
    ReplayPlan,
    ReplayStateError,
    ReplayValidationError,
    build_replay_plan,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "contracts/healthcare-data-platform/replay/v1/replay.schema.json"
FIXTURE_PATH = ROOT / "contracts/healthcare-data-platform/replay/v1/fixtures/valid-replay-plan.json"
T0 = datetime(2026, 8, 29, 0, 0, tzinfo=timezone.utc)
RESULT_HASH = "a" * 64
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


def _plan(*, key: str | None = None, count: int = 3, item_bytes: int = 10):
    return build_replay_plan(
        "source:test",
        "version:001",
        f"version:{count:03d}",
        max_items=count,
        max_bytes=count * item_bytes,
        estimated_bytes=item_bytes,
        idempotency_key=key,
    )


def _result(item):
    return {"result_sha256": RESULT_HASH, "result_ref": f"result/{item.version}", "result_bytes": 1}


def test_range_builder_is_deterministic_and_inclusive() -> None:
    first = _plan(count=3)
    second = _plan(count=3)

    assert first == second
    assert [item.version for item in first.items] == ["version:001", "version:002", "version:003"]
    assert first.plan_id == "replay:plan:" + first.plan_sha256.removeprefix("sha256:")


def test_date_range_expands_inclusively() -> None:
    plan = build_replay_plan(
        "source:test",
        "release:test:2026-08-29",
        "release:test:2026-08-31",
        max_items=3,
        max_bytes=3,
    )

    assert [item.version for item in plan.items] == [
        "release:test:2026-08-29",
        "release:test:2026-08-30",
        "release:test:2026-08-31",
    ]


def test_opaque_range_requires_explicit_versions() -> None:
    with pytest.raises(ReplayValidationError, match="explicit versions"):
        build_replay_plan("source:test", "version:alpha", "version:omega")


def test_submit_is_idempotent_and_conflicting_key_is_fail_closed(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(key="replay:operator:request-1", count=2)
    conflict = _plan(key="replay:operator:request-1", count=3)
    with ReplayController(database, clock=lambda: T0) as controller:
        first = controller.submit(plan)
        duplicate = controller.submit(plan)

        assert first.action == "created"
        assert duplicate.action == "duplicate"
        assert duplicate.plan == first.plan
        with pytest.raises(ReplayCollisionError):
            controller.submit(conflict)
        assert len(controller.list_plans()) == 1


def test_dry_run_is_read_only_and_classifies_existing_work(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(count=3)
    with ReplayController(database, clock=lambda: T0) as controller:
        before = sqlite3.connect(database).execute("SELECT COUNT(*) FROM replay_plans").fetchone()[0]
        diff_before = controller.dry_run(plan)
        after = sqlite3.connect(database).execute("SELECT COUNT(*) FROM replay_plans").fetchone()[0]
        assert before == after == 0
        assert len(diff_before.new_items) == 3

        controller.submit(plan)
        controller.execute(plan.plan_id, _result, max_items=1)
        diff_after = controller.dry_run(plan)
        assert len(diff_after.already_completed) == 1
        assert len(diff_after.new_items) == 2
        assert diff_after.in_progress == ()


def test_dry_run_reports_idempotency_conflict_without_mutating_state(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(key="replay:operator:request-2", count=2)
    conflict = _plan(key="replay:operator:request-2", count=3)
    with ReplayController(database, clock=lambda: T0) as controller:
        controller.submit(plan)
        before = controller.get_plan(plan.plan_id)
        diff = controller.dry_run(conflict)
        after = controller.get_plan(plan.plan_id)

    assert diff.conflicts == tuple(item.item_id for item in conflict.items)
    assert before == after


def test_checkpoint_resume_after_restart_never_replays_completed_item(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(count=3)
    seen: list[str] = []
    with ReplayController(database, clock=lambda: T0) as controller:
        controller.submit(plan)
        first = controller.execute(plan.plan_id, lambda item: seen.append(item.version) or _result(item), max_items=1)
        assert first.completed_items == 1
        assert first.state == "running"
    with ReplayController(database, clock=lambda: T0) as restarted:
        second = restarted.resume(plan.plan_id, lambda item: seen.append(item.version) or _result(item))
        checkpoint = restarted.get_checkpoint(plan.plan_id)

    assert second.state == "completed"
    assert seen == ["version:001", "version:002", "version:003"]
    assert checkpoint.next_ordinal == 3


def test_cancellation_is_bounded_to_item_boundary_and_resume_is_explicit(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(count=3)
    stop = Event()
    seen: list[str] = []

    def runner(item):
        seen.append(item.version)
        stop.set()
        return _result(item)

    with ReplayController(database, clock=lambda: T0) as controller:
        controller.submit(plan)
        cancelled = controller.execute(plan.plan_id, runner, cancel=stop)
        assert cancelled.state == "cancelled"
        assert seen == ["version:001"]
        with pytest.raises(ReplayStateError):
            controller.execute(plan.plan_id, _result)
        resumed = controller.resume(plan.plan_id, _result)

    assert resumed.state == "completed"
    assert seen == ["version:001"]


def test_cancel_called_before_execution_does_not_claim_work(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(count=2)
    with ReplayController(database, clock=lambda: T0) as controller:
        controller.submit(plan)
        controller.cancel(plan.plan_id, reason="operator requested stop")
        receipt = controller.resume(plan.plan_id, _result, max_items=1)
        assert receipt.completed_items == 1
        assert receipt.state == "running"


def test_invalid_runner_result_fails_without_advancing_checkpoint(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(count=2)
    with ReplayController(database, clock=lambda: T0) as controller:
        controller.submit(plan)
        receipt = controller.execute(plan.plan_id, lambda _item: {"result_ref": "missing-hash"})
        assert receipt.state == "failed"
        assert receipt.completed_items == 0
        assert controller.get_checkpoint(plan.plan_id).next_ordinal == 0
        assert controller.get_plan(plan.plan_id).items[0].state == "failed"


def test_runner_exception_is_bounded_and_secret_free(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(count=1)
    with ReplayController(database, clock=lambda: T0) as controller:
        controller.submit(plan)

        def runner(_item):
            raise RuntimeError("Authorization: Bearer super-secret-token")

        receipt = controller.execute(plan.plan_id, runner)
        assert receipt.state == "failed"
        stored = controller.get_plan(plan.plan_id).items[0].error
        assert stored is not None
        assert "super-secret-token" not in stored
        assert "Bearer" not in stored


def test_execution_byte_and_item_limits_are_bounded(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    plan = _plan(count=3, item_bytes=10)
    with ReplayController(database, clock=lambda: T0) as controller:
        controller.submit(plan)
        receipt = controller.execute(plan.plan_id, _result, max_items=2, max_bytes=10)

    assert receipt.attempted_items == 1
    assert receipt.completed_items == 1
    assert receipt.remaining_items == 2


def test_plan_and_fixture_validate_against_schema() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert list(validator.iter_errors(cast(JsonValue, fixture))) == []
    assert list(validator.iter_errors(cast(JsonValue, _plan(count=2, item_bytes=1).as_dict()))) == []


def test_plan_mapping_rejects_unknown_fields_and_hash_drift() -> None:
    payload = _plan(count=2).as_dict()
    payload["unexpected"] = True
    with pytest.raises(ReplayValidationError, match="schema validation"):
        ReplayPlan.from_mapping(payload)

    payload = _plan(count=2).as_dict()
    payload["plan_sha256"] = "sha256:" + "b" * 64
    with pytest.raises(ReplayValidationError, match="plan_sha256"):
        ReplayPlan.from_mapping(payload)


def test_database_does_not_have_payload_columns(tmp_path: Path) -> None:
    database = tmp_path / "replay.sqlite"
    with ReplayController(database) as controller:
        controller.submit(_plan(count=1))
        columns = {
            row[1]
            for row in controller._connection.execute("PRAGMA table_info(replay_items)")  # pyright: ignore[reportPrivateUsage]
        }

    assert "payload" not in columns
    assert "result_sha256" in columns


def test_connection_can_be_caller_owned() -> None:
    connection = sqlite3.connect(":memory:")
    controller = ReplayController(connection)
    controller.submit(_plan(count=1))
    controller.close()
    assert connection.execute("SELECT COUNT(*) FROM replay_plans").fetchone()[0] == 1
    connection.close()
