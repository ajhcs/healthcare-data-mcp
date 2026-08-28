"""Catalog, cadence, and poll-state contract tests for Phase 1."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from shared.utils.source_cadence import (
    PollState,
    SourceCatalogError,
    SourceRegistration,
    apply_probe_result,
    load_catalog,
    mark_missed,
    request_backfill,
    schedule_due,
    validate_poll_state,
    validate_registrations,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/catalog/v1/fixtures"


def _utc(hour: int) -> datetime:
    return datetime(2026, 8, 29, hour, 0, tzinfo=timezone.utc)


def _state(source_id: str = "source:ahrq:lighthouse") -> PollState:
    return PollState.initial(source_id, next_due_at=_utc(0))


def test_catalog_fixture_loads_stable_source_ids_and_change_modes() -> None:
    registrations = load_catalog(FIXTURE_ROOT / "valid-source-catalog.json")

    assert [item.source_id for item in registrations] == [
        "source:ahrq:lighthouse",
        "source:cms:pdc",
        "source:fast:identity-snapshot",
    ]
    assert registrations[0].change_mode == "release_metadata"
    assert registrations[1].cadence.interval_seconds == 86400
    assert registrations[2].enabled is False


def test_poll_state_fixture_validates_explicit_noop_and_backfill_states() -> None:
    values = json.loads((FIXTURE_ROOT / "valid-poll-states.json").read_text(encoding="utf-8"))

    states = tuple(PollState.from_mapping(value) for value in values)
    for state in states:
        validate_poll_state(state)

    assert states[0].state == "no_op"
    assert states[1].state == "backfill_pending"
    assert states[1].backfill_from == "release:cms:pdc:2026-07-01"


def test_schedule_due_is_deterministic_and_skips_unapproved_optional_sources() -> None:
    registrations = load_catalog(FIXTURE_ROOT / "valid-source-catalog.json")
    states = {
        "source:ahrq:lighthouse": _state(),
        "source:cms:pdc": PollState.initial("source:cms:pdc", next_due_at=_utc(0)),
    }

    first = schedule_due(registrations, states, now=_utc(1))
    second = schedule_due(registrations, states, now=_utc(1))

    assert first == second
    assert [item.source_id for item in first] == ["source:ahrq:lighthouse", "source:cms:pdc"]
    assert all(item.reason == "cadence" for item in first)
    assert all("fast" not in item.source_id for item in first)


def test_probe_transitions_preserve_release_on_noop_and_failure() -> None:
    state = _state()
    changed = apply_probe_result(
        state,
        "changed",
        attempted_at=_utc(1),
        next_due_at=_utc(2),
        release_id="release:ahrq:2026-08-22",
        release_fingerprint="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    no_op = apply_probe_result(changed, "no_op", attempted_at=_utc(2), next_due_at=_utc(3))
    failed = apply_probe_result(no_op, "failed_probe", attempted_at=_utc(3), next_due_at=_utc(4))

    assert changed.state == "succeeded"
    assert no_op.state == "no_op"
    assert no_op.last_release_id == changed.last_release_id
    assert failed.state == "failed"
    assert failed.consecutive_failures == 1
    assert failed.last_release_fingerprint == changed.last_release_fingerprint
    assert failed.last_success_at == no_op.last_success_at


def test_missed_and_backfill_states_are_explicit_and_schedulable() -> None:
    registration = SourceRegistration.from_mapping(
        {
            "source_id": "source:ahrq:lighthouse",
            "title": "AHRQ",
            "family": "system",
            "source_url": "https://example.gov/source",
            "change_mode": "release_metadata",
            "cadence": {"interval_seconds": 3600, "jitter_seconds": 0, "missed_run_grace_seconds": 60},
            "release_locator": "https://example.gov/releases",
            "rights_status": "approved_public",
            "enabled": True,
            "owner": "data-platform",
        }
    )
    missed = mark_missed(_state(), now=_utc(2), cadence=registration.cadence)
    backfill = request_backfill(missed, from_release="release:ahrq:old", to_release="release:ahrq:new")

    assert missed.state == "missed"
    assert backfill.state == "backfill_pending"
    assert backfill.backfill_from == "release:ahrq:old"
    intent = schedule_due((registration,), {registration.source_id: backfill}, now=_utc(3))[0]
    assert intent.reason == "backfill"


def test_invalid_catalog_and_rights_boundaries_fail_closed() -> None:
    with pytest.raises(SourceCatalogError, match="schema validation"):
        load_catalog(FIXTURE_ROOT / "invalid-source-catalog.json")

    pending = SourceRegistration.from_mapping(
        {
            "source_id": "source:optional:pending",
            "title": "Pending",
            "family": "optional",
            "source_url": "https://example.gov/source",
            "change_mode": "content_hash",
            "cadence": {"interval_seconds": 3600, "jitter_seconds": 0, "missed_run_grace_seconds": 60},
            "release_locator": "https://example.gov/releases",
            "rights_status": "pending_review",
            "enabled": True,
            "owner": "identity-lane",
        }
    )
    with pytest.raises(SourceCatalogError, match="approved public rights"):
        validate_registrations((pending,))


def test_duplicate_catalog_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"schema_version":"hdp.source-catalog.v1","schema_version":"bad"}', encoding="utf-8")

    with pytest.raises(SourceCatalogError, match="duplicate JSON key"):
        load_catalog(path)
