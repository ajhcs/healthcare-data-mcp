"""Contract and fixture tests for the approval-gated P1-33 receipt."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "contracts/healthcare-data-platform/staging/v1/staging-activation-receipt.schema.json"
FIXTURE = ROOT / "contracts/healthcare-data-platform/staging/v1/fixtures/pending-approval.json"


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_pending_receipt_validates_and_is_bound_to_bundle_base() -> None:
    schema = _load(SCHEMA)
    receipt = _load(FIXTURE)
    errors = sorted(Draft202012Validator(schema).iter_errors(receipt), key=lambda error: error.path)
    assert errors == []
    assert receipt["source_commit"] == "f477e126cc09d5a8ce7edc7ffcd4833cb484f766"
    assert receipt["state"] == "pending_approval"
    assert receipt["activation_performed"] is False


def test_every_activation_outcome_is_approval_gated_and_unexecuted() -> None:
    receipt = _load(FIXTURE)
    outcomes = receipt["outcomes"]
    assert set(outcomes) == {"restart", "no_op", "change", "custody", "lifecycle", "telemetry", "rollback"}
    for outcome in outcomes.values():
        assert outcome["state"] == "pending_approval"
        assert outcome["approval_required"] is True
        assert outcome["mutation_performed"] is False


def test_pending_receipt_records_non_mutating_prohibitions() -> None:
    prohibitions = _load(FIXTURE)["prohibitions"]
    assert all(value is False for value in prohibitions.values())


def test_blocked_verification_records_preflight_and_caller_owned_fixture_paths() -> None:
    receipt = _load(FIXTURE)
    verification = receipt["verification"]
    assert verification["state"] == "blocked_host_custody"
    assert verification["status"] == "pending"
    assert verification["execution"] == "not_run"
    assert verification["activation_claim"] == "not_activated"
    assert verification["source_commit"] == "f477e126cc09d5a8ce7edc7ffcd4833cb484f766"

    preflight = verification["port_preflight"]
    assert preflight["command"] == "ss -H -ltnp '( sport = :8110 or sport = :3020 )'"
    assert {entry["port"] for entry in preflight["ports"]} == {8110, 3020}
    assert all(entry["state"] == "free" and entry["listeners"] == [] for entry in preflight["ports"])

    root = verification["temporary_root"]
    assert root == {
        "ownership": "caller_owned_tmp_path",
        "path_template": "{tmp_path}",
        "provided_by_caller": True,
        "source_bytes_written": False,
    }
    artifacts = verification["artifacts"]
    assert artifacts["scheduler"]["path_template"] == "{tmp_path}/control/scheduler.json"
    assert artifacts["queue"]["path_template"] == "{tmp_path}/control/control.sqlite3"
    assert artifacts["raw_custody"]["path_template"] == "{tmp_path}/raw-custody"
    assert all(item["evidence_state"] == "pending" for item in artifacts.values())
    assert all(item["content_present"] is False for item in artifacts.values())

    custody = verification["host_custody"]
    assert custody["state"] == "blocked_host_custody"
    assert custody["staging_root_present"] is False
    assert custody["parent_custody_verified"] is False
    assert custody["mutations_performed"] is False


def test_blocked_verification_rejects_healthy_or_bound_port_claims() -> None:
    schema = _load(SCHEMA)
    receipt = _load(FIXTURE)

    healthy = json.loads(json.dumps(receipt))
    healthy["verification"]["activation_claim"] = "healthy"
    assert list(Draft202012Validator(schema).iter_errors(healthy))

    bound = json.loads(json.dumps(receipt))
    bound["verification"]["port_preflight"]["ports"][0]["state"] = "bound"
    assert list(Draft202012Validator(schema).iter_errors(bound))
