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
    assert receipt["source_commit"] == "ce43147c30793a4dbb35ed97649998a3a5426441"
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
