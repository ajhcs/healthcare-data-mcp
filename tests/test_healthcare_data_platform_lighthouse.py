"""Fixture/schema gates for the bounded Phase 1 lighthouse spike."""

from __future__ import annotations

import json
import importlib.util
from pathlib import Path
import sys
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/lighthouse/v1/fixtures"
SCHEMA_PATH = ROOT / "contracts/healthcare-data-platform/lighthouse/v1/lighthouse.schema.json"
ENVELOPE_SCHEMA_PATH = ROOT / "contracts/healthcare-data-platform/lighthouse/v1/lighthouse-envelope.schema.json"
RUNTIME_PATH = ROOT / "contracts/healthcare-data-platform/lighthouse/v1/lighthouse_runtime.py"


def _runtime() -> Any:
    spec = importlib.util.spec_from_file_location("lighthouse_runtime", RUNTIME_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load lighthouse runtime")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _validate(value: object) -> list[str]:
    from jsonschema import Draft202012Validator, FormatChecker

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return [error.message for error in Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value)]


@pytest.mark.parametrize(
    "name",
    ["official-source-noop.json", "official-source-changed.json", "official-source-failed-probe.json"],
)
def test_official_source_fixtures_are_schema_valid(name: str) -> None:
    assert _validate(_load(name)) == []


def test_unbounded_budget_fixture_is_rejected() -> None:
    assert _validate(_load("invalid-unbounded-budget.json"))


def test_fixture_families_are_explicit_and_bounded() -> None:
    changed = _load("official-source-changed.json")
    assert changed["probe_state"] == "changed"
    assert changed["budget"]["max_bytes"] <= 131072
    assert changed["budget"]["max_chunks"] <= 128
    assert changed["budget"]["max_seconds"] <= 60


def test_changed_fixture_streams_once_and_emits_contract_envelope() -> None:
    runtime = _runtime()
    fixture = runtime.load_fixture(FIXTURE_ROOT / "official-source-changed.json")
    receiver = runtime.DisposableReceiver()
    receipt = runtime.LighthouseSpike(receiver).run(fixture)

    assert receipt.state == "changed"
    assert receipt.acknowledged is True
    assert receipt.received_bytes == fixture["artifact"]["byte_length"]
    assert receipt.chunk_count == 3
    assert receipt.idempotency_key in receiver.deliveries
    assert len(receiver.artifacts) == 1


def test_noop_is_a_cheap_receipt_without_delivery() -> None:
    runtime = _runtime()
    fixture = runtime.load_fixture(FIXTURE_ROOT / "official-source-noop.json")
    receiver = runtime.DisposableReceiver()
    receipt = runtime.LighthouseSpike(receiver).run(fixture)

    assert receipt.state == "no_op"
    assert receipt.acknowledged is False
    assert receipt.received_bytes == 0
    assert receiver.deliveries == {}


def test_interruption_does_not_ack_and_retry_is_idempotent() -> None:
    runtime = _runtime()
    fixture = runtime.load_fixture(FIXTURE_ROOT / "official-source-changed.json")
    receiver = runtime.DisposableReceiver()
    spike = runtime.LighthouseSpike(receiver)

    interrupted = spike.run(fixture, interrupt_after_chunks=1)
    retried = spike.run(fixture)
    duplicate = spike.run(fixture)

    assert interrupted.state == "interrupted"
    assert interrupted.acknowledged is False
    assert retried.state == "changed"
    assert retried.acknowledged is True
    assert duplicate.state == "duplicate"
    assert duplicate.acknowledged is True
    assert len(receiver.deliveries) == 1
    assert len(receiver.artifacts) == 1


def test_failed_probe_preserves_prior_state() -> None:
    runtime = _runtime()
    fixture = runtime.load_fixture(FIXTURE_ROOT / "official-source-failed-probe.json")
    receiver = runtime.DisposableReceiver()
    receipt = runtime.LighthouseSpike(receiver).run(fixture)

    assert receipt.state == "failed_probe"
    assert receipt.acknowledged is False
    assert receipt.failure_reason
    assert receiver.deliveries == {}
    assert receiver.artifacts == {}


def test_same_release_is_noop_even_when_changed_fixture_is_replayed() -> None:
    runtime = _runtime()
    fixture = runtime.load_fixture(FIXTURE_ROOT / "official-source-changed.json")
    receipt = runtime.LighthouseSpike().run(
        fixture,
        prior_release_fingerprint=fixture["release_fingerprint"],
    )

    assert receipt.state == "no_op"
    assert receipt.acknowledged is False


def test_envelope_schema_is_strict_and_versioned() -> None:
    runtime = _runtime()
    fixture = runtime.load_fixture(FIXTURE_ROOT / "official-source-changed.json")
    receiver = runtime.DisposableReceiver()
    receipt = runtime.LighthouseSpike(receiver).run(fixture)
    envelope = receiver.deliveries[receipt.idempotency_key].envelope

    assert envelope["schema_version"] == "hdp.lighthouse-envelope.v1"
    assert runtime._schema_validate(envelope, ENVELOPE_SCHEMA_PATH, "lighthouse envelope") is None
    envelope["unexpected"] = "reject"
    with pytest.raises(runtime.LighthouseContractError, match="failed schema validation"):
        runtime._schema_validate(envelope, ENVELOPE_SCHEMA_PATH, "lighthouse envelope")
