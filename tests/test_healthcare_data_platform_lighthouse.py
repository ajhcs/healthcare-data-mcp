"""Fixture/schema gates for the bounded Phase 1 lighthouse spike."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/lighthouse/v1/fixtures"
SCHEMA_PATH = ROOT / "contracts/healthcare-data-platform/lighthouse/v1/lighthouse.schema.json"


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
