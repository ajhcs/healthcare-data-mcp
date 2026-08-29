"""Static conformance checks for the reproducible fixture dependency lock."""

from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "requirements/staging-fixture.lock"
EXPECTED = {
    "attrs": "26.1.0",
    "duckdb": "1.4.4",
    "jsonschema": "4.26.0",
    "jsonschema-specifications": "2025.9.1",
    "numpy": "2.4.4",
    "pandas": "3.0.2",
    "pyyaml": "6.0.3",
    "python-dateutil": "2.9.0.post0",
    "referencing": "0.37.0",
    "rpds-py": "0.30.0",
    "six": "1.17.0",
    "typing-extensions": "4.15.0",
}


def test_staging_fixture_lock_is_fully_pinned_and_scope_is_explicit() -> None:
    lines = LOCK.read_text(encoding="utf-8").splitlines()
    entries: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9_.-]*)==([0-9][A-Za-z0-9.+!-]*)", stripped)
        assert match is not None, f"lock entry is not an exact pin: {stripped}"
        name, version = match.groups()
        entries[name.lower()] = version
    assert entries == EXPECTED
    assert "one-shot fixture control plane" in LOCK.read_text(encoding="utf-8")
