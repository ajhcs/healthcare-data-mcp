"""Acceptance tests for the deterministic Data MCP staging one-shot."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/run_data_mcp_staging_fixture.py"
CANDIDATE_SHA = "63539f8412831ee62b067c76c3b5c395108481cc"


def _run(root: Path) -> dict[str, object]:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--candidate-sha", CANDIDATE_SHA, "--root", str(root)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def test_fixture_runner_proves_all_required_boundaries(tmp_path: Path) -> None:
    receipt = _run(tmp_path / "run")
    checks = receipt["checks"]
    assert isinstance(checks, dict)
    assert checks
    assert all(isinstance(value, dict) and value.get("state") == "passed" for value in checks.values())

    assert receipt["candidate_sha"] == CANDIDATE_SHA
    assert receipt["base_sha"] == CANDIDATE_SHA
    assert receipt["runs"] == 3
    assert receipt["network"] == {
        "audit_events": [],
        "binds": [],
        "egress": "deny",
        "listener_ports": [],
        "source_probes": "disabled",
    }
    assert receipt["prohibitions"] == {
        "credentials_resolved": False,
        "listeners_started": False,
        "migrations_applied": False,
        "production_state_written": False,
        "source_egress": False,
    }

    paths = receipt["paths"]
    assert isinstance(paths, dict)
    assert all(isinstance(value, str) and not Path(value).is_absolute() for value in paths.values())
    receipt_path = tmp_path / "run" / cast(str, receipt["receipt_path"])
    assert receipt_path.is_file()
    assert str(tmp_path / "run") not in receipt_path.read_text(encoding="utf-8")


def test_fixture_runner_is_byte_deterministic_across_isolated_roots(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first = _run(first_root)
    second = _run(second_root)
    assert first == second
    first_bytes = (first_root / "control/staging-fixture-receipt.json").read_bytes()
    second_bytes = (second_root / "control/staging-fixture-receipt.json").read_bytes()
    assert first_bytes == second_bytes


def test_fixture_runner_rejects_protected_host_custody_root(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--candidate-sha", CANDIDATE_SHA, "--root", "/mnt/d/services"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 4
    assert "protected production or host-custody path" in result.stderr
    assert not (tmp_path / "control").exists()
