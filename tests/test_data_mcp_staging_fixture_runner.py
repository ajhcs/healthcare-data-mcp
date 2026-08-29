"""Acceptance tests for the deterministic Data MCP staging one-shot."""

from __future__ import annotations

import json
import os
from pathlib import Path
import site
import subprocess
import sys
from typing import cast

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/run_data_mcp_staging_fixture.py"
MANIFEST = ROOT / "ops/staging/data-mcp-staging-fixture-manifest.json"
BASE_SHA = "63539f8412831ee62b067c76c3b5c395108481cc"
MANIFEST_VALUE = cast(dict[str, object], json.loads(MANIFEST.read_text(encoding="utf-8")))
CANDIDATE_SHA = cast(str, MANIFEST_VALUE["reviewed_candidate_sha"])


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
    root = tmp_path / "run"
    receipt = _run(root)
    checks = receipt["checks"]
    assert isinstance(checks, dict)
    assert checks
    assert all(isinstance(value, dict) and value.get("state") == "passed" for value in checks.values())

    assert receipt["candidate_sha"] == CANDIDATE_SHA
    assert receipt["base_sha"] == BASE_SHA
    assert receipt["runs"] == 3
    binding = receipt["manifest_binding"]
    assert isinstance(binding, dict)
    assert binding["reviewed_candidate_sha"] == CANDIDATE_SHA
    assert binding["artifact_count"] >= 10
    artifact_paths = {
        cast(dict[str, object], artifact)["path"]
        for artifact in cast(list[object], MANIFEST_VALUE["artifacts"])
        if isinstance(artifact, dict)
    }
    assert {
        "contracts/healthcare-data-platform/catalog/v1/fixtures/valid-source-catalog.json",
        "contracts/healthcare-data-platform/scheduler/v1/scheduler-state.schema.json",
        "contracts/healthcare-data-platform/queue/v1/queue.schema.json",
        "contracts/healthcare-data-platform/storage/v1/raw-artifact.schema.json",
        "shared/utils/source_cadence.py",
        "shared/utils/source_scheduler.py",
        "shared/queue/durable.py",
        "shared/storage/raw_custody.py",
    } <= artifact_paths
    queue = receipt["queue"]
    assert isinstance(queue, dict)
    assert queue["journal_mode"] == "wal"
    assert receipt["ordering"] == {
        "durable_ack_before_scheduler_checkpoint": True,
        "events": ["acknowledgement_checkpoint", "raw_custody", "scheduler_checkpoint"],
    }
    assert receipt["network"] == {
        "audit_events": [],
        "binds": [],
        "egress": "deny",
        "listener_ports": [],
        "source_probes": "disabled",
        "socketpair_sendmsg_guard": True,
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
    receipt_path = root / cast(str, receipt["receipt_path"])
    assert receipt_path.is_file()
    assert str(root) not in receipt_path.read_text(encoding="utf-8")
    assert not list((root / "control").glob(".*.tmp"))

    changed = cast(dict[str, object], cast(dict[str, object], receipt["custody"])["changed"])["artifact_id"]
    custody = cast(dict[str, object], receipt["custody"])
    assert custody["changed_artifact_absent_from_current"] is True
    projections = cast(dict[str, object], json.loads((root / "control/projections.json").read_text(encoding="utf-8")))
    current = cast(dict[str, object], projections["current"])
    assert current["artifact_id"] != changed
    as_of = cast(dict[str, object], projections["as_of"])
    assert cast(dict[str, object], as_of["release:ahrq.fixture.20260830"])["artifact_id"] == changed


def test_fixture_runner_rejects_candidate_mismatch_and_all_zero_before_root_creation(tmp_path: Path) -> None:
    environment = os.environ.copy()
    environment["PYTHONNOUSERSITE"] = "1"
    for candidate in (BASE_SHA, "0" * 40):
        root = tmp_path / candidate[:4]
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--candidate-sha", candidate, "--root", str(root)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        assert result.returncode == 4
        assert not root.exists()
        assert "candidate" in result.stderr


def test_fixture_runner_declares_all_resolution_and_socket_events_denied() -> None:
    from scripts import run_data_mcp_staging_fixture as runner

    assert {
        "socket.getaddrinfo",
        "socket.getnameinfo",
        "socket.gethostbyname",
        "socket.gethostbyname_ex",
        "socket.gethostbyaddr",
        "socket.getfqdn",
        "socket.sendmsg",
    } <= runner.BLOCKED_NETWORK_EVENTS


def test_fixture_runner_socketpair_sendmsg_probe_is_denied() -> None:
    from scripts import run_data_mcp_staging_fixture as runner

    assert runner._probe_socketpair() is True
    assert runner._NETWORK_ATTEMPTS == []


def test_shared_package_imports_do_not_create_home_cache(tmp_path: Path) -> None:
    home = tmp_path / "home"
    environment = os.environ.copy()
    environment["HOME"] = str(home)
    environment["PYTHONNOUSERSITE"] = "1"
    user_site = site.getusersitepackages()
    environment["PYTHONPATH"] = os.pathsep.join(part for part in (user_site, environment.get("PYTHONPATH", "")) if part)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import shared.storage.raw_custody; import shared.utils.source_cadence",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert result.returncode == 0, result.stderr
    assert not (home / ".healthcare-data-mcp" / "cache").exists()


def test_fixture_runner_enforces_stream_bytes_chunks_and_deadline() -> None:
    from scripts import run_data_mcp_staging_fixture as runner

    bounds = {
        "max_stream_bytes": 10,
        "max_stream_chunks": 2,
        "max_chunk_bytes": 5,
        "max_stream_seconds": 60,
    }
    assert runner._bounded_chunks(b"1234567890", bounds) == (b"12345", b"67890")
    with pytest.raises(runner.FixtureError, match="max_stream_bytes"):
        runner._bounded_chunks(b"12345678901", bounds)
    with pytest.raises(runner.FixtureError, match="max_stream_chunks"):
        runner._bounded_chunks(b"123456789", {**bounds, "max_stream_bytes": 100, "max_chunk_bytes": 4})


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
