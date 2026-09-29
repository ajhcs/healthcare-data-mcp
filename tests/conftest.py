"""Keep irreversible staging-fixture audit hooks outside the shared test process."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest


# These immutable, manifest-pinned tests intentionally import a runner that
# denies networking process-wide. Run their unchanged assertions in children;
# the application guard and its provenance manifest remain byte-identical.
_GUARDED_NODE_IDS = frozenset({
    "tests/test_data_mcp_staging_fixture_runner.py::test_fixture_runner_declares_all_resolution_and_socket_events_denied",
    "tests/test_data_mcp_staging_fixture_runner.py::test_fixture_runner_socketpair_sendmsg_probe_is_denied",
    "tests/test_data_mcp_staging_fixture_runner.py::test_fixture_runner_enforces_stream_bytes_chunks_and_deadline",
})
_CHILD_NODE_ENV = "HDP_STAGING_FIXTURE_TEST_CHILD"
_REPO_ROOT = Path(__file__).resolve().parents[1]


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    """Dispatch the three process-guard tests without contaminating later tests."""
    node_id = pyfuncitem.nodeid
    if node_id not in _GUARDED_NODE_IDS or os.environ.get(_CHILD_NODE_ENV) == node_id:
        return None
    environment = {**os.environ, _CHILD_NODE_ENV: node_id}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", node_id, "-q", "-p", "no:cacheprovider"],
        cwd=_REPO_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        pytest.fail("Isolated staging guard test failed:\n" + result.stdout + result.stderr, pytrace=False)
    return True
