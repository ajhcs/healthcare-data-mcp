# P1-33 R3 deterministic fixture correction receipt v2

This additive receipt supersedes neither the original R3 candidate note nor
the prior correction receipt. It records the final bounded clean-bootstrap,
manifest-closure, and network-guard corrections. The artifact remains an
offline, prepared-not-applied candidate; it is not source-native Phase 1
runtime evidence, a staging activation, or a promotion decision.

## Exact release

| Field | Value |
| --- | --- |
| Repository | `healthcare-data-mcp` |
| Worktree | `/tmp/healthcare-data-mcp-r3-fixture-20260829-v2` |
| Branch | `codex/r3-data-mcp-fixture-20260829-v2` |
| Baseline main | `63539f8412831ee62b067c76c3b5c395108481cc` |
| Correction base | `0e4cf50d88be0c190694f74b2b822521e2b9e10d` |
| Reviewed implementation candidate | `2f2dca2f3e22238942dee14c808a13f6024c77d5` |
| Manifest rebind commit | `2f8fe84` |
| Manifest | `ops/staging/data-mcp-staging-fixture-manifest.json` |

The new focused commits are:

```text
76f8007 fix(staging): harden clean fixture bootstrap
2f2dca2 test(staging): format fixture bootstrap coverage
2f8fe84 docs(staging): rebind fixture closure manifest
```

The receipt-document commit is the final branch head reported with this
handoff. It does not change the reviewed candidate SHA. No remote operation,
reset, rebase, or production-main modification occurred.

## Lock and clean-bootstrap proof

`requirements/staging-fixture.lock` now pins every package imported by the
runner's package-initialization and runtime seams:

```text
attrs==26.1.0
duckdb==1.4.4
jsonschema==4.26.0
jsonschema-specifications==2025.9.1
numpy==2.4.4
pandas==3.0.2
PyYAML==6.0.3
referencing==0.37.0
rpds-py==0.30.0
python-dateutil==2.9.0.post0
six==1.17.0
typing-extensions==4.15.0
```

The exact runner was executed from the reviewed checkout with user-site
discovery disabled and only the already available dependency site made
explicit:

```bash
PYTHONNOUSERSITE=1 \
PYTHONPATH=/home/plumbob/.local/lib/python3.12/site-packages \
python3 scripts/run_data_mcp_staging_fixture.py \
  --candidate-sha 2f2dca2f3e22238942dee14c808a13f6024c77d5 \
  --root /tmp/hdp-r3-clean-bootstrap-NyhDAe
```

The run exited `0` and wrote
`control/staging-fixture-receipt.json` with SHA-256
`sha256:280c0dc3d5c9d8ba42b0fa0b31fa9cf7e9dca26f85d8d7dfac3d098628b0dcff`.
The focused lock test verifies exact pins and rejects ranges or undeclared
packages. A fresh wheel installation could not be completed in this
environment: the isolated `uv` install was attempted both offline and online,
but the required package artifact/index boundary was unavailable (`attrs` was
not present in the offline cache and DNS resolution for `pypi.org` failed).
No clean-install success is claimed from that blocked attempt; the lock is
ready for an approved index or verified wheelhouse.

## Bootstrap and manifest corrections

- The runner installs its audit hook using only standard-library imports, then
  verifies the requested candidate SHA, manifest binding, Git object, ancestor
  relationship, and all artifact hashes before importing repository packages
  or creating the caller-owned root.
- The 28-entry manifest closes the executable and imported local closure,
  including `shared/__init__.py`, `shared/queue/__init__.py`,
  `shared/storage/__init__.py`, `shared/storage/lifecycle.py`,
  `shared/utils/__init__.py`, `shared/utils/cache.py`,
  `shared/utils/cms_url_resolver.py`, `shared/utils/column_detection.py`,
  `shared/utils/cost_report.py`, `shared/utils/duckdb_helpers.py`,
  `shared/utils/extraction.py`, `source-catalog.schema.json`, and
  `poll-state.schema.json`. Each entry matches both the working tree and
  `git show <reviewed-candidate>:<path>`.
- `shared.utils.cms_url_resolver` no longer creates
  `~/.healthcare-data-mcp/cache` during package import; cache-directory
  creation remains inside the explicit cache-save operation.
- `socket.sendmsg` is denied in the process audit policy. The direct local
  socketpair probe is required and recorded as
  `network.socketpair_sendmsg_guard=true`; no listener, bind, source probe, or
  egress occurred.

Prior safety corrections remain active: durable acknowledgement precedes raw
custody finalization and scheduler checkpoint, SQLite WAL is observed on
restart, control JSON writes use fsync/atomic replace/reopen/schema validation,
stream and queue budgets/deadlines are enforced, and rollback updates both
`current.json` and `projections.json.current` while preserving prior raw data.

## Verification

```text
python3 -m pytest -q tests/test_data_mcp_staging_bundle.py tests/test_data_mcp_staging_fixture_runner.py tests/test_data_mcp_staging_lock.py
  -> 15 passed
ruff format --check scripts/run_data_mcp_staging_fixture.py tests/test_data_mcp_staging_fixture_runner.py tests/test_data_mcp_staging_lock.py
  -> passed
ruff check scripts/run_data_mcp_staging_fixture.py tests/test_data_mcp_staging_fixture_runner.py tests/test_data_mcp_staging_lock.py shared/utils/cms_url_resolver.py
  -> passed
python3 -m py_compile scripts/run_data_mcp_staging_fixture.py tests/test_data_mcp_staging_fixture_runner.py tests/test_data_mcp_staging_lock.py shared/utils/cms_url_resolver.py
  -> passed
pyright scripts/run_data_mcp_staging_fixture.py
  -> 0 errors, 0 warnings, 0 informations
git diff --check
  -> passed
```

The receipt reports all 16 fixture checks as passed, including no-op input,
changed-input admission, immutable custody, durable acknowledgement ordering,
duplicate/replay idempotency, lease recovery, quarantine without partial
publication, current/as-of projection, pointer-only rollback, complete
lineage/missingness, stream bounds, three complete runs, and the socketpair
network-guard probe.

## Review and boundaries

The writer self-reviewed the exact correction range
`0e4cf50d..2f2dca2f` plus the manifest rebind and found no architecture
change, scope expansion, historical receipt rewrite, or host/runtime/source/
database/credential mutation. The candidate is ready for the single
distinct non-writer Luna review against the exact base/head range; any later
fix must be reviewed only as a correction diff unless architecture changes.

The canonical Data MCP main worktree remains at `63539f8`, ahead of origin by
146 and behind by 1, with its modified managed `.beads/issues.jsonl`
preserved. No Bead export was hand-edited; the prior embedded Beads lock
remains the tracker blocker. Grok Build repository egress was unavailable
for this lane and was not retried.
