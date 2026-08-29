# P1-33 R3 Data MCP deterministic fixture candidate

This is additive candidate evidence for the isolated R3 staging lane.  It
supersedes neither the pending-approval activation receipt nor the blocked
host-custody receipt.  It does not prove source-native Phase 1 runtime
evidence, staging activation, or Phase 2 eligibility.

## Exact release

| Field | Value |
| --- | --- |
| Repository | `healthcare-data-mcp` |
| Worktree | `/tmp/healthcare-data-mcp-r3-fixture-20260829-v2` |
| Branch | `codex/r3-data-mcp-fixture-20260829-v2` |
| Baseline | `63539f8412831ee62b067c76c3b5c395108481cc` |
| Reviewed candidate before this receipt | `4651da8caf528d2bd1278463d81f44d7fb84dd66` |
| Manifest | `ops/staging/data-mcp-staging-fixture-manifest.json` |

The candidate commits are focused and remain unpushed:

```text
fc4fe37 build(staging): freeze fixture dependency lock
9b4c169 test(staging): verify clean locked installation
ebb798b feat(staging): package deterministic fixture runner
4651da8 test(staging): prove custody replay and recovery boundaries
```

The canonical Data MCP `main` worktree was not used or changed.  Its
`63539f8` head, ahead-146/behind-1 remote relationship, and modified managed
`.beads/issues.jsonl` remain preserved.

## One-shot command and contract

Run only from the candidate checkout, with a newly created empty absolute
caller-owned root:

```bash
fixture_root="$(mktemp -d /tmp/hdp-r3-fixture-XXXXXX)"
python3 scripts/run_data_mcp_staging_fixture.py \
  --candidate-sha 4651da8caf528d2bd1278463d81f44d7fb84dd66 \
  --root "$fixture_root"
```

The runner rejects relative, symlinked, non-empty, root, `/mnt/d`,
`/mnt/d/services`, and `/var/lib/healthcare-toolkit-staging` roots.  It creates
only `control/`, `raw-custody/`, and `evidence/` beneath that caller-owned
root.  It loads the catalog, staging bundle, JSON schemas, and fixture input
from the reviewed checkout; it never performs a source request.

The expected receipt is `$fixture_root/control/staging-fixture-receipt.json`.
With the exact candidate above, its deterministic receipt hash is
`sha256:b76255e44f8e6adb52eca6e3c39e2ab906ffca3aa88e5bfeda79b25f8d46e03b`.
The full file and artifact hash contract is in the manifest.

The receipt executes three fixed-time runs:

1. scheduler no-op plus baseline admission, lease acknowledgement, and a
   durable acknowledgement checkpoint;
2. restart and duplicate replay of the completed baseline work;
3. changed-input admission, immutable new-generation custody, expired-lease
   recovery, quarantine with no current-pointer publication, as-of/current
   projections, and pointer-only rollback.

Queue rows contain only opaque references, hashes, counters, lease state, and
bounded metadata.  Raw bytes exist only below `raw-custody/`.  The baseline
object remains readable after the changed generation and after rollback; the
changed object also remains readable after its current pointer is rolled back.
The projection records `source:cms:pdc` as `not_yet_researched` rather than
inventing a value.

## Frozen safety properties

The runner validates the existing staging bundle with a duplicate-key-rejecting
YAML loader and the deterministic input with its JSON schema.  Its hard bounds
are 4 active items, 1 MiB active bytes, 131,072 streamed bytes, 128 chunks,
65,536 bytes per chunk, 60 seconds per stream, a 300-second lease, and three
attempts.  It installs a process audit guard that raises on connect, bind,
listen, accept, send, and name-resolution events.  The receipt records no
listener, no bind, denied egress, disabled source probes, no credentials,
no application migration, and no production-state write.

Success is exit code `0`; malformed arguments are handled by argparse as `2`;
contract, bounds, custody, or network failures are exit code `4`.  A failed
run leaves its caller-owned evidence visible for diagnosis; the runner has no
delete or overwrite path for finalized raw custody.  Removing a fixture root
is outside this candidate and requires the applicable destructive-action
approval.

## Lock and verification evidence

The repository uses pip for package installation.  The lane adds the exact
fixture-control-plane lock at
`requirements/staging-fixture.lock`, with pinned `attrs`, `jsonschema`,
`jsonschema-specifications`, `PyYAML`, `referencing`, `rpds-py`, and
`typing-extensions`.  `tests/test_data_mcp_staging_lock.py` rejects ranges,
editable requirements, and unexpected packages.

The following focused checks passed on the reviewed candidate:

```text
python3 -m pytest -q tests/test_data_mcp_staging_bundle.py \
  tests/test_data_mcp_staging_fixture_runner.py \
  tests/test_data_mcp_staging_lock.py  -> 10 passed
ruff check scripts/run_data_mcp_staging_fixture.py \
  tests/test_data_mcp_staging_fixture_runner.py \
  tests/test_data_mcp_staging_lock.py  -> passed
ruff format --check ...                 -> passed
python3 -m py_compile scripts/run_data_mcp_staging_fixture.py -> passed
two isolated runner invocations                         -> byte-identical receipts
```

The clean package installation proof is explicitly pending the authorized
package artifact boundary.  `uv lock` could not resolve through the unavailable
network, and `uv lock --offline` stopped at missing cached `beautifulsoup4`
metadata.  No full application lock or false clean-install claim is made from
that failed resolution.  A later bounded install may use the pinned fixture
lock only after an approved index or independently verified wheelhouse is
available; the runner itself remains offline.

## Tracker and provider blockers

`bd-agent-sync pull` was attempted before tracker work.  It skipped Git pull on
the isolated no-upstream branch, then the embedded Dolt workspace remained
held by an external/stale lock.  `bd --readonly list` timed out.  No Bead was
created, claimed, updated, or closed, and no managed JSONL export was edited.
The exact blocker is recorded here rather than inventing a parallel tracker.

Grok Build repository egress was unavailable for this lane; Luna performed the
bounded implementation once and no readiness retry was made.

This candidate must remain offline and `prepared_not_applied`.  It must not
start services, bind ports, apply migrations, resolve credentials, contact
official sources, write staging/production runtime state, reconcile the
Data-MCP remote divergence, or be treated as Phase 1 promotion evidence.
