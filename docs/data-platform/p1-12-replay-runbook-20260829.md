# P1-12 replay and backfill runbook

Status: bounded implementation complete for local review.  This lane is
transport-neutral and has no network acquisition, queue publication,
projection write, deployment, credential, or production activation path.

## Plan and admission

Use `build_replay_plan()` to construct a deterministic plan from a source ID,
an inclusive `release:` or `version:` range, a contract version, and finite
item/byte limits.  Numeric and ISO-date ranges are expanded inclusively.  An
opaque range must provide the ordered `versions` list so the controller cannot
silently skip a release it cannot enumerate offline.

Numeric suffixes are compared numerically (`version:9` through `version:11`)
and fixed-width padding is preserved (`version:009` through `version:011`).
Mixed zero-padding is rejected rather than silently changing a version ID.

```python
from shared.replay import ReplayController

controller = ReplayController("replay.sqlite")
plan = controller.create_plan(
    "source:example",
    "version:001",
    "version:003",
    contract_version="hdp.replay.v1",
    max_items=3,
    max_bytes=30,
    estimated_bytes=10,
    idempotency_key="replay:operator:request-2026-08-29",
)
```

Admission is idempotent on the caller key and canonical plan hash.  Repeating
the same request returns a `duplicate` submission and the existing plan.
Reusing the key with a different range, contract version, or budget raises
`ReplayCollisionError` without replacing the original plan.

## Dry run and execution

Call `controller.dry_run(plan)` before admission when an operator needs a
read-only diff.  The result separates `new_items`, `already_completed`,
`in_progress`, and `conflicts`; the method performs no INSERT or UPDATE.

Execution is caller-owned and bounded.  A runner receives one `ReplayItem` and
returns a `ReplayResult` (or a strict mapping with `result_sha256`,
`result_ref`, and `result_bytes`).  A bytes value is accepted only to compute a
fingerprint immediately; bytes are never retained.  Each successful result is
validated before one SQLite transaction marks the item complete and advances
the contiguous checkpoint.  A failed or malformed result leaves the
checkpoint unchanged and records only a bounded, sanitized error.

```python
receipt = controller.execute(
    plan.plan_id,
    lambda item: {
        "result_sha256": "a" * 64,
        "result_ref": f"result/{item.version}",
        "result_bytes": 1,
    },
    max_items=25,
    max_bytes=10_000_000,
)
```

The SQLite checkpoint is available through `get_checkpoint(plan_id)`.  A new
controller opened on the same database resumes at the first incomplete item;
completed items are never offered to the runner again.  `cancel()` requests a
bounded stop.  If an item is active, it finishes its current boundary; the
plan then becomes `cancelled`.  `resume(plan_id, runner, ...)` is required to
continue a cancelled plan and preserves the original plan identity and
limits.

Each in-progress item also carries a finite claim lease and random owner
fence.  A second executor cannot steal a live claim.  A restart may reclaim
only an expired claim; a stale owner cannot complete or fail the replacement
claim.

## Bounds and privacy

Source IDs, versions, references, errors, item counts, attempts, estimated
bytes, and result bytes have finite limits.  IDs and references are opaque
safe characters only.  Runner exceptions containing authorization, bearer,
token, password, secret, or key material are reduced to a generic redacted
error before persistence.  SQLite tables contain no source payload column;
only plan/item metadata, fingerprints, bounded references, attempts, and
checkpoints are retained.

## Rollback evidence

The controller is additive and local.  To roll back, stop callers that invoke
the replay controller, retain the SQLite database and its records for audit,
and omit or revert the P1-12 commit range on a branch-local copy.  Do not
delete replay records, source custody, scheduler state, or queue state.  A
future resume requires the same plan identity and a verified paired
contract/application commit.  No production state is changed by this lane.

## Verification and handoff

Run from the isolated worktree:

```text
pytest -q tests/test_healthcare_data_platform_replay.py --tb=short
ruff check shared/replay tests/test_healthcare_data_platform_replay.py
ruff format --check shared/replay tests/test_healthcare_data_platform_replay.py
pyright shared/replay tests/test_healthcare_data_platform_replay.py
python3 -m compileall -q shared/replay tests/test_healthcare_data_platform_replay.py
python3 -m jsonschema -i contracts/healthcare-data-platform/replay/v1/fixtures/valid-replay-plan.json contracts/healthcare-data-platform/replay/v1/replay.schema.json
git diff --check 420011739ea49c9f75b9b8c2314cdc37ac29608b..HEAD
```

The supplied W11 worktree is:

```text
/tmp/.healthcare-data-mcp-p1-w11-source-20260829-worktrees/p1-12-replay
codex/healthcare-toolkit-rrna.p1-12-replay-20260829
```

The worktree-bootstrap CLI found no manifest for this pre-provisioned path,
so no synthetic WTB receipt is claimed.  The handoff owner should record the
output of `git rev-parse HEAD`, `git status --short --branch`, and the exact
three-commit implementation chain from this path.
