# P1-12 replay and backfill controller

Tracking bead: `healthcare-toolkit-rrna.p1-12-replay-20260829`

## Goal

Provide a deterministic, restart-safe replay controller that plans bounded
version ranges, preserves idempotency and checkpoints, exposes a dry-run diff,
and supports cancellation and resumption without replaying completed work.

## Problem

The scheduler and durable queue now preserve source poll intent and leased
work, but operators still need a typed control seam for replaying an explicit
release/version range. A replay request must be inspectable before execution,
must resume from durable progress after interruption, and must not silently
duplicate an already acknowledged version.

## Scope

- Add a versioned replay plan and item/checkpoint contract under
  `contracts/healthcare-data-platform/replay/v1/`.
- Add a local, SQLite-backed controller under `shared/replay/` that accepts
  explicit source/version ranges, derives stable idempotency keys, and keeps
  only bounded metadata and result fingerprints.
- Support dry-run plan diffs, bounded batch execution through a caller-owned
  runner, cancellation at item boundaries, and restart/resume from a durable
  checkpoint.
- Add deterministic fixtures, focused tests, and an operator runbook.

## Out of Scope

- Network acquisition, source-specific parsing, raw payload/object custody,
  projection writes, deployment, credentials, production database migration,
  or automatic production activation.
- Replacing the P1-02 scheduler, P1-11 queue, or source producer checkpoints.
- Storing payload bytes, PHI, bearer material, or arbitrary runner output.

## Acceptance Criteria

- An explicit source ID, inclusive version range, contract version, and bounded
  item/byte budgets produce a deterministic replay plan without network I/O.
- Re-submitting the same plan is idempotent; a conflicting plan with the same
  idempotency key is rejected without replacement.
- A dry-run reports new, already-completed, in-progress, and conflicting work
  without changing durable state.
- Each successful item advances a durable checkpoint only after its runner
  returns a validated result fingerprint; a restart resumes at the first
  incomplete item and never re-runs completed items.
- Cancellation is explicit and bounded to an item boundary; a cancelled plan
  can be resumed by an operator, subject to the same plan identity and limits.
- Attempts, versions, bytes, result references, and error text are finite and
  secret-free; malformed contracts fail closed and schema fixtures validate.
- Focused tests cover plan range/version validation, duplicate/conflicting
  submission, dry-run diff, checkpoint resume, cancellation, and bounded
  execution. No production writes are made.

## Traceability

Affected PER IDs:
- N/A — replay control infrastructure does not populate or expose profile facts.

Affected DTR IDs:
- N/A — no UI, route, component, design token, or interaction surface.

## Files Likely Touched

- `shared/replay/__init__.py`
- `shared/replay/controller.py`
- `contracts/healthcare-data-platform/replay/v1/replay.schema.json`
- `contracts/healthcare-data-platform/replay/v1/fixtures/valid-replay-plan.json`
- `tests/test_healthcare_data_platform_replay.py`
- `docs/data-platform/p1-12-replay-runbook-20260829.md`

## Verification

```bash
pytest -q tests/test_healthcare_data_platform_replay.py
ruff check shared/replay tests/test_healthcare_data_platform_replay.py
ruff format --check shared/replay tests/test_healthcare_data_platform_replay.py
pyright shared/replay tests/test_healthcare_data_platform_replay.py
python3 -m compileall -q shared/replay tests/test_healthcare_data_platform_replay.py
python3 -m jsonschema -i contracts/healthcare-data-platform/replay/v1/fixtures/valid-replay-plan.json contracts/healthcare-data-platform/replay/v1/replay.schema.json
git diff --check 420011739ea49c9f75b9b8c2314cdc37ac29608b..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run once by the wave
coordinator against the coherent candidate, not per replay commit.

## Rollback

The controller is additive and has no production activation path. Stop replay
consumers, retain the plan/checkpoint database for audit, and omit or revert
the reviewed P1-12 range. Do not delete replay records or source custody as a
rollback action. Resuming after rollback requires the same plan identity and a
verified paired contract/application SHA.

## Risk and Privacy

Risk is medium: an incorrect identity or checkpoint transition can duplicate
or skip source work. Stable plan hashes, SQLite transactions, item-level
checkpoints, bounded limits, and explicit cancellation keep failures
fail-closed. Only IDs, versions, hashes, counters, bounded references, and
bounded error text are retained; source payloads and credentials remain out of
scope.

## Dependencies and Blockers

- Depends on the reviewed P1-11 durable queue and the P1-02 scheduler replay
  contract at the supplied base SHA.
- Grok Build repository egress remains unavailable; use direct Luna execution
  with no readiness retry.
- Beads synchronization is controlled by the parent orchestrator; the exact
  bead and packet are recorded here for handoff.

## Owner and Status

- Owner: Luna Max direct writer
- Status: implementation in progress
