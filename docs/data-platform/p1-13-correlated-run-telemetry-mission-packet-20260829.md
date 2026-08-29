# P1-13 correlated run telemetry

Tracking task: `hdp-p1-13-telemetry-20260829`

## Goal

Provide a versioned, source-safe telemetry contract and local recorder for
correlating one data-platform run across its source, artifact, and observation
envelope identities.  Operators must be able to inspect freshness, failures,
lag, byte and row volumes, retries, and dead-letter outcomes without retaining
payloads, PHI, credentials, or high-cardinality labels.

## Problem

The scheduler, queue, producers, and envelope contracts expose operational
identities independently, but there is no bounded run-level record that ties
those identities to measurable outcomes.  Without a common correlation seam,
operators cannot distinguish a stale source from a slow transfer, a failed
envelope from a queue retry, or a dead-lettered item from a successful run.

## Scope

- Add a versioned telemetry schema and valid fixture under
  `contracts/healthcare-data-platform/telemetry/v1/`.
- Add strict typed identity, metric, event, and summary models under
  `shared/telemetry/`.
- Add a local append-only SQLite recorder with idempotent event identity,
  bounded labels, redacted failure text, and deterministic summary queries.
- Record run/source/artifact/envelope correlation plus freshness, failure,
  lag, bytes, rows, retries, and DLQ measurements.
- Add focused tests for correlation, metric bounds, redaction, cardinality,
  duplicate admission, schema validation, and absence of payload storage.
- Add an operator runbook with rollback evidence and a portable handoff note.

## Out of Scope

- Network acquisition, queue/scheduler mutation, source parsing, raw custody,
  projection writes, deployment, credentials, dashboards, alerts, or automatic
  production activation.
- Storing source payloads, PHI, arbitrary exception text, request URLs, query
  parameters, stack traces, or unbounded dimensions.
- Changing consumers under `shared/queue`, `shared/replay`, or existing
  acquisition/envelope modules.

## Acceptance Criteria

- Every accepted telemetry record carries valid `run_id`, `source_id`,
  `artifact_id`, and `envelope_id` correlation IDs, with optional references
  remaining explicit rather than inferred.
- Freshness age, processing lag, byte counts, row counts, retry counts, and
  DLQ outcomes use finite non-negative bounds; failure outcomes use a bounded
  code and redacted message.
- Labels and dimensions use an allowlist, bounded lengths, and a finite
  cardinality budget; sensitive-looking values fail closed or are replaced by
  a stable non-secret category.
- Repeating an event identity is idempotent; a different canonical event under
  the same identity is rejected without replacing the original record.
- The recorder is local and append-only, stores no payload columns, and emits a
  deterministic run summary suitable for operator inspection.
- Schema fixture and serialized model validate against the versioned schema;
  focused tests cover duplicate, bounds, redaction/cardinality, correlation,
  summary, and rollback-safe local behavior.

## Traceability

Affected PER IDs:

- N/A — telemetry records operational data only and do not populate profile
  facts.

Affected DTR IDs:

- N/A — no UI, route, component, design token, or interaction surface.

## Files Likely Touched

- `shared/telemetry/__init__.py`
- `shared/telemetry/recorder.py`
- `contracts/healthcare-data-platform/telemetry/v1/telemetry.schema.json`
- `contracts/healthcare-data-platform/telemetry/v1/fixtures/valid-run-telemetry.json`
- `tests/test_healthcare_data_platform_telemetry.py`
- This mission packet and `docs/data-platform/p1-13-correlated-run-telemetry-runbook-20260829.md`.

## Verification

```bash
pytest -q tests/test_healthcare_data_platform_telemetry.py
ruff check shared/telemetry tests/test_healthcare_data_platform_telemetry.py
ruff format --check shared/telemetry tests/test_healthcare_data_platform_telemetry.py
pyright shared/telemetry tests/test_healthcare_data_platform_telemetry.py
python3 -m compileall -q shared/telemetry tests/test_healthcare_data_platform_telemetry.py
python3 -m jsonschema -i contracts/healthcare-data-platform/telemetry/v1/fixtures/valid-run-telemetry.json contracts/healthcare-data-platform/telemetry/v1/telemetry.schema.json
git diff --check d5d3d0697cad7cb75cf836cb7872ce4ec4f7a248..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run by the wave
coordinator against the coherent candidate, not per telemetry commit.

## Rollback

Telemetry is additive and local.  Stop recorder callers, retain the SQLite
database and exported summaries for audit, and omit or revert the reviewed
P1-13 commit range.  Do not delete telemetry records or alter queue, scheduler,
custody, or envelope state.  Re-enabling recording requires the matching
contract/application commit pair.

## Risk and Privacy

Risk is medium: incorrect correlation could misattribute a failure or hide
freshness lag.  Strict IDs, canonical hashes, bounded counters, transactionally
idempotent admission, and schema validation keep records fail-closed.  The
recorder retains only operational metadata, safe references, bounded labels,
and redacted failure categories; payloads and secrets remain out of scope.

## Dependencies and Blockers

- Depends on the source, artifact, and envelope identity conventions already
  present at base `d5d3d0697cad7cb75cf836cb7872ce4ec4f7a248`.
- Grok Build repository egress remains unavailable; no readiness retry is
  attempted.
- Beads/Dolt synchronization is controlled by the parent orchestrator; the
  accepted-only task ID and this packet are recorded here for handoff.

## Owner and Status

- Owner: Luna Max direct writer
- Worktree: `/tmp/.healthcare-data-mcp-p1-w12-source-20260829-worktrees/p1-13-telemetry`
- Branch: `codex/healthcare-toolkit-rrna.p1-13-telemetry-20260829`
- Base: `d5d3d0697cad7cb75cf836cb7872ce4ec4f7a248`
- Status: implementation complete pending independent review
