# P1-11 durable queue, leases, and backpressure

Tracking bead: `healthcare-toolkit-rrna.p1-11-durable-queue-20260829`

## Goal

Provide a restart-safe, database-backed source-plane work queue that claims
bounded work with renewable leases, enforces per-source concurrency and byte
budgets, isolates poison work, and recovers expired worker claims
idempotently.

## Problem

The scheduler emits durable poll intent, but there is not yet a durable worker
seam to claim intent, prove liveness, bound source pressure, or recover work
after a worker dies. An in-memory queue would lose those guarantees on restart
and could allow one source to starve the others.

## Scope

- Add a SQLite/DB-API-backed queue schema and typed work-item/lease receipts.
- Admit work idempotently by source and work identity without retaining source
  payloads; store only bounded references, hashes, and operational metadata.
- Atomically claim eligible queued or expired work, issue owner-bound leases,
  renew heartbeats, complete successful work, and reschedule retryable failure.
- Enforce global and per-source active-item/byte budgets inside the claim
  transaction; expose explicit backpressure decisions rather than silently
  dropping work.
- Move exhausted or explicitly poisonous work to an isolated poison state with
  bounded reason evidence, and recover expired claims without double execution.

## Out of Scope

- Network acquisition, source adapters, raw payload/object custody, envelope
  admission, projections, deployment, credentials, migrations against a
  production database, or queue-hosting infrastructure.
- HCU, hospital discovery, payer, or other producer-specific files.

## Acceptance Criteria

- A queue can initialize a durable database, enqueue idempotently, claim work
  atomically, heartbeat only with the active owner/token, and complete only an
  active lease.
- Duplicate enqueue of the same work identity and payload hash is a no-op;
  the same identity with a different hash is rejected without replacement.
- Lease TTL, heartbeat extension, retry visibility, and expired-claim recovery
  are finite and owner-bound. Recovery is repeatable and does not create a
  second active claim.
- Claiming honors global and source-specific active item and byte budgets in a
  single transaction; blocked work remains queued with an explicit reason.
- Retry attempts are bounded. Exhausted or explicitly poison-marked work is
  isolated and cannot be claimed by normal workers; its source queue remains
  usable.
- Operational records contain no source payload, credentials, bearer material,
  or PHI; identifiers, hashes, counters, and bounded reason text are retained.
- Focused tests cover restart, worker death, lease ownership, heartbeat,
  budget backpressure, idempotency, poison isolation, and concurrent claims.

## Traceability

Affected PER IDs:

- N/A — this is source-plane control infrastructure and does not populate or
  expose profile-engine facts.

Affected DTR IDs:

- N/A — no UI, route, component, design-token, table, chart, map, or
  accessibility surface changes.

## Files Likely Touched

- `shared/queue/__init__.py`
- `shared/queue/durable.py`
- `contracts/healthcare-data-platform/queue/v1/queue.schema.json`
- `tests/test_healthcare_data_platform_queue.py`
- This mission packet and a concise integration receipt/runbook if needed.

## Verification

```bash
pytest -q tests/test_healthcare_data_platform_queue.py
ruff check shared/queue tests/test_healthcare_data_platform_queue.py
ruff format --check shared/queue tests/test_healthcare_data_platform_queue.py
pyright shared/queue tests/test_healthcare_data_platform_queue.py
python3 -m compileall -q shared/queue tests/test_healthcare_data_platform_queue.py
python3 -m jsonschema -i contracts/healthcare-data-platform/queue/v1/fixtures/valid-queue-policy.json contracts/healthcare-data-platform/queue/v1/queue.schema.json
git diff --check 44354ed9b77a8fd3373106b226f23fdeaf7a0302..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run once by the wave
coordinator against the coherent candidate, not per queue commit.

## Rollback

The queue implementation is additive. Revert or omit the reviewed P1-11
commit range and stop queue consumers; scheduler intents and source custody
remain available. Never delete queue records or production database files as
part of rollback. A deployment rollback restores the last known-good queue
schema/application pair and leaves poison evidence for later recovery.

## Risk and Privacy

Risk is medium: incorrect claim atomicity can duplicate work, while permissive
budgets can exhaust a worker or starve a source. SQLite transactions,
owner-bound random lease tokens, finite bounds, source-scoped budgets, and
explicit poison states keep failures fail-closed. Queue rows hold only safe
operational metadata and hashes; payloads remain with the separate custody
plane.

## Dependencies and Blockers

- Depends on the reviewed P1-02 scheduler, P1-09 adapter SDK, and P1-10
  validation/quarantine contracts at the exact base SHA.
- Grok Build repository egress remains unavailable; this is direct Luna Max
  execution with no readiness retry.
- Beads synchronization may remain unavailable if the local Dolt service is
  unreachable; the exact bead and packet are recorded for handoff.

## Owner and Status

- Owner: Luna Max direct writer
- Status: implementation in progress
