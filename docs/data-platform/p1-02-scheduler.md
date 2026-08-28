# P1-02 Minimal durable scheduler

The scheduler is a small, restart-safe control-plane component built on the
P1-01 source catalog and poll-state contracts. It produces durable poll
intentions; it does not perform network acquisition, claim a worker lease, or
mutate deployment infrastructure.

## Contract and behavior

- `DurableScheduler.schedule()` considers only enabled sources with
  `approved_public` rights. Intent IDs are derived from source, due timestamp,
  generation, and reason, so replaying a scheduler tick is idempotent.
- `mark_missed()` records an explicit `missed` state after the catalog's
  per-source grace window. The next scheduled intent is classified as a
  bounded `retry`.
- `replay()` accepts an explicit `release:` range and records a
  `backfill_pending` state plus an `operator_replay` intent. It never starts a
  fetch. A source/generation already represented by an intent is not emitted a
  second time by the normal cadence tick.
- `checkpoint()` uses the shared atomic JSON writer. `restore()` rejects
  duplicate keys, unknown fields, malformed timestamps, invalid source states,
  and unapproved source references before constructing a scheduler.

The versioned schemas and fixture live under
`contracts/healthcare-data-platform/scheduler/v1/`. Poll-state fields are
intentionally duplicated in the scheduler envelope so an offline validator can
validate a checkpoint without resolving a network `$ref`.

## Recovery and rollback

The checkpoint is an operational snapshot, not source evidence. A restart
loads the last checkpoint and resumes from its state and intent IDs. A failed
probe advances the generation while preserving the last successful release and
fingerprint. To roll back a scheduler deployment, stop the worker consumer,
restore the last known-good checkpoint, and resume only after the paired
producer/control-store receipt has been verified; no historical source state is
deleted.

## Evidence

The focused scheduler and catalog suite covers due-source admission, disabled
and rights-blocked sources, missed runs, bounded replay, idempotency,
checkpoint/restore parity, schema validation, duplicate-key rejection, and
failure release preservation. The candidate was implemented in the preserved
local fallback worktree because Grok Build repository egress was unavailable;
the primary Luna worktree remains untouched for provenance.
