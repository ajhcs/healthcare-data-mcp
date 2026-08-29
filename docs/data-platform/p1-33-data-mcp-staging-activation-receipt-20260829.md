# P1-33 Data MCP staging activation receipt

Preparation produces the machine-readable receipt fixture at
`contracts/healthcare-data-platform/staging/v1/fixtures/pending-approval.json`,
validated by `staging-activation-receipt.schema.json`. It is bound to the
P1-31 release and exact Data MCP preparation commit
`f477e126cc09d5a8ce7edc7ffcd4833cb484f766`.

The receipt intentionally records `state: pending_approval` and
`activation_performed: false`. Restart, no-op, change detection, raw-custody,
lifecycle, telemetry, and rollback outcomes are each explicit
`pending_approval` entries with `mutation_performed: false`; none is runtime
evidence. Human approval for `staging-activation` is required before any
restart, migration, or activation. Until that approval exists, do not start a
service, bind a port, resolve credentials, probe a source, write runtime state,
or claim that staging is active.

The receipt's prohibition fields are custody evidence for this preparation:
services, ports, migrations, credentials, source probes, and runtime state are
all false. A later approved run must replace pending entries with independently
captured evidence and retain this pending receipt; it must not rewrite it as a
success receipt.

## Fixture-verification blocker

The `verification` object is an explicit blocker receipt, not a health claim.
It records `state: blocked_host_custody`, `status: pending`,
`execution: not_run`, and `activation_claim: not_activated`. The exact source
commit is repeated there so a consumer cannot mistake a later checkout for the
reviewed base.

The permitted read-only socket preflight ran
`ss -H -ltnp '( sport = :8110 or sport = :3020 )'` at
`2026-08-29T16:08:11Z` and found both loopback staging ports free. Port
availability is not service evidence: no service was started and no listener
was bound. The host-custody blocker records the absent
`/mnt/d/services/healthcare-toolkit-staging` root and unverified parent
custody; the documented `plumbob:plumbob 0777` parent remains unsafe.

The fixture paths are templates under a caller-owned temporary root, never
host paths or production storage:

```text
{tmp_path}/control/scheduler.json
{tmp_path}/control/control.sqlite3
{tmp_path}/raw-custody/
```

Each artifact is marked `evidence_state: pending` and `content_present: false`
because the isolated fixture was not run while host custody is blocked. The
receipt therefore captures the intended scheduler, queue, and raw-custody
seams without claiming healthy activation, source access, migration, or
runtime state.
