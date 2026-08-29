# P1-33 Data MCP staging activation receipt

Preparation produces the machine-readable receipt fixture at
`contracts/healthcare-data-platform/staging/v1/fixtures/pending-approval.json`,
validated by `staging-activation-receipt.schema.json`. It is bound to the
P1-31 release and exact preparation base commit
`ce43147c30793a4dbb35ed97649998a3a5426441`.

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
