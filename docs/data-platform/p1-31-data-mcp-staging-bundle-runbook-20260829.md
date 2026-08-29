# P1-31 Data MCP staging bundle runbook

This runbook describes the local, deterministic fixture represented by
`ops/staging/data-mcp-staging-bundle.yaml`. It is a staging release descriptor,
not a deployment manifest. The fixture makes no network request, does not bind
a service port, and cannot activate a production runtime.

## Inputs and isolated roots

Use the repository checkout as the only source of reviewed implementation and
contract references. The fixture catalog is
`contracts/healthcare-data-platform/catalog/v1/fixtures/valid-source-catalog.json`.
It is loaded with the strict catalog loader; the scheduler sees only enabled
sources with `approved_public` rights. Source payloads are not read by the
scheduler.

Create a caller-owned temporary fixture root and keep these paths distinct:

```text
<fixture-root>/control/control.sqlite3   # queue and bounded control metadata
<fixture-root>/control/scheduler.json    # scheduler checkpoint
<fixture-root>/raw-custody/               # immutable content-addressed objects
```

Set `HDP_STAGING_FIXTURE_ROOT` only for the duration of the fixture. The
bundle’s optional environment references (`HDP_STAGING_CONTROL_STORE_PATH`,
`HDP_STAGING_RAW_CUSTODY_ROOT`, and secret-manager references) are names only;
never put credentials, DSNs, tokens, or source bytes in YAML, queue rows, or
logs.

## Fixture execution

1. Load and validate the YAML with a duplicate-key rejecting YAML loader.
   Confirm `environment: staging`, `spec.mode: isolated_fixture`, denied
   egress, loopback bind, empty allowed hosts, and all activation flags false.
2. Load the catalog and initialize one `PollState` per enabled source. Create
   one `DurableScheduler` leader and schedule due work. Persist its checkpoint
   beneath the control root; its intent rows contain source/release metadata,
   not payloads.
3. Initialize `DurableQueue` against the control SQLite path with the bundle’s
   finite active-item, byte, lease, retry, and attempt ceilings. Enqueue only
   an opaque fixture reference plus a content hash and byte estimate. Claim
   work with a bounded worker owner and complete it through the lease API.
4. Initialize `RawArtifactStore` against the separate raw-custody root. Store
   caller-provided bounded fixture chunks using `RawArtifactMetadata`; verify
   the returned object key, metadata key, manifest hash, and content hash with
   `read_metadata` and `read_bytes`. The control store must not contain the
   fixture bytes.
5. Reopen the scheduler and queue from their paths to prove restart behavior.
   Replaying the same scheduler generation or queue work identity is
   idempotent. Replaying raw metadata with the same source/release/content
   identity returns a duplicate; changing immutable metadata is a collision.

The focused test `tests/test_data_mcp_staging_bundle.py` executes this fixture
in a temporary directory and verifies all of the above boundaries without
contacting a source or writing outside the temporary root.

## Control-store migration and secrets

The bundle’s migration strategy is forward-only and application-owned. The
fixture does not run a production migration. A real staging migration requires
the read-only contract preflight, an approved snapshot reference, and explicit
operator approval. The `apply` action is never production-enabled. Secret
entries are optional staging provider references with `inline_values: false`;
the runbook does not resolve them.

## Rollback

Rollback preserves evidence and queue state. With approval, execute the
machine-readable steps in order:

1. pause the scheduler;
2. drain workers and let active leases settle;
3. snapshot the control store;
4. restore the last verified control-store snapshot;
5. verify raw-custody objects and metadata manifests;
6. validate the bundle and rerun the isolated fixture;
7. resume workers;
8. resume the scheduler.

Do not delete queue rows, receipts, raw objects, metadata manifests, partial
objects, or snapshots as a rollback shortcut. A failed fixture remains visible
for review and is not silently retried with unbounded limits.

## Verification

From the repository root:

```bash
python3 -m pytest -q tests/test_data_mcp_staging_bundle.py
ruff check tests/test_data_mcp_staging_bundle.py
ruff format --check tests/test_data_mcp_staging_bundle.py
python3 -m compileall -q tests/test_data_mcp_staging_bundle.py
python3 -c 'import yaml; yaml.safe_load(open("ops/staging/data-mcp-staging-bundle.yaml", encoding="utf-8"))'
git diff --check 0fbf779d00e41920f14ad6950a60c6cf818c6164..HEAD
```

The coordinator owns one repository-wide fast gate for the coherent wave; do
not run `scripts/check-fast.sh` from this lane. No deployment, migration,
network probe, production database, object store, or secret-manager mutation is
authorized by this runbook.

## P1-33 activation receipt (pending approval)

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
