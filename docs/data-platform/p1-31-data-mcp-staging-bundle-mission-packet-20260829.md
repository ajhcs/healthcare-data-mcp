# P1-31 Data MCP staging release bundle

Tracking task: `hdp-p1-31-data-mcp-staging-20260829`

Worktree: `/tmp/.healthcare-data-mcp-p1-w16-20260829-worktrees/p1-31-data-mcp-staging`

Branch: `codex/healthcare-toolkit-rrna.p1-31-data-mcp-staging-20260829`

Base: `0fbf779d00e41920f14ad6950a60c6cf818c6164`

## Goal

Provide one reviewable, environment-safe staging bundle for the source-plane
control path already present in this repository.  The bundle describes how a
single scheduler emits bounded work, a finite worker pool claims that work, a
control store preserves restart/replay state, and raw custody retains immutable
source evidence.  It is a release descriptor and isolated-fixture contract;
it is not a deployment action and has no production activation path.

## Selected topology

```text
fixture/catalog
      |
      v
  scheduler (one leader, no payloads)
      |
      v
 durable queue + control store (SQLite/WAL, metadata only)
      |
      v
 finite source workers (bounded leases and streams)
      |
      v
 raw custody (content-addressed, append-only local staging root)
```

- One scheduler leader reads the versioned source catalog and writes
  deterministic poll intents.  It does not fetch sources or write raw bytes.
- Two workers are the staging default.  They claim queue leases with finite
  item, byte, chunk, retry, and lease limits; an isolated poison state remains
  visible for operator review.
- SQLite with WAL is the selected staging control store because the existing
  queue, replay, and telemetry components are local SQLite implementations.
  The store holds identifiers, hashes, counters, leases, and bounded state;
  source payload bytes and credentials are excluded.
- Raw custody is a separate content-addressed filesystem root.  Workers write
  immutable objects and manifests through the existing raw-custody contract;
  the scheduler and control store have no raw-object write authority.

## Scope and safety boundary

The YAML bundle is limited to staging.  It must declare `environment: staging`,
keep network egress disabled for the fixture, bind any optional service to
loopback, and set activation to false.  Values that could contain credentials
are references only; no token, password, private key, DSN value, or source
payload may be present in the bundle.  The fixture uses caller-created
temporary paths and never reads a live source.

The bundle references, without modifying, these reviewed contracts and
implementations:

- `contracts/healthcare-data-platform/catalog/v1/fixtures/valid-source-catalog.json`;
- `contracts/healthcare-data-platform/scheduler/v1/scheduler-state.schema.json`;
- `contracts/healthcare-data-platform/queue/v1/queue.schema.json`;
- `contracts/healthcare-data-platform/storage/v1/raw-artifact.schema.json`;
- `shared/utils/source_scheduler.py`;
- `shared/queue/durable.py`;
- `shared/storage/raw_custody.py`.

## Migration, secret, and rollback contract

Control-store migration is an explicit, forward-only preflight.  The bundle
records the schema references and requires a snapshot before any migration;
the isolated fixture uses application-owned schema initialization and therefore
does not run a production migration.  Secret entries are namespaced staging
references with no inline values and are optional for the local filesystem
fixture.

Rollback is an ordered, non-destructive action plan: pause the scheduler,
drain/stop workers, restore the last verified control-store snapshot, verify
the raw-custody manifest/index pair, and resume only after the bundle and
contract revisions match.  It never deletes queue rows, raw objects, or
receipts.  Production database, object store, deployment, secret-manager, and
network changes remain outside this task.

## Acceptance criteria

1. The bundle is strict YAML with stable API/kind metadata, exact staging
   environment markers, explicit activation false, and no inline credentials.
2. Scheduler, worker, queue/control-store, and raw-custody components are
   represented with finite replicas, limits, ownership, and one-way data-flow
   references.
3. Existing scheduler, queue, and raw-artifact schema/implementation paths are
   named as contract dependencies; no new persistence schema or migration is
   introduced.
4. Migration and secret references are explicit and safe for a fixture: no
   secret values, source payloads, unbounded paths, or production endpoints.
5. A rollback plan is machine-readable, ordered, approval-aware, and
   non-destructive.
6. Focused tests load the bundle with duplicate-key rejection, validate its
   topology and safety invariants, and execute an isolated temporary-directory
   fixture proving scheduler metadata, worker bounds, control-store isolation,
   and content-addressed raw-custody paths remain separate.

## Out of scope

- Deploying services, binding ports, contacting a source, acquiring data, or
  activating any production runtime;
- adding scheduler, worker, database, queue, or storage implementation code;
- introducing migrations, credentials, secret values, or provider-specific
  source configuration;
- replacing the existing queue, scheduler, replay, telemetry, or custody
  contracts;
- profile population, canonical identity promotion, or public publication.

## Verification

```text
pytest -q tests/test_data_mcp_staging_bundle.py
ruff check tests/test_data_mcp_staging_bundle.py
ruff format --check tests/test_data_mcp_staging_bundle.py
python3 -m compileall -q tests/test_data_mcp_staging_bundle.py
python3 -c 'import yaml; yaml.safe_load(open("ops/staging/data-mcp-staging-bundle.yaml"))'
git diff --check 0fbf779d00e41920f14ad6950a60c6cf818c6164..HEAD
```

The repository-wide CI/release gate is run once by the coordinator against the
coherent wave candidate.  This lane does not start a server or mutate staging
infrastructure.

## Owner and status

- Owner: Luna Max direct writer
- Status: mission packet committed before bundle implementation
