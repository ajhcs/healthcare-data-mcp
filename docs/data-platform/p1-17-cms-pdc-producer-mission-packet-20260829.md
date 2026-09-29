# P1-17 CMS Provider Data Catalog producer

Tracking bead: `hdp-p1-17-cms-pdc-20260829`

Status: bounded local producer candidate; no network acquisition, queue
publication, Toolkit database write, deployment, or production activation.

## Goal

Build a catalog-aware CMS Provider Data Catalog (PDC) producer that converts a
caller-supplied, already-acquired CMS distribution stream into a deterministic
source-scoped artifact manifest and release receipt. The producer must retain
stable CMS dataset/distribution identity, distinguish changed/no-op/schema-drift
and replay outcomes, and consume bytes only through the P1-09 bounded adapter
SDK.

## Frozen inputs and scope

- Exact base: `8e042b5f0548a9410616a5fc199ab09cb352f18d`.
- Source identity: `source:cms:pdc`; catalog registration and approved-public
  rights are mandatory before production work is admitted.
- The caller supplies catalog metadata and a source-native distribution stream.
  This lane performs no HTTP request, credential handling, source download,
  queue publication, database write, projection update, or production release.
- In scope: stable dataset/distribution IDs, catalog binding, explicit release
  and content-change semantics, bounded streaming, deterministic receipts,
  schema-drift rejection, replay/no-op fixtures, tests, and an operator runbook.
- Out of scope: CMS-specific row interpretation, metric promotion, identity
  resolution, raw-object deletion, consumer integration, credentials, and
  runtime/deployment changes.

## Producer protocol

1. Resolve one `source:cms:pdc` registration from a caller-owned catalog. The
   dataset and selected distribution identifiers must be non-empty, stable,
   and cross-referenced; an enabled source must have approved public rights.
2. Validate the release descriptor and compute a canonical release fingerprint
   from semantic metadata (dataset ID, distribution ID, release ID, modified
   timestamp, and source URL). A content fingerprint is computed from the
   bounded stream receipt, never from an unbounded in-memory payload.
3. Consume chunks with `shared.adapters.stream_bounded` and an explicit
   `StreamBudget`. Incomplete byte/chunk/deadline streams are unacknowledged
   and cannot advance a caller-owned cursor or release state.
4. Classify the result as `changed`, `no_op`, `schema_drift`, `failed_probe`, or
   `replayed` using prior receipt metadata. A matching release/content pair is
   a deterministic no-op; a conflicting replay identity is rejected.
5. Return a JSON-safe receipt containing only IDs, fingerprints, bounded
   counters, schema status, and custody locator metadata. The caller decides
   whether to persist or publish the receipt.

## Safety and contract invariants

- All catalog, release, distribution, stream, and receipt values are strict,
  bounded, JSON-schema validated values. Unknown fields and duplicate IDs fail
  closed.
- Dataset and distribution IDs are never derived from mutable display labels;
  an explicit stable identifier is required. Release identity is separate from
  content identity so metadata-only and byte changes remain distinguishable.
- Schema drift is a rejection with `current_projection_preserved=true`; no
  success callback, cursor advance, or release promotion is attempted.
- No-op and replay operations do not consume or publish a second result after
  the prior receipt is known. A replay with changed semantic/content identity
  returns an explicit conflict.
- Source URLs are HTTPS and never include user information. Receipts do not
  include response bodies, row payloads, credentials, query parameters, or
  arbitrary exception text.

## Fixtures and verification

Checked-in fixtures cover a valid changed release, a semantic/content no-op,
schema drift with projection preservation, an interrupted bounded stream, and
an exact replay. Focused tests cover catalog binding, stable IDs, release and
content fingerprints, bounded adapter streaming, no-op/replay classification,
schema-drift rejection, and forbidden mutation callbacks.

Required checks:

```bash
pytest -q tests/test_cms_pdc_producer.py
ruff check shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
ruff format --check shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
pyright shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
python -m compileall -q shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
python -m jsonschema -i contracts/healthcare-data-platform/cms-pdc/v1/fixtures/valid-changed.json contracts/healthcare-data-platform/cms-pdc/v1/cms-pdc.schema.json
git diff --check 8e042b5f0548a9410616a5fc199ab09cb352f18d..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run by the wave
coordinator on the coherent candidate, not as a production activation step.

## Rollback and handoff

Rollback is additive: omit or revert only the P1-17 mission, contract,
producer, fixture, test, and runbook commits in reverse order. No source
object, queue, cursor, database, projection, scheduler, or deployment state
is changed by this lane. Before handoff, record the exact commit chain,
focused evidence, clean-tree status, agentctl receipt, and the authoritative
worktree-bootstrap handoff when a manifest is available. Do not push.

Owner: Luna Max direct writer
