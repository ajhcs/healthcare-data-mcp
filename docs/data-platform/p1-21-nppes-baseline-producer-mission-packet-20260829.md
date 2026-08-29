# P1-21 NPPES V2 full-baseline producer

Tracking task: `hdp-p1-21-nppes-baseline-20260829`

Status: bounded local producer candidate; no network acquisition, queue
publication, Toolkit database write, canonical identity promotion, deployment,
or production activation.

## Goal

Build a catalog-aware NPPES V2 full-baseline producer that accepts
caller-supplied, already-acquired NPPES source streams and emits deterministic,
source-scoped release and observation evidence. The producer preserves the
source URL, probe state, release identity, exact NPI and source-row identifiers,
file kind, byte/count evidence, and deactivation opportunity without treating
an NPI, organization, endpoint, location, or reference row as a Toolkit
canonical entity.

## Frozen inputs and scope

- Exact base: `8e042b5f0548a9410616a5fc199ab09cb352f18d`.
- Source identity: `source:nppes:registry`; the source catalog registration
  and approved-public rights are required before admission.
- The caller supplies a catalog entry, release descriptor, probe result, and
  bounded iterables of NPPES V2 provider, practice-location, endpoint, and
  reference/deactivation rows. This lane performs no HTTP request, credential
  handling, source download, queue publication, database write, projection
  update, identity resolution, or production release.
- In scope: strict catalog/release/row contracts, bounded streaming, exact
  source-native identifiers, deterministic fingerprints and receipts,
  changed/no-op/replay classification, deactivation candidate preservation,
  fixtures, focused tests, and an operator runbook.
- Out of scope: canonical provider or facility promotion, NPI-to-Toolkit
  crosswalks, clinical or quality claims, source payload persistence,
  deletion of raw custody, runtime/deployment wiring, and live source probes.

## Producer protocol

1. Resolve exactly one enabled `source:nppes:registry` catalog registration.
   The registration must use HTTPS, have approved-public rights, identify the
   NPPES V2 full-baseline release locator, and carry explicit byte/chunk/time
   bounds. Display labels never substitute for stable source or release IDs.
2. Validate the release descriptor and probe metadata. Preserve the source
   URL, final URL, HTTP status, probe state, release label, publication date,
   and release fingerprint; `not_modified` is a no-op and failed probes do
   not advance a release.
3. Consume each source file through the shared bounded stream primitive. A
   provider file is required; location, endpoint, reference, and deactivation
   streams are independently typed and may be empty only when the release
   descriptor explicitly marks them unavailable. An interrupted stream is
   unacknowledged and cannot produce a successful baseline receipt.
4. Parse only bounded, source-native row metadata. Preserve exact NPI strings
   (including leading zeroes), source row IDs, file-kind/row-number selectors,
   and row fingerprints. Do not normalize an NPI into a Toolkit identity or
   infer provider completeness from a partial file.
5. Classify the result as `changed`, `no_op`, `failed_probe`, `schema_drift`,
   `replayed`, or `blocked`. Matching release/file digests are deterministic
   no-ops; a replay with conflicting release or content identity is rejected.
   Deactivation rows remain an explicit opportunity for a later review lane,
   never an automatic deletion or identity decision.
6. Return a JSON-safe source receipt containing only IDs, URL/probe metadata,
   fingerprints, bounded counters, file statuses, and custody/evidence
   locators. The caller owns durable storage and any later observation
   admission or review.

## Safety and contract invariants

- Catalog, release, probe, row, file, and receipt values are strict,
  JSON-schema-valid, bounded values. Unknown fields, duplicate file kinds,
  duplicate source row IDs, malformed NPIs, and malformed URLs fail closed.
- NPPES NPIs are exactly ten ASCII digits and are retained as strings. A
  source NPI is evidence only; it is not a canonical provider identifier.
- Provider, location, endpoint, other-name/reference, and deactivation files
  retain their source-native file kind and exact row selector. Missing optional
  files are represented as explicit `unavailable_public` or `not_applicable`
  states rather than empty placeholder rows.
- Schema drift yields an explicit rejection with
  `current_projection_preserved=true`; no success callback, cursor advance,
  release promotion, or deletion callback is attempted.
- Receipts never include source row payloads, response bodies, credentials,
  query parameters, or arbitrary exception text. Deactivation opportunities
  contain only bounded IDs, hashes, counts, and source locators.
- A no-op/replay does not reread or publish another result after the prior
  receipt is known. A conflicting replay is a deterministic error.

## Planned commits

1. `docs(nppes): define full-baseline producer mission packet`
2. `feat(nppes): add bounded full-baseline producer contracts`
3. `test(nppes): cover stream, replay, and deactivation fixtures`
4. `docs(nppes): document baseline runbook and handoff evidence`

The coordinator may squash or preserve this four-commit chain; each commit is
additive and independently reviewable.

## Fixtures and acceptance criteria

Checked-in fixtures and focused tests must cover:

- a changed baseline with provider, location, endpoint, reference, and
  deactivation rows;
- a large stream that stops at byte/chunk/deadline bounds without
  acknowledgement;
- an explicit no-op/304 probe and an exact deterministic replay;
- schema drift, duplicate identifiers, malformed NPIs, missing provider data,
  and conflicting replay rejection;
- preservation of source URL, probe/status metadata, exact identifiers,
  row/file fingerprints, and deactivation opportunity state;
- Draft 2020-12 validation of catalog, release, and receipt fixtures.

Acceptance requires that all exported producer functions have focused tests,
strict typing passes for touched files, and no path outside the declared
contracts/shared-acquisition/tests/docs lane is changed.

## Verification

```bash
pytest -q tests/test_nppes_baseline_producer.py
ruff check shared/acquisition/nppes tests/test_nppes_baseline_producer.py
ruff format --check shared/acquisition/nppes tests/test_nppes_baseline_producer.py
pyright shared/acquisition/nppes tests/test_nppes_baseline_producer.py
python -m compileall -q shared/acquisition/nppes tests/test_nppes_baseline_producer.py
python -m jsonschema -i contracts/healthcare-data-platform/nppes/v2/fixtures/valid-changed.json contracts/healthcare-data-platform/nppes/v2/nppes-baseline.schema.json
git diff --check 8e042b5f0548a9410616a5fc199ab09cb352f18d..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run by the wave
coordinator on the coherent W13 candidate, not as a production activation
step. A distinct reviewer must perform acceptance review; the writer does not
self-review the implementation.

## Rollback and handoff

Rollback is additive: omit or revert only the P1-21 mission, NPPES contract,
producer, fixtures, focused tests, and runbook commits in reverse order. No
source object, queue, cursor, database, projection, scheduler, or deployment
state is changed by this lane. Preserve any copied fixture receipt for audit;
do not replace it with newly probed bytes.

Before handoff, record the exact base-to-head commit chain, focused evidence,
clean-tree status, agentctl receipt, and the authoritative
`worktree-bootstrap handoff` receipt when a manifest is available. Report the
known limitation that source acquisition, custody persistence, observation
admission, canonical promotion, and runtime wiring remain coordinator-owned.
Do not push.

Owner: Luna Max direct writer
