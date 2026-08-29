# P1-07 AHRQ producer observation envelope

Tracking bead: `healthcare-toolkit-rrna.p1-07`

Status: bounded local producer candidate; no network acquisition, queue
publication, Toolkit database write, deployment, or production authority.

## Goal

Parse source-native AHRQ Compendium system and hospital-linkage CSV rows and
emit a deterministic, contract-versioned `hdp.observation-envelope.v1` for the
Toolkit P1-06 admission seam. The producer carries immutable raw-custody
locators and SHA-256 claims through every observation and returns a public-safe
receipt that can be replayed after a crash.

## Frozen inputs and scope

- Data MCP base: `e0d4efefbc980ce319f8d60e55efd9f4ae215567`.
- Source identity: `source:ahrq:lighthouse`; source URLs must be HTTPS.
- The system and facility files are caller-provided bytes. The producer does
  not download, overwrite, or delete them.
- CSV decoding is strict (CP-1252 by default), headers are unique, required
  source-native identifiers are present, and linked facility system IDs must
  resolve to a system row. Raw lexical values remain strings in observation
  payloads; no metric or current-projection meaning is inferred.
- Each input raw artifact must already be verified by P1-04 custody. The
  emitted envelope uses a deterministic normalized-row artifact whose bytes,
  length, and content hash are calculated from the parsed source rows. Its
  locator is either supplied by the caller or the content-addressed locator
  returned by an optional local `RawArtifactStore`. Delivery additionally
  requires the builder-held trusted capability created by independently
  re-verifying that normalized artifact through the store; an offline or
  JSON-only envelope is construction-only.

## Delivery protocol

1. Validate the detector receipt and both source row sets, then build the
   source-scoped envelope with all-false authority limits and explicit valid /
   transaction time.
2. Call the caller-supplied acknowledgement function (or the local durable
   acknowledgement adapter). A missing or false acknowledgement never advances
   the producer checkpoint.
3. Only after acknowledgement, perform a compare-and-swap checkpoint update
   using the expected generation and cursor. A stale CAS is reported as a
   conflict; the envelope and acknowledgement remain replayable.
4. External admission and checkpoint publication are separate retryable
   operations. This module makes no distributed-transaction claim.

Byte-identical retries use the same envelope, idempotency key, and record
identities. A reused idempotency key whose canonical envelope bytes differ is
rejected. A replay after durable acknowledgement returns the original receipt
and does not advance the checkpoint again.

## Contract invariants

- The envelope is validated against the pinned shared observation schema and
  `shared.contracts.healthcare_data_platform` lineage checks before delivery.
- Source release, artifact, receipt, activity, source scopes, and lineage all
  cross-reference exactly; observation order is deterministic.
- Raw artifact hashes and locators are retained in activity input refs and row
  values. The normalized artifact is immutable and source/release-bound.
- Observations are unpromoted source facts. Publication, mutation, deletion,
  release, production, runtime, current projection, and identity promotion
  remain disabled.
- Parse failures, hash/locator mismatch, acknowledgement rejection, replay
  conflicts, and stale checkpoint CAS fail closed without mutating the
  checkpoint.

## Verification and rollback

Focused tests cover valid system/facility parsing, malformed/duplicate rows,
strict malformed headers, immutable source-row fields, orphan links, envelope
hash and locator lineage, strict schema rejection, byte-identical replay,
conflicting replay, acknowledgement rejection, trusted-custody relabel
rejection, and checkpoint CAS ordering. Required checks are the focused pytest
suite, Ruff check/format, Python compileall, JSON Schema validation,
`git diff --check`, and a final self-review on the exact committed tree.

Rollback is additive: revert the docs, tests, and implementation commits in
reverse order. No raw object, queue, database, scheduler, runtime, or
production state is changed by this candidate.

Beads/Dolt was unavailable during dispatch; no Beads export is edited.
