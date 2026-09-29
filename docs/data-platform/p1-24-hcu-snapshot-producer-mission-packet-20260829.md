# P1-24 HCU snapshot producer

Tracking bead: `healthcare-toolkit-rrna.p1-24-hcu-snapshot-20260829`

## Goal

Accept an already-authorized Healthcare Universe (HCU) manifest and snapshot
as an external, receipt-bound source and emit source-scoped
`hdp.observation-envelope.v1` observations. Preserve HCU's source-local IDs,
ten ontology layers, temporal values, and review/held states without granting
Toolkit identity, current-projection, publication, or production authority.

## Scope and boundary

This lane is transport-neutral. It does not locate, download, decrypt, store,
publish, or promote HCU data. The caller supplies JSON-compatible manifest and
snapshot mappings plus a public-safe external receipt. The producer verifies
their content fingerprints, rights state, bounded shape, and source lineage,
then returns a validated shared envelope. HCU remains the authority for its
source-record ledger and snapshots; a future Toolkit adapter must make a
separate reviewed admission decision.

In scope:

- strict external manifest, artifact receipt, ontology-layer, source-record,
  and review-state value objects;
- deterministic snapshot fingerprinting and envelope construction;
- preservation of source IDs, source values, ten layer order, review states,
  checkpoint identity, and replay/idempotency lineage;
- explicit source-scoped and `unpromoted_observation` envelope authority;
- focused tests for receipt mismatch, source-state preservation, malformed
  input, replay, and contract validation.

Out of scope:

- network or filesystem acquisition, HCU implementation changes, queue/DB
  writes, Toolkit canonical identity or fact admission, credentials,
  deployment, production activation, and pushes;
- interpreting HCU identifiers as Toolkit entities or silently crosswalking
  records across sources.

## Acceptance criteria

- A supplied manifest and snapshot must have a matching external receipt and
  deterministic SHA-256 content fingerprints; mismatch fails closed.
- Exactly ten non-empty ontology layers are retained in source order and are
  carried as source metadata, not canonical ontology terms.
- Every source record retains its opaque source ID, layer ID, review state,
  source-native value, valid date, and source selector. Review/held/conflict
  states are represented without promotion.
- Every emitted observation is source-scoped, linked to the external receipt,
  has a deterministic source-record identity key, and uses the pinned v1
  envelope validator.
- The envelope records artifact custody, manifest/snapshot/checkpoint hashes,
  deterministic replay identity, and all authority limits as false.
- No source payload is logged or sent anywhere by this module.

## Planned commits

1. `docs(hcu): add P1-24 mission packet`
2. `feat(hcu): add snapshot producer`
3. `test(hcu): preserve source and review states`
4. `docs(hcu): record upstream authority boundary`

The first documentation commit is required by the mission-packet process; the
remaining commits stay near the delivery plan's three-commit implementation
chain.

## Verification and handoff

Run the focused HCU tests, Ruff check/format, Pyright, compileall, the
observation-envelope schema check, and `git diff --check` against the exact
base. The wave coordinator owns the single `scripts/check-fast.sh` run for a
coherent candidate. Generate a `worktree-bootstrap handoff` receipt containing
the exact base/head SHAs, diff, tests, limitations, and rollback range. No
push, deployment, credentials, or production action is authorized.

## Rollback

Revert or omit the reviewed P1-24 commit range. The producer is additive and
does not mutate HCU custody, a Toolkit projection, or a production database.

## Risks and known dependency

The HCU repository/snapshot is an upstream dependency and is not available in
this clean Data MCP checkout. Tests use synthetic, non-sensitive mappings and
exercise the receipt-preserving seam only. This lane must not infer missing
HCU semantics or claim a production snapshot was imported.

Owner: Luna Max direct writer

Status: implementation in progress
