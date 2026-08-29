# P1-26 mission packet: hospital discovery v3

## Objective

Deliver the hospital-discovery acquisition seam as a sealed, hospital-scoped
v3 manifest. The lane records source URL/probe receipts and candidate evidence
for later review; it does not promote a hospital, owner, system membership, or
other identity claim to authority.

## Scope

In scope are `shared/acquisition/hospital_discovery.py`, the corresponding
hospital-discovery v3 JSON Schema and sealed fixture, focused contract tests,
and this packet. The contract requires absolute HTTP(S) source URLs, explicit
`url_state`, `probe_state`, and `candidate_state` values, and strict rejection
of unknown fields, duplicate identifiers, unregistered source references, and
invalid state combinations.

Every registered evidence row is fixed to `authority_state=
non_authoritative` and `owner_promotion_state=outstanding`. URL presence,
successful probing, naming similarity, and candidate status are not ownership
authority and must not be used as a substitute for owner review.

Out of scope are central discovery/profile wiring, health-system-profiler
changes, live probes, credentials, deployment, owner adjudication, roster
aggregation, and any downstream metric or score.

## Dependencies and handoff

The implementation depends only on existing shared atomic JSON writing and the
standard JSON Schema/Python test dependencies. A later owner-review lane may
consume the sealed manifest and independently perform identity, scope, and
ownership promotion. That promotion is a separate dependency and remains
outstanding at this handoff.

Expected consumer inputs are the v3 manifest schema and fixture plus the typed
loader/validator. No consumer may infer authority from a candidate row.

## Rollback

Rollback is a Git revert of the implementation commit
`c0c61d2` (`feat(hospital): add sealed discovery v3 evidence contract`) and
this packet commit, leaving the parent tree at `44354ed`. Preserve any copied
manifest receipts for audit; do not replace them with newly probed bytes.
Rollback does not authorize deleting source custody or changing owner-review
status.

## Evidence and verification

Implementation evidence at the prior commit:

- `tests/test_hospital_discovery.py`: 4 focused tests passed.
- Ruff passed for the touched module and tests.
- Python compilation passed for the touched module.
- The sealed fixture passed Draft 2020-12 JSON Schema validation.
- `git diff --check` passed and the implementation worktree was clean.

The final documentation commit must be verified with the same focused checks;
no broad suite, live probe, WTB wait, deployment, or credential operation is
required for this packet.

## Acceptance criteria

1. The manifest is sealed and explicitly hospital-scoped.
2. URL, probe, and candidate states are explicit and fail closed.
3. Evidence registration is receipt-oriented, non-authoritative, and cannot
   claim owner promotion.
4. Schema, fixture, focused tests, and rollback instructions are committed.
5. The handoff identifies the exact final commit and leaves the worktree clean.
