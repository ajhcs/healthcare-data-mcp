# P1-19 CMS Provider of Services producer

Tracking task: `hdp-p1-19-cms-pos-20260829`

## Goal

Provide a transport-neutral, release-aware CMS Provider of Services (POS)
source producer. The producer must bind one approved public CMS distribution to
stable release and distribution identities, stream bounded source-native
facility rows and identifiers through the shared adapter SDK, and return a
secret-free receipt that makes changed, no-op, drift, failed-probe, and replay
outcomes explicit.

## Problem

Existing CMS POS consumers treat a date-specific download as a convenient CSV
file. That loses the official release/distribution boundary, cannot safely
distinguish a conditional no-op from a changed artifact, and gives no
fail-closed response to same-release content or metadata drift. It also risks
turning source-native provider identifiers into canonical Toolkit identities
before an independently reviewed admission step.

## Scope and authority boundary

This lane adds only the CMS POS source contract, producer, synthetic fixtures,
focused tests, and operator documentation. The caller supplies resolved
release metadata, a conditional response, and an iterable of source bytes. The
producer validates the catalog registration, consumes bytes with the existing
bounded adapter SDK, parses CSV rows without network or filesystem I/O, and
returns source-scoped rows plus deterministic custody/receipt identities.

In scope:

- strict `source:cms:pos` release and distribution metadata;
- stable release, distribution, content, row, artifact, and receipt
  fingerprints;
- conditional `304` no-op handling and exact replay identity;
- same-release metadata/content drift quarantine (fail closed);
- preservation of source-native `PRVDR_NUM` identifiers and all CSV fields;
- explicit stream acknowledgement/bounds and adapter catalog rights checks;
- versioned JSON Schema and non-sensitive changed/no-op/drift/replay fixtures;
- focused tests for parsing, provenance, bounds, state transitions,
  idempotency, malformed input, and schema validation;
- an operator runbook covering preview, rollback, and authority limits.

Out of scope:

- HTTP clients, CMS credentials, source discovery, network/filesystem
  acquisition, object-store or database writes, queues, scheduling, or
  deployment;
- canonical identity/entity matching, metric/fact admission, profile
  population, publication, or any cross-source join;
- edits to existing CMS loaders, shared adapter SDK modules, or shared
  admission cores;
- logging source payloads or exposing unredacted facility data in receipts.

## Acceptance criteria

- A catalog entry must identify the CMS source URL and release locator, have
  `approved_public` rights, and use an explicit bounded change mode.
- A release must carry a stable release ID, source period, source/landing URLs,
  publication timestamp, and semantic release fingerprint. A distribution must
  carry a stable distribution ID, HTTPS URL, media type, and content
  fingerprint; validators such as ETag/Last-Modified remain metadata only.
- Conditional `304` or an exact prior identity yields an acknowledged no-op
  without rows or cursor advancement. A `2xx` changed response yields rows only
  after bounded stream completion and strict CSV validation.
- If a previously observed release ID has a different semantic release or
  content identity, the result is `drift`, unacknowledged, and contains no
  admitted rows. Drift must fail closed rather than silently replace custody.
- Replaying identical release/distribution bytes produces the same row,
  artifact, and receipt identities and is classified `replayed` when the prior
  receipt is supplied; no duplicate side effect is implied.
- Rows preserve the exact source-native header names and values, use
  `PRVDR_NUM` as an opaque source identifier, and expose no canonical entity ID.
  Duplicate/missing identifiers, malformed CSV, reserved canonical fields, and
  over-bound streams are rejected or quarantined.
- Serialized changed, no-op, drift, and replay outputs validate against the
  pinned `hdp.cms-pos-producer.v1` schema. Fixtures contain synthetic values
  only and receipts contain no payload bytes.
- The module remains deterministic and transport-neutral: no HTTP, database,
  queue, object-store, secrets, or payload logging are introduced.

## Traceability

Affected PER IDs: N/A — this producer emits source-scoped acquisition material
and does not populate the profile engine.

Affected DTR IDs: N/A — no UI, route, component, or design-token changes.

## Planned commits

1. `docs(cms-pos): add P1-19 mission packet`
2. `feat(cms-pos): add release-aware source producer and contract`
3. `test(cms-pos): cover release states and source-row custody`
4. `docs(cms-pos): add operator preview and rollback runbook`

The mission packet is committed before implementation. The remaining work is
kept near a three-commit delivery chain (feature/tests/docs).

## Verification and handoff

Run the focused CMS POS tests, Ruff check/format, strict type checking,
`compileall`, JSON Schema validation for every fixture, and `git diff --check`
against base `8e042b5f0548a9410616a5fc199ab09cb352f18d`. The wave coordinator
owns the repository-wide `scripts/check-fast.sh` gate against the coherent
candidate. Produce a worktree handoff with exact base/head SHAs, commit chain,
changed paths, commands/results, limitations, and rollback range. No Grok
retry, Beads/Dolt wait, push, deployment, credential, or production action is
authorized by this lane.

## Rollback

The producer and contract are additive. Omit or revert the reviewed P1-19
commit range, disable any future caller, and retain any external custody for
audit; do not delete source records or mutate existing CMS loaders, queues,
databases, or canonical projections. Re-enable only with the matching contract
and producer commits.

## Risks and dependencies

Risk is medium: confusing a CMS release change with an unchanged distribution,
or treating `PRVDR_NUM` as a Toolkit identity, could misattribute facility
observations. Stable semantic hashes, bounded SDK streaming, strict source
row validation, explicit drift quarantine, and source-only authority flags
keep the boundary fail-closed.

Dependencies are the existing shared adapter contracts/bounds at base
`8e042b5` and the coordinator's independent review. The CMS official release
and distribution are represented by caller-supplied metadata and synthetic
fixtures; this checkout does not establish production availability.

## Owner and status

- Owner: Luna Max direct writer
- Worktree: `/tmp/.healthcare-data-mcp-p1-w13-source-20260829-worktrees/p1-19-cms-pos`
- Branch: `codex/healthcare-toolkit-rrna.p1-19-cms-pos-20260829`
- Base: `8e042b5f0548a9410616a5fc199ab09cb352f18d`
- Status: implementation in progress
