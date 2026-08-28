# P1-01 source catalog and cadence

Status: reviewed-candidate implementation for the durable source-plane
foundation. The catalog is declarative and fixture-only; it does not poll a
network, enqueue work, use credentials, or mutate runtime state.

## Exact candidate

- Tracking bead: `healthcare-toolkit-rrna.1` (P1-01 under the delivery plan's
  `healthcare-toolkit-rrna` parent).
- Dispatch base: `0f22718f38d43e781d791501738a2172e75246b7`.
- Planned commits:
  - `7e52803` — `feat(catalog): add source and cadence models`.
  - `214a5ff` — `test(catalog): cover change-mode states`.
  - this packet — `docs(catalog): document source registration`.
- Branch: `codex/healthcare-toolkit-rrna.p1-01-catalog-cadence-fallback-20260828`.
- Execution path: Grok Build repository egress remains unavailable and the
  Luna channel was idle; the primary Luna worktree was preserved and this
  isolated fallback worktree completed the scoped implementation. No Grok
  readiness retry was attempted.

## Frozen catalog contract

Each registration has a stable `source_id`, source family, official URL and
release locator, one explicit change mode (`etag`, `last_modified`,
`release_metadata`, or `content_hash`), an interval/jitter/grace budget, owner,
enabled flag, and rights status. Enabled sources must be `approved_public`;
pending or blocked sources remain inert. The fixture records AHRQ lighthouse
and CMS PDC as enabled public sources and FAST as a disabled pending optional
source, so FAST cannot hold the required AHRQ path.

Poll state is explicit: `never_run`, `succeeded`, `no_op`, `failed`, `missed`,
`backfill_pending`, `backfill_running`, or `blocked`. It carries generation,
failure count, due/attempt/success timestamps, and last release identity. A
backfill state must carry a bounded release range; other states cannot carry
one. The scheduler emits deterministic, source-sorted poll intents only for
enabled, approved, due sources and labels cadence, retry, or backfill intent.

## Evidence

- Focused catalog tests: `7 passed`.
- Existing source-catalog regression tests: `7 passed`.
- Ruff check and format check: passed.
- In-memory Python compile check: passed.
- Draft 2020-12 schema validation covers the valid catalog/poll-state fixtures
  and rejects the invalid change-mode/unbounded-cadence fixture.
- Exact-base `git diff --check`: passed.

The transition tests prove deterministic scheduling, no-op and changed release
semantics, failure preservation of the last good release, missed-run grace,
bounded backfill, duplicate-ID rejection, explicit rights gating, and strict
duplicate-JSON-key rejection. No scheduler, queue, source download, or
production mutation is claimed.

## Next dependency and rollback

P1-02 (minimal durable scheduler) and P1-03 (AHRQ change detector) may depend on
this catalog contract after exact reviewed integration. The local rollback is
to revert the three focused commits in reverse order, preserving the P0-14
lighthouse and P0-15 bundle. Staging/production activation, credentials,
remote pushes, and destructive cleanup remain separately human-authorized.
