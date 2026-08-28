# P0-14 lighthouse spike and Phase 1 decision

Status: bounded fixture-only proof on the Data MCP branch. This is an
evidence packet, not a live-source, queue, object-store, hosted, or production
activation. The receiver is an in-memory disposable test double.

## Exact candidate

- Tracking bead: `healthcare-toolkit-rrna.14`.
- Dispatch base: `61f97c8b6fdd99b5bc042fc820f95dc1c1e30eda`.
- Planned commits:
  - `6f3867e47899644be31d1b8454e8e58fa43cd288` —
    `test(live-data): add lighthouse source fixtures`.
  - `d7fe343af3cacad8b566e7bdad0aa804cb773b42` —
    `feat(live-data): implement bounded end-to-end spike`.
  - this decision packet — `docs(live-data): publish measurements and Phase 1 decision`.
- Branch: `codex/healthcare-toolkit-rrna.14-p0-lighthouse-phase1-fallback-20260828`.
- Repository mapping: the mission packet's `services/healthcare-data-mcp/**`
  paths are not present in this checkout, so the contract remains under
  `contracts/healthcare-data-platform/lighthouse/v1/**` and the authoritative
  test is `tests/test_healthcare_data_platform_lighthouse.py`.

## Fixtures and measured budgets

The fixtures model one approved-public AHRQ-shaped source without contacting
it. Fingerprints are intentionally deterministic fixture values; the changed
artifact hash is verified against its exact 85 UTF-8 bytes.

| case | release | response fingerprint | artifact | result |
| --- | --- | --- | --- | --- |
| no-op | `release:ahrq:2026-08-15` | `sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb` | no bytes | cheap receipt, no envelope |
| changed | `release:ahrq:2026-08-22` | `sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` | `artifact:lighthouse:ahrq-changed`, `sha256:83bd13b2f570b2b55d37cf6b8baaa3071a280d501bb48fd8441db98cbdd554cf`, 85 bytes | one envelope, three 32-byte chunks |
| failed probe | `release:ahrq:unknown` | `sha256:1111111111111111111111111111111111111111111111111111111111111111` | no bytes | failure receipt, prior state preserved |

Every fixture is capped at 4,096 bytes, 8 chunks, and 5 seconds. The contract
also rejects values above the platform-wide 131,072-byte, 128-chunk, and
60-second ceilings. The changed run measured 85 bytes and 3 chunks; an
interruption after chunk 1 measured 32 bytes and did not acknowledge. A retry
reused the same idempotency key, stored exactly one artifact/envelope, and a
third delivery returned `duplicate` without writing another copy.

## Invariants and evidence

- No-op emits a receipt only; it does not create custody or an envelope.
- Changed bytes are hash- and length-checked before acknowledgement. The
  envelope carries source, release, response, artifact, custody locator,
  idempotency, chunk, and record linkage.
- Interruption leaves the partial buffer unacknowledged; retry is safe and
  deterministic.
- Duplicate delivery is idempotent; an artifact-id collision with different
  bytes fails closed.
- Failed probes do not change receiver state or imply a new release.
- Publication, current projection, and production authority are explicitly
  false in every envelope.

Focused evidence on the final implementation head:

```text
python3 -m pytest -q tests/test_healthcare_data_platform_lighthouse.py
11 passed
ruff check ...
All checks passed!
ruff format --check ...
2 files already formatted
python3 -m compileall -q ...
passed
```

Draft 2020-12 validation covers the three valid fixtures and the bounded-budget
negative fixture. `git diff --check` is required before handoff. Class A
custody, replay, provenance, and bounded-resource invariants pass. Class C
wall-clock timing is a non-gating variance owned by Sol; no arbitrary soak or
provider-backed measurement is claimed.

## Phase 1 decision

**Decision: admit the next Phase 1 implementation wave, with the lighthouse
semantics frozen and live activation still approval-gated.** The implementation
order is:

1. source catalog and cadence state;
2. durable scheduler and AHRQ release detector;
3. content-addressed custody with interruption/collision recovery;
4. Toolkit observation-store migration and inbox/outbox admission;
5. AHRQ producer envelope plus Toolkit current/as-of projection;
6. adapter extraction, validation/quarantine, queue/lease controls, and
   correlated telemetry;
7. staging bundles and separately approved activation receipts.

No-op, changed, failed-probe, interruption, and duplicate semantics are the
compatibility boundary for later producers. The spike's in-memory receiver is
replaced by the durable admission protocol only after its own review and
focused crash-matrix evidence. Optional FAST and TiC branches remain recorded
as independent conditional work and cannot block the required AHRQ lighthouse
vertical slice.

## Rollback and activation boundary

Rollback is additive and local: revert the documentation, implementation, and
fixture commits in reverse order, preserving the dispatch base and all prior
contract-adoption work. No production rollback is claimed because no runtime,
credential, network, queue, object-store, or hosted state was touched. Any
live-source credentials, staging/production activation, push, or destructive
cleanup requires a separate human authorization and an exact-SHA activation
receipt.
