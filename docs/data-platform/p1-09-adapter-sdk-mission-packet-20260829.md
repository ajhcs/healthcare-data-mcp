# P1-09 Adapter SDK extraction

Tracking bead: `healthcare-toolkit-rrna.p1-09-adapter-sdk-20260829`

## Goal

Extract a source-neutral, bounded adapter SDK from the proven AHRQ release
detector so future public-source producers can share conditional HTTP,
catalog, cursor, fingerprint, streaming, and rate-limit semantics without
coupling acquisition to network or production state.

## Problem

The AHRQ producer currently owns release metadata, response fingerprinting,
and source-specific transition details in one module. Later producers need the
same safety properties, but copying those rules would create drift in
conditional requests, cursors, byte budgets, and replay evidence.

## Scope

- Define typed interfaces and small value objects for catalog registrations,
  conditional HTTP probes, source cursors, and release/response fingerprints.
- Keep the AHRQ detector behavior and receipt compatibility intact while
  exposing an adapter-facing implementation through the shared SDK.
- Add bounded byte streaming and deterministic rate-limit helpers that do not
  perform network I/O themselves and never retain source payloads.
- Add a producer conformance suite proving changed/no-op/failed transitions,
  conditional request semantics, cursor monotonicity, bounded streaming, and
  rate-limit behavior.

## Out of Scope

- Network acquisition, HTTP client configuration, database or queue writes,
  deployment, credentials, production activation, or source-specific CMS/PDC
  adapters.
- Changes to the Healthcare Data Platform envelope, raw custody, admission,
  current projection, or source-rights policy.

## Acceptance Criteria

- Adapter interfaces are importable from a stable `shared.adapters` module and
  have no source-specific or network-client dependency.
- AHRQ release metadata and detector receipts can be adapted without changing
  existing detector outputs or replay/checkpoint semantics.
- Conditional requests distinguish `not_modified` from a changed response and
  fail closed when an ETag/last-modified validator is malformed.
- Catalog and cursor values validate source identity, HTTPS rights, bounded
  pagination, and monotonic checkpoint advancement.
- Streaming stops at configured byte/chunk/deadline bounds, reports an exact
  digest/byte count, and does not acknowledge incomplete data.
- Rate limiting is monotonic-clock based, bounded, and testable with injected
  time/sleep functions; it does not log tokens or payloads.
- Existing AHRQ detector and producer tests remain green.

## Traceability

Affected PER IDs:

- N/A — this lane is a backend acquisition SDK; it does not populate or expose
  profile-engine facts.

Affected DTR IDs:

- N/A — no UI, route, component, design-token, table, chart, map, or
  accessibility surface changes.

## Files Likely Touched

- `shared/adapters/__init__.py`
- `shared/adapters/contracts.py`
- `shared/adapters/bounds.py`
- `shared/acquisition/ahrq_detector.py` (compatibility adapter only if needed)
- `tests/test_adapter_sdk.py`
- Existing AHRQ detector tests only when an import/export compatibility check is
  required.

## Verification

```bash
pytest -q tests/test_adapter_sdk.py tests/test_healthcare_data_platform_ahrq_detector.py
ruff check shared/adapters tests/test_adapter_sdk.py shared/acquisition/ahrq_detector.py
ruff format --check shared/adapters tests/test_adapter_sdk.py shared/acquisition/ahrq_detector.py
pyright shared/adapters tests/test_adapter_sdk.py shared/acquisition/ahrq_detector.py
python -m compileall -q shared/adapters shared/acquisition/ahrq_detector.py tests/test_adapter_sdk.py
python -m jsonschema -i contracts/healthcare-data-platform/ahrq/v1/fixtures/official-release-changed.json contracts/healthcare-data-platform/ahrq/v1/ahrq-release.schema.json
git diff --check 72dfba40078c187d990e96ed99b9eef549ad9494..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run once by the wave
coordinator against the coherent W8 candidate, not per documentation or
contract commit.

## Rollback

The coordinator can fast-forward only this reviewed branch range and revert
the three P1-09 commits as one unit. The SDK is additive; rollback leaves the
existing AHRQ detector and producer modules available and does not touch raw
custody, database state, credentials, or deployed services.

## Risk and Privacy

Risk is medium: shared interfaces can accidentally weaken source validation or
permit unbounded input. All values are validated, all byte/chunk/deadline
limits are explicit, and tests cover malformed validators, cursor rewinds,
partial streams, and retry behavior. No PHI, source payload, bearer token, or
credential is logged or persisted by this lane. Source rights remain owned by
the catalog and producer-specific policy.

## Dependencies and Blockers

- Depends on the reviewed P1-08 evidence/consumer seam and the existing P1-03
  detector contract; this branch starts at the exact P1-W8 base SHA.
- Grok Build repository egress is unavailable, so implementation is direct
  Luna execution per the acceleration mode.
- Beads synchronization is unavailable because the local Dolt service is not
  reachable; the exact bead and packet remain recorded here for handoff.

## Owner and Status

- Owner: Luna Max direct writer
- Status: implementation in progress
