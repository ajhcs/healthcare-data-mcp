# P1-10 Validation, drift, and quarantine

Tracking bead: `healthcare-toolkit-rrna.p1-10-validation-drift-20260829`

## Goal

Add a source-plane validation seam that detects schema, row, key, and
distribution drift in source adapters and `hdp.observation-envelope.v1`
payloads. Unsafe batches must be rejected into structured, redacted,
recoverable quarantine evidence while the last known-good current projection
remains unchanged.

## Execution chain

Sol 5.6 Medium program orchestrator -> persistent Luna Max controller ->
Luna Max direct implementation (Grok Build repository egress is unavailable)
-> writer self-review and focused gates -> distinct non-writer Luna Max review
-> coordinator merge recommendation. No network, production database,
credentials, deployment, push, or production activation is in scope.

## Dependencies and scope

- Exact base: `6dd23fae2df6d5daf5fde729cc3ddcad17e6ad8e` (P1-09 adapter SDK).
- Depends on P1-09 adapter interfaces and P1-07 observation-envelope and raw
  custody contracts already present at the base.
- In scope: typed drift reports, schema/row/key/distribution checks, bounded
  redacted reject samples, custody-bound quarantine records, and a
  fail-closed projection-preservation decision.
- Out of scope: source acquisition/network I/O, database writes, credentials,
  deployment, identity promotion, or changing source rights.

## Acceptance criteria

- Schema drift detects malformed/unknown envelope fields through the pinned
  v1 validator and returns a stable, secret-free issue code and path.
- Row drift detects duplicate/missing/reordered observation IDs, malformed
  source rows, and row-count changes against an explicit baseline.
- Key drift detects source/release/artifact/lineage/checkpoint identity
  changes and rejects conflicting idempotency reuse.
- Distribution drift uses explicit denominator/cardinality and ratio bounds;
  zero/empty and non-finite values fail closed rather than being inferred.
- Quarantine samples retain only bounded metadata, hashes, paths, and safe
  structural summaries; source payloads, PHI, secrets, and bearer material are
  never stored in the report.
- Failed validation produces `reject`/`quarantine` evidence and an explicit
  `current_projection_preserved=true` result. No successful current projection
  callback is invoked for rejected input.
- A valid unchanged or replayed envelope remains accepted and deterministic.

## Planned commits

1. `feat(validation): add drift and quarantine contracts`
2. `test(validation): cover schema and denominator drift`
3. `docs(validation): document recovery workflow`

## Likely files

- `shared/validation/drift.py`
- `shared/validation/__init__.py`
- `tests/test_healthcare_data_platform_validation.py`
- this mission packet and the recovery runbook in `docs/data-platform/`

## Verification

```bash
pytest -q tests/test_healthcare_data_platform_validation.py
ruff check shared/validation tests/test_healthcare_data_platform_validation.py
ruff format --check shared/validation tests/test_healthcare_data_platform_validation.py
pyright shared/validation tests/test_healthcare_data_platform_validation.py
python -m compileall -q shared/validation tests/test_healthcare_data_platform_validation.py
python -m jsonschema -i contracts/healthcare-data-platform/observation/v1/fixtures/valid-observation-envelope.json contracts/healthcare-data-platform/observation/v1/observation-envelope.schema.json
git diff --check 6dd23fae2df6d5daf5fde729cc3ddcad17e6ad8e..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run once by the wave
coordinator on the coherent Toolkit/Data MCP wave candidate, not per this
documentation or contract branch.

## Rollback

Revert or omit this additive three-commit range. Existing adapter,
observation-envelope, raw-custody, and current-projection consumers remain
available. Quarantine is recoverable evidence; this lane never deletes source
objects or mutates a production projection.

## Risk, privacy, and evidence

Risk is medium: under-detection can admit drift, while over-detection can
pause a source. Checks are explicit, bounded, and source-scoped. Quarantine
records are canonical JSON containing only allow-listed identifiers, hashes,
counts, issue codes, and redacted structural samples. Source rights and
replay/checkpoint semantics remain owned by their existing contracts.

## Handoff requirements

Before handoff, verify the managed task/worktree binding, self-review the exact
base/head diff, run the focused checks above, generate the authoritative
`worktree-bootstrap handoff` receipt, and report the final head SHA,
handoff-path SHA, known limitations, and rollback range. Do not push.

Owner: Luna Max direct writer
Status: implementation in progress
