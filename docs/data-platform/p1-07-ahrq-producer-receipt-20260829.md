# P1-07 AHRQ producer receipt

Tracking bead: `healthcare-toolkit-rrna.p1-07`
Status: bounded implementation complete for local review; no push, deploy,
network acquisition, queue publication, Toolkit database write, or production
activation occurred.

## Reviewed commit chain

- Dispatch base: `e0d4efefbc980ce319f8d60e55efd9f4ae215567`.
- `2406d55` — `feat(ahrq): emit source observation envelopes`.
- `7b534af` — `test(ahrq): cover rejection replay and checkpoint CAS`.
- Final docs commit: this receipt plus the mission packet, with the exact
  final SHA recorded by the handoff command after commit.

The branch is
`codex/healthcare-toolkit-rrna.p1-07-ahrq-envelope-20260829` in the isolated
worktree supplied for this task. The implementation was performed directly by
Luna because Grok Build repository egress was unavailable.

## Delivered behavior

- `shared/acquisition/ahrq_observation_envelope.py` parses caller-provided
  AHRQ system and facility CSVs with strict CP-1252 decoding, unique canonical
  headers, required source-native columns, duplicate row-ID rejection, and
  cross-file `health_sys_id` linkage checks. Raw lexical cells remain strings.
- Verified P1-04 raw artifact locators, byte lengths, and `sha256:` claims are
  checked before parsing. A normalized-row artifact is hashed deterministically
  and can be finalized through the existing bounded `RawArtifactStore`.
- The emitted payload is validated by the pinned shared
  `hdp.observation-envelope.v1` JSON Schema and lineage validator. Release,
  artifact, receipt, activity, observation source scopes, row custody locators,
  hashes, replay IDs, and deterministic order are joined explicitly. All
  authority limits remain `false`.
- Acknowledgement is a required caller-owned durable seam. The local in-memory
  and atomic JSON adapters return the original acknowledgement on a
  byte-identical idempotent retry and reject a reused key whose canonical bytes
  differ.
- Checkpoint publication is a separate generation/cursor CAS after durable
  acknowledgement. Rejected acknowledgements and stale CASs leave the
  checkpoint unchanged. This is deliberately not a distributed transaction.

## Verification evidence

Run from the isolated worktree:

```text
pytest -q tests/test_healthcare_data_platform_ahrq_observation_envelope.py --tb=short
# 11 passed
ruff check shared/acquisition/ahrq_observation_envelope.py tests/test_healthcare_data_platform_ahrq_observation_envelope.py
# All checks passed!
pyright --level error shared/acquisition/ahrq_observation_envelope.py tests/test_healthcare_data_platform_ahrq_observation_envelope.py
# 0 errors (the default Pyright invocation reports only the environment's missing pytest stub warning)
python3 -m compileall -q shared/acquisition/ahrq_observation_envelope.py tests/test_healthcare_data_platform_ahrq_observation_envelope.py
git diff --check
```

The focused tests exercise the pinned JSON Schema through
`validate_observation_envelope`, including unknown-field rejection. The
normalized-custody test also reads the finalized bytes back through P1-04 and
checks the emitted content hash. The repository has no `scripts/check-fast.sh`
in this base, so no unavailable fast-gate command is claimed here.

## Self-review checklist

- [x] Worktree and branch match the dispatch target; no canonical/root files
  were edited.
- [x] Mission packet was written under `docs/data-platform` before code.
- [x] Required AHRQ system/facility columns and source-native linkage are
  fail-closed; no values are coerced into profile metrics.
- [x] Raw and normalized custody locators, lengths, and SHA-256 values are
  carried without local paths or credentials.
- [x] Pinned packet, bead, dispatch base, schema, lineage, replay, and
  all-false authority limits are validated before acknowledgement.
- [x] A durable acknowledgement is checked before any checkpoint CAS; no
  distributed-transaction claim is made.
- [x] Duplicate replay, conflicting replay, acknowledgement failure, stale
  CAS, source mismatch, malformed rows, orphan links, and hash mismatch have
  focused coverage.
- [x] Focused pytest, Ruff, Pyright, compile, schema, and diff checks were run
  on the candidate tree.
- [x] No push, deploy, database migration, queue publication, or Beads export
  edit was performed.

## Known limitations and rollback

This is a local, transport-neutral producer seam. It does not acquire AHRQ
bytes, call the Toolkit admission API, own a database transaction, publish an
outbox event, or coordinate a distributed checkpoint. Callers must provide
already verified P1-04 raw artifacts and a durable acknowledgement adapter.
The optional normalized artifact store is subject to P1-04's 131,072-byte,
128-chunk bound. Without a store or supplied normalized locator, the builder
can emit only an explicitly `verified: false` derived locator for offline
contract tests; delivery still rejects unverified raw input artifacts.

The safe rollback is to stop invoking this producer and revert the three P1-07
commits in reverse order on a branch-local copy. No raw object, checkpoint,
queue, database, scheduler, runtime, or production state is changed by these
commits. Preserve any already issued external acknowledgement and reconcile its
checkpoint separately; do not force a stale CAS.

## Handoff

```text
cd /tmp/.healthcare-data-mcp-p1-w6-source-20260829-worktrees/healthcare-toolkit-rrna.p1-07-ahrq-envelope-20260829
git status --short --branch
git rev-parse HEAD
git log --oneline --decorate -4
pytest -q tests/test_healthcare_data_platform_ahrq_observation_envelope.py --tb=short
```

Beads/Dolt was unavailable, and `.beads` exports were not edited. A reviewer
can inspect this receipt, the mission packet, and the exact branch tree before
any human-authorized integration into the Toolkit P1-06 admission seam.
