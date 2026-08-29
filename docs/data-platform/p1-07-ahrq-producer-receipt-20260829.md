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
- A post-review correction commit follows the planned three-commit chain. It
  closes parser custody verification, normalized-artifact binding, offline
  delivery, malformed-CSV handling, blank facility linkage, and cross-process
  acknowledgement races; its exact SHA is recorded by the handoff command.
- A second review correction follows that commit. It replaces the relabelable
  offline-prefix delivery guard with a builder-held capability backed by
  independently re-verified P1-04 custody, closes malformed-header error
  leakage, and freezes source-row fields so row hashes cannot become stale.
- A final integrity correction follows that commit. It holds the capability in
  a private weak registry, binds it to the original canonical envelope digest
  and material artifact/lineage references, and verifies caller-supplied row
  hashes against canonical fields before envelope construction.

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
- Public system/facility parsers verify and then parse the exact bytes returned
  by the custody claim check. Strict CSV mode rejects malformed records before
  row emission, and blank facility `health_sys_id` values fail before orphan
  comparison.
- Caller-supplied normalized artifacts are required to be verified, `system`
  role, and bound to the exact AHRQ source and detector release. A builder with
  no normalized store emits an explicitly marked offline locator for contract
  construction only; acknowledgement and checkpoint paths require a trusted
  capability and reject it even if mutable artifact or lineage labels change.
- A trusted capability is created only for normalized bytes checked through the
  bounded `RawArtifactStore`; delivery re-reads the immutable manifest and
  object bytes and matches source, release, role, hash, length, custody, and
  lineage references. The capability is held outside mutable payload keys and
  is bound to the original canonical envelope digest plus all material
  artifact/lineage references, so payload edits or capability grafts fail.
  Serialization to JSON intentionally drops the capability and must be rebuilt
  or rehydrated through trusted custody before delivery.
- The file acknowledgement adapter uses a sidecar OS lock around read,
  idempotency comparison, and atomic write, preserving records and rejecting
  cross-process conflicts safely.

## Verification evidence

Run from the isolated worktree:

```text
pytest -q tests/test_healthcare_data_platform_ahrq_observation_envelope.py --tb=short
# 17 passed
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
checks the emitted content hash. The review-regression tests cover exact-byte
public parser verification, blank facility linkage, strict malformed CSV,
normalized source/release/role/custody binding, offline delivery rejection,
and two-process acknowledgement preservation/conflict behavior. The repository
has no `scripts/check-fast.sh` in this base, so no unavailable fast-gate
command is claimed here.

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
- [x] Public parser custody claims cover the exact parsed bytes; normalized
  custody is source/release/role-bound and verified; offline-only envelopes
  cannot be delivered, acknowledged, or checkpointed.
- [x] Delivery requires an independently checked normalized-custody capability,
  so relabeling mutable artifact or lineage fields cannot promote offline or
  JSON-only envelope data.
- [x] Malformed CSV headers are normalized to `AhrqRowParseError`, and source
  row fields are copied/frozen so their row hashes remain valid.
- [x] The custody capability is private-registry-held, immutable, canonical
  digest-bound, and material-reference-bound; grafted or modified payloads are
  rejected before acknowledgement/CAS.
- [x] Caller-supplied source-row hashes are recomputed from canonical fields and
  unrelated hashes fail closed.
- [x] The file acknowledgement store is protected by an OS-level sidecar lock
  and has cross-process preservation and conflict regression coverage.
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
128-chunk bound. Without a store, the builder can emit only an explicitly
marked `verified: false` derived locator for offline contract tests; delivery
rejects it because no trusted capability exists. A caller-supplied normalized
locator must already be P1-04-verified and exact-match the deterministic bytes,
source, release, and `system` role; delivery additionally requires the caller
to provide the corresponding `RawArtifactStore` so custody can be checked
independently.

The safe rollback is to stop invoking this producer and revert the post-review
correction followed by the three planned P1-07 commits in reverse order on a
branch-local copy. No raw object, checkpoint,
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
pytest -q tests/test_healthcare_data_platform_ahrq_observation_envelope.py tests/test_healthcare_data_platform_ahrq_detector.py tests/test_healthcare_data_platform_raw_custody.py tests/test_healthcare_data_platform_contract_adoption.py --tb=short
```

Beads/Dolt was unavailable, and `.beads` exports were not edited. A reviewer
can inspect this receipt, the mission packet, and the exact branch tree before
any human-authorized integration into the Toolkit P1-06 admission seam.
