# P1-24 HCU authority boundary

Tracking bead: `healthcare-toolkit-rrna.p1-24-hcu-snapshot-20260829`
Status: bounded, transport-neutral producer complete for local review.

The HCU snapshot producer accepts an already-authorized external manifest,
snapshot, checkpoint, and public-safe receipt. It verifies deterministic
SHA-256 fingerprints, retains HCU's source-local identifiers and ten ordered
ontology layers, and emits source-scoped `hdp.observation-envelope.v1`
observations. HCU remains authoritative for its manifest, source-record
ledger, review/held states, and checkpoint cursor.

The producer has no acquisition, decryption, filesystem custody, queue, or
database behavior. It does not interpret an HCU identifier as a Toolkit
entity, crosswalk records, establish canonical identity, write a current
projection, publish, release, or claim production authority. Every emitted
observation is explicitly `unpromoted_observation`; all shared envelope
authority limits are `false`. Review and held states remain in the
source-native value and never become an admission decision.

The external receipt is a prerequisite, not a promotion grant. A future
Toolkit adapter must separately review admission, source conflicts, and any
identity mapping. A receipt mismatch, unauthorized rights state, malformed
shape, duplicate source record, unknown layer, or invalid selector fails
closed. No source payload is logged by this module.

## Delivered files and verification

- `shared/acquisition/hcu_snapshot.py` — deterministic receipt verifier and
  envelope builder.
- `tests/test_hcu_snapshot.py` — receipt mismatch, rights/shape rejection,
  ten-layer preservation, source-native IDs/values, held state, checkpoint,
  and authority-boundary coverage.

Focused evidence on the final candidate:

```text
pytest -q tests/test_hcu_snapshot.py
# 6 passed
python3 -m compileall -q shared/acquisition/hcu_snapshot.py tests/test_hcu_snapshot.py
ruff check shared/acquisition/hcu_snapshot.py tests/test_hcu_snapshot.py
# All checks passed!
git diff --check
```

No broad suite, server, deployment, credential, push, or production action was
performed. The safe rollback is to stop invoking the additive producer and
revert the HCU commit range on a branch-local copy; no HCU custody or Toolkit
projection is mutated by this lane.
