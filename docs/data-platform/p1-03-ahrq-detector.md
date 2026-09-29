# P1-03 AHRQ official-release change detector

`shared/acquisition/ahrq_detector.py` is the source-specific boundary between
the AHRQ official release endpoint and the later raw-custody producer. It is
fixture-driven for this lane: no network request, object-store write, queue
admission, or current projection occurs here.

## Release semantics

The detector validates an official response fixture, resolves the source ID,
HTTPS URL, release ID, label, and timezone-aware publication timestamp, then
computes two independent SHA-256 values:

- `release_fingerprint` hashes canonical semantic release metadata, so a
  formatting-only response change is an `unchanged` release;
- `response_fingerprint` hashes the exact UTF-8 response bytes and remains in
  the receipt for custody and audit correlation.

The first observed release is `changed`. A subsequent matching release
fingerprint is `unchanged`, even when its raw response bytes differ. A changed
release receives both current and prior fingerprints. A failed probe produces
`failed_probe` and preserves the prior release/response receipt rather than
advancing a checkpoint or claiming a new release.

## Contract and recovery

The strict v1 schema is under
`contracts/healthcare-data-platform/ahrq/v1/ahrq-release.schema.json`; fixtures
cover changed, semantic no-op, failed probe, and invalid inputs. Duplicate
JSON keys, non-HTTPS endpoints, malformed release IDs/timestamps, metadata/body
mismatches, unknown fields, and cross-source prior receipts fail closed.

The receipt is evidence, not the raw artifact. P1-04 owns immutable byte
custody. If a downstream producer fails, retain this detector receipt and
retry the same fixture/release; do not delete or rewrite the prior receipt.
The candidate was implemented in the preserved local fallback worktree because
Grok Build repository egress was unavailable; the primary Luna worktree remains
untouched.
