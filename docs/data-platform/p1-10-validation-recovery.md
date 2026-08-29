# P1-10 validation and quarantine recovery

P1-10 treats source drift as a fail-closed admission event.  A rejected
adapter batch or observation envelope is not allowed to update the current
projection.  The prior known-good projection remains queryable while an
operator repairs the source contract or producer.

## Decision states

`validate_drift` returns an `hdp.validation-report.v1` report.  `accepted`
means that the pinned observation contract (when applicable), row keys,
lineage keys, and configured distributions passed.  `rejected` means that at
least one `schema.*`, `row.*`, `key.*`, or `distribution.*` invariant failed.
The report contains counts, paths, and hashes only; it never contains source
row values.

`validate_and_project` invokes its caller-owned projection callback only for
an accepted candidate.  A rejected candidate returns
`current_projection_preserved=true`, creates a `QuarantineRecord`, and may
persist that record with `QuarantineStore`.  The store writes one immutable,
idempotent JSON record per deterministic quarantine identity.  Retrying the
same evidence at a different time returns a duplicate receipt and retains the
first recording time.

## Operator recovery sequence

1. Stop admission for the affected source/release and retain the exact
   validation report and quarantine receipt.  Do not delete or overwrite raw
   custody objects.
2. Verify the quarantine record's `envelope_sha256`, artifact hash, custody
   locator, reason codes, and report SHA against the immutable source receipt.
   Quarantine samples are structural summaries; retrieve source bytes only
   through the separately authorized raw-custody workflow.
3. Classify the finding: schema contract change, missing/duplicate/reordered
   row, source/release/artifact/lineage key conflict, or denominator/category
   distribution drift.  A release may not be relabeled as current merely to
   silence a finding.
4. Repair the adapter or publish an explicitly reviewed source-contract
   update.  Rebuild from the immutable raw artifact into a new envelope and
   rerun validation.  Keep the old quarantine record and prior current pointer
   for audit and rollback.
5. After the corrected envelope is accepted and durably acknowledged by the
   downstream admission seam, advance the producer checkpoint using its
   compare-and-swap precondition.  A failed or missing acknowledgement never
   advances the checkpoint.
6. If the corrected publication fails a later invariant, restore the prior
   current pointer and repeat from step 2.  This is a pointer/projection
   rollback; source evidence and quarantine receipts remain recoverable.

## Evidence and privacy

The quarantine record is limited to source/release/artifact identifiers,
SHA-256 hashes, a custody locator, issue codes, structural paths, bounded
counts, and an event timestamp.  Payload fields, names, addresses, provider
details, credentials, bearer tokens, and request headers are not stored in a
report or quarantine record.  The source-rights policy still controls whether
raw custody may be read or replayed.

The expected row count and distribution rules are source-specific policy, not
completeness claims.  A denominator change is a review signal and blocks only
the affected source admission until explained.  Unrelated source lanes may
continue, and no calendar soak is required.

## Rollback

The P1-10 range is additive.  Revert or omit the validation package and its
tests/runbook to restore the pre-P1-10 behavior; existing adapter, envelope,
raw-custody, and projection components remain available.  Do not remove
quarantine directories or raw objects as part of rollback.
