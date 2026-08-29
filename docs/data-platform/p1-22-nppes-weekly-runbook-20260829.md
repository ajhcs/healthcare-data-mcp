# P1-22 NPPES weekly change producer runbook

Status: source-local candidate. This runbook describes the bounded producer in
`shared/acquisition/nppes/weekly.py`; it does not authorize a network probe,
download, queue publish, database write, Toolkit projection update, canonical
identity decision, or production activation.

## Inputs and boundaries

The caller supplies:

- an enabled `NppesCatalogEntry` (or `NppesCatalog`) for
  `source:nppes:registry` with `approved_public` rights and explicit
  `NppesStreamBudget` limits;
- an `NppesWeeklyRelease` containing the week window, release sequence,
  source/final/evidence locators, probe/status metadata, and file descriptors;
- one bounded iterable of `bytes` per declared present file; and
- optional prior successful receipt and source-local current-state values.

The producer reuses the reviewed NPPES V2 catalog, file descriptor, file
receipt, deactivation opportunity, and stream-budget contracts. No new
database schema or migration is required in this lane. Returned observations
contain exact source identifiers and fingerprints only; raw CSV fields are not
retained.

## Normal weekly execution

Construct a changed release with at least one present provider, location,
endpoint, reference, or deactivation descriptor. Pass fresh byte iterables to
`produce_nppes_weekly`. The parser accepts the NPI column as `NPI` or `NPI
Number`, and recognizes source-row, effective/update, deactivation, operation,
and reference/endpoint/location aliases. Every row must have an exact ten-digit
ASCII NPI; source-row IDs and dates are validated and row/file digests are
included in the receipt.

Each accepted row is emitted to the optional row sink as a
`NppesWeeklyObservation`. Its key is source-local (`file_kind:npi`), never a
Toolkit provider or facility identity. A deactivation row produces a bounded
`review_required` `NppesDeactivationOpportunity`; it does not delete a record,
alter a projection, or grant authority.

## Merge and ordering rules

For each source-local key, competing observations are ordered by:

1. `effective_date`;
2. `release_sequence`;
3. `source_row_id`;
4. file kind and row number as deterministic tie-breakers.

The greatest event is the applied candidate. Rows arriving below prior
source-local state are retained as late evidence and cannot overwrite that
state. Rows arriving below the immediately preceding row for the same key are
counted as out of order; the same ordering produces the same winner regardless
of input order. A row absent from a weekly file is not a deletion or an
implicit deactivation. The caller owns persistence of the full source-local
state and event history; receipts expose bounded applied/late samples and
counters.

## Missingness, probes, and retries

- An HTTP 304 (`not_modified`) returns `no_op` without consuming any stream.
- A failed probe returns `failed_probe` and does not advance source-local
  state.
- An omitted optional descriptor materializes an
  `unavailable_public` receipt with `file_descriptor_omitted`.
- A declared `unavailable_public` or `not_applicable` descriptor materializes
  its explicit state and must not receive a stream.
- A present descriptor without a stream is `not_consumed` and the weekly
  outcome is `blocked`.
- Invalid rows, duplicate source-row IDs, content-hash mismatches, and unsafe
  source data produce `schema_drift`; byte/chunk/time limit violations produce
  `blocked`. Neither outcome advances a cursor or projection.
- Only a prior `changed` or `replayed` receipt with the same release digest
  suppresses replay row emission. `schema_drift` and `blocked` receipts remain
  retryable. A successful replay with different file fingerprints raises
  `NppesWeeklyReplayConflictError` for caller quarantine.

All release, final, file, and HTTPS evidence hosts must match the catalog's
approved NPPES source or release locator hosts. Unknown fields and malformed
locators fail closed. Receipts preserve source URL, final URL, HTTP status,
probe state, release digest, per-file state/fingerprints, and
`current_projection_preserved=true`.

## Verification

From the isolated worktree, run the focused checks (the W14 coordinator owns
the repository-wide fast gate):

```bash
pytest -q tests/test_nppes_weekly_producer.py
ruff check shared/acquisition/nppes/weekly.py tests/test_nppes_weekly_producer.py
ruff format --check shared/acquisition/nppes/weekly.py tests/test_nppes_weekly_producer.py
pyright shared/acquisition/nppes/weekly.py tests/test_nppes_weekly_producer.py
python3 -m compileall -q shared/acquisition/nppes/weekly.py tests/test_nppes_weekly_producer.py
git diff --check 9bd57dd532a3db0e5a04cb1e2c7878730d2fa862 -- \
  shared/acquisition/nppes/weekly.py tests/test_nppes_weekly_producer.py \
  docs/data-platform/p1-22-nppes-weekly-mission-packet-20260829.md \
  docs/data-platform/p1-22-nppes-weekly-runbook-20260829.md
```

The focused tests cover changed updates, deactivation review, deterministic
out-of-order/late merges, explicit optional-file missingness, no-op and failed
probes, successful replay suppression, retry after failed receipts, bound
interruptions, approved-host binding, malformed rows, and receipt round trips.

## Rollback and ownership

Rollback is a Git revert of the P1-22 mission packet, weekly module, focused
tests, and this runbook. Preserve caller-owned source receipts and state; do
not delete custody or alter a current projection. Source acquisition, custody
persistence, observation admission, canonical promotion, runtime wiring, and
production release remain coordinator-owned.
