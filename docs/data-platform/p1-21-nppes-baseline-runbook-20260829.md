# P1-21 NPPES V2 full-baseline runbook

This runbook covers the source-plane producer in
`shared/acquisition/nppes`. It is an offline, transport-neutral seam: an
operator or later acquisition adapter supplies already-acquired byte streams.
The producer does not request CMS URLs, persist raw bytes, write a database,
publish a queue message, or promote an NPI to a Toolkit provider/facility.

## Required inputs

1. Start with the checked-in catalog registration for
   `source:nppes:registry`. Keep `rights_status=approved_public` and
   `enabled=true`; choose explicit `max_bytes`, `max_chunks`,
   `max_chunk_bytes`, and `max_seconds` bounds appropriate for the reviewed
   baseline. Do not derive identity from the display title.
2. Supply a release descriptor with a stable `release_id`, release label,
   source URL, final URL, HTTP status, publication timestamp, probe state, and
   evidence locator. A changed release must declare a present provider file;
   optional location, endpoint, reference, and deactivation files use an
   explicit `unavailable_public` or `not_applicable` state when they cannot be
   supplied. If an optional descriptor is omitted, the producer materializes
   an `unavailable_public` file receipt instead of silently dropping that kind.
3. Supply each present file as an iterable of non-empty UTF-8 byte chunks. The
   producer accepts CSV headers containing `NPI` (or `NPI Number`) and optional
   source row, reference, and deactivation-date columns. It retains only
   bounded row identity metadata; source names, addresses, taxonomy payloads,
   and other row fields are not put in receipts.

The five source file kinds are `provider`, `location`, `endpoint`,
`reference`, and `deactivation`. An NPI is retained as an exact ten-digit
string, including leading zeroes. It is a source identifier, not a canonical
Toolkit identity.

## Normal changed-release flow

```python
from shared.acquisition.nppes import NppesBaselineProducer

receipt = NppesBaselineProducer(catalog).produce(
    release,
    {"provider": provider_chunks, "location": location_chunks},
    row_sink=stage_source_identity,
)
```

The optional `row_sink` receives typed source-row metadata suitable for a
caller-owned staging ledger. It must not write a current projection or perform
identity promotion. The returned receipt includes per-file byte/chunk/row
counts and content/row fingerprints, plus a bounded sample of exact NPIs and
deactivation opportunities. A deactivation row is always
`review_required`; it is never an automatic deletion or status transition.

`receipt.state=changed` means all supplied present streams completed within
their bounds and matched their declared content digests. The receipt always
sets `current_projection_preserved=true` because this lane has no projection
authority.

## No-op, replay, and failure handling

- A `probe_state=not_modified` descriptor must carry HTTP 304 and no file
  descriptors. The producer returns `state=no_op` without iterating any file
  stream.
- Passing a prior successful (`changed` or `replayed`) receipt for the same
  semantic release suppresses the row sink while the streams are checked.
  Failed (`schema_drift` or `blocked`) receipts remain retryable and do not
  suppress a corrected retry. Matching file digests and counters return
  `state=replayed`; a changed file identity raises `NppesReplayConflictError`
  and must be quarantined by the caller.
- A failed probe returns `state=failed_probe` and never consumes a file.
- A malformed header, duplicate source row ID, malformed NPI, invalid
  deactivation date, content-hash mismatch, or row-shape change returns
  `state=schema_drift` with a stable `failure_code`.
- A stream that exceeds byte, chunk, parser-line, or monotonic-time bounds
  returns `state=blocked` and a file receipt with `state=interrupted`.
  Partial counters and hashes are evidence only; do not acknowledge or
  advance a source cursor from them.

For `schema_drift`, `blocked`, and replay-conflict outcomes, retain the
receipt and source/release identifiers, stop current projection work, and
retry only after the source descriptor or bounded stream is corrected. Never
replace a prior good receipt with a hand-edited JSON file.

## Evidence and privacy checks

The receipt is safe to pass to a review or admission queue only after the
caller verifies the source catalog rights and release locator. The producer
also rejects release, final, file, and HTTPS evidence URLs outside the hosts
approved by the catalog. Check that:

- `source_url`, `final_url`, file URLs, and evidence locators are HTTPS or
  approved locator forms and contain no credentials;
- `release_sha256`, each accepted `content_sha256`, and `receipt_sha256` are
  present and stable;
- exact NPIs and source row IDs remain source-native and are not used as
  canonical identity keys;
- deactivation samples are routed to a human/source-owner review queue;
- no raw row payload, response body, bearer token, or arbitrary exception text
  appears in persisted evidence.

## Offline verification

Run the focused suite and static checks from the repository root:

```bash
pytest -q tests/test_nppes_baseline_producer.py
ruff check shared/acquisition/nppes tests/test_nppes_baseline_producer.py
ruff format --check shared/acquisition/nppes tests/test_nppes_baseline_producer.py
pyright shared/acquisition/nppes tests/test_nppes_baseline_producer.py
python -m compileall -q shared/acquisition/nppes tests/test_nppes_baseline_producer.py
python -m jsonschema -i contracts/healthcare-data-platform/nppes/v2/fixtures/valid-changed.json contracts/healthcare-data-platform/nppes/v2/nppes-baseline.schema.json
```

The checked-in fixtures cover catalog/release validation, a changed baseline,
304 no-op, exact replay, and a large bounded stream that remains blocked.
The wave coordinator owns the repository-wide fast/full gates and any later
acquisition, custody, observation admission, or runtime integration.

## Rollback and ownership boundary

Rollback is a Git revert of the P1-21 additive range beginning at the mission
packet commit and ending at the runbook commit. Preserve existing source
receipts for audit; do not delete raw custody or alter a current projection as
part of rollback. A later independent reviewer must accept the exact
base-to-head diff before coordinator integration. This writer does not push,
deploy, or self-approve the producer.
