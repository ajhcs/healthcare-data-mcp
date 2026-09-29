# P1-22 NPPES weekly change producer

Tracking task: `hdp-p1-22-nppes-weekly-20260829`

Status: bounded local weekly-change producer candidate; no network acquisition,
queue publication, Toolkit database write, canonical identity promotion,
deployment, or production activation.

## Goal

Build a source-scoped NPPES weekly producer that accepts caller-supplied,
already-acquired update and deactivation streams and emits deterministic
source-local merge evidence. The producer preserves source URL, probe/status
metadata, weekly release identity, exact NPI and source-row identifiers,
per-file fingerprints, late/out-of-order handling, and deactivation review
opportunities without treating an NPI as a Toolkit provider or facility.

## Frozen inputs and scope

- Exact base: `9bd57dd532a3db0e5a04cb1e2c7878730d2fa862`.
- Reviewed upstream baseline: `fe11fc7f274d0dd704cffa37db88269a89046e16`.
- Source identity: `source:nppes:registry`; the enabled, approved-public
  catalog registration and its approved NPPES hosts are required.
- The caller supplies a catalog, weekly release descriptor, bounded byte
  iterables, and optional prior source-local state. This lane performs no HTTP
  request, credential handling, source download, queue publication, database
  write, projection update, identity resolution, or production release.
- In scope: strict weekly descriptor/receipt/observation values in the new
  `shared/acquisition/nppes/*weekly*` module; reuse of NPPES V2 catalog,
  file, source-row, deactivation, and `NppesStreamBudget` contracts; exact
  source-native identifiers; deterministic source-local merge; replay and
  failure classification; explicit missingness; fixtures-by-test data; and an
  operator runbook.
- Out of scope: canonical provider/facility promotion, NPI-to-Toolkit
  crosswalks, completeness claims, raw payload persistence, deletion of raw
  custody, runtime/deployment wiring, live source probes, shared registry
  edits, or changes to the reviewed baseline module.

## Producer protocol

1. Resolve exactly one enabled `source:nppes:registry` catalog with
   `rights_status=approved_public`, explicit byte/chunk/time bounds, and
   approved source/release hosts. Release source, final, file, and HTTPS
   evidence URLs must remain on those catalog-approved hosts.
2. Validate weekly release metadata before consuming streams. A changed weekly
   release has a stable release ID, week label, source/final/evidence
   locators, probe/status metadata, and at least one declared present file.
   `not_modified` returns a no-op without consuming streams; failed probes do
   not advance source-local state.
3. Consume each declared present update/deactivation file through the reused
   `NppesStreamBudget`. Provider, location, endpoint, reference, and
   deactivation kinds are independent. Missing optional descriptors materialize
   `unavailable_public` or `not_applicable` file receipts; a supplied stream
   without a descriptor is rejected. Missing rows are not deletions.
4. Parse only bounded source-native row metadata. Preserve exact ten-digit NPI
   strings, source row IDs, effective/deactivation dates, operation, file kind,
   row selectors, and hashes. A deactivation row remains `review_required` and
   is never an automatic deletion or identity decision.
5. Merge deterministically in source-local keys only. For each file kind and
   NPI, order competing weekly rows by effective date, release sequence, and
   source-row tie-breakers. Late rows are retained as evidence but cannot
   overwrite a newer current source-local observation; out-of-order input is
   normalized by the same ordering. Only an explicit deactivation operation
   changes source-local state to `deactivated`.
6. Classify the result as `changed`, `no_op`, `failed_probe`, `schema_drift`,
   `replayed`, or `blocked`. Only a prior successful (`changed` or `replayed`)
   receipt suppresses replay emission. Failed receipts remain retryable. A
   conflicting successful replay is rejected/quarantined by the caller.
7. Return a JSON-safe receipt containing IDs, URL/probe metadata, fingerprints,
   bounded counters, explicit file statuses, merge counters/samples, and
   review-required deactivation opportunities. Current projection preservation
   is always true; no canonical authority is granted.

## Safety and contract invariants

- Unknown fields, duplicate file kinds, malformed NPIs, unsafe URLs, invalid
  dates, duplicate source row IDs, stream bound violations, and ambiguous
  operation values fail closed or produce an explicit non-success receipt.
- Source-local merge keys never become Toolkit identity keys. A missing weekly
  row never means deletion, and an out-of-order or late row never causes a
  nondeterministic overwrite.
- `schema_drift` and `blocked` receipts are evidence only and cannot advance a
  cursor or current projection. A corrected retry may consume the same weekly
  release and emit source rows again.
- Receipts exclude source payloads, response bodies, credentials, arbitrary
  exception text, and unbounded row lists. Exact NPIs, row IDs, hashes, and
  bounded samples remain source evidence only.

## Planned commits

1. `docs(nppes): define weekly change producer mission packet`
2. `feat(nppes): add bounded weekly change producer`
3. `test(nppes): cover weekly merge, replay, and missingness`

The coordinator may squash or preserve this focused chain. No shared baseline
files or unrelated W14 paths may be changed.

## Acceptance criteria

Focused tests must cover:

- changed weekly updates and explicit deactivation review opportunities;
- source-local merge of shuffled/out-of-order rows and preservation of late
  rows without overwriting newer state;
- missing optional files, missing rows, malformed rows, and bounded streams;
- no-op/failed probes, successful replay suppression, failed-receipt retry,
  and conflicting replay rejection;
- catalog host binding for release/source/final/file URLs;
- source URL, probe/status, exact identifiers, file/row fingerprints, and
  `current_projection_preserved=true` preservation.

Acceptance requires strict typing/static checks for touched files, compile and
relevant NPPES contract validation, clean diff, and no path outside the four
declared weekly-only globs/files.

## Verification and handoff

Run the focused weekly pytest, Ruff check/format, Pyright or mypy, compileall,
relevant NPPES schema checks, and `git diff --check` against the exact base.
The W14 coordinator owns `scripts/check-fast.sh`; this lane must not run it.
Before handoff, record the exact commit chain, clean-tree state, agentctl
receipt, and reviewer handoff to `p1_16`. The independent reviewer owns
correction-capable acceptance; the writer does not self-approve.

## Rollback and ownership boundary

Rollback is a Git revert of only this mission packet, weekly module, focused
tests, and runbook. Preserve source receipts and any caller-owned source-local
state; do not delete custody or alter a current projection. Source acquisition,
custody persistence, observation admission, canonical promotion, and runtime
wiring remain coordinator-owned.

Owner: Luna Max direct writer
