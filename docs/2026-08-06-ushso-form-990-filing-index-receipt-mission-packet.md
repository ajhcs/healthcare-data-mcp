# USHSO Form 990 Filing-Index Source Receipt

Tracking bead: healthcare-toolkit-k1d8.12

## Goal

Freeze a producer-owned, versioned, tamper-evident receipt for a bounded set of
official IRS electronic Form 990 filing-index opportunities so Healthcare
Toolkit can later admit legal-filer and return metadata without duplicating the
connector or inferring a health-system relationship.

## Problem

Healthcare Data MCP can retrieve IRS annual e-file indexes and exact XML
returns, but its existing loaders cache files without a complete public-safe
artifact receipt and select a single filing rather than preserving same-period
ambiguity. Healthcare Toolkit has no national legal-filer or Form 990 return
grain and must not consume those mutable caches directly.

The first cross-repository boundary therefore needs to freeze the official
annual index bytes, the exact reviewed query scope, every matching source row,
and explicit unmatched opportunities. This is source custody only. An EIN,
taxpayer name, filing, AHRQ row, brand, facility overlap, or source-ID match is
not evidence of ownership, operation, continuing-enterprise identity, merger,
succession, reporting perimeter, financial comparability, or public-release
authority.

## Scope

- Add a strict `ushso.irs-form-990-filing-index-receipt.v1` producer contract.
- Add a reviewed pilot query scope derived from exact legal-filer EIN,
  tax-period year, annual index year, and expected IRS Object ID anchors. The
  scope contains no AHRQ system, HSID, FAST, brand, facility, ownership, or
  reporting-perimeter relationship.
- Acquire only the explicitly allowlisted annual indexes at
  `https://apps.irs.gov/pub/epostcard/990/xml/<year>/index_<year>.csv`.
- Reject redirects, non-HTTPS URLs, non-IRS hosts, private/reserved resolved
  addresses, non-200 responses, unexpected content types, empty files, size
  overflow, and path escape.
- Preserve annual artifact URL, final URL, response status, content type, ETag,
  Last-Modified, retrieval time, exact byte length and SHA-256, row count, and
  ordered-header schema fingerprint.
- Preserve every matching `990` or `990A` source row for each reviewed query,
  including nullable Return ID, exact EIN, tax period, submission metadata,
  taxpayer name, return type, DLN, Object ID, XML batch ID, artifact reference,
  and canonical source-row hash.
- Preserve multiple same-EIN/same-period filings and amended variants. Never
  pick a first, latest, or preferred return.
- Represent a reviewed query with no match as
  `not_found_in_selected_annual_indexes`; never claim that the IRS has no
  filing.
- Verify every expected Object ID anchor is present before a receipt can be
  admitted.
- Build a deterministic receipt checksum from the scope, artifact identities,
  query results, and canonical filing-row hashes.
- Keep raw IRS index bytes outside Git. Commit only the reviewed scope, generated
  JSON Schema, and public-safe receipt.
- Add focused blocking CI coverage for the receipt contract and acquisition
  transaction.

## Out of Scope

- IRS XML batch acquisition or XML fact extraction.
- Form 990EZ, 990PF, 990T, Schedule H, compensation, or financial metric
  promotion.
- Toolkit models, migrations, imports, APIs, routes, public projections, profile
  objects, comparison facts, or release bindings.
- AHRQ-to-filer, filer-to-system, facility-to-filer, ownership, operation,
  management, merger, succession, registry identity, HSID, FAST, canonical
  identity, or continuing-enterprise decisions.
- Treating a legal-filer return as a consolidated health-system statement.
- Staging, production, systemd, plugin installation, service restart, DNS,
  Cloudflare, database, or public-site changes.
- Changing or merging the dirty/diverged primary Healthcare Data MCP checkout or
  its existing local Form 990 plugin commit.

## Acceptance Criteria

- The committed scope validates against a frozen typed model and contains only
  exact legal-filer queries plus explicit interpretation limits.
- The generated schema is byte-for-byte identical to the runtime Pydantic
  schema and rejects unknown fields.
- Each admitted annual artifact records exact official provenance, response
  metadata, byte hash and length, schema fingerprint, and row count.
- Every selected filing is traceable to one artifact and one or more reviewed
  queries by canonical source-row hash; duplicate-period and amended rows remain
  distinct.
- Blank Return ID remains `null`; no missing field becomes an empty assertion,
  zero, or fabricated identifier.
- Query matching uses exact nine-digit EIN, tax-period year, annual index year,
  and supported return type. Unreviewed EINs and unsupported forms never enter
  the receipt.
- A missing expected Object ID, duplicate Object ID with conflicting canonical
  content, malformed EIN/tax period/DLN/Object ID/batch ID, duplicate query key,
  source-row drift, byte drift, schema drift, selection drift, response drift,
  redirect, unsafe host, or size overflow fails closed before promotion.
- Double retrieval of each admitted annual index is byte-identical. Validation
  against the committed receipt is idempotent and does not rewrite it.
- Atomic promotion leaves the previous cache and receipt unchanged after an
  injected mid-promotion failure.
- The receipt contains no raw annual index, local path, secret, credential,
  private URL, patient data, system relationship, financial fact, or public
  authority.
- Focused tests, Ruff, Pyright, the complete repository test suite, distribution
  checks, and all GitHub checks pass at the exact published head.
- Healthcare Toolkit, staging, production, and public projections remain
  unchanged.

## Traceability

Affected PER IDs:

- `PER-701`: the work is governed by Bead `healthcare-toolkit-k1d8.12` and this
  mission packet.
- `PER-802`: Healthcare Data MCP owns the versioned source receipt that a later
  Toolkit child will consume without manual intermediate copying.

Affected DTR IDs:

- N/A. This source-custody child changes no UI, route, component, design token,
  chart, table, map, accessibility behavior, copy pattern, or interaction state.

## Files Likely Touched

- `docs/2026-08-06-ushso-form-990-filing-index-receipt-mission-packet.md`
- `shared/acquisition/irs_form_990_filing_index_receipt.py`
- `scripts/download_irs_form_990_filing_indexes.py`
- `scripts/export_contract_schemas.py`
- `contracts/source-receipts/irs-form-990-pilot-scope-v1.json`
- `contracts/source-receipts/irs-form-990-filing-index-receipt-v1.schema.json`
- `contracts/source-receipts/irs-form-990-pilot-receipt.json`
- `contracts/README.md`
- `tests/test_irs_form_990_filing_index_receipt.py`
- `tests/test_download_irs_form_990_filing_indexes.py`
- `tests/test_gateway_http_integration.py` (test-harness pipe drainage only;
  production gateway behavior is unchanged)
- `.github/workflows/ci.yml`
- `.beads/issues.jsonl` only through the canonical Toolkit Beads synchronization
  workflow and never in the Data MCP implementation commit.

## Verification

- `git diff --check`
- `ruff check shared/acquisition/irs_form_990_filing_index_receipt.py scripts/download_irs_form_990_filing_indexes.py tests/test_irs_form_990_filing_index_receipt.py tests/test_download_irs_form_990_filing_indexes.py`
- `pyright shared/acquisition/irs_form_990_filing_index_receipt.py scripts/download_irs_form_990_filing_indexes.py`
- `pytest tests/test_irs_form_990_filing_index_receipt.py tests/test_download_irs_form_990_filing_indexes.py -q`
- `python scripts/export_contract_schemas.py`
- a schema no-diff check after generation
- double retrieval of each reviewed official annual index into disposable
  custody, followed by exact receipt validation and idempotent replay
- `pytest -q`
- distribution, secret, compile, and package gates used by `.github/workflows/ci.yml`
- exact pushed-head Luna Max read-only source-contract and security reviews
- exact GitHub check completion before merge

Local frozen-tree evidence on 2026-08-06:

- Official IRS acquisition and an independent refetch/replay both passed. The
  independently generated receipt was byte-identical to the committed receipt.
  The three annual artifacts contain 728,719, 748,906, and 353,650 rows and
  match their committed SHA-256 and byte-length metadata.
- The reviewed pilot contains 36 exact legal-filer queries across 16 unique
  EINs. All 36 expected Object IDs were present; no amended, multi-match, or
  unmatched query occurred in this bounded pilot. This result is not a system
  identity, ownership, reporting-perimeter, financial-fact, or release claim.
- The receipt/downloader suite passed 31 tests. Both bounded Luna Max frozen-
  tree reviews passed after the identified CSV-shape, checksum-provenance,
  connected-peer, proxy, concurrency, crash-consistency, TOCTOU, collision, and
  response-status defects were remediated and re-reviewed.
- The exact-head final review also found that receipt caveats were not covered
  by the release checksum. The five interpretation-boundary caveats are now
  fixed, length-exact, checksum-covered, and protected by a tamper test. The
  final release checksum is
  `sha256:0a8e5c3ec965b98cbb6d6a51b6ffea9dffa212a11d2394418e09d2dc693d2797`.
- Ruff and `git diff --check` passed. Targeted Pyright reported 0 errors (two
  environment-resolution warnings for installed `httpx` and `pydantic`); the
  repository's whole-tree Pyright gate remains informational with its existing
  typing backlog.
- The complete repository suite passed on the final post-review implementation
  tree: 1,070 passed and 4 skipped in 120.46 seconds. The five gateway HTTP
  integration tests separately passed after replacing undrained child-process
  pipes in the test harness with temporary files.
- Secret scanning, the repository security policy gate, and strict dependency
  audit passed with no known vulnerabilities. Schema regeneration was
  byte-identical; shell, installer, registry-render, environment, Compose-render,
  CLI doctor, eight MCP protocol, and MCP Inspector checks passed.
- MCPB validation/build, isolated sdist and wheel builds, and Twine metadata
  checks passed. An isolated Docker image passed version/source-label and CLI
  checks. Because the standard ports are occupied by the existing local stack,
  a uniquely named no-host-port Compose project exercised all 17 zero-config
  services; every internal health check passed, and its containers, network,
  and volume were removed. Existing services and production were unchanged.

## Rollback

This child creates no database or public state. Stop using the new acquisition
command, retain the immutable external source custody, and revert the producer
commit if the contract is rejected. A cache promotion failure restores the
previous validated artifact set and receipt. Never delete admitted receipts,
raw custody, Beads history, or unrelated local plugin work. A later Toolkit
consumer must pin this exact schema and producer commit and can be disabled
without changing this source receipt.

## Risk and Privacy

Risk is high for source provenance and identity interpretation, moderate for
network and cache integrity, and low for privacy. IRS e-file index rows are
public organizational filing metadata and contain no patient-level data or PHI.
Primary risks are mutable-source drift, redirect or SSRF exposure, incomplete
same-period filing preservation, legal-filer/health-system conflation, false
absence claims, local-path leakage, and unsupported downstream authority. The
implementation must fail closed and preserve the explicit legal-filer-only
boundary throughout.
