# Revised 2023 AHRQ Source Receipt Mission Packet

Tracking beads: `HDM-nct`, upstream program dependency
`healthcare-toolkit-k1d8.2`

## Goal

Acquire and freeze one admissible, public-safe receipt for the current official
revised 2023 AHRQ system file and 2023 hospital linkage file so Toolkit can
build the first national USHSO slice from exact, reproducible source bytes.

## Problem

The validated local AHRQ cache has the revised Medicare Advantage columns and
the official 639-system shape, but its dataset manifest records the superseded
pre-revision system-file URL. AHRQ publishes no checksum and direct HTTP clients
are blocked by its WAF. Toolkit must not claim that the cache equals the current
official file until the existing browser acquisition path proves it.

## Scope

- Centralize current official release, landing-page, and technical-document
  URLs in the Data MCP acquisition layer.
- Update active loaders and discovery metadata to use the revised system URL.
- Replace the one-file-at-a-time browser script with a fail-closed two-artifact
  acquisition transaction.
- Validate exact bytes, lengths, header fingerprints, row counts, identifiers,
  jurisdictions, linkage coverage, missing CCNs, and cross-file system IDs.
- Preserve browser response status, final URL, content type, ETag and
  Last-Modified when supplied, and a timezone-aware retrieval timestamp.
- Emit a deterministic public-safe receipt with separate artifact hashes and a
  derived release checksum; exclude raw rows and local paths.
- Acquire the live files into an isolated location and compare their hashes to
  the previously validated cache before any shared-cache replacement.

## Out of Scope

- Toolkit database import, migration, public projection, UI, deployment, or
  production mutation.
- Historical AHRQ vintages, cross-year entity continuity, FAST HSIDs, current
  ownership, Form 990, or financial fact promotion.
- Rewriting historical evidence packets that truthfully record their original
  June 2026 source receipt and URL.

## Acceptance Criteria

- The current system source URL ends in `chsp-compendium-2023-rev.csv` and all
  active acquisition/discovery paths use it.
- Acquisition stages both files, validates the combined release, and changes no
  destination artifact on download or validation failure.
- Default production expectations admit only the known revised receipt:
  639 systems, 6,800 hospital rows, 4,193 linked rows, 2,607 unlinked rows,
  6,676 nonblank CCNs, 124 missing CCNs, 51 jurisdictions in each file, no
  duplicate nonblank CCNs, no orphan system links, and the frozen hashes and
  schema fingerprints from the national baseline.
- The receipt contains two artifact entries, exact response and integrity
  metadata, official landing and technical-document URLs, source period,
  revision, parser version, rights classification, caveats, and a combined
  release checksum.
- The receipt JSON contains no local path, raw row, credential, account
  identifier, or private URL.
- Focused receipt, cache-manager, discovery, and health-system-profiler tests
  pass; Ruff passes; the repository-required health-system-profiler suite
  passes.
- A live browser acquisition either produces a matching admitted receipt or
  fails closed with the old shared cache untouched.

## Files Likely Touched

- `shared/acquisition/ahrq_compendium_receipt.py`
- `shared/utils/ahrq_data.py`
- `servers/health-system-profiler/data_loaders.py`
- `servers/discovery/server.py`
- `shared/cache_manager/core.py`
- `scripts/download_ahrq.py`
- `contracts/source-receipts/ahrq-compendium-2023-revised.json`
- `tests/test_ahrq_compendium_receipt.py`
- focused existing tests whose active URL assertion changes

## Verification

- `ruff check` on touched Python files.
- `pytest -q tests/test_ahrq_compendium_receipt.py tests/test_cache_manager.py tests/test_discovery_metadata.py`
- `pytest -q tests/servers/health_system_profiler`
- Live: `python -m scripts.download_ahrq --output-dir <isolated-dir> --force`.
- Compare live receipt hashes and assertions with the baseline and confirm the
  shared cache is unchanged unless a separately reviewed promotion is needed.

## Rollback

Revert the focused connector, receipt, metadata, test, and documentation
commit. A failed live acquisition leaves destination files and manifests
unchanged. Do not delete the prior validated cache or historical manifests.

## Risk and Privacy

The principal risk is admitting HTML/WAF content, a silent AHRQ revision, a
partial two-file release, or an incorrect system/hospital association. The
connector therefore requires CSV response metadata, exact source URLs and
bytes, cross-artifact assertions, and atomic post-validation replacement. The
source is public organization-level government data; no PHI, patient-level
data, credentials, or confidential partner data is in scope.

## Result

Completed on 2026-08-05 without changing the shared cache or any production
runtime. A browser session first loaded the official AHRQ landing page, then
retrieved both current CSV artifacts through the same browser context. The
admitted public-safe receipt records retrieval at
`2026-08-05T04:24:47.540421Z` and release checksum
`sha256:dd2198b868fe990e3d93ae36a88f541f5b3a5c0f01703f63ef7a5e8d8519393b`.

The live system artifact was 106,647 bytes with SHA-256
`7bd62db33d2241236c662afdbd0ff9b30032da817f5ec0a2326311f77c5371b6`;
the live hospital-linkage artifact was 1,528,734 bytes with SHA-256
`a86146f10c8de626fea1da3a24b756e6a68165e449ae3687f1e90d6bdf129727`.
Both hashes exactly matched the separately reviewed baseline cache, so no cache
promotion was necessary. All expected counts, identifiers, jurisdictions,
linkage, CCN missingness, and cross-file assertions passed.

The admission path now stages the two CSVs and receipt, then promotes the three
files as a rollback-capable transaction. An injected second-file promotion
failure test proves that the prior versions of all three destinations are
restored and transaction files are removed.

Verification evidence:

- Ruff passed across the complete repository.
- The focused receipt, transactional admission, cache-manager, discovery, and
  health-system-profiler gate passed: 141 tests.
- The complete repository suite passed: 1,023 tests, with 4 intentional skips.
- Python bytecode compilation passed for servers, shared modules, scripts,
  tests, and the live-data smoke module.
