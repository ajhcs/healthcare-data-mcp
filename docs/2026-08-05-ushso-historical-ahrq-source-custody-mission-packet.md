# USHSO Historical AHRQ Source Custody Mission Packet

Tracking bead: `healthcare-toolkit-k1d8.6`

Date: 2026-08-05

## Goal

Acquire, validate, and freeze a public-safe two-artifact source receipt for
every official pre-2023 AHRQ Compendium system and hospital-linkage release
currently available: 2016, 2018, 2020, 2021, and 2022.

## Problem

USHSO has an admitted receipt only for the revised 2023 AHRQ release. AHRQ's
current catalog still publishes system and hospital-linkage files for five
earlier snapshots, but the producer is hard-coded to the revised 2023 URLs,
schema, expected hashes, and filenames. Importing historical snapshots without
fresh receipts would make cross-year coverage non-reproducible and would risk
treating source-ID reuse, field changes, corrected files, or linkage
differences as verified continuing-enterprise history.

## Official Release Inventory

The inventory was verified from AHRQ's current catalog and per-year landing
pages on 2026-08-05. Landing-page system totals are acquisition expectations,
not admitted artifact claims:

| Source year | Landing-page systems | System file | Hospital linkage file |
| --- | ---: | --- | --- |
| 2016 | 626 | current official CSV link | current official CSV link |
| 2018 | 637 | January 2021 updated CSV | current official CSV link |
| 2020 | 629 | current revised CSV | current revised CSV |
| 2021 | 635 | current revised CSV | current revised CSV |
| 2022 | 640 | current revised CSV | current revised CSV |

The release registry must preserve the exact official landing, artifact, and
technical-documentation URLs observed for each year. URL values belong in the
typed producer registry and receipts; source acquisition logs must not expose
unrelated internal paths, credentials, or account identifiers.

## Scope

- Add an explicit reviewed AHRQ release registry for 2016, 2018, 2020, 2021,
  2022, and the existing revised 2023 release.
- Preserve the committed revised-2023 receipt model, generated schema,
  contract file, default acquisition behavior, and public imports unchanged.
- Add an additive versioned historical receipt contract whose release identity,
  source period, filenames, URLs, technical documents, and expectations are
  parameterized by the reviewed registry rather than accepted as free-form CLI
  input.
- Use the existing Playwright-compatible browser acquisition path for AHRQ's
  WAF and require the per-year landing page to succeed before artifact requests.
- Acquire each system/hospital pair into a temporary directory first. Record
  public-safe response status, final URL, content type, ETag and last-modified
  when supplied, and one shared UTC retrieval time.
- Validate each pair as a unit: required identity/location columns, exact
  bytes, content lengths, header/schema fingerprints, row counts, unique
  source IDs, system and hospital jurisdictions, linked/unlinked partitions,
  nonblank/missing/duplicate CCNs, linked system coverage, and orphan links.
- Repeat acquisition or independently re-fetch every artifact before freezing
  expectations; any byte difference blocks admission.
- Freeze exact per-release expectations only after inspecting both successful
  acquisitions, then rerun strict admission from the official URLs.
- Transactionally promote both CSVs and their receipt together so a partial
  release can never replace a prior admitted release.
- Commit five public-safe receipts and a generated JSON Schema. Raw CSV bytes
  remain in the managed cache and do not enter Git.
- Add deterministic fixtures and mutation tests for release selection,
  backwards compatibility, schema drift, URL/final-URL drift, byte drift,
  missing columns, duplicate IDs, linkage partitions, orphan links,
  transactional promotion, and public-safety constraints.

## Out of Scope

- Importing historical AHRQ rows into Healthcare Toolkit or changing its
  database, API, web application, production deployment, or release binding.
- Cross-year identity resolution, name-based merging, HSID assignment,
  succession claims, or continuing-enterprise assertions.
- Acquiring group-practice, outpatient-site, nursing-home, or home-health
  linkage files in this slice.
- Replacing or rewriting the admitted revised-2023 v1 receipt or changing its
  checksum.
- Treating landing-page totals as sufficient artifact validation.
- Committing raw source rows, local cache paths, browser profiles, credentials,
  tokens, account identifiers, or private operational URLs.

## Acceptance Criteria

- The release registry resolves exactly six supported source releases and
  rejects unknown years or caller-supplied URL substitution.
- Existing calls that omit a release still acquire the revised 2023 pair and
  produce the unchanged v1 contract behavior.
- Fresh browser acquisition succeeds from every official 2016, 2018, 2020,
  2021, and 2022 landing/artifact URL, and a repeated or independent retrieval
  matches every admitted artifact checksum.
- Five historical receipts validate against the generated schema and bind
  their exact landing page, source period, release ID/revision, artifact roles,
  source/final URLs, response metadata, byte hashes and lengths, schema
  fingerprints, row counts, technical documents, parser version, rights basis,
  assertions, and caveats.
- Artifact system-row counts equal AHRQ's current landing-page totals: 626,
  637, 629, 635, and 640 respectively. Every other aggregate is derived from
  and then frozen against the acquired bytes.
- Every receipt records duplicate source IDs, duplicate nonblank CCNs,
  linked-system coverage, and orphan links explicitly; admission fails on
  duplicate source IDs or orphan linked system IDs and never infers missing
  linkage.
- All release promotions are atomic across the two CSVs and receipt, including
  an injected mid-promotion failure that restores the prior complete release.
- Receipt JSON contains no raw rows, absolute/local paths, secrets, browser
  state, account identifiers, or internal operational URLs.
- Focused tests, Ruff, the repository-configured Pyright check, the full Data MCP test suite, and repository CI
  pass on the exact pushed SHA.
- Toolkit and all running services, databases, DNS, and Cloudflare resources
  remain unchanged.

## Admission Evidence

Two independent browser processes retrieved all ten official CSVs on
2026-08-05 and produced byte-identical files. The strict producer then fetched
each artifact twice again before admitting the release pair. The generated
receipts retain the exact artifact hashes, byte lengths, header fingerprints,
response metadata, jurisdictions, ID uniqueness, CCN completeness, linked
system coverage, and orphan-link result.

| Source year | Systems | Hospitals | Linked | Unlinked | Missing CCNs | Duplicate nonblank CCNs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2016 | 626 | 6,762 | 3,949 | 2,813 | 548 | 0 |
| 2018 | 637 | 6,742 | 3,887 | 2,855 | 151 | 1 |
| 2020 | 629 | 6,701 | 4,037 | 2,664 | 127 | 0 |
| 2021 | 635 | 6,725 | 4,073 | 2,652 | 126 | 0 |
| 2022 | 640 | 6,764 | 4,173 | 2,591 | 132 | 0 |

Every release has zero duplicate system IDs, zero duplicate hospital IDs,
complete linked-system coverage, and zero orphan linked system IDs. The one
duplicate nonblank CCN in the 2018 source is explicitly preserved as a source
condition; it is not used to infer or merge affiliations.

## Traceability

Affected PER IDs:

- N/A. This producer slice freezes source custody and does not populate or
  change Toolkit profile-engine facts.

Affected DTR IDs:

- N/A. This slice adds no user interface, route, control, interaction, or
  design-system behavior.

## Files Likely Touched

- `shared/acquisition/ahrq_compendium_receipt.py`
- additive historical release/receipt modules under `shared/acquisition/`
- `scripts/download_ahrq.py`
- `contracts/source-receipts/`
- `tests/test_ahrq_compendium_receipt.py`
- `tests/test_download_ahrq.py`
- focused new historical receipt tests if separation improves reviewability
- this mission packet

## Verification

- focused receipt, acquisition, schema-parity, compatibility, and transaction
  tests
- repeated official browser acquisition for all five historical release pairs
- independent checksum, byte-length, header, row-count, ID, jurisdiction,
  linkage, CCN, and orphan-link reconciliation
- `ruff check` on changed Python modules and tests
- Pyright type checking with the repository's configured command
- full `pytest` suite
- Git diff review proving the revised-2023 committed receipt and checksum are
  unchanged
- GitHub checks on the exact pushed head

## Rollback

Before admission, temporary downloads can be discarded without changing the
managed cache. Transactional promotion retains and restores the prior complete
artifact/receipt set on any error. Repository rollback reverts the additive
historical registry, contract, receipts, tests, and packet while leaving the
existing revised-2023 producer intact. No Toolkit or production rollback is
required because this slice performs no downstream import or runtime mutation.

## Risk and Privacy

Risk is moderate for source integrity and low for privacy. The main hazards are
freezing a superseded AHRQ file, accepting WAF or HTML bytes as CSV, silently
normalizing schema differences, incomplete paired promotion, and interpreting
source-year IDs as cross-year enterprise identity. The producer therefore uses
official reviewed URLs, browser response evidence, repeated byte validation,
release-specific required columns, exact frozen expectations, atomic
promotion, and explicit source-snapshot caveats.

All inputs are public organizational data. No PHI, patient-level data,
credentials, contact information, browser session material, or confidential
partner data is in scope.
