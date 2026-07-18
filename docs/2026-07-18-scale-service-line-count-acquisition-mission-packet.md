# Scale Service-Line Count Acquisition Mission Packet and Implementation Record

Tracking bead: `HDM-kh4`

Classification: evidence acquisition and explicit public-source missingness

Downstream coordinator: `healthcare-toolkit-2rr9.6.3.7`

## Goal and authority boundary

Evaluate authoritative public sources for six comparable `service_line_count`
inputs without hand-counting marketing pages or converting claims activity into
offered services. Data MCP may freeze source bytes, exact AHRQ identity rows,
schema absence, taxonomy applicability, portable receipts, and explicit
missingness. It may not infer affiliation, aggregate facilities or claims,
substitute organizational boundaries or periods, impute, fabricate zero,
calculate Scale, run sensitivities, project, adjudicate, recommend, promote,
write a profile, mutate a production cache/API, or deploy.

The reviewed desired definition is: count of distinct clinical service lines
offered by the product system under one preapproved common taxonomy, at the
frozen roster boundary and aligned period. No evaluated primary source meets
that definition. All six cells are therefore `unavailable_public`; zero is not
reported for any system.

This is stage 1 of the fourth serial cycle. The exact physician predecessor is
pinned as Toolkit binding PR #365 merge
`581265a2f2c80f71832b87de787b8b93e3ac8b1c`, binding tracker PR #367 merge
`4f62f957c4389a80101d15902d2b72cc4e089e07`, admission PR #371 merge
`cc3ccb3d26e44d410546003b7dec073a2b74ab17`, and admission tracker PR #372
merge `208b2ab97594316f0a3bd64649423091c11e6bbf`. Its cumulative packet,
review, and assurance semantic roots are respectively
`sha256:282a369b9121a27afebbb20fec4810464d1b7efa3d67a07ea119537cbbed9aa5`,
`sha256:181691932c17f47e42865422f30be923f9ed739cbacb8be23266dea5342f4d30`,
and `sha256:8f82c8573ecea197d5ea79784e5f0c806a5ce4fb6a98e70d1ea1ec71a3ca28b8`.
Transport hashes and the reusable-manifest semantic/transport hashes are
recorded separately in the machine contract. The predecessor remains
`blocked` / `human_review_required`.

## Primary-source evaluation and custody

### AHRQ Compendium 2023 system file

The official AHRQ system file is the only frozen primary source in local
custody that both identifies all six systems at a common highest-ownership
level and has exact row receipts. Its exact 40-column header contains no
service-line field. It therefore supports identity and a schema-absence
finding, not a service-line value.

- Official URL:
  `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv`
- payload SHA-256:
  `7bd62db33d2241236c662afdbd0ff9b30032da817f5ec0a2326311f77c5371b6`
- content length: 106,647 bytes; 639 data rows; Windows-1252
- exact header SHA-256:
  `9cc022051910c61c2f66e60b81a450985996cca7fa981c85bd38fa8a9853a79f`
- validated manifest SHA-256:
  `0e647fdcd3ec8bf7f95c0936ae214de422132022b6ce53eabe090988eb529bea`
- schema fingerprint:
  `65e32ad895ad8f21964650352978e59c23b3ee739268ab631a9ad487600f487f`
- evaluation query semantic SHA-256:
  `sha256:7d43aeffec30bf0971881d3c73f7d54e9b49e475c6400385632664ef54e2228e`

Exact product-to-AHRQ bindings and physical rows remain ChristianaCare
`HSI00000218` row 110, Jefferson Health `HSI00000048` row 18, Temple Health
`HSI00001065` row 466, Penn Medicine `HSI00000820` row 361, Cooper University
Health Care `HSI00001079` row 475, and Main Line Health `HSI00000608` row 268.
Their exact full-row hashes are carried in v4 and match the immutable v3
physician acquisition.

### CMS RBCS Release Year 2025

CMS RBCS was evaluated because it is an authoritative common classification
of clinically meaningful service categories. It is not an offered-service
taxonomy: the report states that RBCS classifies HCPCS codes from paid Medicare
Part B activity, and its limitations are claims-specific. It has no product
system identity join and no system-level service-line count. Converting it
would require prohibited claims aggregation, payer/setting substitution, and
organizational-boundary inference.

- Official report URL:
  `https://data.cms.gov/sites/default/files/2025-12/a167eaff-5167-4c2c-a133-9ec94f0ee112/RBCS%20Final%20Report_RY2025.pdf`
- official landing page:
  `https://data.cms.gov/provider-summary-by-type-of-service/provider-service-classifications/restructured-betos-classification-system`
- HTTP status 200; `application/pdf`; 839,953 bytes; 48 pages
- Last-Modified: `Mon, 08 Dec 2025 17:51:06 GMT`
- retrieval: `2026-07-18T18:15:29Z`
- payload SHA-256:
  `68ac55dcc2812c6d692134dec827ffc5056f60b5ddcf605575fb6f2025b193e4`
- canonical HTTP receipt:
  `sha256:20ce1b137bd38903bc6a3df8944008ea04c9425186f273feb0401a557f4ea033`
- rights: `unknown_review_required`; the publicly accessible report discusses
  CPT/HCPCS material and no independent redistribution-rights determination is
  frozen
- evaluation query semantic SHA-256:
  `sha256:8a79dca0acbdc80b828360c6582343016aa6afc492edeb428b7287543a3aa50e`
- exact locators: physical PDF pages 8 and 22, covering the HCPCS/Part B
  inclusion rules and Data Limitations

Raw source bytes remain outside Git. The portable contract contains hashes,
lengths, exact locators, rights classification, retrieval metadata, query
hashes, and no local filesystem path.

Exact-byte end-to-end verification is deliberately opt-in and path-neutral.
Set `HDM_KH4_AHRQ_CACHE_ROOT` to the reviewed AHRQ cache root and
`HDM_KH4_CMS_RBCS_REPORT` to the reviewed external PDF. With neither variable,
the real-custody test skips while always-running adversarial tests cover missing
files, length/hash drift, PDF page-count/marker drift, and rights/receipt drift.
No test assumes a current user, home directory, or `/tmp` custody path.

## Six explicit missing cells and open gates

Every system retains the same nine common blockers: no receipted common
service-line taxonomy, no offered-service presence receipt, no system-level
count, unresolved organization/current-roster/period comparability,
facility-to-system aggregation prohibited, claims activity not equivalent to
offered service, and marketing-page hand-count prohibited. Evidence-specific
West Grove, LVHN/33-facility, Fox Chase/faculty-practice, post-vintage Penn,
Cape Regional/Children's, and rehabilitation/service-boundary gates remain
open. The adapter emits:

- six exact health-system entities;
- zero numeric observations;
- six `unavailable_public` coverage rows;
- six open taxonomy/boundary conflicts;
- seven receipts (six AHRQ identity receipts and one shared CMS evaluation);
- two exact source artifacts; and
- zero Scale, sensitivity, projection, adjudication, recommendation, or
  promotion output.

## Additive contract and generated identities

Contracts v1-v3 and their fixtures are byte-immutable. Additive contract
`ushso.scale-service-line-count-acquisition.v4` validates runtime/schema parity,
exact source receipts and source-evaluation meanings, all-six identity order,
the exact absent header, exact predecessor lineage, semantic self-hash, and the
complete no-execution state. Its Public Evidence Bundle v1 adapter emits no
observation and cannot promote missingness.

Pre-merge generated identities:

- acquisition semantic hash:
  `sha256:87b8b2ded72ad667ed51c9d99cc9df8f7e86adff4472b1fa883175a96091c5ca`
- acquisition fixture byte SHA-256:
  `59a1debb97e6dd3cb2cbc6ce680c996cac8dbd17050c3b55563d3c90fa1f3946`
- evidence-input fixture byte SHA-256:
  `22321f105525f32475d395739021ba6730e4b86ab044e85b24fac639e0b265f4`
- generated v4 schema byte SHA-256:
  `77f5b05729b09a655cf7d79b7777506629633c552777c5e0b98fc11837250ab7`

The checked-in evidence input retains a forty-zero producer placeholder. After
merge, the handoff owner must rebuild from the exact clean merge commit, bind
that producer SHA only in temporary output, and require two independent clean
installed-tree builds to emit byte-identical acquisition and producer-bound
evidence files.

## Verification, privacy, runtime, and rollback

Tests reject commit/hash drift, dirty-source execution, roster narrowing,
identity substitution, source/header/query/locator/taxonomy/period/finding
drift, missing receipts, marketing hand-counts, claims or facility aggregation,
imputation, fabricated zero, a numeric candidate, weakened no-go, closed
conflicts, approval, or forbidden output injection. Required final gates are
focused and whole tests, Ruff, strict mypy, targeted Pyright, schema parity,
source line limits, sdist/wheel/twine/installed-tree checks, repository release
checks, two clean deterministic rebuilds, and independent Standards and
mission/spec reviews after corrections.

There is no route, migration, server registry, production configuration,
API/cache mutation, profile write, or deployed behavior change. Runtime impact
is limited to an additive repository CLI family dispatch and importable private
contract modules. Artifacts contain public aggregate metadata only: no
patient-level data, PHI, credentials, paid-source content, or local cache path.

Rollback is a focused revert of v4 modules, generated schema/fixtures, CLI
dispatch, tests, and this record. Do not rewrite v1-v3, delete upstream
evidence, alter raw source custody, or repair Beads history.
