# Scale Physician Count Acquisition Mission Packet and Implementation Record

Tracking bead: `HDM-auf`

Classification: evidence acquisition

Downstream coordinator: `healthcare-toolkit-2rr9.6.3.5`

## Goal and authority boundary

Freeze the AHRQ Compendium 2023 `total_mds` source-local cells for the six
systems in the Scale roster. The output is candidate evidence, not six
approved `physician_count` inputs. Data MCP may verify source bytes, exact CSV
rows, identity keys, source period, custody, and portable receipts. It may not
hand-count marketing pages, combine physicians with advanced-practice
providers, infer affiliation, convert a lower bound into an exact count,
aggregate rosters, substitute a period or boundary, impute, calculate Scale,
run sensitivities, project, adjudicate, recommend, promote, write a profile,
mutate a production cache/API, or deploy.

This is stage 1 of the third serial cycle in the seven-cycle mission. The exact
completed annual-discharge predecessor is pinned as Toolkit binding PR #361
merge `76e16247cecce818d777b4a4ade56dc13dd7b2a8`, binding tracker PR #362
merge `420d35d8024de1c484c1b16128836e0f8b00375c`, admission PR #363 merge
`9aed9059962cbf2a03c7c02e6056aee4281ee340`, and admission tracker PR #364
merge `2d33cab9264e636bd392b89757f8b05ed2729ecb`. Its cumulative packet,
review, and assurance roots are respectively
`sha256:bb569b3dde1fa4435c658488b11493ebcfe88898f8d7b0571231ce66ca7621a6`,
`sha256:b83433afce89012b9584c8a5df4449e78f112916fd2db4894495f4e6b1bcf1d6`,
and `sha256:4caa86f1c57a8ce45cc3df304bd4f03e841f563418db1babfe39668343ff5cf1`.
The machine contract stores the four SHA-1 commit IDs in reversible
8-4-4-4-12-8 hyphen groups so generated JSON is not mistaken for credential
material; runtime validation removes the separators and requires these exact
40 hexadecimal characters.
That predecessor remains `blocked` / `human_review_required`. The later
Toolkit binding, Agents physician-workforce fitness review, and Toolkit
admission stages remain separate, unassigned owners until this exact Data
producer merge and hashes are verified.

## Frozen custody

The validated cache manifest is the source of custody. The raw CSV remains
outside Git because redistribution rights are unreviewed. No local filesystem
path appears in the portable contract.

| Artifact | Exact custody |
| --- | --- |
| Validated manifest | SHA-256 `0e647fdcd3ec8bf7f95c0936ae214de422132022b6ce53eabe090988eb529bea`; 2,895 bytes; dataset artifact `ahrq_health_system_compendium-977328e42c6e`; run `local-promote-20260615-v040`; validation `pass` |
| AHRQ system CSV | Official URL `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv`; SHA-256 `7bd62db33d2241236c662afdbd0ff9b30032da817f5ec0a2326311f77c5371b6`; 106,647 bytes; 639 data rows; schema fingerprint `65e32ad895ad8f21964650352978e59c23b3ee739268ab631a9ad487600f487f`; header SHA-256 `9cc022051910c61c2f66e60b81a450985996cca7fa981c85bd38fa8a9853a79f`; Windows-1252 encoding |

The frozen manifest has empty ETag and Last-Modified values. No locally
receipted official codebook or technical definition for `total_mds`, raw HTTP
response receipt, or redistribution license was found. Existing Toolkit
profile evidence uses materially different employed, affiliated, mixed
physician/APP, faculty-practice, and lower-bound claims; those rows are conflict
context only and are not imported as Scale authority.

## Exact source-local candidates

| Product system | AHRQ ID | Physical row | Raw `total_mds` | Full-row SHA-256 |
| --- | --- | ---: | ---: | --- |
| ChristianaCare | `HSI00000218` | 110 | `1054` | `1b6d38a9266101556969e8f00be474811ffc62c6a94216f763eedb777d6a8d46` |
| Jefferson Health | `HSI00000048` | 18 | `3811` | `6204ec1b304e076d15c96032dfbe732e172e43e293a45abf5eaf4980d6062b54` |
| Temple Health | `HSI00001065` | 466 | `1281` | `4e1ed4e4546f3a6bd74c3e8a9828b13efca87f1b2f2bd72214d63ba9c477e8c4` |
| Penn Medicine | `HSI00000820` | 361 | `4336` | `99ffd315130cc094008d33628785b3e5610aaa476de0f7a9da4ef245ac7e2d27` |
| Cooper University Health Care | `HSI00001079` | 475 | `1012` | `7246f2affd1de40325df2add2de4428d0ebad4e0370ef819ec12ea73c004cc1f` |
| Main Line Health | `HSI00000608` | 268 | `1084` | `af058d7b4dd8ec51d74d75647b58870266c14aaa6f712f827a512c3f207db185` |

Every row also freezes exact AHRQ name, city/state, `prim_care_mds`, hospital
counts, multistate flag, row key/column, raw lexical value, and full-row hash.
An immutable product-slug-to-HSI/source-name map rejects coherent identity
substitution. No hospital linkage receipt is invented because `total_mds` is a
system-file field.

All six remain `blocked_source_conflict`. Common unresolved gates are source
vintage, highest-ownership versus current product boundary, current roster
membership, official physician definition, employed/affiliated/total basis,
active status, duplicate treatment, and variation across source systems.
System-specific post-vintage, faculty-practice, medical-staff,
rehabilitation, Cape Regional, Children's, LVHN, Fox Chase, West Grove, and
33-facility boundary issues remain explicit.

## Additive contract and generated artifacts

Revenue v1 and annual-discharge v2 are byte-immutable. The strict additive
`ushso.scale-physician-count-acquisition.v3` contract owns physician-local
literals and adapts to unchanged Public Evidence Bundle v1. It emits six
source-local candidate observations, six blocked coverage rows, six open
conflicts, one exact source artifact, and no approved input or downstream
output. The committed evidence input keeps the forty-zero producer placeholder;
only a clean committed-tree rebuild may bind the real producer SHA.

Pre-merge generated identities:

- acquisition semantic hash: `sha256:3de71b8961509cae7086d812761b0cb89eda27b1b10a905f55e8ba4049448d0a`
- acquisition fixture byte SHA-256: `e7964104e56b389a19540b541cc490656578aede63d2dcbcbb8ab73571b3192b`
- evidence input canonical hash: `sha256:79814b6f05423576e5657292922bf1413c21a9aa061f78914901ab14f688edfb`
- evidence input fixture byte SHA-256: `2c2734cd58f5b97cb6b73c326493c9794e3eb6fd3ded05d7f2ed503033dababa`
- placeholder-bound bundle semantic hash: `sha256:9fceab5187d924efe1cb581e15084668125dbb7f4718ffa4d4331e0f196549df`
- generated v3 schema byte SHA-256: `e3faa390fa8c7f982eda0e279ad35f92645672c6898f22fe65ff1b4c57784a55`

After merge, the handoff owner must rebuild from the exact clean merge commit,
replace the placeholder only in temporary output, and publish exact
producer-bound bytes and semantic hashes. Two independent clean exports must
match before Toolkit may activate its next bead.

## Verification, privacy, runtime, and rollback

Tests cover schema/runtime parity, self-hash, exact roster/HSI/rows/values,
source manifest/artifact/header/row/raw-cell validation, prior lineage,
identity substitution, roster narrowing, missing receipts, definition/period/
basis/value drift, fabricated zero, imputation, aggregation, approval,
weakened no-go, forbidden output injection, producer pins, dirty trees,
in-repository outputs, and v1/v2 byte immutability. Required final gates are
the focused and whole test suites, Ruff, strict mypy, targeted Pyright, schema
parity, source line limits, sdist/wheel/twine/installed-tree checks, repository
product-readiness CI, two clean deterministic rebuilds, and independent
Standards and mission/spec reviews after corrections.

Implementation-tree verification completed on 2026-07-18:

- revenue, annual-discharge, and physician-count focused suites: 28 passed;
- the complete repository suite: 864 passed, with one environment-only failure
  in the pre-existing nested wheel smoke because its uv-base
  `--system-site-packages` environment cannot inherit the outer venv's pandas
  installation; this same host-only failure was reproduced during the annual
  predecessor, while build and `twine check` pass inside the test;
- a separate dependency-complete Python 3.12 wheel environment installed 56
  declared dependencies, imported all four v3 modules, rebuilt the exact
  acquisition, returned six blocked coverage rows, and passed `hc-mcp
  --version`;
- two separately cloned clean Git trees at the same producer commit verified
  the exact frozen cache and emitted byte-identical acquisitions and
  producer-bound evidence files without dirtying either source tree;
- whole-repository Ruff, targeted strict mypy, targeted Pyright, schema parity,
  detect-secrets, security, shell, registry-render, Compose/config, MCPB, and
  doctor (`ready`, 20 servers) gates passed;
- independent Standards and mission/spec reviews both found the initial
  coherent-mutation gap; after an exact row/blocker/finding oracle and
  adversarial regressions were added, both re-reviews passed with no remaining
  hard, high, or medium finding. A low duplication smell in source-byte
  verification remains non-blocking and does not alter custody behavior.

The final merge review additionally required exact canonical cache-receipt
equality and schema-only behavioral parity. The runtime now rejects any
manifest/run/artifact/retrieval/loader/validator receipt drift, even when a
fabricated manifest and receipt are rehashed together. The generated v3 schema
retains its structural definitions and adds the complete exact self-hashed
packet as a root `const`, so roster, rows, values, hashes, blockers, findings,
prohibitions, receipt, and semantic self-hash drift are rejected by both
Pydantic and Draft 2020-12 validation.

There is no route, migration, server registry, production configuration,
API/cache mutation, profile write, or deployed behavior change. Runtime impact
is limited to an additive repository CLI family dispatch and importable private
contract modules. The artifacts contain public aggregate source rows only: no
patient-level data, PHI, credentials, paid-source content, or local cache paths.

Final analytical disposition remains `blocked_source_conflict` for all six
systems. Zero Scale calculations, component scores, sensitivity runs,
projections, adjudications, recommendations, promotions, profile writes,
production mutations, and deployments are authorized or produced.

Rollback is a focused revert of the additive v3 contract, declaration,
adapter, generated schema/fixtures, CLI dispatch, tests, and this record. Do
not rewrite v1/v2, delete upstream evidence, alter raw cache custody, or repair
Beads history.
