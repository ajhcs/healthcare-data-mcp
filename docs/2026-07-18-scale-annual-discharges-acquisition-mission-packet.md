# Scale Annual Discharges Acquisition Mission Packet and Implementation Record

Tracking bead: `HDM-d22`

Classification: evidence acquisition

Downstream coordinator: `healthcare-toolkit-2rr9.6.3.3`

## Goal and authority boundary

Freeze the AHRQ Compendium 2023 `sys_dsch` source-local rows for the six
systems in the Scale roster. The output is evidence context, not six approved
`annual_discharges` inputs. Data MCP may verify source bytes, exact CSV rows,
identity keys, linkage context, and portable receipts. It may not aggregate
facility rows, substitute a period or roster, impute, calculate Scale, run a
sensitivity, project, adjudicate, recommend, promote, write a profile, mutate a
production cache/API, or deploy.

This is stage 1 of the second serial cycle in the governing seven-cycle packet.
The predecessor operating-revenue cycle used Data MCP PR #41 (merge
`b1fdfad94e65239fa73928990c086a63423b7c94`) and tracker PR #42 (merge
`a32bac3d110e339ead2568917965279c11e45622`). Toolkit scenario/binding PR #357
merged as `370dd2da1cb233eea8f89cb4773ed669a8c37b58`; tracker PR #358 merged as
`4b2f71843d5fee310421caba6605d9befea827fd`. The Agents review and Toolkit
admission remain separate downstream owners. This acquisition activates
neither.

## Frozen custody

The validated local manifest is the source of cache custody. Full CSVs remain
outside Git under the governed cache because the repository forbids committing
downloaded raw datasets and no locally frozen official license/redistribution
receipt was found. The committed v2 acquisition contains exact hash-bound row
extracts and no local filesystem paths.

| Artifact | Exact custody |
| --- | --- |
| Validated manifest | SHA-256 `0e647fdcd3ec8bf7f95c0936ae214de422132022b6ce53eabe090988eb529bea`; 2,895 bytes; dataset artifact `ahrq_health_system_compendium-977328e42c6e`; run `local-promote-20260615-v040`; validation `pass` |
| AHRQ system CSV | Official URL `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv`; SHA-256 `7bd62db33d2241236c662afdbd0ff9b30032da817f5ec0a2326311f77c5371b6`; 106,647 bytes; 639 data rows; schema fingerprint `65e32ad895ad8f21964650352978e59c23b3ee739268ab631a9ad487600f487f` |
| AHRQ linkage CSV | Official URL `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv`; SHA-256 `a86146f10c8de626fea1da3a24b756e6a68165e449ae3687f1e90d6bdf129727`; 1,528,734 bytes; 6,800 data rows; schema fingerprint `fec5c12e352c26259b8691b4fa9568f9c6e3f433b272c58e063b0dab97a4d634` |

Both CSVs require Windows-1252-compatible decoding. The manifest has empty
ETag, Last-Modified, and source-modified fields, and its generic source-period
text is not a technical definition. No locally receipted official codebook or
technical definition containing `sys_dsch` was found. The contract therefore
records only the literal column label and blocks denominator, setting, payer,
rehabilitation, organizational-boundary, and shared-CCN interpretations.

## Exact source-local candidates

| Product system | AHRQ ID | Physical row | Raw `sys_dsch` | Row SHA-256 |
| --- | --- | ---: | ---: | --- |
| ChristianaCare | `HSI00000218` | 110 | `71250` | `1b6d38a9266101556969e8f00be474811ffc62c6a94216f763eedb777d6a8d46` |
| Jefferson Health | `HSI00000048` | 18 | `147361` | `6204ec1b304e076d15c96032dfbe732e172e43e293a45abf5eaf4980d6062b54` |
| Temple Health | `HSI00001065` | 466 | `37387` | `4e1ed4e4546f3a6bd74c3e8a9828b13efca87f1b2f2bd72214d63ba9c477e8c4` |
| Penn Medicine | `HSI00000820` | 361 | `144099` | `99ffd315130cc094008d33628785b3e5610aaa476de0f7a9da4ef245ac7e2d27` |
| Cooper University Health Care | `HSI00001079` | 475 | `31354` | `7246f2affd1de40325df2add2de4428d0ebad4e0370ef819ec12ea73c004cc1f` |
| Main Line Health | `HSI00000608` | 268 | `59916` | `af058d7b4dd8ec51d74d75647b58870266c14aaa6f712f827a512c3f207db185` |

The acquisition also freezes 30 exact linkage rows with physical row, row
hash, Compendium hospital ID, CCN, hospital name, `acutehosp_flag`,
`health_sys_id`, exact AHRQ source name, and raw `hos_dsch`. An immutable
product-slug-to-HSI/source-name map rejects coherent identity substitution. An
annual-specific ordered 30-row declaration is checked both against the payload
and against the complete slice derived from the full hash-verified linkage CSV;
payload self-consistency is not trusted. The rows are identity/definition context only;
the producer never sums them. Material examples retained as open conflicts are
Jefferson's pre-LVHN nine-row boundary, Temple Jeanes' blank acute linkage
value and Fox Chase's non-acute value, Penn's rehabilitation/behavioral rows,
Main Line's non-acute Bryn Mawr Rehabilitation `2067` row, ChristianaCare's
combined-CCN/current-membership gap, and Cooper's pre-Cape Camden-only row.

## Additive contract and generated artifacts

The PDF/audited `ushso.scale-input-family-acquisition.v1` model and its revenue
fixtures are unchanged. A separate strict
`ushso.scale-tabular-input-family-acquisition.v2` model records validated
tabular custody and adapts to the unchanged Public Evidence Bundle v1. The
adapter uses `validated_official_tabular_snapshot`, never `audited`, and emits
six source-local observations alongside six blocked coverage rows and six open
conflicts. It emits no approved input or downstream output.

Pre-merge generated identities (producer commit is the required forty-zero
placeholder until a clean committed-tree rebuild):

- acquisition semantic hash:
  `sha256:fc5b401a8f63a98b0bbdc601e47948336e6200ad1177bbac7ded34df387ebcae`
- acquisition fixture byte SHA-256:
  `aa0027e2af3dc5e29fc2e5245b6e3d36370b83560ed8bbf64f9de12c6908495a`
- evidence input canonical hash:
  `sha256:8a490bc0dd68e709ff32569177cacae788885391afe5520a7ab1197a56d4dfe1`
- evidence input fixture byte SHA-256:
  `29229692c230073770d5ecbd766d385bd2b9f44eb5c6be2d8640d5480b0fc1d3`
- placeholder-bound Public Evidence Bundle semantic hash:
  `sha256:6f7a9d9ff4f086a8b708a116ddeaba3b35c0684e0088edd512560861ee6e009f`
- generated v2 schema byte SHA-256:
  `9a663fe06ea77903a466185f20f1f319b0b35b2a5f3fee022e051a80804fd8aa`
- unchanged revenue acquisition/input fixture byte SHA-256:
  `ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0`
  and `04fadae952898bc6dac87d0aaf4a3b04711cc9acc387ec751612f4b937b5b89f`

After merge, the handoff owner must rebuild from the exact clean merge commit,
replace the placeholder only in the temporary evidence input, build the Public
Evidence Bundle, and publish its exact producer-bound byte and semantic hashes.
The committed fixture itself remains normalized to forty zeroes.

## Verification and disposition

Tests cover schema/runtime parity, semantic hashes, exact six rows and 30
linkage rows, byte identity across repeated builds, source manifest/artifact/
header/row verification, producer pin validation, row/key/column/raw lexical
drift, roster narrowing, period/definition/boundary substitution, missing
receipts/blockers, imputation, fabricated zeroes, aggregation, weakened no-go,
approval, and forbidden output fields. Focused tests, Ruff, type checking,
release/build/wheel checks, and two isolated clean rebuilds are required before
handoff.

Implementation-tree verification completed on 2026-07-18:

- annual plus unchanged revenue focused suites: 15 passed;
- two separately cloned clean Git trees at the same validation-only producer
  commit emitted byte-identical acquisition and evidence files; neither source
  tree was dirtied;
- dirty-source CLI execution failed closed before reading or writing evidence;
- whole-repository Ruff, targeted Pyright, and strict mypy passed;
- sdist and wheel builds, `twine check`, installed-wheel imports, and
  `hc-mcp --version` passed;
- the whole test tree reached 852 passed with one environment-only failure in
  the pre-existing nested wheel-smoke test: its uv-base `--system-site-packages`
  environment could not inherit the outer venv's pandas installation. The same
  run's sdist/wheel build and `twine check` passed, and a dependency-installed
  fresh wheel environment subsequently passed the import and CLI smoke.

No route, migration, server registry, production configuration, API/cache
mutation, or deployed runtime behavior changes. Runtime impact is limited to a
repository-only CLI family dispatch and importable internal contract modules.
The artifacts contain public aggregate source rows only: no patient-level data,
PHI, credentials, paid source content, or local cache paths.

Final analytical disposition remains `blocked_source_conflict` for all six
systems. Zero Scale calculations, component scores, sensitivity runs,
projections, adjudications, recommendations, promotions, profile writes,
production mutations, and deployments are authorized or produced.

Rollback is a focused revert of the additive v2 model, declaration, generated
schema/fixtures, CLI dispatch, tests, and this record. Do not rewrite v1,
delete upstream evidence, alter raw cache custody, or repair Beads history.
