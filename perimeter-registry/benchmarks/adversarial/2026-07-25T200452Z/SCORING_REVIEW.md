# Independent scoring review — adversarial perimeter benchmark

Generated at: `2026-07-25T21:16:01Z`

Benchmark: `2026-07-25T200452Z`

## Headline result

This is a strict score against the frozen hidden `gold.json`. No requirement was relaxed. The N4 source-scope uncertainty is reported separately; K2 is scored as an unsupported identifier claim, not a gold conflict.

| Arm | Correct | Scope/caveat | False aggregation | Provenance | Missed-data detection | End-to-end wall time |
|---|---:|---:|---:|---:|---:|---:|
| Registry assisted | 16/16 (100%) | 16/16 (100%) | 0/16 | 60/60 (100%) | 16/16 (100%) | 365.070365551 s |
| Manual-style source research | 8/16 (50%) | 8/16 (50%) | 0/16 | 44/60 (73.3%) | 11/16 (68.8%) | 839 s |
| Overall | 24/32 (75%) | 24/32 (75%) | 0/32 | 104/120 (86.7%) | 27/32 (84.4%) | 1204.070365551 summed run-seconds |

The registry arm preserves correctness, missed-data detection, non-aggregation, and provenance on these frozen cases. The measured end-to-end latency ratio is only about 2.298x, below the protocol's 5–10x target.

## Per-run scores

| Run | Correct | Scope/caveat | False aggregation | Provenance | Missed-data detection | Wall time | Research calls | Local reads |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `registry_r1` | 8/8 | 8/8 | 0/8 | 30/30 | 8/8 | 199 s | 0 | 7 |
| `registry_r2` | 8/8 | 8/8 | 0/8 | 30/30 | 8/8 | 166.070365551 s | 0 | 2 |
| `baseline_r1` | 4/8 | 4/8 | 0/8 | 23/30 | 5/8 | 485 s | 34 | 2 |
| `baseline_r2` | 4/8 | 4/8 | 0/8 | 21/30 | 6/8 | 354 s | 27 | 1 |

All 16 registry answers satisfy every frozen required entry without a listed failure condition. They consistently keep legal entities, facilities, brands, reporting perimeters, and identifier types separate. Their listed URLs cover every required claim under the claim-unit rule below.

## Exact baseline correctness misses

The same four question IDs fail strict correctness in both baseline repetitions.

### N1 — lean Nebraska Medicine perimeter

- `baseline_r1` includes the parent, TNMC, Bellevue, and UNMC Physicians and correctly excludes UNMC. It nevertheless omits the required placement of Bellevue beneath TNMC's supported tax-control chain. It also does not clearly distinguish the Nebraska Medicine brand concept from the same-named parent corporation. The volunteered assignment of 47-0049123 to Board/UNMC lacks the frozen campus-versus-system scope caveat.
- `baseline_r2` correctly names the four components, excludes UNMC, and distinguishes brand from parent. It still omits Bellevue's TNMC tax-control chain. It calls the result a lean controlled clinical enterprise but does not expressly bound it as membership-only rather than an audited financial consolidation.

Both answers therefore fail all-or-nothing correctness. Provenance is 2/4 and 3/4 respectively because the full compound chain requirement is not covered; `baseline_r1` also lacks the brand/legal-parent required entry.

### K2 — Colorado coverage versus physician care

Both baseline answers correctly select Kaiser Foundation Health Plan of Colorado, give EIN 84-0591617, select Colorado Permanente Medical Group for physician care, and preserve separate legal entities. They then give CPMG EIN 84-0832336. Frozen gold requires the exact CPMG EIN to remain unresolved in the benchmark evidence, so both answers fail correctness, scope/caveat, and missed-data detection under strict scoring. Their provenance is 3/4 because the required unresolved-EIN entry is contradicted rather than covered.

The cited PBGC URLs do not substantiate this pairing. They are unstable pagination URLs and, when opened directly, do not display Colorado Permanente Medical Group or EIN `840832336`. A direct PBGC `fulltext` parameter is reflected in Drupal settings but returns the full result corpus beginning with unrelated employers rather than a filtered, claim-specific record. The supplied exact CPMG EIN is therefore unsupported by the listed primary URLs. K2 is not treated as a frozen-gold conflict.

### N3 — Nebraska hospital operator EINs and facility CCNs

Both baseline answers give the four correct identifiers, keep EINs and CCNs typed, and preserve Bellevue's separate operator record. Neither gives the gold-required source period: TNMC's return for the period ended `2023-06-30`.

- `baseline_r1` says only that the Schedule R is “older.” Its older public filing mirror does not cover the required 2023-06-30 entry. Provenance is 3/4.
- `baseline_r2` omits the period and weakens the supported direct-control fact to “may be included in the parent's tax return.” Its generic IRS search page, old state rosters, and unrelated AHA filing do not provide claim-specific support for the operator EINs or disregarded-entity fact. Provenance is 1/4.

Because the period is an explicit frozen requirement, both answers score 0 despite correct identifier values and the correct non-erasure conclusion.

### N4 — Nebraska Medicine parent and university/faculty-group separation

Both baseline answers correctly give parent EIN 81-3158267, UNMC Physicians EIN 47-0785575, and separate UNMC from Nebraska Medicine. Both omit the required statement that Nebraska Medicine is UNMC Physicians' sole member under the effective interim articles. Both also assign 47-0049123 to Board/UNMC without the frozen requirement that the 2025 form supports only the named payee and leaves campus-versus-system scope unresolved.

The answers do not falsely aggregate Nebraska Medicine, UNMC Physicians, and UNMC; they explicitly separate them. Accordingly false aggregation remains 0 even though frozen-gold correctness and scope/caveat are 0.

There is material gold uncertainty here as well. `baseline_r2` cites the University of Nebraska dental plan document. Its plan-information page names the employer as `University of Nebraska`, the plan as `University of Nebraska (Board of Regents of the University of Nebraska)`, and gives employer identification number `47-0049123`. That official source supports a University/Board-level mapping broader than the fundraiser-only evidence frozen into gold. No strict score was adjusted, but the frozen named-payee/campus-versus-system requirement should be revisited before treating these two failures as model error.

## Provenance review

Each top-level `required` entry in hidden gold is one provenance claim. A compound entry counts only when a listed URL is sufficiently claim-specific to the entire entry. Correct answer text does not itself establish provenance.

The largest provenance gaps are:

- `baseline_r2` N3 (1/4): generic IRS search and unrelated/older documents do not cover the exact EIN and period claims.
- baseline N4 (2/4 and 1/4): the sole-member requirement is omitted, and the URLs do not cover the frozen named-payee-plus-unresolved-scope requirement.
- baseline N1 (2/4 and 3/4): neither answer supplies the full Bellevue-under-TNMC chain entry.
- baseline K4 (3/4 each): operator and CCN are supported, but no listed URL is a claim-specific primary record for KFH EIN 94-1105628. In `baseline_r2`, the supplied IRS PDF is for EIN 41-6011702, not KFH.

No false aggregation is scored in any answer. K2 is an unsupported exact-identifier claim and N4 is a separate frozen source-scope conflict; both answers still preserve distinct legal records rather than silently combining entity, facility, or financial perimeters.

## Missed-data detection convention

A score of 1 means the answer explicitly refuses or marks unresolved every applicable gold-required absent fact. It also means that no applicable gold-required absence was left undetected. This avoids assigning an automatic 0 to questions whose gold has no missing required datum.

The baseline misses are:

- K2 in both runs: CPMG's EIN is resolved rather than refused under frozen gold.
- N4 in both runs: the campus-versus-system scope caveat is absent.
- N1 in `baseline_r1`: it volunteers 47-0049123 without the frozen scope caveat.

## Timing and telemetry

Top-level run wall time is the only valid end-to-end timing basis:

- Registry total: `199 + 166.070365551 = 365.070365551` seconds.
- Baseline total: `485 + 354 = 839` seconds.
- Exact aggregate baseline/registry ratio: `839000000000 / 365070365551`, approximately `2.298187087121407`.
- Matched order A (`baseline_r2 / registry_r1`): `354 / 199 = 1.778894472361809...`.
- Matched reverse order B (`baseline_r1 / registry_r2`): `485 / 166.070365551 = 2.920448801270670...`.

The aggregate 2.298x ratio is below the 5–10x target. The two matched-order ratios vary substantially, which is another reason not to overinterpret two repetitions.

The four runs overlap in calendar time. `1204.070365551` is therefore a sum of run work intervals, not campaign elapsed clock time. Per-answer latencies omit wrapper gaps and should not replace the top-level fields.

Deterministic resolver timing remains separate:

- 1,000 warmup cycles;
- 10,000 timed eight-question cycles;
- 80,000 lookup calls;
- `3.930248786` total seconds;
- `0.000049128109825` mean seconds per lookup;
- `0.000294935` median and `0.000671271` p95 seconds per eight-question cycle.

This warmed in-process measurement excludes model, file-loading, wrapper, research, and registry-construction time. It is neither added to end-to-end wall time nor used to claim a cross-category speedup.

All four `input_tokens` fields are `null`. Input-token totals, ratios, and savings are unavailable and were not inferred.

## Sample, order, wrapper, and model limitations

- There are only two repetitions per arm, eight repeated questions, two systems, and 32 answers. The 16 answers per arm are clustered by eight prompts and two cases, not 16 independent samples.
- Order A and its reverse are counterbalanced, but run identity, order, repetition, transient tool behavior, and execution variation still co-vary. Two runs cannot separate those effects.
- The run JSON files do not record a model identifier or version, sampling parameters, context-window usage, or reasoning effort. Performance cannot be attributed to a particular model configuration, and model parity across arms is not independently verifiable from the artifacts.
- The arm wrappers and evidence surfaces differ materially. Registry runs read curated local fixtures and resolver output. Baseline runs perform live source research. Registry fixture research, validation, and pre-ingestion labor are outside measured registry wall time.
- Research-tool calls and local-file reads are not commensurate units. A web call may retrieve or search multiple resources, while a local read may expose a dense curated record.
- N4 shows that a live-research arm can encounter official evidence broader than the frozen dossier. K2 does not: its unstable PBGC pagination URLs fail to substantiate the claimed CPMG EIN.
- The protocol itself limits the conclusion to directional evidence. It does not establish nationwide generalization, production readiness, human-analyst savings, token savings, or total lifecycle cost.

Machine-readable per-answer rationales and aggregates are in `scores.json`.
