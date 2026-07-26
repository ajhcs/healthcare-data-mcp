# Scoring review — rerun 2026-07-24T203452Z

Generated at: `2026-07-24T20:59:22Z`

This is an independent score of only the frozen question/gold snapshots and the four run JSON files. No prior reports, scores, fixtures, resolver notes, or source notes were used.

## Headline result

| Arm | Frozen strict correctness | Reviewer-adjusted correctness | Strict scope/caveat | Adjusted scope/caveat | False aggregation | Strict provenance | Adjusted provenance |
|---|---:|---:|---:|---:|---:|---:|---:|
| Manual research baseline | 14/24 (58.3%) | 20/24 (83.3%) | 16/24 (66.7%) | 20/24 (83.3%) | 0/24 | 57/70 (81.4%) | 60/66 (90.9%) |
| Registry assisted | 24/24 (100%) | 24/24 (100%) | 24/24 (100%) | 24/24 (100%) | 0/24 | 66/70 (94.3%) | 62/66 (93.9%) |
| Overall | 38/48 (79.2%) | 44/48 (91.7%) | 40/48 (83.3%) | 44/48 (91.7%) | 0/48 | 123/140 (87.9%) | 122/132 (92.4%) |

The adjusted score changes only three predeclared items:

- J1: the unasked FY2025/11-month detail is not required.
- P2: the unasked operator EIN is not required.
- U3: a newer official FY2023 parent return is accepted without the FY2022 caveat.

U2 and U4 remain unrelaxed. Both baseline repetitions fail both questions. U2 either supplies a dated EIN without the required source-period handling or asserts a conflicting EIN; U4 omits the mandatory dated-EIN/current-continuity caveat.

## Run aggregates

| Run | Strict correct | Adjusted correct | Strict scope | Adjusted scope | False aggregation | Strict provenance | Adjusted provenance | Wall seconds | Research calls | Local reads |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline_r1 | 7/12 | 10/12 | 8/12 | 10/12 | 0/12 | 30/35 | 31/33 | 244.746 | 19 | 1 |
| baseline_r2 | 7/12 | 10/12 | 8/12 | 10/12 | 0/12 | 27/35 | 29/33 | 454.792 | 24 | 1 |
| registry_r1 | 12/12 | 12/12 | 12/12 | 12/12 | 0/12 | 33/35 | 31/33 | 122.816 | 0 | 10 |
| registry_r2 | 12/12 | 12/12 | 12/12 | 12/12 | 0/12 | 33/35 | 31/33 | 138.313 | 0 | 9 |

Per-answer rationales and machine-readable values are in `scores.json`.

## System aggregates

| System | Strict correct | Adjusted correct | Strict scope | Adjusted scope | False aggregation | Strict provenance | Adjusted provenance |
|---|---:|---:|---:|---:|---:|---:|---:|
| Jefferson Health | 14/16 | 16/16 | 14/16 | 16/16 | 0/16 | 38/48 | 41/48 |
| Penn Medicine / UPHS | 14/16 | 16/16 | 16/16 | 16/16 | 0/16 | 38/40 | 36/36 |
| UPMC | 10/16 | 12/16 | 10/16 | 12/16 | 0/16 | 47/52 | 45/48 |

## Question aggregates

| Question | Strict correct | Adjusted correct | Strict scope | Adjusted scope | False aggregation | Strict provenance | Adjusted provenance |
|---|---:|---:|---:|---:|---:|---:|---:|
| J1 | 2/4 | 4/4 | 2/4 | 4/4 | 0/4 | 9/12 | 12/12 |
| J2 | 4/4 | 4/4 | 4/4 | 4/4 | 0/4 | 5/12 | 5/12 |
| J3 | 4/4 | 4/4 | 4/4 | 4/4 | 0/4 | 8/8 | 8/8 |
| J4 | 4/4 | 4/4 | 4/4 | 4/4 | 0/4 | 16/16 | 16/16 |
| P1 | 4/4 | 4/4 | 4/4 | 4/4 | 0/4 | 12/12 | 12/12 |
| P2 | 2/4 | 4/4 | 4/4 | 4/4 | 0/4 | 10/12 | 8/8 |
| P3 | 4/4 | 4/4 | 4/4 | 4/4 | 0/4 | 8/8 | 8/8 |
| P4 | 4/4 | 4/4 | 4/4 | 4/4 | 0/4 | 8/8 | 8/8 |
| U1 | 4/4 | 4/4 | 4/4 | 4/4 | 0/4 | 12/12 | 12/12 |
| U2 | 2/4 | 2/4 | 2/4 | 2/4 | 0/4 | 8/8 | 8/8 |
| U3 | 2/4 | 4/4 | 2/4 | 4/4 | 0/4 | 14/16 | 12/12 |
| U4 | 2/4 | 2/4 | 2/4 | 2/4 | 0/4 | 13/16 | 13/16 |

## Provenance method and unresolved coverage

Each `gold.required` entry is treated as one claim. Coverage is awarded only where at least one listed URL is sufficiently claim-specific; correctness does not imply provenance.

The main unresolved provenance is J2. All four answers state the correct filer/EIN/year, but the supplied URLs do not include a claim-specific 2023 Form 990 record. The registry runs receive 1/3 because their URLs substantiate the brand/legal-entity mapping, not the exact 2023 return. Baseline r1 receives 2/3 from its insurer examination plus brand materials; baseline r2 receives 1/3.

Baseline U4 r2 has no claim-specific EIN filing URL (2/4). Baseline U4 r1 cites an older FY2017 filing and omits the required FY2022/current-continuity treatment (3/4). These are provenance gaps as well as, for the caveat, correctness failures.

## Operational totals and telemetry validity

Top-level telemetry totals:

- Wall latency: 960.667 seconds across runs (baseline 699.538; registry 261.129).
- Research-tool calls: 43 (all baseline).
- Local-file-read calls: 21 (baseline 2; registry 19).
- Input tokens: unavailable/null for all four runs; no token total can be reported.

Per-answer allocation caveats:

- `baseline_r1` per-answer timing is invalid: every answer repeats the full 244.746-second run interval, producing 2936.952 summed answer-seconds. Its per-answer research-call counts also sum to 38 versus 19 top-level calls; the run itself states batched calls overlap answers.
- `registry_r2` has a valid top-level wall time, but its per-answer timing is an equal allocation (11 answers at exactly 11.526 seconds and one at 11.527), not credible independently observed timing.
- `baseline_r2` has internally consistent sequential answer timing and research-call allocation.
- `registry_r1` has internally consistent sequential timing. In both registry runs, local reads are top-level only and cannot be allocated to answers.

Operational comparisons should therefore use top-level run telemetry, not per-answer timing/call sums.
