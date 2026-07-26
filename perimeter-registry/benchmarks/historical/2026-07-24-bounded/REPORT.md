# Health-System Perimeter Registry bounded benchmark

> **Historical clarification (2026-07-26):** Treat these results as a study of
> pre-resolved scope assistance. The registry arm received curated fixtures and
> resolver output sufficient to answer the identity/scope questions, while the
> baseline discovered those facts from sources. This was not a fair end-to-end
> financial retrieval comparison.

Research date: 2026-07-24

## Decision

**Revise and retest; do not claim a material accuracy win or production/nationwide generalization yet.** The registry-assisted workflow was directionally faster and more repeatable, but the independently corrected accuracy advantage was small: 22/24 (91.7%) versus 20/24 (83.3%). Both workflows had zero false aggregations, so this benchmark did not demonstrate the registry's intended false-aggregation reduction. Adjusted primary-source provenance tied at 90.3%. Input tokens were unobservable, so no token-cost conclusion is supported.

The comparator is a **manual-style source-research baseline**: a fresh AI agent researching official primary sources without registry records. It is not a human analyst, and these results do not establish manual-human time savings.

## Scope and fixtures

The benchmark used exactly three structurally different cases and deliberately did not attempt exhaustive entity censuses:

- **Jefferson Health:** university, delivery system, health plan, LVHN entry effective 2024-08-01, and the existing fixture.
- **Penn Medicine / UPHS:** Penn Medicine governance umbrella (PSOM + UPHS), whole-University consolidation, HUP facility/operator, and Doylestown entry effective 2025-04-01.
- **UPMC:** consolidated provider-insurer enterprise, Health Services and Insurance Services components, close but non-consolidated University of Pittsburgh affiliation, a multi-company UPMC Health Plan marketing name, and distinct Presbyterian/Shadyside campuses with combined records.

Every added fact has an official source URL, locator, source period, confidence, and limitation. Explicit gaps preserve absent entity inventories, brand-level EIN ambiguity, dated identifier continuity, and non-consolidated university affiliations.

## Method

`questions.json` froze 12 natural scope-selection questions (four per system), and `gold.json` froze required answers, accepted ambiguity, and failure conditions before the answer runs. Answering agents could not read gold or prior runs. The registry arm could read deterministic resolver output and fixtures but could not browse. The baseline received the same questions and researched official primary sources without reading registry artifacts.

There were two fresh repetitions per arm (24 answers per arm, 48 total). Orders A and reverse-B were counterbalanced so each order appeared once in each arm. Registry R1 and baseline R2 used order A; registry R2 and baseline R1 used order B. One baseline used a separate ephemeral Codex context after the in-thread agent tree hit its four-thread limit; it remained blinded, but the differing execution wrapper is a limitation.

An independent source reviewer scored correctness, scope/caveat labeling, false aggregation, and claim-level provenance. The reviewer found three gold defects after response collection: J1 required unasked FY2025/11-month detail, P2 required an unasked operator EIN, and U3 penalized newer official FY2023 evidence. The frozen gold was retained. Results below show both the original strict score and a separately documented reviewer-adjusted score; no other criteria were relaxed.

## Results

| Arm | Correct (original gold) | Correct (reviewer-adjusted) | Scope/caveat (adjusted) | False aggregation | Provenance (adjusted mean) | Wall time, 2 runs | Research calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Registry-assisted | 22/24 (91.7%) | 22/24 (91.7%) | 22/24 (91.7%) | 0/24 | 90.3% | 232.484s | 0 |
| Manual-style source-research baseline | 14/24 (58.3%) | 20/24 (83.3%) | 20/24 (83.3%) | 0/24 | 90.3% | 657.186s | 46 |

Paired wall time was 162.304s registry versus 422.186s baseline for order A, and 70.180s versus 235.000s for reverse order B. Total wall time was 64.6% lower (2.83x faster) for the registry arm. This is an end-to-end directional result, not pure model inference time: local reads, approvals, research, and serialization are included. Per-answer latency allocations had inconsistent unallocated intervals and are not used for the headline.

Adjusted correctness by case:

| Case | Registry | Baseline | What it says |
| --- | ---: | ---: | --- |
| Jefferson | 8/8 | 8/8 | Existing fixture was repeatable, but did not improve accuracy over strong source research. |
| Penn/UPHS | 8/8 | 8/8 | The record types generalized to university/governance/facility/date distinctions; no measured accuracy advantage. |
| UPMC | 6/8 | 4/8 | The registry better preserved the Health Plan brand ambiguity; both arms missed the dated-EIN continuity caveat for the combined hospital record. |

Input-token counts were unavailable in all four run interfaces and are stored as `null`. No token estimate or savings claim is made.

## Deterministic lookup versus model latency

The deterministic benchmark used 10,000 warmed resolutions per question and 300 cold load/validate/resolve iterations per fixture. The equal-weight mean of question-level warm means was 0.041310ms; the equal-weight mean cold path was 0.370789ms. Warm per-question medians ranged from 0.005380ms to 0.105184ms; cold fixture means ranged from 0.306552ms to 0.461957ms. These figures are separate from agent wall time. Raw timing samples were not retained, so summary quantiles cannot be independently recomputed.

## Failures and post-review fixes

- **Benchmark fairness:** registry-shaped, unasked details inflated the original apparent advantage. The adjusted 2/24-answer advantage is the decision basis.
- **UPMC dated identifier:** all four U4 answers gave the correct EIN/CCN and campus distinction but omitted that the EIN is supported by FY2022 evidence whose current continuity was not reverified. The fixture limitation and resolver now emit that caveat. Frozen run artifacts were not rewritten.
- **Penn evidence link:** HUP's facility result returned the Trustees EIN but omitted the direct Penn EIN evidence record. The facility evidence list now includes `upenn_ein`.
- **Jefferson exact-EIN provenance:** the legacy `irs_teos` observation has no exact filing URL. This remains explicit and unresolved; no URL was invented. It depressed provenance, especially J2.
- **False-aggregation discrimination:** neither arm silently aggregated a brand or affiliate. That is good behavior, but zero events in both arms means no comparative reduction was measured.
- **Timing instrumentation:** wall time is usable; answer-level timing is not sufficiently consistent for a finer claim.

## Validation

Passed locally:

- `ruff check .`
- `python3 -m pytest perimeter-registry/tests -q` — 40 passed
- `python3 -m pytest tests/servers/health_system_profiler -q` — 91 passed
- `python3 perimeter-registry/benchmarks/validate_runs.py` — four runs / 48 answers validated
- fixture JSON Schema validation for Jefferson, Penn, and UPMC
- JSON parsing and independent 48-row score uniqueness/aggregate checks
- `git diff --check`

The full local `python3 -m pytest tests -q` did not reach tests: collection stopped with four import errors because this environment lacks declared repository dependencies `geopandas` and `networkx` (`geo_demographics`, `live_gateway`, and `provider_enrollment`). Those dependencies are installed by repository CI; no global environment change or bypass was made.

## Limitations

Two repetitions per arm are deliberately small and support no significance test. Answers within a run share context. Fixture-authored knowledge also shaped the original gold, which the independent review showed can bias criteria. The benchmark covers three systems, not the country; it measures AI source research, not human work; it cannot measure token cost; and several exact identifiers are bounded to dated official evidence. Results do not establish production readiness, nationwide completeness, or human-labor savings.

## Next smallest experiment

First rerun one fresh repetition per arm on six newly frozen adversarial questions across the same three cases: ambiguous plan brand, close university affiliate, combined campus/filer, pre-close merger date, parent-versus-group EIN, and a deliberately non-additive Form 990 request. Require only facts asked in each question, retain raw timing events, and make at least one question likely to expose false aggregation. If that confirmatory run remains clean, add Nebraska Medicine/UNMC as the fourth public/university case rather than claiming nationwide generalization.

## Publication status

Focused branch: `codex/perimeter-registry-benchmark`; focused PR [#58](https://github.com/ajhcs/healthcare-data-mcp/pull/58) targets `codex/perimeter-registry`. Merge is gated on its checks.
