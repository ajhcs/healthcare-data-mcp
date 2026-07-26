# Independent benchmark review

> **Historical clarification (2026-07-26):** The assisted input was an
> answer-ready scope fact sheet for these questions. Any accuracy or latency
> comparison below characterizes pre-resolved scope assistance, not end-to-end
> financial retrieval.

## Decision

The registry result is directionally promising but the benchmark does **not** yet establish a robust accuracy advantage. Under the frozen gold, registry correctness is 22/24 (91.7%) versus baseline 14/24 (58.3%). After correcting three confirmed rubric defects, registry remains 22/24 (91.7%) while the manual-style source-research baseline rises to 20/24 (83.3%). Both arms have zero false aggregations. The defensible conclusion is that the registry materially reduced latency/tool use and made answers more repeatable, while its adjusted correctness advantage is small and provenance is not better.

This is a fresh-agent, primary-source workflow baseline, not a human analyst or manual-human benchmark.

## Confirmed rubric corrections

- **J1:** the question does not specify FY2025 and the protocol excludes broad finance retrieval, but gold requires “11 months of LVHN.” Adjusted scoring accepts a clearly labeled current enterprise or dated audited enterprise, with university/delivery/insurance scope and the Jefferson Health–TJUH distinction.
- **P2:** the question asks for CCN and whether HUP is a separate enterprise, not an operator EIN. Both baseline answers become correct after removing the unasked EIN requirement.
- **U3:** both baseline runs cite UPMC's official FY2023 parent return/index. A newer official primary source must be accepted; requiring an FY2022-evidence caveat in that case is obsolete.

No other post-hoc relaxation was made. U2 baseline answers still fail because they select UPMC Health Plan, Inc. for an ambiguous brand question without the full clarification/source-period discipline. All four U4 answers still fail because none explicitly says the combined EIN is only validated by FY2022 evidence and current continuity was not established.

## Results

| Arm | Rubric | Correct | Scope/caveat | False aggregations | Mean provenance coverage |
| --- | --- | ---: | ---: | ---: | ---: |
| Registry-assisted | Strict original | 22/24 | 22/24 | 0/24 | 88.9% |
| Baseline | Strict original | 14/24 | 14/24 | 0/24 | 82.6% |
| Registry-assisted | Reviewer-adjusted | 22/24 | 22/24 | 0/24 | 90.3% |
| Baseline | Reviewer-adjusted | 20/24 | 20/24 | 0/24 | 90.3% |

Per-run, per-system, and per-question aggregates plus all 48 rationales are in `scores.json`. Provenance is the mean of per-answer supported-claim fractions, not a URL count. Both cited IRS XML URLs for baseline J2 currently redirect to IRS 404, so only the remaining reproducible official support received credit.

## Latency, calls, and tokens

- Baseline wall time totals 657.186s; registry wall time totals 232.484s, a 64.6% directional reduction. Baseline used 46 research-tool invocations; registry used 0 browsing calls.
- Recorded per-answer latencies sum to 451.743s baseline and 61.745s registry. They are not cleanly comparable: unallocated run time is 0s and 205.443s in baseline R1/R2, versus 139.066s and 31.672s in registry R1/R2. Run wall time is the safer end-to-end measure.
- Tool-call totals equal the sums of answer-level calls in every run, and answer order matches `orders.json`.
- Input tokens are `null` in all four runs and are unobservable, so no token-cost conclusion is supported.
- Lookup timing is properly separated: equal-weight mean warm resolution is 0.041310ms across questions and cold load/validate/resolve is 0.370789ms across fixtures. Raw samples are absent, so published medians/p95/max cannot be independently recomputed; the cold test uses only the first question per fixture.

## Confirmed design and implementation findings

1. **Rubric leakage toward registry-shaped detail:** J1 and P2 reward information not asked by the natural question. This inflated the strict registry advantage from 2/24 answers after review to 8/24 before review.
2. **UPMC dated-identifier defect:** `upmc.json` stores the FY2022 Presbyterian/Shadyside EIN on an active entity/facility without identifier-effective metadata. The resolver emits the EIN but no source-period warning, causing both registry U4 failures.
3. **Penn evidence-link defect:** the HUP facility embeds Trustees EIN 23-1352685, but its facility evidence list and P2 resolver output omit `upenn_ein`. Registry R1 consequently cites no direct EIN source.
4. **Jefferson provenance defect:** `irs_teos` has `url: null`; J2 and J4 resolver outputs can return exact EINs without a citable direct filing. This depresses registry provenance and violates the intended “document every source” standard.
5. **Timing instrumentation is inconsistent:** answer-latency fields omit materially different setup/gap/trailing intervals across runs. Wall time remains usable directionally; fine-grained latency comparisons do not.
6. **No false-aggregation discrimination occurred:** zero events in both arms means the benchmark cannot support the decision rule's central claim that the registry reduces false aggregation.

## Limitations and recommendation

There are only two repetitions per arm, answers within a run share one agent context, and the same fixture-authored gold shaped registry outputs. There is no significance test, no nationwide generalization, no human-time estimate, and no observable token cost. Some cited PDFs/URLs are dated or currently unavailable.

**Recommendation: revise, then run the next smallest experiment.** Fix the three evidence/date defects, freeze questions whose required facts are all actually asked, and add a few adversarial brand/affiliate questions likely to produce false aggregation. Repeat the same matched design with at least one additional public/university system. Do not claim production readiness or a material accuracy win from this run; do retain the demonstrated deterministic lookup speed and lower end-to-end wall/tool burden as directional evidence.
