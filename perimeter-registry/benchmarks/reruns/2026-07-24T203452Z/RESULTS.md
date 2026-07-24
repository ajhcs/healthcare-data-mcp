# Minimal-instruction perimeter benchmark rerun

Rerun ID: `2026-07-24T203452Z`  
Started: `2026-07-24T20:34:52Z`  
Results generated: `2026-07-24T20:59:22Z`

## Method

The original benchmark evidence was preserved. This rerun copied the same 12 frozen questions, gold, and counterbalanced orders into this UTC-stamped directory. It used exactly Jefferson Health, Penn Medicine/UPHS, and UPMC, with two fresh repetitions per arm and 24 answers per arm.

Registry contexts could read only the question/order snapshots, the timestamped deterministic resolver snapshot, and the three registry fixture files. They could not browse. Manual-style source-research contexts could read only the question/order snapshots and research official primary web sources. Every run used a fresh context. An independent scorer then read only the frozen snapshots and four run JSON files.

Both frozen strict scoring and the previously declared adjusted rubric are reported. The adjusted rubric removes only the invalid J1 FY2025 detail, P2 operator-EIN requirement, and U3 obsolete FY2022 caveat when newer FY2023 evidence is cited. U2 and U4 were not relaxed.

## Measured result

| Arm | Strict correctness | Adjusted correctness | Adjusted scope/caveat | False aggregation | Adjusted provenance | Wall time | Research calls |
|---|---:|---:|---:|---:|---:|---:|---:|
| Registry assisted | 24/24 (100%) | 24/24 (100%) | 24/24 (100%) | 0/24 | 62/66 (93.9%) | 261.129s | 0 |
| Manual-style source-research baseline | 14/24 (58.3%) | 20/24 (83.3%) | 20/24 (83.3%) | 0/24 | 60/66 (90.9%) | 699.538s | 43 |

Registry wall time was 62.7% lower, or 2.68x faster. Order A was 122.816s registry versus 454.792s baseline, or 3.70x. Reverse order B was 138.313s versus 244.746s, or 1.77x. The 5–10x end-to-end target was not reached.

Registry correctness improved from the prior 22/24 to 24/24 because both U4 answers now included the FY2022 EIN evidence and current-continuity warning. Both baseline repetitions still failed U2 and U4. They selected conflicting or insufficiently dated plan-company EINs for the multi-company Health Plan brand and omitted the required dated-EIN caveat for the combined Presbyterian/Shadyside record.

## Timing and token limits

Use top-level wall time only. Baseline R1 repeated the full run interval for every answer, and Registry R2 evenly allocated run time across answers; their per-answer timing is not independently valid. The two orders also differed materially, and there is only one run per arm/order cell.

Input-token telemetry was unavailable in all four contexts and is recorded as `null`. Research-call counts and wall time are not token proxies; no token-saving claim is made.

The separately rerun deterministic path averaged 0.037688ms for warm resolution and 0.343829ms for cold fixture-load/validation/resolution. Those times are not included as model latency claims.

## Change audit

At `2026-07-24T20:59:22Z`, the rerun contained no production-system, deployment, network, secret, fixture, or IRS 990 Evidence Service change. No Form 990 fact, identifier, source locator, or service behavior was changed. Existing official Form 990 evidence was read by the benchmark only. The prior deterministic timing artifact was restored byte-for-byte with SHA-256 `4a5632c1e0111cad01362afccf4cbf6b23b9e936c3281aa53f5b16ac40340175`.

## Decision

The minimal rerun validates the targeted correctness repair on this fixed set: 24/24 registry answers. It does not validate a 5–10x end-to-end speed target, token savings, nationwide generalization, or a false-aggregation reduction because both arms recorded zero false aggregations.
